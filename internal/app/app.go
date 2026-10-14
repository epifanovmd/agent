//go:build unix

// Package app — сборка агента: воркеры и сокет агента для них, хранилище, связь с бэкендом,
// желаемое состояние и команды, метрики, журнал, локальный API, самообновление. Предметная
// область — только в воркерах.
package app

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sys/unix"

	"github.com/epifanovmd/agent/internal/backend"
	"github.com/epifanovmd/agent/internal/backoff"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/identity"
	"github.com/epifanovmd/agent/internal/lifecycle"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/store"
	"github.com/epifanovmd/agent/internal/sysmetrics"
	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/internal/worker"
	"github.com/epifanovmd/agent/internal/workerapi"
	"github.com/epifanovmd/agent/protocol"
)

// BuiltinUpdateKey — ключ проверки сборок, вшитый при сборке (cmd/agent, -ldflags
// "-X main.updateKey=…"); ключи из настроек (update.publicKey, update.publicKeys) действуют
// вместе с ним.
var BuiltinUpdateKey string

// InstallMode — как агент поставлен на узел (agent install: systemd, user, manual; "" —
// запущен иначе): в hello.agent.install (cmd/agent).
var InstallMode string

// ErrRestart — агент остановлен для перезапуска (обновление): процесс завершается, менеджер
// (systemd, Docker) запускает его снова.
var ErrRestart = errors.New("agent: перезапуск")

// App — агент.
type App struct {
	cfgMu sync.Mutex
	cfg   config.Config
	// configPath — файл настроек, с которым агент запущен ("" — только окружение).
	configPath string
	reloadMu   sync.Mutex
	version    string
	bootID     string
	started    time.Time
	host       protocol.Host

	log     *slog.Logger
	logCtl  *logx.Control
	forward *logx.Forwarder
	journal *logx.Journal
	client  *http.Client
	lock    *Lock

	store   *store.Store
	auth    *auth
	workers *worker.Supervisor
	sock    *workerapi.Server
	// backend — связь с бэкендом (nil — работа без бэкенда).
	backend *backend.Client
	runDir  string
	metrics *sysmetrics.Collector
	update  update.Paths
	// keys — ключи проверки подписи сборок: вшитый при сборке и из настроек.
	keys update.Keys

	desired desiredState
	calls   calls
	obs     observer

	errMu     sync.Mutex
	lastErr   string
	lastErrAt time.Time

	// latest — новая версия агента из последней проверки (nil — нет).
	latest atomic.Pointer[protocol.AgentUpdateInfo]

	restart atomic.Bool
	cancel  context.CancelFunc

	// life — состояние агента (pending, active, revoked; internal/lifecycle).
	lifeMu sync.Mutex
	life   lifecycle.State
	// runCtx — контекст Run: в нём запускаются воркеры, когда агент становится active.
	runCtx context.Context
	// workersDone — воркеры запущены (закрывается, когда остановлены); workersStop — остановить.
	workersDone chan struct{}
	workersStop context.CancelFunc
}

// New — агент по настройкам.
func New(cfg config.Config, version string) (*App, error) {
	journal := logx.NewJournal(cfg.Log.Buffer)
	log, logCtl := logx.New(os.Stderr, journal, logx.Options{Level: cfg.Log.Level, Format: cfg.Log.Format})
	forward := logx.NewForwarder(cfg.Log.Forward)
	logCtl.SetForwarder(forward)
	if cfg.Server.AllowInsecure && cfg.Server.URL != "" {
		log.Warn("server.allowInsecure: связь без шифрования разрешена — ключ агента и данные могут идти открытым текстом",
			"server", cfg.Server.URL)
	}
	_, enrolled, _ := identity.NewStore(cfg.DataDir).Load()
	if err := cfg.RequireServer(enrolled); err != nil {
		return nil, err
	}
	lock, err := LockDataDir(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	ok := false
	defer func() {
		if !ok {
			lock.Unlock()
		}
	}()
	st, err := store.Open(cfg.DataDir, cfg.Outbox.MaxMessages)
	if err != nil {
		return nil, err
	}
	defer func() {
		if !ok {
			st.Close()
		}
	}()
	if st.Recovered != "" {
		log.Error("хранилище агента было повреждено — отложено, агент начинает с чистого: очередь неотправленного и журнал команд потеряны, желаемое придёт от бэкенда",
			"broken", st.Recovered)
	}
	client, err := httpClient(cfg.Server)
	if err != nil {
		return nil, err
	}
	a := &App{
		cfg: cfg, version: version, bootID: protocol.NewID(), started: time.Now(), host: HostInfo(),
		log: log, logCtl: logCtl, forward: forward, journal: journal, client: client, lock: lock,
		store: st, runDir: RunDir(cfg.DataDir), metrics: sysmetrics.NewCollector(sysmetrics.DefaultSettings()),
	}
	a.obs.init()
	a.calls.init()
	if a.keys, err = update.ParseKeys(append([]string{BuiltinUpdateKey}, cfg.Update.Keys()...)...); err != nil {
		return nil, err
	}
	if exe, err := os.Executable(); err == nil {
		a.update = update.NewPaths(exe)
	}
	if err := os.MkdirAll(a.runDir, 0o700); err != nil {
		return nil, fmt.Errorf("agent: каталог сокетов: %w", err)
	}
	if a.life, err = lifecycle.Load(cfg.DataDir); err != nil {
		return nil, err
	}
	if cfg.Server.URL == "" && a.life.Status == protocol.StatusPending {
		// Без бэкенда регистрироваться негде: агент работает по файлам desired/.
		if a.life, err = lifecycle.Save(cfg.DataDir, protocol.StatusActive, "без бэкенда"); err != nil {
			return nil, err
		}
	}
	a.auth = &auth{store: identity.NewStore(cfg.DataDir), cfg: cfg, client: client, host: a.host, log: log, onError: a.recordError,
		onEnrolled: func() {
			if err := st.NewIdentity(); err != nil {
				log.Error("хранилище не сброшено после регистрации", "err", err)
			}
		}}
	if creds, ok, err := a.auth.store.Load(); err == nil && ok {
		a.auth.keys.Store(identity.NewKeys(a.auth.store, creds))
	}
	a.workers = worker.New(cfg.Workers, worker.Options{
		DataDir:      cfg.DataDir,
		AgentSocket:  filepath.Join(a.runDir, AgentSocketName),
		Log:          log,
		OnRegistered: a.workerRegistered,
		OnChange:     a.statusChanged,
	})
	if cfg.Server.URL != "" {
		a.backend = backend.New(backend.Options{URL: cfg.Server.URL, HTTP: client, Auth: a.auth, Store: st, Log: log}, backendHandler{a})
	}
	ok = true
	return a, nil
}

// Close — освободить каталог данных у агента, который не запускался.
func (a *App) Close() {
	a.store.Close()
	a.lock.Unlock()
}

// Run — работать до отмены ctx; ErrRestart — процесс нужно запустить снова.
func (a *App) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	defer cancel()
	defer a.lock.Unlock()
	defer a.store.Close()

	sock, err := workerapi.Listen(filepath.Join(a.runDir, AgentSocketName), sockAgent{a}, a.log)
	if err != nil {
		return fmt.Errorf("agent: сокет агента: %w", err)
	}
	a.sock = sock
	defer sock.Close()
	admin, err := listenAdmin(filepath.Join(a.runDir, AdminSocketName), a)
	if err != nil {
		return fmt.Errorf("agent: локальный API: %w", err)
	}
	defer admin.Close()

	_ = os.Remove(filepath.Join(a.config().DataDir, ErrorFile))
	a.loadDesired()
	a.resumeCommands()

	cfg := a.config()
	life := a.lifeState()
	a.log.Info("агент запущен", "version", a.version, "name", cfg.Name, "server", cfg.Server.URL, "workers", len(cfg.Workers), "status", life.Status)
	var wg sync.WaitGroup
	run := func(f func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f(ctx)
		}()
	}
	a.lifeMu.Lock()
	a.runCtx = ctx
	a.lifeMu.Unlock()
	switch life.Status {
	case protocol.StatusActive:
		a.startWorkers()
	case protocol.StatusPending:
		a.log.Info("агент ждёт регистрации и подтверждения бэкендом — воркеры запустятся после")
	case protocol.StatusRevoked:
		a.log.Error("агент отозван бэкендом — воркеры не запускаются, связи нет; вернуть — новая установка с токеном (agent install --token …)",
			"since", time.UnixMilli(life.Since).UTC().Format(time.RFC3339), "reason", life.Reason)
	}
	run(a.checkLoop)
	run(a.metricsLoop)
	run(a.stateLoop)
	run(a.logLoop)
	run(watchdog)
	if a.backend != nil && life.Status != protocol.StatusRevoked {
		run(func(ctx context.Context) {
			// Воркеры работают и без связи; связь — после регистрации.
			if err := a.auth.ensure(ctx); err != nil {
				if ctx.Err() == nil {
					a.log.Error("агент не зарегистрирован — работает без связи", "err", err)
					a.recordError(err)
				}
				return
			}
			a.backend.Run(ctx)
			if a.backend.Revoked() {
				a.revoke("бэкенд ответил 403 " + protocol.CodeAgentRevoked)
			}
		})
	} else {
		a.log.Info("server.url не задан — работа без бэкенда: желаемое — из файлов", "dir", a.localDesiredDir())
	}
	<-ctx.Done()
	wg.Wait()
	a.waitWorkers()
	if a.restart.Load() {
		return ErrRestart
	}
	return ctx.Err()
}

// lifeState — текущее состояние агента.
func (a *App) lifeState() lifecycle.State {
	a.lifeMu.Lock()
	defer a.lifeMu.Unlock()
	return a.life
}

// active — агент active: воркеры работают, их регистрация принимается.
func (a *App) active() bool { return a.lifeState().Status == protocol.StatusActive }

// startWorkers — запустить воркеры (один раз за Run; агент active).
func (a *App) startWorkers() {
	a.lifeMu.Lock()
	defer a.lifeMu.Unlock()
	if a.workersDone != nil || a.runCtx == nil {
		return
	}
	ctx, stop := context.WithCancel(a.runCtx)
	done := make(chan struct{})
	a.workersDone, a.workersStop = done, stop
	go func() {
		defer close(done)
		a.workers.Run(ctx)
	}()
}

// waitWorkers — дождаться остановки воркеров (их контекст отменён).
func (a *App) waitWorkers() {
	a.lifeMu.Lock()
	done := a.workersDone
	a.lifeMu.Unlock()
	if done != nil {
		<-done
	}
}

// activate — бэкенд подтвердил агента (hello, status active): pending → active, воркеры
// запускаются.
func (a *App) activate() {
	a.lifeMu.Lock()
	pending := a.life.Status == protocol.StatusPending
	if pending {
		st, err := lifecycle.Save(a.config().DataDir, protocol.StatusActive, "подтверждён бэкендом")
		if err != nil {
			a.lifeMu.Unlock()
			a.log.Error("состояние агента не записано", "err", err)
			return
		}
		a.life = st
	}
	a.lifeMu.Unlock()
	if pending {
		a.log.Info("агент подтверждён бэкендом (active) — воркеры запускаются")
		a.startWorkers()
		a.statusChanged()
	}
}

// Сколько ждать ./cleanup воркеров при отзыве агента.
const revokeCleanupTimeout = 10 * time.Minute

// revoke — бэкенд отозвал агента (403 AGENT_REVOKED): revoked навсегда (до новой установки),
// воркеры останавливаются и убирают за собой (./cleanup), связи больше нет.
func (a *App) revoke(reason string) {
	a.lifeMu.Lock()
	if a.life.Status == protocol.StatusRevoked {
		a.lifeMu.Unlock()
		return
	}
	cfg := a.config()
	if st, err := lifecycle.Save(cfg.DataDir, protocol.StatusRevoked, reason); err != nil {
		a.log.Error("состояние агента не записано", "err", err)
		a.life = lifecycle.State{Status: protocol.StatusRevoked, Since: time.Now().UnixMilli(), Reason: reason}
	} else {
		a.life = st
	}
	stop, done := a.workersStop, a.workersDone
	a.lifeMu.Unlock()
	a.log.Error("агент отозван бэкендом — воркеры останавливаются и убирают за собой, связь прекращена; вернуть — новая установка с токеном", "reason", reason)
	if stop != nil {
		a.workers.SetExit(worker.ExitShutdown)
		stop()
		<-done
	}
	ctx, cancel := context.WithTimeout(context.Background(), revokeCleanupTimeout)
	defer cancel()
	for _, r := range worker.Cleanup(ctx, cfg.Workers, cfg.DataDir, a.log) {
		if r.Err != nil {
			a.log.Warn("воркер убрал за собой не всё", "worker", r.Worker, "err", r.Err)
		}
	}
	a.statusChanged()
}

// requestRestart — выйти для перезапуска (ErrRestart): воркеры работают дальше.
func (a *App) requestRestart() {
	a.restart.Store(true)
	a.setExit(worker.ExitRestart)
	if a.cancel != nil {
		a.cancel()
	}
}

// setExit — что будет с воркерами при выходе. В контейнере (update.mode: external) воркеры не
// переживают агента: останавливаются всегда.
func (a *App) setExit(e worker.Exit) {
	if a.config().Update.Mode == config.UpdateExternal {
		e = worker.ExitShutdown
	}
	a.workers.SetExit(e)
}

// config — текущие настройки (меняются Reload).
func (a *App) config() config.Config {
	a.cfgMu.Lock()
	defer a.cfgMu.Unlock()
	return a.cfg
}

func (a *App) recordError(err error) {
	a.errMu.Lock()
	defer a.errMu.Unlock()
	a.lastErr, a.lastErrAt = err.Error(), time.Now()
}

// online — есть связь с бэкендом.
func (a *App) online() bool { return a.backend != nil && a.backend.Online() }

// workerRegistered — воркер зарегистрировался: новый манифест — в hello.
func (a *App) workerRegistered(string) {
	if a.backend != nil {
		a.backend.Rehello()
	}
	a.statusChanged()
}

// httpClient — клиент для запросов агента к бэкенду (связь, регистрация, загрузка сборок):
// прокси из окружения, системные корни + server.caFile.
func httpClient(s config.Server) (*http.Client, error) {
	tlsCfg, err := s.TLSConfig()
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.Proxy = proxyFromEnv()
	if tlsCfg != nil {
		transport.TLSClientConfig = tlsCfg
	}
	return &http.Client{Transport: transport}, nil
}

// HostInfo — платформа узла для hello и регистрации.
func HostInfo() protocol.Host {
	h := protocol.Host{OS: goruntime.GOOS, Arch: goruntime.GOARCH}
	h.Hostname, _ = os.Hostname()
	var u unix.Utsname
	if unix.Uname(&u) == nil {
		h.Kernel = unix.ByteSliceToString(u.Release[:])
	}
	return h
}

// Сокеты агента в RunDir: для воркеров и локальный API.
const (
	AgentSocketName = "agent.sock"
	AdminSocketName = "admin.sock"
)

// maxSocketPath — длина пути unix-сокета с запасом (предел ОС — 104–108 байт).
const maxSocketPath = 100

// RunDir — каталог сокетов агента: <dataDir>/run; слишком длинный путь для сокета — каталог
// во временном каталоге системы.
func RunDir(dataDir string) string {
	dir := filepath.Join(dataDir, "run")
	if len(dir)+1+len(AgentSocketName) <= maxSocketPath {
		return dir
	}
	sum := sha256.Sum256([]byte(dataDir))
	return filepath.Join(os.TempDir(), "agent-"+hex.EncodeToString(sum[:6]))
}

// ─── ключ агента ───────────────────────────────────────────────────────

// auth — ключ агента: загрузка, регистрация, регистрация заново, смена секрета через
// ожидающий секрет.
type auth struct {
	store   *identity.Store
	cfg     config.Config
	client  *http.Client
	host    protocol.Host
	log     *slog.Logger
	keys    atomic.Pointer[identity.Keys]
	onError func(error)
	// onEnrolled — агент зарегистрирован (новый ключ).
	onEnrolled func()
}

func (a *auth) Authorization() string {
	if k := a.keys.Load(); k != nil {
		return k.Authorization()
	}
	return ""
}

// agentID — id агента ("" — ещё не зарегистрирован).
func (a *auth) agentID() string {
	if k := a.keys.Load(); k != nil {
		return k.Credentials().AgentID
	}
	return ""
}

// session — заголовок, принятый в последней сессии (загрузка сборок).
func (a *auth) session() string {
	if k := a.keys.Load(); k != nil {
		return k.Session()
	}
	return ""
}

func (a *auth) Fallback() bool {
	k := a.keys.Load()
	return k != nil && k.Fallback()
}

func (a *auth) Accepted(authorization string) {
	k := a.keys.Load()
	if k == nil {
		return
	}
	before := k.Credentials().Secret
	if err := k.Accepted(authorization); err != nil {
		a.log.Error("новый секрет не сохранён", "err", err)
		return
	}
	if k.Credentials().Secret != before {
		a.log.Info("новый секрет агента принят бэкендом")
	}
}

// rotate — agent.rotateKey: новый ожидающий секрет, итог — его хеш.
func (a *auth) rotate() (protocol.RotateKeyResult, error) {
	k := a.keys.Load()
	if k == nil {
		return protocol.RotateKeyResult{}, errors.New("ключа агента нет")
	}
	hash, err := k.Rotate()
	if err != nil {
		return protocol.RotateKeyResult{}, err
	}
	a.log.Info("создан новый секрет агента — ждёт признания бэкендом")
	return protocol.RotateKeyResult{SecretHash: hash}, nil
}

// ensure — ключ есть или агент регистрируется (с повтором, пока бэкенд недоступен;
// отклонённый токен — ошибка).
func (a *auth) ensure(ctx context.Context) error {
	if a.keys.Load() != nil {
		return nil
	}
	if _, ok, err := a.store.Load(); err != nil {
		return err
	} else if ok {
		return errors.New("agent: файл ключа появился после запуска — перезапустите агента")
	}
	if a.cfg.Enroll.Token == "" {
		return errors.New("agent: не зарегистрирован — задайте enroll.token (AGENT_ENROLL_TOKEN)")
	}
	for attempt := 0; ; attempt++ {
		err := a.enroll(ctx)
		if err == nil || errors.Is(err, identity.ErrTokenRejected) {
			return err
		}
		delay := backoff.Default.Delay(attempt)
		var rl *identity.RateLimited
		if errors.As(err, &rl) {
			delay = rl.After
		}
		a.log.Warn("регистрация не удалась — повтор", "err", err, "retryIn", delay.Round(time.Millisecond))
		a.onError(err)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// Renew — ключ не известен бэкенду: регистрация заново, если есть токен.
func (a *auth) Renew(ctx context.Context) error {
	if a.cfg.Enroll.Token == "" {
		return errors.New("agent: ключ агента не известен бэкенду, токена регистрации нет")
	}
	return a.enroll(ctx)
}

// enroll — регистрация по токену.
func (a *auth) enroll(ctx context.Context) error {
	req := protocol.Enroll{Token: a.cfg.Enroll.Token, Name: a.cfg.Name, Labels: a.cfg.Labels, Host: a.host}
	creds, err := identity.Enroll(ctx, a.client, a.cfg.Server.URL, req)
	if err != nil {
		return err
	}
	if k := a.keys.Load(); k != nil {
		err = k.Replace(creds)
	} else {
		err = a.store.Save(creds)
		a.keys.Store(identity.NewKeys(a.store, creds))
	}
	if err != nil {
		return err
	}
	if a.onEnrolled != nil {
		a.onEnrolled()
	}
	a.log.Info("агент зарегистрирован", "agentId", creds.AgentID)
	return nil
}
