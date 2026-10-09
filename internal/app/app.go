//go:build unix

// Package app — сборка агента: связь с сервером, воркеры, настройки
// воркеров, запросы к воркерам, сокет агента, метрики, лог, встроенные
// действия, самообновление. Предметная область — только в воркерах.
package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/internal/agentsock"
	"github.com/epifanovmd/agent/internal/backoff"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/configs"
	"github.com/epifanovmd/agent/internal/fetch"
	"github.com/epifanovmd/agent/internal/identity"
	"github.com/epifanovmd/agent/internal/link"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/outbox"
	"github.com/epifanovmd/agent/internal/requests"
	"github.com/epifanovmd/agent/internal/stream"
	"github.com/epifanovmd/agent/internal/sysmetrics"
	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/internal/worker"
)

// BuiltinUpdateKey — ключ проверки сборок, вшитый при сборке (cmd/agent,
// -ldflags "-X main.updateKey=…"); ключи из настроек (update.publicKey,
// update.publicKeys) действуют вместе с ним.
var BuiltinUpdateKey string

// ErrRestart — агент остановлен для перезапуска (обновление): процесс
// завершается, менеджер (systemd, Docker) запускает его снова.
var ErrRestart = errors.New("agent: перезапуск")

// App — агент.
type App struct {
	cfgMu    sync.Mutex
	cfg      config.Config
	reloadMu sync.Mutex
	version  string
	bootID   string
	started  time.Time

	log     *slog.Logger
	logCtl  *logx.Control
	forward *logx.Forwarder
	journal *logx.Journal
	client  *http.Client

	auth    *auth
	outbox  *outbox.Outbox
	link    *link.Link
	workers *worker.Supervisor
	configs *configs.Store
	tunnel  *fetch.Tunnel
	asks    *requests.Broker
	runDir  string
	update  update.Paths
	// keys — ключи проверки выпусков: вшитый при сборке и из настроек.
	keys update.Keys
	// sysmetricsCmd — запуск встроенного воркера sysmetrics.
	sysmetricsCmd []string

	obs observer
	act actions

	errMu     sync.Mutex
	lastErr   string
	lastErrAt time.Time

	restart atomic.Bool
	cancel  context.CancelFunc
	lock    *Lock
}

// New — агент по настройкам.
func New(cfg config.Config, version string) (*App, error) {
	journal := logx.NewJournal(cfg.Log.Buffer)
	log, logCtl := logx.New(os.Stderr, journal, logx.Options{Level: cfg.Log.Level, Format: cfg.Log.Format})
	forward := logx.NewForwarder(cfg.Log.Forward)
	logCtl.SetForwarder(forward)
	if cfg.Insecure() {
		log.Warn("связь без шифрования — ключ агента и настройки воркеров идут открытым текстом; нужен https://",
			"server", strings.Join(cfg.Server.Addresses(), ", "))
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
	ob, err := outbox.Open(filepath.Join(cfg.DataDir, "outbox"))
	if err != nil {
		return nil, err
	}
	ob.SetLimit(cfg.Outbox.MaxMessages)
	client, err := httpClient(cfg.Server)
	if err != nil {
		return nil, err
	}
	a := &App{
		cfg: cfg, version: version, bootID: message.NewBootID(), started: time.Now(),
		log: log, logCtl: logCtl, forward: forward, journal: journal, client: client,
		outbox: ob, lock: lock, runDir: RunDir(cfg.DataDir),
		sysmetricsCmd: SysmetricsCommand(),
	}
	a.obs.init()
	if a.keys, err = update.ParseKeys(append([]string{BuiltinUpdateKey}, cfg.Update.Keys()...)...); err != nil {
		return nil, err
	}
	if exe, err := os.Executable(); err == nil {
		a.update = update.NewPaths(exe)
	}
	if err := prepareRunDir(cfg, a.runDir); err != nil {
		return nil, err
	}
	a.auth = &auth{store: identity.NewStore(cfg.DataDir), cfg: cfg, client: client, host: HostInfo(), log: log, onError: a.recordError}
	a.link = link.New(link.Options{
		ServerURLs: cfg.Server.Addresses(),
		Auth:       a.auth,
		Outbox:     ob,
		Stream:     stream.New(cfg.Server.StreamBuffer),
		Backoff:    reconnect(cfg.Server),
		HTTPClient: client,
		Log:        log,
	}, a)
	a.workers = worker.New(workerSpecs(cfg, a.sysmetricsCmd), worker.Options{
		RunDir:       a.runDir,
		DataDir:      cfg.DataDir,
		AgentSocket:  filepath.Join(a.runDir, AgentSocketName),
		AgentVersion: version,
		Log:          log,
		OnStarted:    func(name string) { a.configs.Started(name) },
		OnAdopted:    func(name string) { a.configs.Adopted(name) },
		OnChange:     a.statusChanged,
	})
	a.configs, err = configs.Open(filepath.Join(cfg.DataDir, "configs"), configWorkers{a.workers}, a.configApplied, log)
	if err != nil {
		return nil, err
	}
	a.tunnel = fetch.New(configWorkers{a.workers}, log)
	a.asks = requests.New(func() requests.Session {
		if s := a.link.Session(); s != nil {
			return s
		}
		return nil
	})
	ok = true
	return a, nil
}

// prepareRunDir — каталог сокетов: проходимый для воркеров с user (у
// каждого свой подкаталог 0700); каталог данных — тоже проходимый, если
// такие воркеры есть.
func prepareRunDir(cfg config.Config, dir string) error {
	if err := os.MkdirAll(dir, 0o711); err != nil {
		return fmt.Errorf("agent: каталог сокетов: %w", err)
	}
	if err := os.Chmod(dir, 0o711); err != nil {
		return err
	}
	if slices.ContainsFunc(cfg.Workers, func(w config.Worker) bool { return w.User != "" }) {
		return os.Chmod(cfg.DataDir, 0o711)
	}
	return nil
}

// configWorkers — воркеры, которым сервер может слать настройки и запросы:
// встроенный sysmetrics — часть агента, для сервера его нет.
type configWorkers struct{ s *worker.Supervisor }

func (c configWorkers) Has(name string) bool {
	spec, ok := c.s.Spec(name)
	return ok && !spec.Builtin
}

// ConfigRetry — повтор неприменённых настроек воркера (lifecycle.configRetry).
func (c configWorkers) ConfigRetry(name string) time.Duration {
	spec, _ := c.s.Spec(name)
	return spec.Lifecycle.ConfigRetry.Std()
}

// Manifest — манифест воркера (ключи, которые ему можно передавать).
func (c configWorkers) Manifest(name string) *message.WorkerManifest { return c.s.Manifest(name) }

// OpenRoutes — fetch к воркеру без сверки с манифестом (routes: open).
func (c configWorkers) OpenRoutes(name string) bool {
	spec, _ := c.s.Spec(name)
	return spec.OpenRoutes()
}

func (c configWorkers) Client(name string) (*http.Client, error) {
	if !c.Has(name) {
		return nil, worker.ErrUnknown
	}
	return c.s.Client(name)
}

// Close — снять блокировку каталога данных у агента, который не запускался.
func (a *App) Close() { a.lock.Unlock() }

// Run — работать до отмены ctx; ErrRestart — процесс нужно запустить снова.
func (a *App) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	defer cancel()
	defer a.lock.Unlock()

	sock, err := listenSocket(a)
	if err != nil {
		return err
	}
	defer sock.Close()

	cfg := a.config()
	a.log.Info("агент запущен", "version", a.version, "name", cfg.Name, "server", strings.Join(cfg.Server.Addresses(), ", "),
		"workers", len(cfg.Workers))
	var wg sync.WaitGroup
	run := func(f func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f(ctx)
		}()
	}
	run(func(ctx context.Context) {
		_ = heartbeat{path: filepath.Join(cfg.DataDir, StatusFile), snapshot: a.snapshot}.run(ctx)
	})
	run(a.workers.Run)
	run(a.configs.Run)
	run(a.metricsLoop)
	run(a.statusLoop)
	run(a.logLoop)
	run(func(ctx context.Context) {
		// Воркеры работают и без связи; связь — после регистрации.
		if err := a.auth.ensure(ctx); err != nil {
			if ctx.Err() == nil {
				a.log.Error("агент не зарегистрирован — работает без связи", "err", err)
				a.recordError(err)
			}
			return
		}
		_ = a.link.Run(ctx)
	})
	<-ctx.Done()
	wg.Wait()
	if a.restart.Load() {
		return ErrRestart
	}
	return ctx.Err()
}

// requestRestart — выйти для перезапуска (ErrRestart): воркеры — по
// lifecycle.onAgentRestart.
func (a *App) requestRestart() {
	a.restart.Store(true)
	a.setExit(worker.ExitRestart)
	if a.cancel != nil {
		a.cancel()
	}
}

// setExit — что будет с воркерами при выходе. В контейнере (update.mode:
// external) воркеры не переживают агента: останавливаются всегда.
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

// ─── link.Handler ──────────────────────────────────────────────────────

// Hello — первое сообщение соединения (§4).
func (a *App) Hello() message.Hello {
	cfg := a.config()
	h := message.Hello{
		Agent:   message.HelloAgent{Version: a.version, BootID: a.bootID, StartedAt: a.started.UnixMilli()},
		Host:    a.auth.host,
		Labels:  cfg.Labels,
		Configs: a.configs.Versions(),
	}
	for _, st := range a.workers.Status() {
		if !st.Builtin {
			h.Workers = append(h.Workers, message.HelloWorker{Name: st.Name, Version: st.Version, Release: st.Release,
				Manifest: st.Manifest})
		}
	}
	return h
}

// OnWelcome — соединение открыто: частоты от сервера, отметка «новая
// версия вышла на связь», итог agent.update, свежий status (§4).
func (a *App) OnWelcome(_ *link.Session, w message.Welcome) {
	a.obs.setWelcome(w)
	if a.update.Marker != "" {
		update.Healthy(a.update)
	}
	a.finishAgentUpdate()
	a.sendStatus()
}

// OnMessage — сообщение сервера (§5).
func (a *App) OnMessage(s *link.Session, env message.Envelope) {
	switch env.Type {
	case message.TypeConfigPut:
		var p message.ConfigPut
		if err := env.Decode(&p); err != nil {
			a.log.Warn("config.put: неверное сообщение", "err", err)
			return
		}
		a.configs.Put(p)
		a.statusChanged()
	case message.TypeConfigDelete:
		var d message.ConfigDelete
		if err := env.Decode(&d); err != nil {
			a.log.Warn("config.delete: неверное сообщение", "err", err)
			return
		}
		a.configs.Delete(d)
		a.statusChanged()
	case message.TypeFetch:
		a.tunnel.Handle(s, env)
	case message.TypeFetchCancel:
		a.tunnel.Cancel(s, env.Re)
	case message.TypeWatch:
		var w message.Watch
		if err := env.Decode(&w); err != nil {
			a.log.Warn("watch: неверное сообщение", "err", err)
			return
		}
		a.setWatch(w)
	case message.TypeAction:
		a.action(env)
	case message.TypeRequestResult:
		if err := a.asks.Result(env); errors.Is(err, requests.ErrUnexpected) {
			a.log.Debug("request.result: запрос уже не ждёт ответа", "re", env.Re)
		} else if err != nil {
			a.log.Warn("request.result: неверное сообщение", "err", err)
		}
	case message.TypeWelcome:
	default:
		a.log.Warn("незнакомое сообщение сервера — пропущено", "type", env.Type)
	}
}

// OnDisconnect — связь потеряна.
func (a *App) OnDisconnect(err error) {
	if err != nil {
		a.recordError(err)
	}
}

// configApplied — итог применения настроек серверу (важное сообщение).
func (a *App) configApplied(c message.ConfigApplied) {
	if err := a.link.Important(message.MustNew(message.TypeConfigApplied, c)); err != nil {
		a.log.Error("config.applied не записан в outbox", "err", err)
	}
	a.statusChanged()
}

// ─── сокет агента ──────────────────────────────────────────────────────

func listenSocket(a *App) (*agentsock.Server, error) {
	sock, err := agentsock.Listen(filepath.Join(a.runDir, AgentSocketName), sockAgent{a}, a.log)
	if err != nil {
		return nil, fmt.Errorf("agent: сокет агента: %w", err)
	}
	return sock, nil
}

func newKeys(a *App, creds identity.Credentials) *identity.Keys {
	return identity.NewKeys(a.auth.store, creds)
}

// snapshot — что агент сообщает о себе в отметке для agent status.
func (a *App) snapshot() AgentStatus {
	cfg := a.config()
	a.errMu.Lock()
	errText, errAt := a.lastErr, a.lastErrAt
	a.errMu.Unlock()
	st := a.status()
	s := AgentStatus{
		Online: a.link.Connected(), StartedAt: a.started.UnixMilli(), Version: a.version, Name: cfg.Name,
		AgentID: a.auth.agentID(), Server: a.link.ServerURL(), Outbox: st.Outbox, Workers: st.Workers, LastError: errText,
	}
	if !errAt.IsZero() {
		s.LastErrorAt = errAt.UnixMilli()
	}
	return s
}

type sockAgent struct{ a *App }

func (s sockAgent) ByToken(token string) (string, bool) { return s.a.workers.ByToken(token) }

func (s sockAgent) Event(e message.Event) error {
	env, err := message.New(message.TypeEvent, e)
	if err != nil {
		return err
	}
	return s.a.link.Important(env)
}

// Declared — тип события объявлен в манифесте воркера (§12). Событие,
// присланное сразу после запуска, ждёт итога первой проверки регистрации.
func (s sockAgent) Declared(ctx context.Context, name, typ string) bool {
	return s.a.workers.WaitManifest(ctx, name).DeclaresEvent(typ)
}

// DeclaredRequest — тип запроса к серверу объявлен в манифесте воркера (§12).
func (s sockAgent) DeclaredRequest(ctx context.Context, name, typ string) bool {
	return s.a.workers.WaitManifest(ctx, name).DeclaresRequest(typ)
}

// Request — запрос воркера к серверу в текущем соединении.
func (s sockAgent) Request(ctx context.Context, name string, p message.RequestPost) (json.RawMessage, error) {
	return s.a.asks.Do(ctx, name, p)
}

func (s sockAgent) Config(name, key string) (message.ConfigValue, bool) {
	return s.a.configs.Get(name, key)
}

func (s sockAgent) Context() message.Context {
	cfg := s.a.config()
	return message.Context{
		Agent:  message.ContextAgent{ID: s.a.auth.agentID(), Name: cfg.Name, Version: s.a.version, Labels: cfg.Labels},
		Online: s.a.link.Connected(),
	}
}

// ─── воркеры ───────────────────────────────────────────────────────────

// SysmetricsCommand — как запустить встроенный воркер sysmetrics: эта же
// программа в режиме `agent sysmetrics`.
func SysmetricsCommand() []string {
	exe, err := os.Executable()
	if err != nil {
		exe = os.Args[0]
	}
	return []string{exe, sysmetrics.Command}
}

// workerSpecs — воркеры из настроек и встроенный sysmetrics, если есть
// группы метрик узла (telemetry.metrics).
func workerSpecs(cfg config.Config, sysmetricsCmd []string) []config.Worker {
	specs := slices.Clone(cfg.Workers)
	if len(cfg.Telemetry.Metrics) > 0 {
		set, _ := json.Marshal(sysmetrics.Settings{
			Metrics: cfg.Telemetry.Metrics, Disks: cfg.Telemetry.Disks, ExcludeInterfaces: cfg.Telemetry.ExcludeInterfaces,
		})
		specs = append(specs, config.Worker{
			Name: config.SysmetricsWorker, Command: sysmetricsCmd, Builtin: true,
			Env:       map[string]string{sysmetrics.EnvSettings: string(set)},
			Lifecycle: config.Lifecycle{StopTimeout: config.Duration(5 * time.Second)},
		})
	}
	return specs
}

// reconnect — пауза переподключения и повтора регистрации (server.reconnect).
func reconnect(s config.Server) backoff.Policy {
	return backoff.Policy{Min: s.Reconnect.Min.Std(), Max: s.Reconnect.Max.Std()}
}

// httpClient — клиент для запросов агента к серверу (WebSocket,
// регистрация, загрузка сборок): прокси из окружения, системные корни +
// server.caFile, клиентский сертификат.
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

// ─── ключ агента ───────────────────────────────────────────────────────

// auth — ключ агента: загрузка, регистрация, регистрация заново, смена
// секрета через ожидающий секрет.
type auth struct {
	store   *identity.Store
	cfg     config.Config
	client  *http.Client
	host    message.Host
	log     *slog.Logger
	keys    atomic.Pointer[identity.Keys]
	onError func(error)
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

// Session — заголовок, принятый в последнем соединении (загрузка сборок).
func (a *auth) Session() string {
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
		a.log.Info("новый секрет агента принят сервером")
	}
}

// rotate — agent.rotateKey: новый ожидающий секрет, итог — его хеш.
func (a *auth) rotate() (message.RotateKeyResult, error) {
	k := a.keys.Load()
	if k == nil {
		return message.RotateKeyResult{}, errors.New("ключа агента нет")
	}
	hash, err := k.Rotate()
	if err != nil {
		return message.RotateKeyResult{}, err
	}
	a.log.Info("создан новый секрет агента — ждёт признания сервером")
	return message.RotateKeyResult{SecretHash: hash}, nil
}

// ensure — ключ есть или агент регистрируется (с повтором, пока сервер
// недоступен; отклонённый токен — ошибка).
func (a *auth) ensure(ctx context.Context) error {
	creds, ok, err := a.store.Load()
	if err != nil {
		return err
	}
	if ok {
		a.keys.Store(identity.NewKeys(a.store, creds))
		return nil
	}
	if a.cfg.Enroll.Token == "" {
		return errors.New("agent: не зарегистрирован — задайте enroll.token (AGENT_ENROLL_TOKEN)")
	}
	for attempt := 0; ; attempt++ {
		err := a.enroll(ctx)
		if err == nil || errors.Is(err, identity.ErrTokenRejected) {
			return err
		}
		delay := reconnect(a.cfg.Server).Delay(attempt)
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

// Renew — ключ отозван: регистрация заново, если есть токен.
func (a *auth) Renew(ctx context.Context) error {
	if a.cfg.Enroll.Token == "" {
		return errors.New("agent: ключ агента отозван, токена регистрации нет")
	}
	return a.enroll(ctx)
}

// enroll — регистрация по токену: адреса сервера по порядку, пока один не
// ответит (отклонённый токен — сразу ошибка).
func (a *auth) enroll(ctx context.Context) error {
	req := message.Enroll{Token: a.cfg.Enroll.Token, Name: a.cfg.Name, Labels: a.cfg.Labels,
		Host: message.Host{OS: a.host.OS, Arch: a.host.Arch, Hostname: a.host.Hostname}}
	var creds identity.Credentials
	var err error
	for _, addr := range a.cfg.Server.Addresses() {
		creds, err = identity.Enroll(ctx, a.client, addr, req)
		var rl *identity.RateLimited
		if err == nil || errors.Is(err, identity.ErrTokenRejected) || errors.As(err, &rl) || ctx.Err() != nil {
			break
		}
		a.log.Warn("регистрация: адрес недоступен", "server", addr, "err", err)
	}
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
	a.log.Info("агент зарегистрирован", "agentId", creds.AgentID)
	return nil
}
