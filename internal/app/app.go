//go:build unix

// Package app — сборка агента: связь, задачи, воркеры, команды, желаемое
// состояние, телеметрия, самообновление. Предметная область — только в
// воркерах; агент один для всех проектов.
package app

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/internal/backoff"
	"github.com/epifanovmd/agent/internal/commands"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/identity"
	"github.com/epifanovmd/agent/internal/jobs"
	"github.com/epifanovmd/agent/internal/link"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/outbox"
	"github.com/epifanovmd/agent/internal/runtime"
	"github.com/epifanovmd/agent/internal/state"
	"github.com/epifanovmd/agent/internal/stream"
	"github.com/epifanovmd/agent/internal/telemetry"
	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/internal/worker"
	"github.com/epifanovmd/agent/sdk/go/message"
	"github.com/epifanovmd/agent/sdk/go/sealed"
)

// SDK — версия Go SDK агента в hello (agent.sdk).
const SDK = "go/1.1.0"

// streamLimit — потоковых сообщений в памяти до подтверждения.
const streamLimit = 2000

// ErrRestart — агент остановлен для перезапуска (команда или обновление):
// процесс завершается, менеджер (systemd, Docker) запускает его снова.
var ErrRestart = errors.New("agent: перезапуск")

// App — агент.
type App struct {
	// cfgMu — cfg меняется при перечитывании настроек (Reload).
	cfgMu    sync.Mutex
	cfg      config.Config
	reloadMu sync.Mutex
	version  string
	log      *slog.Logger
	logCtl   *logx.Control
	// forward — записи лога для сервера (сообщение log).
	forward *logx.Forwarder
	ring    *logx.Ring
	client  *http.Client

	rt        *runtime.Runtime
	link      *link.Link
	auth      *auth
	jobs      *jobs.Manager
	commands  *commands.Registry
	state     *state.Manager
	workers   *worker.Supervisor
	telemetry *telemetry.Collector
	outbox    *outbox.Outbox
	update    update.Paths
	pubKey    ed25519.PublicKey

	restart atomic.Bool
	cancel  context.CancelFunc
}

// New — агент по конфигурации.
func New(cfg config.Config, version string) (*App, error) {
	ring := logx.NewRing(5000)
	log, logCtl := logx.NewSwitchable(os.Stderr, ring, logx.Options{Level: cfg.Log.Level, Format: cfg.Log.Format})
	forward := logx.NewForwarder(cfg.Log.Forward)
	logCtl.SetForwarder(forward)
	if cfg.Insecure() {
		log.Warn("связь без шифрования — ключ агента и состояние (в нём могут быть секреты) идут открытым текстом; нужен https://",
			"server", strings.Join(cfg.Server.Addresses(), ", "))
	}
	if err := os.MkdirAll(cfg.DataDir, 0o700); err != nil {
		return nil, fmt.Errorf("agent: каталог данных: %w", err)
	}
	ob, err := outbox.Open(filepath.Join(cfg.DataDir, "outbox"))
	if err != nil {
		return nil, err
	}
	client, err := httpClient(cfg.Server)
	if err != nil {
		return nil, err
	}
	a := &App{
		cfg:     cfg,
		version: version,
		log:     log,
		logCtl:  logCtl,
		forward: forward,
		ring:    ring,
		client:  client,
		outbox:  ob,
	}
	if cfg.Update.PublicKey != "" {
		if a.pubKey, err = update.ParsePublicKey(cfg.Update.PublicKey); err != nil {
			return nil, err
		}
	}
	if exe, err := os.Executable(); err == nil {
		a.update = update.NewPaths(exe)
	}

	ctx := context.Background()
	host := telemetry.HostInfo(ctx)
	codeHash, _ := update.FileHash(a.update.Binary)
	// Пара ключей для запечатанных значений в состоянии: открытый — в hello.
	encKey, err := identity.EncryptionKey(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	a.rt = runtime.New(runtime.Info{
		Name: cfg.Name, Version: version, SDK: SDK, CodeHash: codeHash,
		Labels: cfg.Labels, Host: host, EncryptionKey: sealed.EncodeKey(encKey.PublicKey().Bytes()),
	}, log)
	a.auth = &auth{store: identity.NewStore(cfg.DataDir), cfg: cfg, client: a.client, host: host, log: log}
	a.link = link.New(link.Options{
		ServerURLs: cfg.Server.Addresses(),
		Transport:  cfg.Server.Transport,
		Auth:       a.auth,
		Outbox:     ob,
		Stream:     stream.New(streamLimit),
		HTTPClient: a.client,
		Log:        log,
	}, a.rt)
	a.rt.SetSender(a.link)
	a.rt.SetOutbox(ob.Len)

	a.telemetry = telemetry.New(telemetryOptions(cfg), log)
	a.rt.SetMetrics(a.telemetry)
	a.rt.SetTelemetry(cfg.Telemetry.Backlog, cfg.Telemetry.InventoryInterval.Std())
	a.jobs = jobs.New(a.link, log, a.rt.Changed)
	a.jobs.SetUnreported(ob.JobRefs)
	a.commands = commands.New(a.link, log)
	a.state = state.New(filepath.Join(cfg.DataDir, "state"), a.link, log)
	a.state.SetUnseal(func(spec json.RawMessage) (json.RawMessage, error) { return sealed.Unseal(encKey, spec) })
	a.state.SetResync(cfg.State.ResyncInterval.Std())
	a.workers = worker.New(cfg.Workers, a.jobs, log, a.rt.Changed, version)
	if cfg.Server.CAFile != "" {
		// Файлы задач воркеры скачивают сами: им тоже нужен CA сервера.
		ca, _ := filepath.Abs(cfg.Server.CAFile)
		env := map[string]string{"AGENT_SERVER_CA_FILE": ca}
		if _, ok := os.LookupEnv("NODE_EXTRA_CA_CERTS"); !ok {
			env["NODE_EXTRA_CA_CERTS"] = ca // Node.js добавляет его к системным корням
		}
		a.workers.SetEnv(env)
	}
	a.workers.SetBridge(worker.Bridge{
		Commands:  a.commands,
		State:     a.state,
		Telemetry: a.telemetry,
		Events:    a.link,
		Changed:   a.rt.CapabilitiesChanged,
	})

	a.applyBuiltins(cfg)
	forward.SetOnline(a.link.Connected)
	a.rt.OnLogLevel(func(level string) {
		if err := forward.Override(level); err != nil {
			log.Warn("сервер прислал неверный порог лога (subscription.logLevel) — пропущен", "err", err)
		}
	})
	// Контекст воркеров (worker.context): связь, частота метрик, порог лога, подписки на каналы.
	a.rt.WhenContextChanged(a.pushWorkerContext)
	a.pushWorkerContext()
	a.rt.WhenWelcomed(func(message.Welcome) {
		if a.update.Marker != "" {
			update.Healthy(a.update)
		}
	})
	return a, nil
}

// Run — работать до отмены ctx (или команды перезапуска); ErrRestart —
// процесс нужно запустить снова.
func (a *App) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	a.cancel = cancel
	defer cancel()

	if err := a.auth.ensure(ctx); err != nil {
		return err
	}
	a.pushWorkerContext() // id агента известен

	a.rt.Register(a.jobs)
	a.rt.Register(a.commands)
	a.rt.Register(a.workers)
	// Домены могут появиться позже — от воркеров; без доменов возможность не объявляется.
	a.rt.Register(a.state)
	a.rt.Register(telemetryCapability{a.telemetry})
	a.rt.Register(updateCapability{mode: a.config().Update.Mode})
	a.rt.Register(logCapability{f: a.forward, link: a.link})

	cfg := a.config()
	a.log.Info("агент запущен", "version", a.version, "name", cfg.Name, "server", strings.Join(cfg.Server.Addresses(), ", "), "workers", len(cfg.Workers))
	err := a.rt.Run(ctx, a.link.Run, a.stopTimeout())
	if a.restart.Load() {
		return ErrRestart
	}
	return err
}

// stopTimeout — срок доработки при остановке: самая долгая из воркеров.
func (a *App) stopTimeout() time.Duration {
	timeout := 30 * time.Second
	for _, w := range a.config().Workers {
		timeout = max(timeout, w.StopTimeout.Std())
	}
	return timeout + 5*time.Second
}

func (a *App) requestRestart() {
	a.restart.Store(true)
	if a.cancel != nil {
		a.cancel()
	}
}

// config — текущие настройки (меняются Reload).
func (a *App) config() config.Config {
	a.cfgMu.Lock()
	defer a.cfgMu.Unlock()
	return a.cfg
}

// applyBuiltins — встроенные команды по настройкам: выключенные
// (commands.disabled) снимаются, остальные подключаются.
func (a *App) applyBuiltins(cfg config.Config) {
	builtins := map[string]commands.Handler{
		message.CommandLogs:          a.logsCommand,
		message.CommandDrain:         a.drainCommand,
		message.CommandResume:        a.resumeCommand,
		message.CommandRotateKey:     a.auth.rotate,
		message.CommandRestart:       a.restartCommand,
		message.CommandUpdate:        a.updateCommand,
		message.CommandWorkerRestart: a.workerRestartCommand,
		message.CommandWorkerUpdate:  a.workerUpdateCommand,
		message.CommandWorkerPause:   a.workerPauseCommand,
		message.CommandWorkerResume:  a.workerResumeCommand,
	}
	for name, h := range builtins {
		enabled := !slices.Contains(cfg.Commands.Disabled, name)
		switch name {
		case message.CommandUpdate:
			enabled = enabled && cfg.Update.Mode == message.UpdateSelf
		case message.CommandWorkerRestart, message.CommandWorkerPause, message.CommandWorkerResume:
			enabled = enabled && len(cfg.Workers) > 0
		case message.CommandWorkerUpdate:
			// Только если есть воркер из выпуска и обновление не выключено.
			enabled = enabled && cfg.Update.Mode != message.UpdateDisabled &&
				slices.ContainsFunc(cfg.Workers, func(w config.Worker) bool { return w.Release })
		}
		if enabled {
			a.commands.Register(name, h)
		} else {
			a.commands.Unregister(name)
		}
	}
}

func (a *App) logsCommand(_ context.Context, args json.RawMessage, out io.Writer) (any, error) {
	var req struct {
		Lines int `json:"lines"`
	}
	_ = json.Unmarshal(args, &req)
	if req.Lines <= 0 || req.Lines > 5000 {
		req.Lines = 500
	}
	lines := a.ring.Tail(req.Lines)
	_, _ = io.WriteString(out, strings.Join(lines, "\n")+"\n")
	return map[string]int{"lines": len(lines)}, nil
}

func (a *App) drainCommand(context.Context, json.RawMessage, io.Writer) (any, error) {
	a.rt.Drain()
	return a.rt.Status(), nil
}

func (a *App) resumeCommand(context.Context, json.RawMessage, io.Writer) (any, error) {
	a.rt.Resume()
	return a.rt.Status(), nil
}

func (a *App) restartCommand(context.Context, json.RawMessage, io.Writer) (any, error) {
	// Ответ уходит до остановки: перезапуск — чуть позже.
	time.AfterFunc(time.Second, a.requestRestart)
	return nil, nil
}

func (a *App) workerRestartCommand(ctx context.Context, args json.RawMessage, out io.Writer) (any, error) {
	var req struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(args, &req)
	names := a.workers.Names()
	if req.Name != "" {
		names = []string{req.Name}
	}
	for _, name := range names {
		fmt.Fprintf(out, "перезапуск %s…\n", name)
		if err := a.workers.Restart(ctx, name); err != nil {
			return nil, commands.Errorf("WORKER_RESTART", "%s: %v", name, err)
		}
	}
	return map[string]any{"restarted": names}, nil
}

// workerPauseCommand — worker.pause {name, queues?}: места воркера по
// очередям (без queues — по всем) — 0; пауза сервера, отдельная от паузы
// самого воркера.
func (a *App) workerPauseCommand(_ context.Context, args json.RawMessage, _ io.Writer) (any, error) {
	return a.workerPauseDo(args, a.workers.Pause)
}

// workerResumeCommand — worker.resume {name, queues?}: снять паузу сервера.
func (a *App) workerResumeCommand(_ context.Context, args json.RawMessage, _ io.Writer) (any, error) {
	return a.workerPauseDo(args, a.workers.Resume)
}

func (a *App) workerPauseDo(args json.RawMessage, do func(name string, queues []string) error) (any, error) {
	var req message.WorkerPauseArgs
	if err := json.Unmarshal(args, &req); err != nil || req.Name == "" {
		return nil, commands.Errorf("WORKER_ARGS", "нужно name — имя воркера")
	}
	if err := do(req.Name, req.Queues); err != nil {
		if errors.Is(err, worker.ErrUnknownWorker) {
			return nil, commands.Errorf("WORKER_UNKNOWN", "воркера %q нет в настройках агента", req.Name)
		}
		return nil, err
	}
	return map[string]any{"name": req.Name, "paused": a.workers.Paused(req.Name)}, nil
}

// pushWorkerContext — текущий контекст агента воркерам (worker.context);
// одинаковый повторно не отправляется.
func (a *App) pushWorkerContext() {
	cfg := a.config()
	a.workers.SetContext(message.WorkerContext{
		Mode: message.WorkerModeRun,
		Agent: message.WorkerContextAgent{
			ID: a.auth.agentID(), Name: cfg.Name, Version: a.version, Labels: cfg.Labels,
		},
		Online:            a.rt.Online(),
		MetricsIntervalMs: a.rt.MetricsInterval().Milliseconds(),
		// Все подписанные каналы; каждый воркер получает только свои (Supervisor).
		Channels: a.rt.Subscription().Channels,
		LogLevel: a.forward.Level(),
	})
}

// workerUpdateCommand — worker.update: сборка воркера из выпуска скачивается
// с сервера агента (url от корня — с его ключом), проверяется (sha256,
// подпись ключом update.publicKey) и ставится с откатом (Supervisor.Update).
func (a *App) workerUpdateCommand(ctx context.Context, args json.RawMessage, out io.Writer) (any, error) {
	var req message.WorkerUpdate
	if err := json.Unmarshal(args, &req); err != nil || req.Name == "" || req.Version == "" || req.URL == "" || req.SHA256 == "" {
		return nil, commands.Errorf("UPDATE_ARGS", "нужны name, version, url, sha256, signature")
	}
	if !a.workers.Released(req.Name) {
		return nil, commands.Errorf(message.ErrWorkerNotReleased, "воркер %q не из выпуска (нет в настройках или release: true не задан)", req.Name)
	}
	if a.pubKey == nil {
		return nil, commands.Errorf(message.ErrUpdateNotVerified, "не задан ключ проверки сборок (update.publicKey) — сборку нельзя проверить")
	}
	url := req.URL
	if strings.HasPrefix(url, "/") {
		url = strings.TrimRight(a.link.ServerURL(), "/") + url
	}
	fetch := func(dst string) error {
		return update.Fetch(ctx, a.client, a.auth.Session(), a.pubKey, url, req.SHA256, req.Signature, dst)
	}
	res, err := a.workers.Update(ctx, req.Name, req.Version, fetch, out)
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (a *App) updateCommand(ctx context.Context, args json.RawMessage, out io.Writer) (any, error) {
	var rel update.Release
	if err := json.Unmarshal(args, &rel); err != nil || rel.URL == "" || rel.SHA256 == "" {
		return nil, commands.Errorf("UPDATE_ARGS", "нужны version, url, sha256, signature")
	}
	if strings.HasPrefix(rel.URL, "/") {
		rel.URL = strings.TrimRight(a.link.ServerURL(), "/") + rel.URL
	}
	a.rt.SetUpdating(true)
	defer a.rt.SetUpdating(false)
	fmt.Fprintf(out, "загрузка %s…\n", rel.Version)
	if err := update.Install(ctx, a.client, a.auth.Session(), a.update, a.pubKey, rel); err != nil {
		if errors.Is(err, update.ErrNotVerified) {
			return nil, commands.Errorf(message.ErrUpdateNotVerified, "%v", err)
		}
		return nil, commands.Errorf("UPDATE_FAILED", "%v", err)
	}
	fmt.Fprintln(out, "установлено, перезапуск после доработки задач")
	time.AfterFunc(time.Second, a.requestRestart)
	return map[string]string{"version": rel.Version}, nil
}

// telemetryCapability — объявление каналов телеметрии.
type telemetryCapability struct{ c *telemetry.Collector }

func (t telemetryCapability) Declare(caps *message.Capabilities) {
	caps.Telemetry = &message.TelemetryCapability{Channels: t.c.Channels()}
}
func (telemetryCapability) Handles() []string                              { return nil }
func (telemetryCapability) Handle(context.Context, message.Envelope) error { return nil }

// logCapability — отправка записей лога серверу пачками (сообщение log),
// пока агент работает.
type logCapability struct {
	f    *logx.Forwarder
	link *link.Link
}

func (logCapability) Declare(*message.Capabilities)                  {}
func (logCapability) Handles() []string                              { return nil }
func (logCapability) Handle(context.Context, message.Envelope) error { return nil }
func (l logCapability) Start(ctx context.Context) error {
	l.f.Run(ctx, logx.ForwardEvery, func(b message.LogBatch) { l.link.Stream(message.TypeLog, b) })
	return nil
}

// updateCapability — режим обновления.
type updateCapability struct{ mode string }

func (u updateCapability) Declare(caps *message.Capabilities) {
	caps.Update = &message.UpdateCapability{Mode: u.mode}
}
func (updateCapability) Handles() []string                              { return nil }
func (updateCapability) Handle(context.Context, message.Envelope) error { return nil }

// ─── учётные данные ────────────────────────────────────────────────────

// auth — учётные данные агента: загрузка, регистрация, повторная регистрация,
// смена секрета (agent.rotateKey) через ожидающий секрет.
type auth struct {
	store  *identity.Store
	cfg    config.Config
	client *http.Client
	host   message.Host
	log    *slog.Logger
	keys   atomic.Pointer[identity.Keys]
}

// Authorization — заголовок для подключения (сначала ожидающий секрет).
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

// Session — заголовок, принятый в последней сессии (запросы вне подключения).
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
func (a *auth) rotate(context.Context, json.RawMessage, io.Writer) (any, error) {
	k := a.keys.Load()
	if k == nil {
		return nil, commands.Errorf("ROTATE_KEY", "учётных данных нет")
	}
	hash, err := k.Rotate()
	if err != nil {
		return nil, commands.Errorf("ROTATE_KEY", "%v", err)
	}
	a.log.Info("создан новый секрет агента — ждёт признания сервером")
	return message.RotateKeyResult{SecretHash: hash}, nil
}

// ensure — учётные данные есть или агент регистрируется (с повтором, пока
// сервер недоступен; отклонённый токен — ошибка запуска).
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
		delay := backoff.Default.Delay(attempt)
		a.log.Warn("регистрация не удалась — повтор", "err", err, "retryIn", delay.Round(time.Millisecond))
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// Renew — учётные данные отозваны: регистрация заново, если есть токен.
func (a *auth) Renew(ctx context.Context) error {
	if a.cfg.Enroll.Token == "" {
		return errors.New("agent: учётные данные отозваны, токена регистрации нет")
	}
	_ = a.store.Forget()
	return a.enroll(ctx)
}

// enroll — регистрация по токену: адреса сервера по порядку, пока один не
// ответит (отклонённый токен — сразу ошибка).
func (a *auth) enroll(ctx context.Context) error {
	req := identity.EnrollRequest{
		Token:  a.cfg.Enroll.Token,
		Name:   a.cfg.Name,
		Labels: a.cfg.Labels,
		Host:   &identity.EnrollHost{Hostname: a.host.Hostname, OS: a.host.OS, Arch: a.host.Arch},
	}
	var creds identity.Credentials
	var err error
	for _, addr := range a.cfg.Server.Addresses() {
		creds, err = identity.Enroll(ctx, a.client, addr, req)
		if err == nil || errors.Is(err, identity.ErrTokenRejected) || ctx.Err() != nil {
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
