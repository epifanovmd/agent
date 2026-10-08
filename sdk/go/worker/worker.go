// Package worker — SDK воркера на Go (контракт — sdk/README.md §1).
//
// Воркер — процесс, который запускает агент (workers в его конфигурации) и с
// которым держит канал IPC (sdk/spec §10). Сеть, повторы, досылка итогов
// после обрыва связи, учётные данные и обновление — забота агента; воркер
// знает только свою предметную область:
//
//	w := worker.New("report", "1.0.0")
//	w.Job("example.convert", 2, convert)                  // задачи очереди
//	w.Command("example.app.reload", reload)             // команда агенту
//	w.State("example.app", apply)                       // желаемое состояние
//	w.Telemetry("example.app", 15*time.Second, stats)   // канал в metrics
//	w.OnContext(func(c worker.Context) { … })           // контекст агента
//	w.Cleanup(removeRules)                                // уборка при удалении агента
//	if err := w.Run(context.Background()); err != nil { … }
//
// Остановка: worker.drain от агента и SIGTERM/SIGINT — новые задачи не
// берутся, текущие задачи и команды дорабатываются, Run возвращает nil.
// Канал закрыт (агента нет) — всё отменяется, Run возвращает nil. Отмена ctx
// у Run — немедленная остановка без итогов. Типы сообщений — sdk/go/message.
package worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"runtime/debug"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// SDKVersion — версия SDK; в worker.register — "go/<версия>".
const SDKVersion = "1.1.0"

// JobHandler — обработчик задачи очереди: результат — в job.complete, ошибка —
// job.fail (Fail — с кодом). ctx отменяется при job.cancel и остановке.
type JobHandler func(ctx context.Context, job *Job) (any, error)

// CommandHandler — обработчик команды: результат — итог cmd.done, ошибка —
// CommandError с кодом. ctx отменяется при cmd.cancel (срок истёк).
type CommandHandler func(ctx context.Context, cmd *Command) (any, error)

// StateHandler — применение снимка домена (идемпотентно): результат — report
// в state.applied, ошибка — ok: false (агент повторит).
type StateHandler func(ctx context.Context, version int64, spec json.RawMessage) (any, error)

// CleanupHandler — уборка при удалении агента с узла (`agent cleanup`): снять
// то, что воркер поставил на узле. Ошибка (паника) — worker.cleaned ok: false.
type CleanupHandler func(ctx context.Context) error

// Option — настройка воркера.
type Option func(*Worker)

// WithConn — свой транспорт: двусторонний поток строк JSON (тесты, unix-сокет).
// По умолчанию — канал из AGENT_IPC_FD.
func WithConn(conn io.ReadWriteCloser) Option { return func(w *Worker) { w.conn = conn } }

// WithLogger — лог SDK (по умолчанию — текст в stderr: агент пишет его в свой
// журнал с именем воркера).
func WithLogger(log *slog.Logger) Option { return func(w *Worker) { w.log = log } }

// WithoutSignals — не перехватывать SIGTERM/SIGINT (остановка — Drain или ctx).
func WithoutSignals() Option { return func(w *Worker) { w.noSignals = true } }

type queue struct {
	name        string
	concurrency int
	fn          JobHandler
	sem         chan struct{}
}

type poller struct {
	channel  string
	interval time.Duration
	fn       func() any
}

// Worker — объявления воркера и цикл приёма сообщений агента.
type Worker struct {
	name, version string
	log           *slog.Logger
	conn          io.ReadWriteCloser
	noSignals     bool

	queues   []*queue
	byQueue  map[string]*queue
	commands map[string]CommandHandler
	cmdNames []string
	domains  map[string]StateHandler
	domNames []string
	domLocks map[string]*sync.Mutex
	channels []string
	pollers  []poller
	cleanup  CleanupHandler
	invalid  []string // имена не по правилу message.NamePattern: Run вернёт ошибку

	ch     *channel
	ctx    context.Context // отменяется при остановке: задачи, команды, телеметрия
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu        sync.Mutex
	started   bool
	rejected  []string
	jobs      map[string]*Job
	cmds      map[string]*Command
	busy      int // команды и применения состояния в работе
	draining  bool
	drainCh   chan struct{}
	wake      chan struct{}
	readyOnce sync.Once

	// Контекст агента (worker.context): ctxChanged закрывается при новом.
	wctx       Context
	onContext  []func(Context)
	ctxChanged chan struct{}
}

// New — воркер с именем (пусто — AGENT_WORKER) и версией.
func New(name, version string, opts ...Option) *Worker {
	if name == "" {
		name = os.Getenv("AGENT_WORKER")
	}
	if name == "" {
		name = "worker"
	}
	w := &Worker{
		name: name, version: version,
		byQueue:    map[string]*queue{},
		commands:   map[string]CommandHandler{},
		domains:    map[string]StateHandler{},
		domLocks:   map[string]*sync.Mutex{},
		jobs:       map[string]*Job{},
		cmds:       map[string]*Command{},
		drainCh:    make(chan struct{}),
		wake:       make(chan struct{}, 1),
		wctx:       Context{Mode: message.WorkerModeRun},
		ctxChanged: make(chan struct{}),
	}
	for _, opt := range opts {
		opt(w)
	}
	if w.log == nil {
		// Время ставит агент, записывая строку в свой журнал.
		w.log = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
			ReplaceAttr: func(groups []string, a slog.Attr) slog.Attr {
				if len(groups) == 0 && a.Key == slog.TimeKey {
					return slog.Attr{}
				}
				return a
			},
		}))
	}
	w.log = w.log.With("worker", w.name)
	return w
}

// ─── объявления (до Run) ───────────────────────────────────────────────

// Имена очередей, команд, доменов и каналов — по правилу message.NamePattern
// (латиница, цифры, «.», «_», «-», начало — буква или цифра, до 64 символов);
// имя не по правилу — Run сразу возвращает ошибку с перечнем таких имён.

// checkName — запомнить имя не по правилу (сообщит Run).
func (w *Worker) checkName(kind, name string) {
	if message.ValidName(name) {
		return
	}
	entry := fmt.Sprintf("%s %q", kind, name)
	if !slices.Contains(w.invalid, entry) {
		w.invalid = append(w.invalid, entry)
	}
}

// Job — обработчик очереди; concurrency — задач одновременно (не меньше 1).
func (w *Worker) Job(queueName string, concurrency int, fn JobHandler) {
	if queueName == "" || fn == nil {
		panic("worker: Job — нужны очередь и обработчик")
	}
	w.checkName("очередь", queueName)
	concurrency = max(concurrency, 1)
	q := &queue{name: queueName, concurrency: concurrency, fn: fn, sem: make(chan struct{}, concurrency)}
	if prev := w.byQueue[queueName]; prev != nil {
		w.queues = slices.DeleteFunc(w.queues, func(x *queue) bool { return x == prev })
	}
	w.queues = append(w.queues, q)
	w.byQueue[queueName] = q
}

// Command — обработчик команды. Имена agent.* и worker.* принадлежат агенту.
func (w *Worker) Command(name string, fn CommandHandler) {
	if name == "" || fn == nil {
		panic("worker: Command — нужны имя и обработчик")
	}
	w.checkName("команда", name)
	if _, ok := w.commands[name]; !ok {
		w.cmdNames = append(w.cmdNames, name)
	}
	w.commands[name] = fn
}

// State — домен желаемого состояния. Агент присылает последний снимок после
// каждой регистрации и при новой версии; применения домена не пересекаются.
func (w *Worker) State(domain string, fn StateHandler) {
	if domain == "" || fn == nil {
		panic("worker: State — нужны домен и обработчик")
	}
	w.checkName("домен", domain)
	if _, ok := w.domains[domain]; !ok {
		w.domNames = append(w.domNames, domain)
		w.domLocks[domain] = &sync.Mutex{}
	}
	w.domains[domain] = fn
}

// Cleanup — обработчик уборки при удалении агента (`agent cleanup` запускает
// воркер без связи с сервером и шлёт worker.cleanup). Без обработчика уборка
// сразу успешна.
func (w *Worker) Cleanup(fn CleanupHandler) {
	w.cleanup = fn
}

// Channel — объявить канал телеметрии; данные — Report.
func (w *Worker) Channel(name string) {
	if name == "" {
		panic("worker: Channel — нужно имя")
	}
	w.checkName("канал", name)
	if !slices.Contains(w.channels, name) {
		w.channels = append(w.channels, name)
	}
}

// Telemetry — канал телеметрии с опросом: fn вызывается раз в interval после
// worker.ready; interval AutoInterval (0 и меньше) — «авто»: частота подписки
// на канал из контекста, иначе частота метрик агента (пока неизвестна — 15 с).
// Сбой (паника) — в лог, nil — пропуск.
func (w *Worker) Telemetry(channelName string, interval time.Duration, fn func() any) {
	if fn == nil {
		panic("worker: Telemetry — нужен источник")
	}
	if interval < 0 {
		interval = AutoInterval
	}
	w.Channel(channelName)
	w.pollers = append(w.pollers, poller{channel: channelName, interval: interval, fn: fn})
}

// ─── из обработчиков ───────────────────────────────────────────────────

// Report — последние данные канала телеметрии: уйдут в ближайший metrics агента.
func (w *Worker) Report(channelName string, data any) error {
	if !slices.Contains(w.channels, channelName) {
		return fmt.Errorf("worker: канал %q не объявлен (Channel)", channelName)
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return fmt.Errorf("worker: телеметрия %s: %w", channelName, err)
	}
	return w.send(message.TypeTelemetry, message.Telemetry{Channel: channelName, Data: raw}, "")
}

// Event — событие воркера серверу (надёжно: агент хранит до подтверждения).
func (w *Worker) Event(typ string, data any) error {
	e := message.Event{Type: truncate(typ, 50)}
	if data != nil {
		raw, err := json.Marshal(data)
		if err != nil {
			return fmt.Errorf("worker: событие %s: %w", typ, err)
		}
		e.Data = raw
	}
	return w.send(message.TypeEvent, e, "")
}

// Rejected — имена, которые агент отклонил (зарезервированы или заняты).
func (w *Worker) Rejected() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.rejected)
}

// Stopping — канал закрывается, когда воркер уходит (worker.drain, SIGTERM,
// Drain): свои фоновые циклы (пробы, переключения) по нему останавливаются.
func (w *Worker) Stopping() <-chan struct{} { return w.drainCh }

// Drain — не брать новых задач; Run завершится после текущих задач и команд.
func (w *Worker) Drain() {
	w.mu.Lock()
	if !w.draining {
		w.draining = true
		close(w.drainCh)
		w.log.Info("воркер дорабатывает задачи и завершается")
	}
	w.mu.Unlock()
	w.poke()
}

func (w *Worker) send(typ string, data any, re string) error {
	w.mu.Lock()
	ch := w.ch
	w.mu.Unlock()
	if ch == nil {
		return errors.New("worker: воркер ещё не запущен (Run)")
	}
	err := ch.message(typ, data, re)
	if err != nil && !errors.Is(err, errClosed) {
		w.log.Warn("агенту не отправлено", "type", typ, "err", err)
	}
	return err
}

func (w *Worker) poke() {
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

// ─── работа ────────────────────────────────────────────────────────────

// register — worker.register: queues всегда (в message.WorkerRegister — omitempty).
type register struct {
	message.WorkerRegister
	Queues []message.QueueCapacity `json:"queues"`
}

func (w *Worker) registration() register {
	reg := register{
		WorkerRegister: message.WorkerRegister{
			Name: w.name, Version: w.version, SDK: "go/" + SDKVersion,
			Commands: w.cmdNames, Domains: w.domNames, Channels: w.channels,
		},
		Queues: []message.QueueCapacity{},
	}
	for _, q := range w.queues {
		reg.Queues = append(reg.Queues, message.QueueCapacity{Name: q.name, Concurrency: q.concurrency})
	}
	return reg
}

// Run — зарегистрироваться у агента и работать до остановки (см. пакет).
func (w *Worker) Run(ctx context.Context) error {
	if len(w.queues)+len(w.commands)+len(w.domains)+len(w.channels) == 0 && w.cleanup == nil {
		return errors.New("worker: нечего объявить: нужны очередь, команда, домен, канал телеметрии или уборка")
	}
	if len(w.invalid) > 0 {
		return fmt.Errorf("worker: имена не по правилу %s: %s", message.NamePattern, strings.Join(w.invalid, ", "))
	}
	w.mu.Lock()
	if w.started {
		w.mu.Unlock()
		return errors.New("worker: Run уже вызван")
	}
	w.started = true
	w.mu.Unlock()

	conn := w.conn
	if conn == nil {
		var err error
		if conn, err = connFromEnv(); err != nil {
			return err
		}
	}
	ch := newChannel(conn)
	w.ctx, w.cancel = context.WithCancel(context.Background())
	defer w.cancel()
	w.mu.Lock()
	w.ch = ch
	w.mu.Unlock()

	var sig chan os.Signal
	if !w.noSignals {
		sig = make(chan os.Signal, 1)
		signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT)
		defer signal.Stop(sig)
	}

	if err := ch.message(message.TypeWorkerRegister, w.registration(), ""); err != nil {
		ch.close()
		return fmt.Errorf("worker: регистрация: %w", err)
	}
	readDone := make(chan struct{})
	go func() {
		defer close(readDone)
		w.read(ch)
	}()

	var result error
loop:
	for {
		if w.drained() {
			w.log.Info("воркер остановлен: текущие задачи доработаны")
			break
		}
		select {
		case <-readDone:
			w.log.Info("канал с агентом закрыт: текущая работа отменена")
			break loop
		case <-ctx.Done():
			result = ctx.Err()
			break loop
		case <-sig:
			w.Drain()
		case <-w.wake:
		}
	}
	// Сначала канал (итоги прерванного никому не нужны), затем отмена.
	ch.close()
	w.cancel()
	<-readDone // после него новых задач и команд нет: wg.Add не гонится с Wait
	w.wg.Wait()
	return result
}

func (w *Worker) drained() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.draining && len(w.jobs) == 0 && w.busy == 0
}

// read — сообщения агента до закрытия канала.
func (w *Worker) read(ch *channel) {
	for {
		env, err := ch.next()
		if err != nil {
			return
		}
		w.dispatch(env)
	}
}

func (w *Worker) dispatch(env message.Envelope) {
	switch env.Type {
	case message.TypeWorkerReady:
		var ready message.WorkerReady
		_ = env.Decode(&ready)
		w.mu.Lock()
		w.rejected = ready.Rejected
		w.mu.Unlock()
		if len(ready.Rejected) > 0 {
			w.log.Warn("агент отклонил имена (зарезервированы или заняты)", "rejected", ready.Rejected)
		}
		w.log.Info("воркер зарегистрирован у агента", "agentVersion", ready.AgentVersion)
		w.readyOnce.Do(w.startPollers)
	case message.TypeJobAssign:
		var a message.JobAssign
		if err := env.Decode(&a); err != nil {
			w.log.Warn("job.assign не разобран", "err", err)
			return
		}
		w.assign(a)
	case message.TypeJobCancel, message.TypeJobStop:
		var ref message.JobRef
		_ = env.Decode(&ref)
		w.mu.Lock()
		job := w.jobs[ref.JobID]
		w.mu.Unlock()
		if job == nil || job.Attempt != ref.Attempt {
			return
		}
		if env.Type == message.TypeJobCancel {
			job.abort()
		} else {
			job.requestStop()
		}
	case message.TypeCmdRun:
		var run message.CommandRun
		if err := env.Decode(&run); err != nil {
			w.log.Warn("cmd.run не разобран", "err", err)
			return
		}
		w.runCommand(run)
	case message.TypeCmdCancel:
		var ref message.CommandRef
		_ = env.Decode(&ref)
		w.mu.Lock()
		cmd := w.cmds[ref.CommandID]
		w.mu.Unlock()
		if cmd != nil {
			cmd.abort()
		}
	case message.TypeStatePut:
		var put message.StatePut
		if err := env.Decode(&put); err != nil {
			w.log.Warn("state.put не разобран", "err", err)
			return
		}
		w.applyState(env.ID, put)
	case message.TypeWorkerContext:
		var c Context
		if err := env.Decode(&c); err != nil {
			w.log.Warn("worker.context не разобран", "err", err)
			return
		}
		w.setContext(c)
	case message.TypeWorkerDrain:
		w.Drain()
	case message.TypeWorkerCleanup:
		w.runCleanup(env.ID)
	case message.TypeError:
		var e message.Error
		_ = env.Decode(&e)
		w.log.Warn("ошибка от агента", "code", e.Code, "message", e.Message, "re", env.Re)
	default:
		w.log.Debug("неизвестное сообщение агента пропущено", "type", env.Type)
	}
}

// safeCall — вызов обработчика; паника — ошибка, а не падение воркера.
func (w *Worker) safeCall(what string, fn func() (any, error)) (res any, err error) {
	defer func() {
		if r := recover(); r != nil {
			w.log.Error("паника обработчика", "what", what, "panic", r, "stack", string(debug.Stack()))
			err = fmt.Errorf("паника: %v", r)
		}
	}()
	return fn()
}

func marshalResult(v any) (json.RawMessage, error) {
	if v == nil {
		return nil, nil
	}
	if raw, ok := v.(json.RawMessage); ok {
		return raw, nil
	}
	return json.Marshal(v)
}

// ─── задачи ────────────────────────────────────────────────────────────

func (w *Worker) assign(a message.JobAssign) {
	q := w.byQueue[a.Queue]
	w.mu.Lock()
	if w.draining || q == nil {
		w.mu.Unlock()
		_ = w.send(message.TypeJobFail, message.JobFail{
			JobRef: a.Ref(), Code: CodeWorkerStopping, Retryable: true,
			Message: fmt.Sprintf("Воркер %s не берёт задачу очереди %s", w.name, a.Queue),
		}, "")
		return
	}
	if prev := w.jobs[a.JobID]; prev != nil && prev.Attempt == a.Attempt {
		w.mu.Unlock() // повторная доставка той же попытки
		return
	}
	job := newJob(w, a)
	w.jobs[a.JobID] = job
	w.wg.Add(1)
	w.mu.Unlock()
	go w.execute(q, job)
}

func (w *Worker) execute(q *queue, job *Job) {
	defer w.wg.Done()
	defer func() {
		job.close()
		w.mu.Lock()
		if w.jobs[job.ID] == job {
			delete(w.jobs, job.ID)
		}
		w.mu.Unlock()
		w.poke()
	}()
	select {
	case q.sem <- struct{}{}:
	case <-job.ctx.Done():
		return
	}
	defer func() { <-q.sem }()

	result, err := w.safeCall("job "+q.name, func() (any, error) { return q.fn(job.ctx, job) })
	if job.cancelled.Load() || w.ch.isClosed() {
		return // отменена или агента нет: итог не нужен
	}
	job.flush()
	if err == nil {
		raw, merr := marshalResult(result)
		if merr == nil {
			_ = w.send(message.TypeJobComplete, message.JobComplete{JobRef: job.ref(), Result: raw}, "")
			return
		}
		err = fmt.Errorf("результат не сериализуется: %w", merr)
	}
	fail := message.JobFail{JobRef: job.ref(), Code: CodeWorkerError, Message: err.Error(), Retryable: true}
	var jf *JobFailed
	if errors.As(err, &jf) {
		fail.Code, fail.Message, fail.Retryable = jf.Code, jf.Message, jf.Retryable
	} else {
		w.log.Warn("задача провалена", "job", job.ID, "err", err)
	}
	fail.Code, fail.Message = normalizeCode(fail.Code, fail.Message, CodeWorkerError)
	fail.Message = truncate(fail.Message, 2000)
	_ = w.send(message.TypeJobFail, fail, "")
}

// ─── команды и состояние ───────────────────────────────────────────────

func (w *Worker) runCommand(run message.CommandRun) {
	w.mu.Lock()
	if w.cmds[run.CommandID] != nil {
		w.mu.Unlock() // уже выполняется
		return
	}
	cmd := newCommand(w, run)
	w.cmds[run.CommandID] = cmd
	w.busy++
	w.wg.Add(1)
	w.mu.Unlock()
	go func() {
		defer w.wg.Done()
		defer func() {
			cmd.cancel()
			w.mu.Lock()
			delete(w.cmds, run.CommandID)
			w.busy--
			w.mu.Unlock()
			w.poke()
		}()
		done := message.CommandDone{CommandID: run.CommandID}
		fn := w.commands[run.Name]
		if fn == nil {
			done.Error = &message.CommandError{Code: CodeCommandUnknown, Message: "Команда " + run.Name + " не поддерживается"}
		} else {
			res, err := w.safeCall("command "+run.Name, func() (any, error) { return fn(cmd.ctx, cmd) })
			cmd.flush()
			if err == nil {
				if done.Result, err = marshalResult(res); err != nil {
					err = fmt.Errorf("результат не сериализуется: %w", err)
				}
			}
			if err == nil {
				done.OK = true
			} else {
				e := message.CommandError{Code: CodeCommandFailed, Message: err.Error()}
				var cf *CommandFailed
				if errors.As(err, &cf) {
					e.Code, e.Message = cf.Code, cf.Message
				}
				e.Code, e.Message = normalizeCode(e.Code, e.Message, CodeCommandFailed)
				e.Message = truncate(e.Message, 2000)
				done.Error, done.Result = &e, nil
			}
		}
		if cmd.cancelled.Load() || w.ch.isClosed() {
			return // срок истёк: итог уже не нужен
		}
		_ = w.send(message.TypeCmdDone, done, "")
	}()
}

func (w *Worker) applyState(id string, put message.StatePut) {
	w.mu.Lock()
	w.busy++
	w.wg.Add(1)
	w.mu.Unlock()
	go func() {
		defer w.wg.Done()
		defer func() {
			w.mu.Lock()
			w.busy--
			w.mu.Unlock()
			w.poke()
		}()
		applied := message.StateApplied{Domain: put.Domain, Version: put.Version}
		fn, lock := w.domains[put.Domain], w.domLocks[put.Domain]
		if fn == nil {
			applied.Error = fmt.Sprintf("домен %s не обслуживается воркером %s", put.Domain, w.name)
		} else {
			lock.Lock()
			report, err := w.safeCall("state "+put.Domain, func() (any, error) { return fn(w.ctx, put.Version, put.Spec) })
			lock.Unlock()
			if err == nil {
				applied.Report, err = marshalResult(report)
			}
			if err != nil {
				w.log.Warn("снимок не применён", "domain", put.Domain, "version", put.Version, "err", err)
				applied.Error, applied.Report = truncate(err.Error(), 2000), nil
				// StateFailed — отчёт уходит вместе с ошибкой.
				var sf *StateFailure
				if errors.As(err, &sf) {
					if raw, rerr := marshalResult(sf.Report); rerr == nil {
						applied.Report = raw
					} else {
						w.log.Warn("отчёт ошибки состояния не сериализован", "domain", put.Domain, "err", rerr)
					}
				}
			} else {
				applied.OK = true
			}
		}
		_ = w.send(message.TypeStateApplied, applied, id)
	}()
}

// runCleanup — worker.cleanup: обработчик уборки, итог — worker.cleaned (re = id).
func (w *Worker) runCleanup(id string) {
	w.mu.Lock()
	w.busy++
	w.wg.Add(1)
	w.mu.Unlock()
	go func() {
		defer w.wg.Done()
		defer func() {
			w.mu.Lock()
			w.busy--
			w.mu.Unlock()
			w.poke()
		}()
		cleaned := message.WorkerCleaned{OK: true}
		if fn := w.cleanup; fn != nil {
			if _, err := w.safeCall("cleanup", func() (any, error) { return nil, fn(w.ctx) }); err != nil {
				w.log.Warn("уборка не удалась", "err", err)
				cleaned = message.WorkerCleaned{Error: truncate(err.Error(), 2000)}
			}
		}
		_ = w.send(message.TypeWorkerCleaned, cleaned, id)
	}()
}

// ─── телеметрия ────────────────────────────────────────────────────────

// pollWait — дождаться следующего опроса канала (от last); интервал с
// частотой метрик пересчитывается при новом контексте. false — остановка.
func (w *Worker) pollWait(p poller, last time.Time) bool {
	for {
		interval, changed := w.pollInterval(p)
		timer := time.NewTimer(time.Until(last.Add(interval)))
		select {
		case <-timer.C:
			return true
		case <-changed:
			timer.Stop()
		case <-w.drainCh:
			timer.Stop()
			return false
		case <-w.ctx.Done():
			timer.Stop()
			return false
		}
	}
}

func (w *Worker) startPollers() {
	for _, p := range w.pollers {
		w.wg.Add(1)
		go func() {
			defer w.wg.Done()
			for {
				data, err := w.safeCall("telemetry "+p.channel, func() (any, error) { return p.fn(), nil })
				if err == nil && data != nil {
					if err := w.Report(p.channel, data); err != nil && !errors.Is(err, errClosed) {
						w.log.Warn("телеметрия не отправлена", "channel", p.channel, "err", err)
					}
				}
				if !w.pollWait(p, time.Now()) {
					return
				}
			}
		}()
	}
}
