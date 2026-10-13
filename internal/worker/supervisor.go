//go:build unix

// Package worker — воркеры агента (§11–§16): запуск процессов по настройкам, подхват
// переживших перезапуск агента, перезапуск после падения с растущей паузой, регистрация и
// пульс от самих воркеров (через сокет агента), плановая замена с ожиданием занятого воркера,
// обновление сборки с возвратом прежней.
package worker

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/internal/backoff"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/protocol"
)

// Сроки; переменные — для тестов.
var (
	// stableRun — проработал столько — счётчик падений подряд сбрасывается.
	stableRun = time.Minute
	// busyPoll — как часто отложенная замена смотрит, занят ли воркер.
	busyPoll = time.Second
	// watchEvery — как часто агент проверяет регистрацию и пульс.
	watchEvery = time.Second
	// restartBackoff — пауза перед перезапуском упавшего воркера.
	restartBackoff = backoff.Policy{Min: time.Second, Max: 30 * time.Second}
	// registerTimeout — сколько ждать регистрации после запуска.
	registerTimeout = protocol.RegisterTimeout
	// healthSilence — пульса нет дольше — воркер нездоров; ещё healthRestart — перезапуск.
	healthSilence = protocol.HealthSilence
	healthRestart = protocol.HealthRestartAfter
	// adoptRetry — как часто повторять подхват процесса, который агент не может остановить.
	adoptRetry = 10 * time.Second
	// busyTimeout — плановая замена занятого воркера ждёт не дольше.
	busyTimeout = protocol.BusyTimeout
	// startTimeout — новая сборка воркера должна заработать за это время.
	startTimeout = time.Minute
)

// Ошибки доступа к воркеру.
var (
	ErrUnknown       = errors.New("воркера нет в настройках агента")
	ErrUnavailable   = errors.New("воркер не запущен")
	ErrNotRegistered = errors.New("воркер не зарегистрирован")
)

// Exit — почему агент останавливает воркеры (Run завершается).
type Exit int32

const (
	// ExitShutdown — остановить все воркеры (по умолчанию).
	ExitShutdown Exit = iota
	// ExitRestart — агент перезапускается: воркеры работают дальше.
	ExitRestart
	// ExitStop — агент останавливается: воркеры работают дальше.
	ExitStop
)

// Options — что нужно воркерам от агента.
type Options struct {
	// DataDir — каталог данных агента: файлы процессов, вывода и данных воркеров.
	DataDir     string
	AgentSocket string
	// Env — переменные агента для всех воркеров (KEY=VALUE).
	Env []string
	Log *slog.Logger
	// OnRegistered — воркер зарегистрировался (после запуска или переподключения).
	OnRegistered func(name string)
	// OnChange — изменилось то, что видно в состоянии.
	OnChange func()
}

// StateDir — каталог своих данных воркера (WORKER_STATE_DIR).
func StateDir(dataDir, name string) string { return filepath.Join(dataDir, "state", name) }

// Supervisor — все воркеры агента.
type Supervisor struct {
	opts      Options
	stateDir  string
	logDir    string
	stableRun time.Duration
	exit      atomic.Int32

	mu      sync.Mutex
	ctx     context.Context
	workers map[string]*Worker
	order   []string
	wg      sync.WaitGroup
}

// Worker — один воркер: настройки, текущий процесс, регистрация и пульс.
type Worker struct {
	sup  *Supervisor
	name string

	mu    sync.Mutex
	token string
	spec  config.Worker
	state string
	// reason — причина состояния invalid.
	reason   string
	restarts int
	health   *protocol.Health
	// beat — время последнего пульса (или регистрации).
	beat     time.Time
	manifest *protocol.WorkerManifest
	version  string
	proc     *process
	// registered — закрыт, когда текущий процесс зарегистрировался.
	registered chan struct{}
	updating   bool
	// removed — воркер удалён из настроек: останавливается всегда.
	removed bool
	// gen — номер запуска процесса.
	gen int
	// waiting — сколько замен каждого вида ждут, пока воркер занят.
	waiting map[string]int

	requests chan request
	cancel   context.CancelFunc
	done     chan struct{}
}

// request — заменить процесс: остановить текущий, выполнить swap (смена сборки), запустить
// новый; done — итог (регистрация нового).
type request struct {
	swap func() error
	done chan error
	// gen — перезапуск запуска номер gen: процесс с тех пор уже запущен заново — заменять
	// нечего (0 — заменить в любом случае).
	gen int
}

func (r *request) reply(err error) {
	if r != nil && r.done != nil {
		r.done <- err
	}
}

// New — воркеры по настройкам specs (запускаются Run).
func New(specs []config.Worker, opts Options) *Supervisor {
	if opts.OnRegistered == nil {
		opts.OnRegistered = func(string) {}
	}
	if opts.OnChange == nil {
		opts.OnChange = func() {}
	}
	s := &Supervisor{opts: opts, workers: map[string]*Worker{}, stableRun: stableRun,
		stateDir: filepath.Join(opts.DataDir, ProcessesDir), logDir: filepath.Join(opts.DataDir, LogsDir)}
	for _, spec := range specs {
		s.add(spec)
	}
	return s
}

func (s *Supervisor) add(spec config.Worker) *Worker {
	spec.FillDefaults()
	w := &Worker{sup: s, name: spec.Name, token: newToken(), spec: spec, state: protocol.WorkerStopped,
		waiting: map[string]int{}, requests: make(chan request), done: make(chan struct{}),
		registered: make(chan struct{})}
	s.workers[spec.Name] = w
	s.order = append(s.order, spec.Name)
	return w
}

func newToken() string {
	var b [24]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// SetExit — почему агент сейчас остановит воркеры (до отмены ctx Run).
func (s *Supervisor) SetExit(e Exit) { s.exit.Store(int32(e)) }

// Run — запустить воркеры (подхватив переживших перезапуск агента) и держать их до отмены ctx;
// возвращается, когда процессы остановлены или оставлены работать (SetExit).
func (s *Supervisor) Run(ctx context.Context) {
	s.mu.Lock()
	s.ctx = ctx
	for _, name := range s.order {
		s.startLoop(s.workers[name])
	}
	known := map[string]bool{}
	for name := range s.workers {
		known[name] = true
	}
	s.mu.Unlock()
	s.stopOrphans(known)
	<-ctx.Done()
	s.wg.Wait()
}

// stopOrphans — процессы воркеров, которых больше нет в настройках, останавливаются (в фоне).
func (s *Supervisor) stopOrphans(known map[string]bool) {
	for _, name := range stateNames(s.stateDir) {
		if known[name] {
			continue
		}
		st, ok := readState(s.stateDir, name)
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if ok && stopOrphan(st) {
				s.opts.Log.Info("воркера нет в настройках — его процесс остановлен", "worker", name, "pid", st.PID)
			}
			if ok && st.alive() {
				s.opts.Log.Error("воркера нет в настройках, но его процесс не остановлен (нет прав) — остановите его сами",
					"worker", name, "pid", st.PID)
				return
			}
			if ok && st.Worker != nil {
				Retire(context.Background(), *st.Worker, s.opts.DataDir, s.opts.Log)
			}
			removeState(s.stateDir, name)
		}()
	}
}

// startLoop — цикл жизни воркера (под s.mu, после Run).
func (s *Supervisor) startLoop(w *Worker) {
	ctx, cancel := context.WithCancel(s.ctx)
	w.cancel = cancel
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		w.loop(ctx)
	}()
}

// Apply — новые настройки воркеров (перечитывание agent.yaml): новые запускаются, удалённые
// останавливаются сразу, изменённые заменяются плановой заменой (ждёт, пока воркер занят);
// изменение только stopTimeout применяется без замены.
func (s *Supervisor) Apply(specs []config.Worker) {
	s.mu.Lock()
	var stop, replace []*Worker
	keep := map[string]bool{}
	for _, spec := range specs {
		spec.FillDefaults()
		keep[spec.Name] = true
		w, ok := s.workers[spec.Name]
		if !ok {
			w = s.add(spec)
			if s.ctx != nil {
				s.startLoop(w)
			}
			continue
		}
		w.mu.Lock()
		changed := specHash(w.spec) != specHash(spec)
		w.spec = spec
		w.mu.Unlock()
		if changed {
			replace = append(replace, w)
		}
	}
	for _, name := range slices.Clone(s.order) {
		if !keep[name] {
			stop = append(stop, s.workers[name])
			delete(s.workers, name)
			s.order = slices.DeleteFunc(s.order, func(n string) bool { return n == name })
		}
	}
	s.mu.Unlock()
	for _, w := range stop {
		w.opts().Log.Info("воркер удалён из настроек — остановка и уборка", "worker", w.name)
		w.mu.Lock()
		w.removed = true
		spec := w.spec
		w.mu.Unlock()
		if w.cancel != nil {
			w.cancel()
		}
		s.wg.Add(1)
		go func() {
			defer s.wg.Done()
			if w.cancel != nil {
				<-w.done
			}
			Retire(context.Background(), spec, s.opts.DataDir, s.opts.Log)
		}()
	}
	for _, w := range replace {
		w.opts().Log.Info("настройки воркера изменились — замена", "worker", w.name)
		go func() { _ = w.replace(context.Background(), nil, protocol.PendingRestart, false) }()
	}
	s.opts.OnChange()
}

func (w *Worker) opts() Options { return w.sup.opts }

// life — текущие настройки остановки воркера.
func (w *Worker) life() config.Lifecycle {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.spec.Lifecycle
}

func (s *Supervisor) get(name string) *Worker {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.workers[name]
}

// Has — воркер есть в настройках.
func (s *Supervisor) Has(name string) bool { return s.get(name) != nil }

// Spec — настройки воркера.
func (s *Supervisor) Spec(name string) (config.Worker, bool) {
	w := s.get(name)
	if w == nil {
		return config.Worker{}, false
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.spec, true
}

// Names — имена воркеров по порядку настроек.
func (s *Supervisor) Names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.order)
}

// ByToken — имя воркера по его токену (AGENT_WORKER_TOKEN).
func (s *Supervisor) ByToken(token string) (string, bool) {
	if token == "" {
		return "", false
	}
	s.mu.Lock()
	ws := make([]*Worker, 0, len(s.workers))
	for _, w := range s.workers {
		ws = append(ws, w)
	}
	s.mu.Unlock()
	for _, w := range ws {
		w.mu.Lock()
		ok := w.token == token
		w.mu.Unlock()
		if ok {
			return w.name, true
		}
	}
	return "", false
}

// Token — токен воркера name.
func (s *Supervisor) Token(name string) string {
	if w := s.get(name); w != nil {
		w.mu.Lock()
		defer w.mu.Unlock()
		return w.token
	}
	return ""
}

// Manifest — манифест из последней регистрации (nil — нет).
func (s *Supervisor) Manifest(name string) *protocol.WorkerManifest {
	w := s.get(name)
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.manifest
}

// Ready — воркер запущен и зарегистрирован; иначе — ошибка с причиной.
func (s *Supervisor) Ready(name string) error {
	w := s.get(name)
	if w == nil {
		return ErrUnknown
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case w.proc == nil:
		return ErrUnavailable
	case w.state != protocol.WorkerRunning:
		return ErrNotRegistered
	}
	return nil
}

// Register — регистрация воркера (§13): манифест и версия; воркер работает.
func (s *Supervisor) Register(name string, reg protocol.Register) error {
	w := s.get(name)
	if w == nil {
		return ErrUnknown
	}
	w.mu.Lock()
	if w.proc == nil {
		w.mu.Unlock()
		return ErrUnavailable
	}
	m := reg.Manifest
	w.manifest, w.version = &m, reg.Version
	w.state, w.reason = protocol.WorkerRunning, ""
	w.beat = time.Now()
	select {
	case <-w.registered:
	default:
		close(w.registered)
	}
	w.mu.Unlock()
	w.opts().Log.Info("воркер зарегистрирован", "worker", name, "version", reg.Version)
	w.opts().OnChange()
	w.opts().OnRegistered(name)
	return nil
}

// Heartbeat — пульс воркера (§16).
func (s *Supervisor) Heartbeat(name string, h protocol.Health) error {
	w := s.get(name)
	if w == nil {
		return ErrUnknown
	}
	w.mu.Lock()
	switch {
	case w.proc == nil:
		w.mu.Unlock()
		return ErrUnavailable
	case w.state != protocol.WorkerRunning:
		// Агент перезапущен или воркер не зарегистрирован: воркеру — зарегистрироваться.
		w.mu.Unlock()
		return ErrNotRegistered
	}
	changed := w.health == nil || w.health.OK != h.OK || w.health.Busy != h.Busy || w.health.Message != h.Message
	w.health, w.beat = &h, time.Now()
	w.mu.Unlock()
	if changed {
		w.opts().OnChange()
	}
	return nil
}

// Metrics — metrics из последнего пульса зарегистрированных воркеров.
func (s *Supervisor) Metrics() map[string]json.RawMessage {
	out := map[string]json.RawMessage{}
	for _, name := range s.Names() {
		w := s.get(name)
		if w == nil {
			continue
		}
		w.mu.Lock()
		if w.state == protocol.WorkerRunning && w.health != nil && len(w.health.Metrics) > 0 {
			out[name] = w.health.Metrics
		}
		w.mu.Unlock()
	}
	return out
}

// Info — воркер для hello: версия, сборка, манифест.
func (s *Supervisor) Info() []protocol.HelloWorker {
	var out []protocol.HelloWorker
	for _, name := range s.Names() {
		w := s.get(name)
		if w == nil {
			continue
		}
		w.mu.Lock()
		out = append(out, protocol.HelloWorker{Name: name, Version: w.versionLocked(), Release: w.spec.Release,
			Manifest: w.manifest})
		w.mu.Unlock()
	}
	return out
}

// versionLocked — версия сборки (у воркера со сборкой) или из регистрации.
func (w *Worker) versionLocked() string {
	if w.spec.Release {
		return cmp.Or(releaseVersion(w.spec), w.version)
	}
	return w.version
}

// Status — состояние воркеров (Configs заполняет агент).
func (s *Supervisor) Status() []protocol.WorkerState {
	var out []protocol.WorkerState
	for _, name := range s.Names() {
		w := s.get(name)
		if w == nil {
			continue
		}
		w.mu.Lock()
		st := protocol.WorkerState{Name: name, State: w.state, Version: w.versionLocked(), Release: w.spec.Release,
			Restarts: w.restarts, Health: w.health}
		if w.health != nil {
			h := *w.health
			h.Metrics = nil
			st.Health = &h
		}
		if w.state == protocol.WorkerInvalid {
			st.Message = w.reason
		}
		switch {
		case w.waiting[protocol.PendingUpdate] > 0:
			st.Pending = protocol.PendingUpdate
		case w.waiting[protocol.PendingRestart] > 0:
			st.Pending = protocol.PendingRestart
		}
		w.mu.Unlock()
		out = append(out, st)
	}
	return out
}

// Restart — worker.restart: остановить и запустить заново; итог — регистрация нового
// процесса. Без force замена ждёт, пока воркер занят; отсрочку видит контекст WithDeferred.
func (s *Supervisor) Restart(ctx context.Context, name string, force bool) error {
	w := s.get(name)
	if w == nil {
		return ErrUnknown
	}
	return w.replace(ctx, nil, protocol.PendingRestart, force)
}

// deferredKey — ключ контекста: кому сообщить, что замена отложена.
type deferredKey struct{}

// WithDeferred — контекст замены (Restart, Update), в котором fn узнаёт, что воркер занят и
// замена отложена до окончания его работы (pending — PendingRestart или PendingUpdate).
func WithDeferred(ctx context.Context, fn func(pending string)) context.Context {
	var once sync.Once
	return context.WithValue(ctx, deferredKey{}, func(pending string) { once.Do(func() { fn(pending) }) })
}

func notifyDeferred(ctx context.Context, pending string) {
	if fn, ok := ctx.Value(deferredKey{}).(func(string)); ok {
		fn(pending)
	}
}

// replace — заменить процесс (сначала уходит прежний) и дождаться регистрации нового. Без
// force сначала ждёт, пока воркер занят; kind — какая замена ждёт.
func (w *Worker) replace(ctx context.Context, swap func() error, kind string, force bool) error {
	w.mu.Lock()
	gen := w.gen
	w.mu.Unlock()
	if !force && w.waitIdle(ctx, kind) {
		return nil
	}
	req := request{swap: swap, done: make(chan error, 1)}
	if kind == protocol.PendingRestart && swap == nil && gen > 0 {
		req.gen = gen
	}
	select {
	case w.requests <- req:
	case <-w.done:
		return errors.New("воркер остановлен")
	case <-ctx.Done():
		return ctx.Err()
	}
	select {
	case err := <-req.done:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// waitIdle — плановая замена ждёт, пока воркер занят по пульсу (не дольше busyTimeout). true
// — заменять не нужно: это перезапуск, а воркер за время ожидания уже запущен заново.
func (w *Worker) waitIdle(ctx context.Context, kind string) bool {
	w.mu.Lock()
	gen := w.gen
	w.mu.Unlock()
	restarted := func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return kind == protocol.PendingRestart && w.gen != gen
	}
	log := w.opts().Log.With("worker", w.name, "pending", kind)
	var deadline time.Time
	defer func() {
		if !deadline.IsZero() {
			w.mu.Lock()
			w.waiting[kind]--
			w.mu.Unlock()
			w.opts().OnChange()
		}
	}()
	for {
		w.mu.Lock()
		busy := w.proc != nil && w.health != nil && w.health.Busy
		message := ""
		if w.health != nil {
			message = w.health.Message
		}
		w.mu.Unlock()
		if !busy {
			if !deadline.IsZero() {
				log.Info("воркер освободился — замена")
			}
			return restarted()
		}
		if deadline.IsZero() {
			deadline = time.Now().Add(busyTimeout)
			w.mu.Lock()
			w.waiting[kind]++
			w.mu.Unlock()
			w.opts().OnChange()
			log.Info("воркер занят — замена отложена до окончания работы", "busyTimeout", busyTimeout, "message", message)
			notifyDeferred(ctx, kind)
		} else if time.Now().After(deadline) {
			log.Warn("воркер занят дольше busyTimeout — замена", "busyTimeout", busyTimeout)
			return false
		}
		select {
		case <-ctx.Done():
			return false
		case <-w.done:
			return false
		case <-time.After(busyPoll):
		}
	}
}

func (w *Worker) setState(state, reason string) {
	w.mu.Lock()
	changed := w.state != state || w.reason != reason
	w.state, w.reason = state, reason
	if state != protocol.WorkerRunning && state != protocol.WorkerInvalid {
		w.health = nil
	}
	w.mu.Unlock()
	if changed {
		w.opts().OnChange()
	}
}

// keepOnExit — агент уходит, а воркер работает дальше (SetExit); удалённый из настроек —
// никогда.
func (w *Worker) keepOnExit() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.removed {
		return false
	}
	switch Exit(w.sup.exit.Load()) {
	case ExitRestart, ExitStop:
		return true
	}
	return false
}

// loop — жизнь воркера до отмены ctx: подхват или запуск, ожидание регистрации, работа,
// перезапуск после выхода, замена по запросу.
func (w *Worker) loop(ctx context.Context) {
	defer close(w.done)
	log := w.opts().Log.With("worker", w.name)
	failures := 0
	first := true
	var pending *request
	stale := func(req request) bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return req.gen != 0 && req.gen != w.gen
	}
	defer func() { pending.reply(errors.New("агент останавливается")) }()
	for {
		if ctx.Err() != nil {
			w.setState(protocol.WorkerStopped, "")
			return
		}
		w.mu.Lock()
		spec := w.spec
		w.mu.Unlock()
		var p *process
		var st procState
		var err error
		adopted := false
		if first {
			var blocked error
			p, st, blocked = w.adopt(spec)
			if blocked != nil {
				log.Error("воркер не запущен", "err", blocked)
				w.setState(protocol.WorkerInvalid, blocked.Error())
				select {
				case <-ctx.Done():
				case <-time.After(adoptRetry):
				}
				continue
			}
			first = false
			adopted = p != nil
		}
		if p == nil {
			w.setState(protocol.WorkerStarting, "")
			p, err = w.start(ctx, spec)
		}
		failed := true
		if err != nil {
			pending.reply(err)
			pending = nil
		} else {
			started, procSpec := time.Now(), specHash(spec)
			if adopted {
				started, procSpec = time.UnixMilli(st.StartedAt), st.Spec
			}
			w.mu.Lock()
			w.proc = p
			w.gen++
			w.registered = make(chan struct{})
			registered := w.registered
			if adopted {
				// Подхваченный воркер переподключится и зарегистрируется заново.
				w.state = protocol.WorkerStarting
			}
			token := w.token
			w.mu.Unlock()
			w.saveState(p, token, procSpec, started, p.out.start)
			wctx, stopWatch := context.WithCancel(ctx)
			go w.watch(wctx, p, registered)
			go func(req *request) {
				req.reply(w.awaitRegistration(wctx, p, registered))
			}(pending)
			pending = nil
			if adopted && (st.Spec != specHash(spec) || st.Version != releaseVersion(spec)) {
				log.Info("воркер подхвачен, но его настройки или сборка изменились — замена")
				go func() { _ = w.replace(ctx, nil, protocol.PendingRestart, false) }()
			}
			var req *request
			exited := false
			for req == nil && !exited && ctx.Err() == nil {
				select {
				case <-ctx.Done():
				case r := <-w.requests:
					if stale(r) {
						r.reply(nil)
						continue
					}
					req = &r
				case <-p.exited:
					exited = true
				}
			}
			stopWatch()
			switch {
			case req != nil:
				log.Info("замена воркера: остановка прежнего процесса")
				p.stop()
				removeState(w.sup.stateDir, w.name)
				w.clearProc()
				pending = w.swap(*req)
				failures = 0
				continue
			case !exited:
				if w.keepOnExit() {
					offsets := p.detach()
					w.saveState(p, token, procSpec, started, offsets)
					log.Info("агент уходит — воркер работает дальше", "pid", p.pid)
				} else {
					p.stop()
					removeState(w.sup.stateDir, w.name)
				}
				w.clearProc()
				w.setState(protocol.WorkerStopped, "")
				return
			}
			removeState(w.sup.stateDir, w.name)
			w.clearProc()
			if time.Since(started) >= w.sup.stableRun {
				failures = 0
			}
			failed = p.failed()
			log.Warn("воркер завершился", "reason", p.exitReason())
		}
		if err != nil && ctx.Err() == nil {
			log.Error("воркер не запустился", "err", err)
		}
		if ctx.Err() != nil {
			w.setState(protocol.WorkerStopped, "")
			return
		}
		var wait <-chan time.Time
		if failed {
			failures++
			w.mu.Lock()
			w.restarts++
			w.mu.Unlock()
			w.setState(protocol.WorkerBackoff, "")
			wait = time.After(restartBackoff.Delay(failures - 1))
		} else {
			log.Info("воркер завершился сам (exit 0) — не перезапускается до worker.restart или изменения настроек")
			w.setState(protocol.WorkerStopped, "")
		}
		select {
		case <-ctx.Done():
			w.setState(protocol.WorkerStopped, "")
			return
		case req := <-w.requests:
			pending = w.swap(req)
			failures = 0
		case <-wait:
		}
	}
}

// awaitRegistration — итог запуска: регистрация процесса p; не зарегистрировался за
// registerTimeout или завершился раньше — ошибка.
func (w *Worker) awaitRegistration(ctx context.Context, p *process, registered chan struct{}) error {
	timer := time.NewTimer(registerTimeout)
	defer timer.Stop()
	select {
	case <-registered:
		return nil
	case <-p.exited:
		return fmt.Errorf("завершился до регистрации: %s", p.exitReason())
	case <-timer.C:
		return fmt.Errorf("не зарегистрировался за %s", registerTimeout)
	case <-ctx.Done():
		return ctx.Err()
	}
}

// watch — регистрация и пульс текущего процесса: не зарегистрировался за registerTimeout —
// invalid; пульса нет дольше healthSilence — нездоров, ещё healthRestart — перезапуск.
func (w *Worker) watch(ctx context.Context, p *process, registered chan struct{}) {
	started := time.Now()
	t := time.NewTicker(watchEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		select {
		case <-registered:
		default:
			if time.Since(started) >= registerTimeout {
				w.mu.Lock()
				invalid := w.state == protocol.WorkerStarting
				w.mu.Unlock()
				if invalid {
					p.log.Warn("воркер не зарегистрировался — работает, но команды и настройки ему не передаются",
						"timeout", registerTimeout)
					w.setState(protocol.WorkerInvalid, fmt.Sprintf("не зарегистрировался за %s после запуска", registerTimeout))
				}
			}
			continue
		}
		w.mu.Lock()
		silent := time.Since(w.beat)
		unhealthy := (w.health == nil || w.health.OK) && silent >= healthSilence
		if unhealthy {
			w.health = &protocol.Health{OK: false, Message: fmt.Sprintf("нет пульса %s", silent.Round(time.Second))}
		}
		w.mu.Unlock()
		if unhealthy {
			p.log.Warn("нет пульса воркера", "silent", silent.Round(time.Second))
			w.opts().OnChange()
		}
		if silent >= healthSilence+healthRestart {
			p.log.Error("воркер завис: нет пульса — перезапуск", "silent", silent.Round(time.Second))
			p.kill("воркер завис: нет пульса")
			return
		}
	}
}

// adopt — подхватить процесс воркера, переживший перезапуск агента: файл процесса есть,
// процесс жив и это он (время запуска совпадает). Иначе — nil (обычный запуск).
//
// Не подхвачен и не остановлен (процесс другого пользователя: служба сменила пользователя) —
// ошибка: вторую копию воркера агент не запускает.
func (w *Worker) adopt(spec config.Worker) (*process, procState, error) {
	dir := w.sup.stateDir
	st, ok := readState(dir, w.name)
	if !ok {
		return nil, st, nil
	}
	log := w.opts().Log.With("worker", w.name, "pid", st.PID)
	if !st.alive() {
		removeState(dir, w.name)
		return nil, st, nil
	}
	p, err := adoptProcess(st, spec, w.sup.logDir, w.opts().Log, w.life)
	if err != nil {
		log.Warn("воркер не подхвачен — остановка прежнего процесса и запуск заново", "err", err)
		stopOrphan(st)
		if st.alive() {
			return nil, st, fmt.Errorf("прежний процесс воркера (pid %d) работает, но агент не может ни подхватить, ни остановить его (%v): "+
				"остановите его сами (kill %d) — вторую копию агент не запускает", st.PID, err, st.PID)
		}
		removeState(dir, w.name)
		return nil, st, nil
	}
	w.mu.Lock()
	w.token = st.Token
	w.mu.Unlock()
	log.Info("воркер подхвачен после перезапуска агента — ждём его переподключения")
	return p, st, nil
}

// saveState — файл процесса для следующего запуска агента.
func (w *Worker) saveState(p *process, token, spec string, started time.Time, output [2]int64) {
	st := p.state(token, spec, started, output)
	w.mu.Lock()
	cfg := w.spec
	w.mu.Unlock()
	st.Worker = &cfg
	if err := writeState(w.sup.stateDir, w.name, st); err != nil {
		w.opts().Log.Warn("файл процесса воркера не записан — после перезапуска агента воркер запустится заново",
			"worker", w.name, "err", err)
	}
}

// swap — смена сборки между остановкой и запуском; ошибка — итог запроса сразу, запуск идёт
// с тем, что есть.
func (w *Worker) swap(req request) *request {
	if req.swap != nil {
		if err := req.swap(); err != nil {
			req.reply(err)
			return nil
		}
	}
	return &req
}

func (w *Worker) clearProc() {
	w.mu.Lock()
	w.proc = nil
	w.health = nil
	w.mu.Unlock()
}

// start — новый процесс воркера (с новым токеном).
func (w *Worker) start(ctx context.Context, spec config.Worker) (*process, error) {
	o := w.opts()
	w.mu.Lock()
	w.token = newToken()
	token := w.token
	w.mu.Unlock()
	return startProcess(ctx, spec, w.sup.logDir, env{agentSocket: o.AgentSocket, token: token,
		stateDir: StateDir(o.DataDir, spec.Name), extra: o.Env}, o.Log, w.life)
}

// waitHealthy — воркер name зарегистрирован и пульс ok: true не позже timeout.
func (s *Supervisor) waitHealthy(ctx context.Context, name string, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	last := "не зарегистрировался"
	for {
		if w := s.get(name); w != nil {
			w.mu.Lock()
			switch {
			case w.state != protocol.WorkerRunning:
				last = "не зарегистрировался"
			case w.health == nil:
				last = "нет пульса"
			case !w.health.OK:
				last = "ok: false " + w.health.Message
			default:
				w.mu.Unlock()
				return nil
			}
			w.mu.Unlock()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("новая сборка не заработала за %s: %s", timeout, last)
		case <-time.After(200 * time.Millisecond):
		}
	}
}
