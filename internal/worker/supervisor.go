//go:build unix

// Package worker — воркеры агента (§12, §13): одна копия процесса на
// воркер, запуск с переменными окружения, выводом в файлы и ожиданием
// сокета, перезапуск после выхода по lifecycle.restart с растущей паузой,
// регистрация (GET /health и GET /manifest после каждого запуска, §12),
// проверка GET /health, замена «сначала
// остановить» с ожиданием, пока воркер занят, подхват воркеров, переживших
// перезапуск агента, обновление сборки с сервера с возвратом прежней сборки,
// уборка перед удалением агента.
package worker

import (
	"cmp"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/internal/backoff"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
)

// Сроки; переменные — для тестов.
var (
	// stableRun — проработал столько — счётчик падений подряд сбрасывается.
	stableRun = time.Minute
	// busyPoll — как часто отложенная замена спрашивает GET /health.
	busyPoll = time.Second
)

// Ошибки доступа к воркеру.
var (
	ErrUnknown     = errors.New("воркера нет в настройках агента")
	ErrUnavailable = errors.New("воркер не запущен")
	// ErrInvalid — воркер не зарегистрирован (state: invalid, §12).
	ErrInvalid = errors.New("воркер не зарегистрирован")
)

// Exit — почему агент останавливает воркеры (Run завершается).
type Exit int32

const (
	// ExitShutdown — остановить все воркеры (по умолчанию).
	ExitShutdown Exit = iota
	// ExitRestart — агент перезапускается: воркеры с
	// lifecycle.onAgentRestart: keep работают дальше.
	ExitRestart
	// ExitStop — агент останавливается: воркеры с lifecycle.onAgentStop:
	// keep работают дальше.
	ExitStop
)

// Options — что нужно воркерам от агента.
type Options struct {
	// RunDir — каталог сокетов воркеров.
	RunDir string
	// DataDir — каталог данных агента: файлы процессов (ProcessesDir) и
	// вывода (LogsDir) воркеров.
	DataDir      string
	AgentSocket  string
	AgentVersion string
	// Env — переменные агента для всех воркеров (KEY=VALUE).
	Env []string
	Log *slog.Logger
	// OnStarted — запущенный воркер зарегистрирован (агент передаёт ему настройки).
	OnStarted func(name string)
	// OnAdopted — зарегистрирован подхваченный воркер, переживший перезапуск агента.
	OnAdopted func(name string)
	// OnChange — изменилось то, что видно в status.
	OnChange func()
}

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

// Worker — один воркер: настройки, текущий процесс, состояние для status.
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
	health   *message.Health
	manifest *message.WorkerManifest
	proc     *process
	updating bool
	// removed — воркер удалён из настроек: останавливается всегда.
	removed bool
	// gen — номер запуска процесса (растёт с каждым запуском и подхватом).
	gen int
	// waiting — сколько замен каждого вида ждут, пока воркер занят.
	waiting map[string]int
	// settled — закрыт, когда первая проверка регистрации текущего запуска
	// закончилась (или запуска нет): по нему события ждут манифест.
	settled chan struct{}

	requests chan request
	cancel   context.CancelFunc
	done     chan struct{}
}

// request — заменить процесс: остановить текущий, выполнить swap (смена
// сборки), запустить новый; done — итог запуска.
type request struct {
	swap func() error
	done chan error
	// gen — перезапуск запуска номер gen: процесс с тех пор уже запущен
	// заново — заменять нечего (0 — заменить в любом случае).
	gen int
}

func (r *request) reply(err error) {
	if r != nil && r.done != nil {
		r.done <- err
	}
}

// New — воркеры по настройкам specs (запускаются Run).
func New(specs []config.Worker, opts Options) *Supervisor {
	if opts.OnStarted == nil {
		opts.OnStarted = func(string) {}
	}
	if opts.OnAdopted == nil {
		opts.OnAdopted = opts.OnStarted
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
	w := &Worker{sup: s, name: spec.Name, token: newToken(), spec: spec, state: message.WorkerStopped,
		waiting: map[string]int{}, requests: make(chan request), done: make(chan struct{}), settled: make(chan struct{})}
	close(w.settled)
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

// Run — запустить воркеры (подхватив переживших перезапуск агента) и держать
// их до отмены ctx; возвращается, когда процессы остановлены или оставлены
// работать (SetExit).
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

// stopOrphans — процессы воркеров, которых больше нет в настройках,
// останавливаются (в фоне).
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

// Apply — новые настройки воркеров (перечитывание agent.yaml): новые
// запускаются, удалённые останавливаются сразу, изменённые заменяются
// плановой заменой (ждёт, пока воркер занят); изменение только lifecycle
// и logs применяется без замены.
func (s *Supervisor) Apply(specs []config.Worker) {
	s.mu.Lock()
	var stop []*Worker
	var replace []*Worker
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
		w.opts().Log.Info("воркер удалён из настроек — остановка", "worker", w.name)
		w.mu.Lock()
		w.removed = true
		w.mu.Unlock()
		if w.cancel != nil {
			w.cancel()
		}
	}
	for _, w := range replace {
		w.opts().Log.Info("настройки воркера изменились — замена", "worker", w.name)
		go func() { _ = w.replace(context.Background(), nil, message.PendingRestart, false) }()
	}
	s.opts.OnChange()
}

func (w *Worker) opts() Options { return w.sup.opts }

// life — текущая жизнь воркера.
func (w *Worker) life() config.Lifecycle {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.spec.Lifecycle
}

// logs — текущие настройки файлов вывода.
func (w *Worker) logs() config.Logs {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.spec.Logs
}

// get — воркер по имени.
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
		if w.getToken() == token {
			return w.name, true
		}
	}
	return "", false
}

func (w *Worker) getToken() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.token
}

// Token — токен воркера name.
func (s *Supervisor) Token(name string) string {
	if w := s.get(name); w != nil {
		return w.getToken()
	}
	return ""
}

// Client — HTTP-клиент к сокету запущенного воркера (адрес — http://worker/…).
func (s *Supervisor) Client(name string) (*http.Client, error) {
	w := s.get(name)
	if w == nil {
		return nil, ErrUnknown
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	switch {
	case w.proc == nil:
		return nil, ErrUnavailable
	case w.state == message.WorkerInvalid:
		return nil, fmt.Errorf("%w: %s", ErrInvalid, w.reason)
	case w.state != message.WorkerRunning:
		return nil, ErrUnavailable
	}
	return w.proc.client, nil
}

// Manifest — манифест воркера из последней регистрации (nil — нет).
func (s *Supervisor) Manifest(name string) *message.WorkerManifest {
	w := s.get(name)
	if w == nil {
		return nil
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.manifest
}

// WaitManifest — манифест воркера после окончания первой проверки
// регистрации текущего запуска (§12): событие, присланное воркером сразу
// после запуска, ждёт её итога, но не дольше ctx.
func (s *Supervisor) WaitManifest(ctx context.Context, name string) *message.WorkerManifest {
	w := s.get(name)
	if w == nil {
		return nil
	}
	w.mu.Lock()
	settled := w.settled
	w.mu.Unlock()
	select {
	case <-settled:
	case <-ctx.Done():
	}
	return s.Manifest(name)
}

// Running — зарегистрированные воркеры.
func (s *Supervisor) Running() []string {
	var out []string
	for _, name := range s.Names() {
		if _, err := s.Client(name); err == nil {
			out = append(out, name)
		}
	}
	return out
}

// Status — состояние воркеров для status (configs заполняет агент).
func (s *Supervisor) Status() []message.WorkerStatus {
	s.mu.Lock()
	ws := make([]*Worker, 0, len(s.order))
	for _, name := range s.order {
		ws = append(ws, s.workers[name])
	}
	s.mu.Unlock()
	out := make([]message.WorkerStatus, 0, len(ws))
	for _, w := range ws {
		w.mu.Lock()
		st := message.WorkerStatus{Name: w.name, State: w.state, Release: w.spec.Release, Builtin: w.spec.Builtin,
			Restarts: w.restarts, Health: w.health, Manifest: w.manifest}
		if w.state == message.WorkerInvalid {
			st.Message = w.reason
		}
		switch {
		case w.waiting[message.PendingUpdate] > 0:
			st.Pending = message.PendingUpdate
		case w.waiting[message.PendingRestart] > 0:
			st.Pending = message.PendingRestart
		}
		if w.manifest != nil {
			st.Version = w.manifest.Version
		}
		if w.spec.Release {
			// У воркера со сборкой с сервера — версия сборки; манифест передаётся как есть.
			st.Version = cmp.Or(releaseVersion(w.spec), st.Version)
		}
		w.mu.Unlock()
		out = append(out, st)
	}
	return out
}

// deferredKey — ключ контекста: кому сообщить, что замена отложена.
type deferredKey struct{}

// WithDeferred — контекст замены (Restart, Update), в котором fn узнаёт, что
// воркер занят и замена отложена до окончания его работы (pending —
// message.PendingRestart или PendingUpdate). fn вызывается не больше раза,
// до итога замены.
func WithDeferred(ctx context.Context, fn func(pending string)) context.Context {
	var once sync.Once
	return context.WithValue(ctx, deferredKey{}, func(pending string) { once.Do(func() { fn(pending) }) })
}

// notifyDeferred — сообщить контексту замены, что она отложена.
func notifyDeferred(ctx context.Context, pending string) {
	if fn, ok := ctx.Value(deferredKey{}).(func(string)); ok {
		fn(pending)
	}
}

// Restart — worker.restart: остановить и запустить заново; итог — запуск.
// Без force замена ждёт, пока воркер занят (§13); отсрочку видит контекст
// WithDeferred.
func (s *Supervisor) Restart(ctx context.Context, name string, force bool) error {
	w := s.get(name)
	if w == nil {
		return ErrUnknown
	}
	return w.replace(ctx, nil, message.PendingRestart, force)
}

// replace — заменить процесс (§13: сначала уходит прежний) и дождаться
// итога запуска нового. Без force сначала ждёт, пока воркер занят; kind —
// какая замена ждёт (status.workers[].pending).
func (w *Worker) replace(ctx context.Context, swap func() error, kind string, force bool) error {
	w.mu.Lock()
	gen := w.gen
	w.mu.Unlock()
	if !force && w.waitIdle(ctx, kind) {
		return nil
	}
	req := request{swap: swap, done: make(chan error, 1)}
	if kind == message.PendingRestart && swap == nil && gen > 0 {
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

// waitIdle — плановая замена ждёт, пока воркер отвечает busy: true на
// GET /health (не дольше busy.timeout). true — ждать не нужно и заменять
// тоже: это перезапуск, а воркер за время ожидания уже запущен заново.
func (w *Worker) waitIdle(ctx context.Context, kind string) bool {
	w.mu.Lock()
	gen := w.gen
	w.mu.Unlock()
	restarted := func() bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return kind == message.PendingRestart && w.gen != gen
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
		life := w.life()
		if !life.BusyWait() {
			return restarted()
		}
		client, err := w.sup.Client(w.name)
		if err != nil {
			return restarted()
		}
		h, err := probeHealth(ctx, client, life.Health.Timeout.Std())
		if err != nil || h == nil || !h.Busy {
			if !deadline.IsZero() {
				log.Info("воркер освободился — замена")
			}
			return restarted()
		}
		w.setHealth(h)
		if deadline.IsZero() {
			deadline = time.Now().Add(life.Busy.Timeout.Std())
			w.mu.Lock()
			w.waiting[kind]++
			w.mu.Unlock()
			w.opts().OnChange()
			log.Info("воркер занят — замена отложена до окончания работы", "busyTimeout", life.Busy.Timeout.Std(), "message", h.Message)
			notifyDeferred(ctx, kind)
		} else if time.Now().After(deadline) {
			log.Warn("воркер занят дольше busy.timeout — замена", "busyTimeout", life.Busy.Timeout.Std())
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

func (w *Worker) setState(state string) {
	w.mu.Lock()
	changed := w.state != state
	w.state = state
	w.reason = ""
	if state != message.WorkerRunning && state != message.WorkerInvalid {
		w.health = nil
	}
	w.mu.Unlock()
	if changed {
		w.opts().OnChange()
	}
}

// unsettle — начался запуск: события ждут итога регистрации.
func (w *Worker) unsettle() {
	w.mu.Lock()
	defer w.mu.Unlock()
	select {
	case <-w.settled:
		w.settled = make(chan struct{})
	default:
	}
}

// settle — первая проверка регистрации закончилась (или запуска не будет).
func (w *Worker) settle() {
	w.mu.Lock()
	defer w.mu.Unlock()
	select {
	case <-w.settled:
	default:
		close(w.settled)
	}
}

// keepOnExit — агент уходит, а воркер работает дальше (SetExit,
// lifecycle.onAgentRestart / onAgentStop); встроенный и удалённый из
// настроек — никогда.
func (w *Worker) keepOnExit() bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.spec.Builtin || w.removed {
		return false
	}
	switch Exit(w.sup.exit.Load()) {
	case ExitRestart:
		return w.spec.Lifecycle.OnAgentRestart == config.Keep
	case ExitStop:
		return w.spec.Lifecycle.OnAgentStop == config.Keep
	}
	return false
}

// loop — жизнь воркера до отмены ctx: подхват или запуск, ожидание сокета,
// работа, перезапуск после выхода, замена по запросу.
func (w *Worker) loop(ctx context.Context) {
	defer close(w.done)
	defer w.settle()
	log := w.opts().Log.With("worker", w.name)
	failures := 0
	first := true
	var pending *request
	// stale — перезапуск, а процесс уже запущен заново после запроса.
	stale := func(req request) bool {
		w.mu.Lock()
		defer w.mu.Unlock()
		return req.gen != 0 && req.gen != w.gen
	}
	defer func() { pending.reply(errors.New("агент останавливается")) }()
	for {
		if ctx.Err() != nil {
			w.setState(message.WorkerStopped)
			return
		}
		w.mu.Lock()
		spec := w.spec
		w.mu.Unlock()
		var p *process
		var st procState
		var err error
		adopted := false
		w.unsettle()
		if first {
			first = false
			p, st = w.adopt(ctx, spec)
			adopted = p != nil
		}
		if p == nil {
			w.setState(message.WorkerStarting)
			p, err = w.start(ctx, spec)
		}
		if err != nil {
			w.settle()
			pending.reply(err)
			pending = nil
		}
		failed := true
		if err == nil {
			// procSpec — отпечаток настроек, с которыми процесс запущен.
			started, procSpec := time.Now(), specHash(spec)
			if adopted {
				started, procSpec = time.UnixMilli(st.StartedAt), st.Spec
			}
			w.mu.Lock()
			w.proc = p
			w.gen++
			token := w.token
			w.mu.Unlock()
			w.saveState(p, token, procSpec, started, p.out.start)
			hctx, stopHealth := context.WithCancel(ctx)
			go w.healthLoop(hctx, p)
			// Регистрация (§12): первая проверка — до итога запуска, повтор — в фоне.
			registered := w.register(hctx, p)
			w.settle()
			pending.reply(nil)
			pending = nil
			if registered {
				go w.registered(adopted)
			} else {
				go w.retryRegister(hctx, p, adopted)
			}
			if adopted && (st.Spec != specHash(spec) || st.Version != releaseVersion(spec)) {
				log.Info("воркер подхвачен, но его настройки или сборка изменились — замена")
				go func() { _ = w.replace(ctx, nil, message.PendingRestart, false) }()
			}
			// Работа до отмены, запроса замены или выхода процесса. Запрос
			// перезапуска процесса, который уже запущен заново, выполнен.
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
			stopHealth()
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
				w.setState(message.WorkerStopped)
				return
			}
			removeState(w.sup.stateDir, w.name)
			w.clearProc()
			if time.Since(started) >= w.sup.stableRun {
				failures = 0
			}
			failed = p.failed()
			log.Warn("воркер завершился", "reason", p.exitReason())
		} else if ctx.Err() == nil {
			log.Error("воркер не запустился", "err", err)
		}
		if ctx.Err() != nil {
			w.setState(message.WorkerStopped)
			return
		}
		life := w.life()
		var wait <-chan time.Time
		switch {
		case life.Restart == config.RestartNever || life.Restart == config.RestartFailure && !failed:
			log.Info("воркер не перезапускается (lifecycle.restart: "+life.Restart+") — до worker.restart или изменения настроек", "restart", life.Restart)
			w.setState(message.WorkerStopped)
		default:
			failures++
			w.mu.Lock()
			w.restarts++
			w.mu.Unlock()
			if life.MaxRestarts > 0 && failures > life.MaxRestarts {
				log.Error("воркер падает подряд чаще maxRestarts — остаётся остановленным до worker.restart", "maxRestarts", life.MaxRestarts)
				w.setState(message.WorkerStopped)
			} else {
				policy := backoff.Policy{Min: life.Backoff.Min.Std(), Max: life.Backoff.Max.Std()}
				w.setState(message.WorkerBackoff)
				wait = time.After(policy.Delay(failures - 1))
			}
		}
		select {
		case <-ctx.Done():
			w.setState(message.WorkerStopped)
			return
		case req := <-w.requests:
			pending = w.swap(req)
			failures = 0
		case <-wait:
		}
	}
}

// adopt — подхватить процесс воркера, переживший перезапуск агента (§13):
// файл процесса есть, процесс жив, это он (время запуска совпадает) и
// сокет отвечает. Иначе — nil (обычный запуск); процесс, который нельзя
// подхватить (встроенный воркер, сокет не ответил), останавливается.
func (w *Worker) adopt(ctx context.Context, spec config.Worker) (*process, procState) {
	dir := w.sup.stateDir
	st, ok := readState(dir, w.name)
	if !ok {
		return nil, st
	}
	log := w.opts().Log.With("worker", w.name, "pid", st.PID)
	if !st.alive() {
		removeState(dir, w.name)
		return nil, st
	}
	if spec.Builtin {
		stopOrphan(st)
		removeState(dir, w.name)
		return nil, st
	}
	p, err := adoptProcess(st, spec, w.sup.logDir, w.opts().Log, w.life, w.logs)
	if err != nil {
		log.Warn("воркер не подхвачен — запуск заново", "err", err)
		stopOrphan(st)
		removeState(dir, w.name)
		return nil, st
	}
	if err := p.waitReady(ctx, spec.Lifecycle.StartTimeout.Std()); err != nil {
		log.Warn("воркер не подхвачен: сокет не отвечает — запуск заново", "err", err)
		p.detach()
		stopOrphan(st)
		removeState(dir, w.name)
		return nil, st
	}
	w.mu.Lock()
	w.token = st.Token
	w.mu.Unlock()
	log.Info("воркер подхвачен после перезапуска агента")
	return p, st
}

// saveState — файл процесса для следующего запуска агента.
func (w *Worker) saveState(p *process, token, spec string, started time.Time, output [2]int64) {
	if err := writeState(w.sup.stateDir, w.name, p.state(token, spec, started, output)); err != nil {
		w.opts().Log.Warn("файл процесса воркера не записан — после перезапуска агента воркер запустится заново",
			"worker", w.name, "err", err)
	}
}

// swap — смена сборки между остановкой и запуском; ошибка — итог запроса
// сразу, запуск идёт с тем, что есть.
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
	w.mu.Unlock()
}

// start — процесс и ожидание сокета; не дождались — процесс останавливается.
func (w *Worker) start(ctx context.Context, spec config.Worker) (*process, error) {
	o := w.opts()
	p, err := startProcess(spec, o.RunDir, w.sup.logDir,
		env{agentSocket: o.AgentSocket, agentVersion: o.AgentVersion, token: w.getToken(), extra: o.Env}, o.Log, w.life, w.logs)
	if err != nil {
		return nil, err
	}
	if err := p.waitReady(ctx, spec.Lifecycle.StartTimeout.Std()); err != nil {
		p.stop()
		return nil, err
	}
	return p, nil
}

// healthLoop — GET /health раз в health.interval (первый — сразу; 0 — не
// проверять); health.failures раз подряд без ответа — воркер завис и
// перезапускается (0 — не перезапускается), даже если он был занят.
func (w *Worker) healthLoop(ctx context.Context, p *process) {
	misses := 0
	for {
		life := w.life()
		interval := life.HealthInterval()
		if interval > 0 {
			h, err := probeHealth(ctx, p.client, life.Health.Timeout.Std())
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, errNoResponse) {
				misses++
				p.log.Warn("воркер не ответил на GET /health", "misses", misses)
				if limit := life.HealthFailures(); limit > 0 && misses >= limit {
					p.log.Error("воркер завис: нет ответа на GET /health — перезапуск", "misses", misses)
					p.kill("воркер завис: нет ответа на GET /health")
					return
				}
			} else {
				misses = 0
				w.setHealth(h)
			}
		} else {
			interval = time.Second // проверка выключена: ждать, не включат ли её
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

func (w *Worker) setHealth(h *message.Health) {
	w.mu.Lock()
	changed := (w.health == nil) != (h == nil) || (h != nil && (w.health.OK != h.OK || w.health.Busy != h.Busy))
	w.health = h
	w.mu.Unlock()
	if changed {
		w.opts().OnChange()
	}
}

// register — проверка регистрации (§12): GET /health и GET /manifest
// корректны — state: running, иначе — invalid с причиной. Манифест
// обновляется при каждой проверке. true — зарегистрирован.
func (w *Worker) register(ctx context.Context, p *process) bool {
	m, reason := probeRegistration(ctx, p.client, w.life().ProbeTimeout.Std())
	if ctx.Err() != nil {
		return false
	}
	state := message.WorkerRunning
	if reason != "" {
		state = message.WorkerInvalid
	}
	w.mu.Lock()
	if w.proc != p {
		w.mu.Unlock()
		return false // процесс уже заменён
	}
	before, _ := json.Marshal(w.manifest)
	after, _ := json.Marshal(m)
	changed := w.state != state || w.reason != reason || string(before) != string(after)
	w.manifest, w.state, w.reason = m, state, reason
	w.mu.Unlock()
	if changed {
		w.opts().OnChange()
	}
	if reason != "" {
		p.log.Warn("воркер не зарегистрирован: нужны корректные GET /health и GET /manifest — настройки и запросы ему не передаются", "reason", reason)
	}
	return reason == ""
}

// retryRegister — повтор проверки регистрации с растущей паузой
// (lifecycle.backoff), пока воркер не зарегистрируется или процесс не
// сменится.
func (w *Worker) retryRegister(ctx context.Context, p *process, adopted bool) {
	for attempt := 0; ; attempt++ {
		life := w.life()
		policy := backoff.Policy{Min: life.Backoff.Min.Std(), Max: life.Backoff.Max.Std()}
		select {
		case <-ctx.Done():
			return
		case <-time.After(policy.Delay(attempt)):
		}
		if w.register(ctx, p) {
			p.log.Info("воркер зарегистрирован")
			w.registered(adopted)
			return
		}
		if ctx.Err() != nil {
			return
		}
	}
}

// registered — воркер зарегистрирован: агент передаёт ему настройки.
func (w *Worker) registered(adopted bool) {
	if adopted {
		w.opts().OnAdopted(w.name)
	} else {
		w.opts().OnStarted(w.name)
	}
}

// probeRegistration — GET /health и GET /manifest (§12): манифест (nil — нет
// корректного) и причина, почему воркер не зарегистрирован ("" —
// зарегистрирован).
func probeRegistration(ctx context.Context, client *http.Client, timeout time.Duration) (*message.WorkerManifest, string) {
	var problems []string
	status, raw, err := probe(ctx, client, message.HealthPath, timeout, message.MaxHealthBytes)
	switch {
	case err != nil:
		problems = append(problems, err.Error())
	case status/100 != 2:
		problems = append(problems, fmt.Sprintf("GET /health: HTTP %d", status))
	default:
		var h struct {
			OK *bool `json:"ok"`
		}
		if json.Unmarshal(raw, &h) != nil || h.OK == nil {
			problems = append(problems, "GET /health: нужен JSON с полем ok (true или false)")
		}
	}
	var m *message.WorkerManifest
	status, raw, err = probe(ctx, client, message.WorkerManifestPath, timeout, message.MaxManifestBytes)
	switch {
	case err != nil:
		problems = append(problems, err.Error())
	case status/100 != 2:
		problems = append(problems, fmt.Sprintf("GET /manifest: HTTP %d", status))
	default:
		if m, err = message.ParseWorkerManifest(raw); err != nil {
			problems = append(problems, "GET /manifest: "+err.Error())
		}
	}
	return m, strings.Join(problems, "; ")
}

// probe — GET path у воркера: код ответа и тело (не больше limit); нет
// ответа или тело больше limit — ошибка.
func probe(ctx context.Context, client *http.Client, path string, timeout time.Duration, limit int) (int, []byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://worker"+path, nil)
	resp, err := client.Do(req)
	if err != nil {
		return 0, nil, fmt.Errorf("GET %s: нет ответа за %s: %w", path, timeout, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, int64(limit)+1))
	if err != nil {
		return 0, nil, fmt.Errorf("GET %s: %w", path, err)
	}
	if len(raw) > limit {
		return 0, nil, fmt.Errorf("GET %s: ответ больше %d байт", path, limit)
	}
	return resp.StatusCode, raw, nil
}

// probeHealth — GET /health (§9): nil, nil — не поддерживается (404);
// не-2xx или неверное тело — ok: false; нет ответа — errNoResponse.
func probeHealth(ctx context.Context, client *http.Client, timeout time.Duration) (*message.Health, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://worker"+message.HealthPath, nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errNoResponse, err)
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, message.MaxHealthBytes+1))
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errNoResponse, err)
	}
	if resp.StatusCode == http.StatusNotFound {
		return nil, nil
	}
	if resp.StatusCode/100 != 2 {
		return &message.Health{OK: false, Message: fmt.Sprintf("GET /health: HTTP %d", resp.StatusCode)}, nil
	}
	var h message.Health
	if len(raw) > message.MaxHealthBytes || json.Unmarshal(raw, &h) != nil {
		return &message.Health{OK: false, Message: "GET /health: неверное тело ответа"}, nil
	}
	return &h, nil
}

// waitHealthy — воркер name ответил ok: true на GET /health не позже timeout.
func (s *Supervisor) waitHealthy(ctx context.Context, name string, timeout, probe time.Duration) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	last := "нет ответа"
	for {
		if client, err := s.Client(name); err == nil {
			h, err := probeHealth(ctx, client, probe)
			switch {
			case err != nil:
				last = err.Error()
			case h == nil:
				last = "воркер не отвечает на GET /health (404)"
			case h.OK:
				return nil
			default:
				last = "ok: false " + h.Message
			}
		} else {
			last = err.Error()
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("новая сборка не ответила ok: true на GET /health за %s: %s", timeout, last)
		case <-time.After(time.Second):
		}
	}
}
