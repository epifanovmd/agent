//go:build unix

// Package worker — воркеры агента: дочерние процессы (Python-воркеры и
// др.), связанные с агентом локальным IPC (§10 спецификации). Супервизор держит
// заданное число экземпляров, перезапускает упавшие с backoff, заменяет
// экземпляры без простоя (новый рядом, старый дорабатывает задачи).
package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"reflect"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/epifanovmd/agent/internal/backoff"
	"github.com/epifanovmd/agent/internal/cgroup"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/jobs"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// healthyAfter — проработал дольше — счётчик неудач сбрасывается.
const healthyAfter = time.Minute

var restartPolicy = backoff.Policy{Min: time.Second, Max: 30 * time.Second}

// slot — место одного экземпляра: текущий процесс и история неудач.
type slot struct {
	current  *instance
	failures int
	state    string // starting | running | backoff | stopped
	// removed, gone — место убрано (меньше replicas или воркер удалён из
	// настроек): цикл слота завершается, экземпляр дорабатывает и уходит.
	removed bool
	gone    chan struct{}
	// err — почему последний запуск не удался (нет сборки и т. п.).
	err string
	// wake — прервать паузу перед перезапуском (после отката сборки).
	wake chan struct{}
}

func newSlot() *slot {
	return &slot{state: "starting", gone: make(chan struct{}), wake: make(chan struct{}, 1)}
}

type worker struct {
	spec       config.Worker
	slots      []*slot
	generation int
	// build — поколение сборки воркера из выпуска: растёт при каждой замене
	// файла current (обновление, откат).
	build int
	// updating — идёт worker.update.
	updating bool
	// restarting — идёт замена по просьбе воркера (worker.restart по IPC).
	restarting bool
	// pause — пауза очередей от сервера (команды worker.pause/worker.resume).
	pause serverPause
}

// Supervisor — воркеры агента.
type Supervisor struct {
	jobs     *jobs.Manager
	log      *slog.Logger
	changed  func()
	agentVer string
	workers  map[string]*worker

	mu       sync.Mutex
	stopping bool
	wg       sync.WaitGroup
	// ctx — контекст Start (nil до запуска): места, добавленные Apply, живут в нём.
	ctx context.Context
	// leaving — экземпляры убранных мест, ещё дорабатывающие задачи.
	leaving map[*instance]bool
	// env — переменные агента для всех воркеров (KEY=VALUE).
	env []string

	bridge Bridge
	// owners — имя (команда, домен, канал) → воркер, которой оно принадлежит.
	owners map[owned]string
	// bridged — имена, уже подключённые к агенту.
	bridged map[owned]bool
	// cleaning — уборка (`agent cleanup`): имена воркера не подключаются,
	// показатели и события некуда отправлять — отбрасываются.
	cleaning bool
	// wctx — контекст агента для воркеров (worker.context).
	wctx message.WorkerContext

	// cgOnce, cg — группа агента в cgroup v2 для ограничений воркеров:
	// готовится при первом запуске воркера с limits; nil — недоступна.
	cgOnce sync.Once
	cg     *cgroup.Manager
}

// New — супервизор воркеров из конфигурации.
func New(specs []config.Worker, manager *jobs.Manager, log *slog.Logger, changed func(), agentVersion string) *Supervisor {
	s := &Supervisor{
		jobs: manager, log: log, changed: changed, agentVer: agentVersion, workers: map[string]*worker{},
		owners: map[owned]string{}, bridged: map[owned]bool{}, leaving: map[*instance]bool{},
		wctx: message.WorkerContext{Mode: message.WorkerModeRun, Agent: message.WorkerContextAgent{Version: agentVersion}},
	}
	for _, spec := range specs {
		w := &worker{spec: spec}
		s.growLocked(w, spec.Replicas)
		s.workers[spec.Name] = w
	}
	return s
}

// growLocked — добавить места до replicas; после Start их циклы запускаются
// сразу. Под s.mu.
func (s *Supervisor) growLocked(w *worker, replicas int) {
	for len(w.slots) < replicas {
		sl := newSlot()
		w.slots = append(w.slots, sl)
		if s.ctx != nil && !s.stopping {
			s.wg.Add(1)
			go s.keep(s.ctx, w, sl)
		}
	}
}

// dropLocked — место убрано: цикл слота завершается; текущий экземпляр (он
// возвращается) должен доработать задачи и уйти. Под s.mu.
func (s *Supervisor) dropLocked(sl *slot) *instance {
	if sl.removed {
		return nil
	}
	sl.removed = true
	close(sl.gone)
	inst := sl.current
	if inst != nil {
		s.leaving[inst] = true
	}
	return inst
}

// SetEnv — переменные окружения для всех воркеров (до Start); настройки
// воркера (env) важнее.
func (s *Supervisor) SetEnv(env map[string]string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.env = nil
	for k, v := range env {
		s.env = append(s.env, k+"="+v)
	}
	sort.Strings(s.env)
}

// ─── runtime: Capability, Starter, Stopper, StatusContributor ─────────

func (s *Supervisor) Declare(*message.Capabilities) {}
func (s *Supervisor) Handles() []string             { return nil }
func (s *Supervisor) Handle(context.Context, message.Envelope) error {
	return nil
}

// Start — запустить все экземпляры и держать их до отмены ctx.
func (s *Supervisor) Start(ctx context.Context) error {
	s.mu.Lock()
	s.ctx = ctx
	for _, w := range s.workers {
		for _, sl := range w.slots {
			s.wg.Add(1)
			go s.keep(ctx, w, sl)
		}
	}
	s.mu.Unlock()
	<-ctx.Done()
	return nil
}

// Stop — SIGTERM всем экземплярам, доработка задач до срока ctx, затем SIGKILL.
func (s *Supervisor) Stop(ctx context.Context) {
	s.mu.Lock()
	s.stopping = true
	var all []*instance
	for _, w := range s.workers {
		for _, sl := range w.slots {
			if sl.current != nil {
				all = append(all, sl.current)
			}
		}
	}
	for inst := range s.leaving {
		all = append(all, inst)
	}
	s.mu.Unlock()
	var wg sync.WaitGroup
	for _, inst := range all {
		wg.Add(1)
		go func() {
			defer wg.Done()
			stopCtx, cancel := context.WithTimeout(ctx, inst.spec.StopTimeout.Std())
			defer cancel()
			inst.terminate(stopCtx)
		}()
	}
	wg.Wait()
	s.wg.Wait()
}

// ContributeStatus — состояние воркеров; упавший (backoff) или сообщивший о
// неполадке (worker.health) — агент degraded.
func (s *Supervisor) ContributeStatus(st *message.Status) {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.workers))
	for name := range s.workers {
		names = append(names, name)
	}
	sort.Strings(names)
	var degraded, unhealthy []string
	for _, name := range names {
		w := s.workers[name]
		item := message.StatusWorker{Name: name, State: "running", Release: w.spec.Release}
		var insts []*instance
		for _, sl := range w.slots {
			if sl.current != nil {
				insts = append(insts, sl.current)
			}
			if sl.state == "running" {
				item.Instances++
				if sl.current != nil {
					sl.current.mu.Lock()
					item.Version = sl.current.version
					sl.current.mu.Unlock()
				}
			}
		}
		switch {
		case item.Instances == len(w.slots):
		case anyState(w.slots, "backoff"):
			item.State = "backoff"
			reason := name
			for _, sl := range w.slots {
				if sl.state == "backoff" && sl.err != "" {
					reason = sl.err
					break
				}
			}
			degraded = append(degraded, reason)
		case anyState(w.slots, "starting"):
			item.State = "starting"
		default:
			item.State = "stopped"
		}
		item.Health, item.Message = health(insts)
		if item.Health == message.WorkerHealthDegraded {
			reason := "воркер " + name
			if item.Message != "" {
				reason += ": " + item.Message
			}
			unhealthy = append(unhealthy, reason)
		}
		item.Paused = w.paused(w.spec.Queues, insts)
		st.Workers = append(st.Workers, item)
	}
	if (len(degraded) > 0 || len(unhealthy) > 0) && st.State == "" {
		st.State = message.StateDegraded
		var parts []string
		if len(degraded) > 0 {
			parts = append(parts, "воркеры перезапускаются: "+strings.Join(degraded, "; "))
		}
		st.Message = strings.Join(append(parts, unhealthy...), "; ")
	}
}

func anyState(slots []*slot, state string) bool {
	for _, sl := range slots {
		if sl.state == state {
			return true
		}
	}
	return false
}

// ─── управление ────────────────────────────────────────────────────────

// Names — имена воркеров.
func (s *Supervisor) Names() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	names := make([]string, 0, len(s.workers))
	for name := range s.workers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// ErrUnknownWorker — воркера с таким именем нет.
var ErrUnknownWorker = errors.New("worker: нет такого воркера")

// Restart — заменить экземпляры воркера без простоя: новый поднимается
// рядом, после регистрации старый перестаёт брать задачи и завершается
// после текущих.
func (s *Supervisor) Restart(ctx context.Context, name string) error {
	s.mu.Lock()
	w, ok := s.workers[name]
	stopping := s.stopping
	s.mu.Unlock()
	if !ok {
		return ErrUnknownWorker
	}
	if stopping {
		return errors.New("worker: агент останавливается")
	}
	s.mu.Lock()
	slots := append([]*slot(nil), w.slots...)
	s.mu.Unlock()
	return s.replace(ctx, w, slots)
}

// replace — заменить экземпляры мест slots по стратегии воркера (rolling или
// stop-first); новые запускаются по текущей w.spec.
func (s *Supervisor) replace(ctx context.Context, w *worker, slots []*slot) error {
	name := w.spec.Name
	s.mu.Lock()
	stopFirst := w.spec.Restart == config.RestartStopFirst
	s.mu.Unlock()
	if stopFirst {
		return s.restartStopFirst(ctx, w, slots)
	}
	for idx, sl := range slots {
		next, err := s.spawn(w)
		if err != nil {
			return err
		}
		select {
		case <-next.ready:
		case <-next.exited:
			return fmt.Errorf("worker %s: новый экземпляр завершился до регистрации: %s", name, next.exitReason())
		case <-ctx.Done():
			next.terminate(context.Background())
			return ctx.Err()
		}
		s.mu.Lock()
		if sl.removed {
			// Место убрали, пока новый экземпляр запускался.
			s.mu.Unlock()
			next.terminate(context.Background())
			continue
		}
		old := sl.current
		sl.current = next
		sl.state = "running"
		s.mu.Unlock()
		s.jobs.Attach(next)
		if old != nil {
			old.retire()
		}
		s.activate(w)
		s.log.Info("воркер заменён", "worker", name, "slot", idx, "instance", next.id)
		s.changed()
	}
	return nil
}

// restartStopFirst — замена по одному слоту: старый экземпляр перестаёт брать
// задачи, дорабатывает и завершается (не успел за stopTimeout — SIGTERM, затем
// SIGKILL), цикл слота сразу поднимает новый; ждём его регистрации.
func (s *Supervisor) restartStopFirst(ctx context.Context, w *worker, slots []*slot) error {
	for idx, sl := range slots {
		s.mu.Lock()
		old := sl.current
		removed := sl.removed
		s.mu.Unlock()
		if old == nil || removed {
			continue
		}
		s.retireSlotInstance(old)
		for {
			s.mu.Lock()
			next := sl.current
			removed := sl.removed
			s.mu.Unlock()
			if removed {
				break
			}
			if next != nil && next != old && next.registered() {
				s.log.Info("воркер заменён (stop-first)", "worker", w.spec.Name, "slot", idx, "instance", next.id)
				break
			}
			if next != nil && next != old && !next.registered() && next.hasExited() {
				// Цикл слота перезапустит его сам; замена не удалась.
				return fmt.Errorf("worker %s: новый экземпляр завершился до регистрации: %s", w.spec.Name, next.exitReason())
			}
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	return nil
}

func (s *Supervisor) spawn(w *worker) (*instance, error) {
	s.mu.Lock()
	w.generation++
	id := fmt.Sprintf("%s#%d", w.spec.Name, w.generation)
	s.mu.Unlock()
	inst, err := startInstance(s, w, id)
	if err == nil {
		// Наблюдатель экземпляра: при выходе — отцепить от задач, даже если за
		// ним уже не следит цикл слота (несколько замен подряд).
		go func() {
			<-inst.exited
			reason := inst.exitReason()
			if inst.retired.Load() && inst.runningJobs() == 0 {
				reason = ""
			}
			s.jobs.Detach(inst.id, reason)
			s.dropTelemetry(w)
			s.mu.Lock()
			delete(s.leaving, inst)
			s.mu.Unlock()
		}()
	}
	return inst, err
}

// keep — держать слот занятым: запуск, ожидание выхода, перезапуск с backoff.
func (s *Supervisor) keep(ctx context.Context, w *worker, sl *slot) {
	defer s.wg.Done()
	for {
		s.mu.Lock()
		inst := sl.current
		stopping := s.stopping || sl.removed
		s.mu.Unlock()
		if stopping || ctx.Err() != nil {
			return
		}

		if inst == nil {
			s.setState(sl, "starting")
			next, err := s.spawn(w)
			if err != nil {
				s.log.Error("воркер не запустился", "worker", w.spec.Name, "err", err)
				s.mu.Lock()
				sl.err = err.Error()
				s.mu.Unlock()
				if !s.pause(ctx, sl) {
					return
				}
				continue
			}
			s.mu.Lock()
			if sl.removed {
				s.mu.Unlock()
				next.terminate(context.Background())
				return
			}
			if sl.current != nil {
				// Место заняла замена (worker.update), пока экземпляр запускался.
				s.mu.Unlock()
				next.retired.Store(true)
				next.terminate(context.Background())
				continue
			}
			sl.current = next
			sl.err = ""
			s.mu.Unlock()
			inst = next
			go func() {
				select {
				case <-inst.ready:
					s.jobs.Attach(inst)
					s.setState(sl, "running")
					s.activate(w)
				case <-inst.exited:
				}
			}()
		}

		select {
		case <-inst.exited:
		case <-sl.gone:
			return // экземпляр дорабатывает сам (leaving)
		}
		reason := inst.exitReason()
		retired := inst.retired.Load()

		s.mu.Lock()
		if sl.current == inst {
			sl.current = nil
		}
		replaced := sl.current != nil
		stopping = s.stopping
		s.mu.Unlock()
		if retired && replaced {
			s.log.Info("старый экземпляр воркера завершился", "instance", inst.id)
			continue
		}
		if retired && !stopping && ctx.Err() == nil {
			// stop-first: старый ушёл — новый сразу (не сбой: без паузы и degraded).
			s.log.Info("экземпляр заменяется: старый завершился", "instance", inst.id)
			continue
		}
		if stopping || ctx.Err() != nil {
			s.setState(sl, "stopped")
			return
		}
		s.log.Warn("воркер завершился — перезапуск", "instance", inst.id, "reason", reason)
		if time.Since(inst.started) > healthyAfter {
			s.mu.Lock()
			sl.failures = 0
			s.mu.Unlock()
		}
		if !s.pause(ctx, sl) {
			return
		}
	}
}

func (s *Supervisor) pause(ctx context.Context, sl *slot) bool {
	s.setState(sl, "backoff")
	s.mu.Lock()
	delay := restartPolicy.Delay(sl.failures)
	sl.failures++
	s.mu.Unlock()
	select {
	case <-ctx.Done():
		return false
	case <-sl.gone:
		return false
	case <-sl.wake:
		return true
	case <-time.After(delay):
		return true
	}
}

func (s *Supervisor) setState(sl *slot, state string) {
	s.mu.Lock()
	sl.state = state
	s.mu.Unlock()
	s.changed()
}

// dropTelemetry — экземпляр воркера завершился: данные его каналов сбрасываются.
func (s *Supervisor) dropTelemetry(w *worker) {
	s.mu.Lock()
	sink := s.bridge.Telemetry
	var channels []string
	for key, owner := range s.owners {
		if owner == w.spec.Name && key.kind == kindChannel {
			channels = append(channels, key.name)
		}
	}
	s.mu.Unlock()
	for _, ch := range channels {
		sink.Drop(ch)
	}
}

// retireSlotInstance — экземпляр перестаёт брать задачи и завершается после
// текущих; не успел за stopTimeout — SIGTERM, затем SIGKILL.
func (s *Supervisor) retireSlotInstance(inst *instance) {
	inst.retire()
	go func() {
		select {
		case <-inst.exited:
		case <-time.After(inst.spec.StopTimeout.Std()):
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			inst.terminate(ctx)
		}
	}()
}

// ─── перечитывание настроек ────────────────────────────────────────────

// Apply — новый набор воркеров (перечитанные настройки), без остановки
// остальных: новые запускаются; отсутствующие перестают брать задачи,
// дорабатывают текущие и уходят (их команды, разделы состояния и каналы
// снимаются); изменённые заменяются по своей стратегии restart (rolling или
// stop-first) в фоне; изменённое replicas добавляет или убирает места на лету.
// Возвращает имена удалённых воркеров (возможности агента сузились — серверу
// нужен новый hello).
func (s *Supervisor) Apply(specs []config.Worker) (removed []string) {
	s.mu.Lock()
	if s.stopping {
		s.mu.Unlock()
		return nil
	}
	next := map[string]config.Worker{}
	for _, spec := range specs {
		next[spec.Name] = spec
	}
	var leaving []*instance
	for name, w := range s.workers {
		if _, ok := next[name]; ok {
			continue
		}
		delete(s.workers, name)
		for _, sl := range w.slots {
			if inst := s.dropLocked(sl); inst != nil {
				leaving = append(leaving, inst)
			}
		}
		removed = append(removed, name)
	}
	type job struct {
		w     *worker
		slots []*slot
	}
	var replace []job
	for _, spec := range specs {
		w, ok := s.workers[spec.Name]
		if !ok {
			w = &worker{spec: spec}
			s.workers[spec.Name] = w
			s.growLocked(w, spec.Replicas)
			s.log.Info("воркер добавлен", "worker", spec.Name, "replicas", spec.Replicas)
			continue
		}
		changed := !sameSpec(w.spec, spec)
		if !changed && w.spec.Replicas == spec.Replicas {
			continue
		}
		w.spec = spec
		if len(w.slots) > spec.Replicas {
			for _, sl := range w.slots[spec.Replicas:] {
				if inst := s.dropLocked(sl); inst != nil {
					leaving = append(leaving, inst)
				}
			}
			w.slots = w.slots[:spec.Replicas]
		}
		if changed && s.ctx != nil {
			replace = append(replace, job{w: w, slots: append([]*slot(nil), w.slots...)})
		}
		s.growLocked(w, spec.Replicas)
		s.log.Info("воркер изменён", "worker", spec.Name, "replicas", spec.Replicas, "replace", changed)
	}
	ctx := s.ctx
	s.mu.Unlock()

	for _, inst := range leaving {
		s.retireSlotInstance(inst)
	}
	sort.Strings(removed)
	for _, name := range removed {
		s.log.Info("воркер удалён из настроек — дорабатывает задачи и уходит", "worker", name)
		s.release(name)
	}
	for _, r := range replace {
		go func() {
			if err := s.replace(ctx, r.w, r.slots); err != nil && ctx.Err() == nil {
				s.log.Error("воркер не заменён", "worker", r.w.spec.Name, "err", err)
			}
		}()
	}
	s.changed()
	return removed
}

// sameSpec — настройки воркера совпадают без учёта replicas.
func sameSpec(a, b config.Worker) bool {
	a.Replicas, b.Replicas = 0, 0
	return reflect.DeepEqual(a, b)
}

// release — снять имена удалённого воркера: команды, разделы состояния, каналы.
func (s *Supervisor) release(name string) {
	s.mu.Lock()
	var keys []owned
	for key, owner := range s.owners {
		if owner != name {
			continue
		}
		delete(s.owners, key)
		if s.bridged[key] {
			keys = append(keys, key)
		}
		delete(s.bridged, key)
	}
	b := s.bridge
	s.mu.Unlock()
	for _, key := range keys {
		switch key.kind {
		case kindCommand:
			b.Commands.Unregister(key.name)
		case kindDomain:
			b.State.Unregister(key.name)
		case kindChannel:
			b.Telemetry.Undeclare(key.name)
		}
	}
}
