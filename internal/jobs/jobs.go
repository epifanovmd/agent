// Package jobs — возможность `jobs`: назначения сервера раздаются
// исполнителям (воркерам или обработчикам на Go) по свободным местам их
// очередей; прогресс и итоги уходят на сервер, итоги — надёжно (outbox).
package jobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Sender — канал к серверу (link.Link).
type Sender interface {
	Stream(typ string, data any)
	Reliable(typ string, data any) error
	Request(ctx context.Context, typ string, data any, out any) error
}

// Runner — исполнитель задач: экземпляр воркера или обработчик на Go.
type Runner interface {
	ID() string
	// Queues — очередь → сколько задач одновременно.
	Queues() map[string]int
	// Accepting — берёт ли новые задачи (false — дорабатывает и уходит).
	Accepting() bool
	Run(assign message.JobAssign) error
	Cancel(ref message.JobRef)
	Stop(ref message.JobRef)
}

// QueuePauser — исполнитель, который может временно не брать задачи
// отдельных очередей (пауза воркера): его места по ним — 0, выданные
// задачи доделываются.
type QueuePauser interface {
	QueuePaused(queue string) bool
}

// queuePaused — очередь исполнителя на паузе.
func queuePaused(r Runner, queue string) bool {
	p, ok := r.(QueuePauser)
	return ok && p.QueuePaused(queue)
}

// Reporter — чем исполнитель сообщает о задаче (реализует Manager).
type Reporter interface {
	Progress(p message.JobProgress)
	Event(e message.JobEvent)
	URLs(ctx context.Context, req message.JobURLsRequest) (message.JobURLs, error)
	Complete(c message.JobComplete)
	Fail(f message.JobFail)
}

type running struct {
	ref       message.JobRef
	queue     string
	runner    string
	startedAt time.Time
}

// Manager — назначения, места очередей, связь исполнителей с сервером.
type Manager struct {
	sender  Sender
	log     *slog.Logger
	changed func()

	// unreported — задачи с итогом, ещё не подтверждённым сервером (outbox).
	unreported func() []message.JobRef

	mu sync.Mutex
	// cancelled — отмены задач, которых у агента ещё нет: назначение может
	// прийти позже отмены (ответы HTTP sync в другом порядке).
	cancelled map[message.JobRef]time.Time
	runners   map[string]Runner
	jobs      map[string]*running
	accepting bool
	idle      chan struct{}
}

// New — менеджер; changed — сообщить агенту, что status изменился.
func New(sender Sender, log *slog.Logger, changed func()) *Manager {
	return &Manager{
		sender:    sender,
		log:       log,
		changed:   changed,
		runners:   map[string]Runner{},
		jobs:      map[string]*running{},
		cancelled: map[message.JobRef]time.Time{},
		accepting: true,
	}
}

// Attach — исполнитель готов брать задачи.
func (m *Manager) Attach(r Runner) {
	m.mu.Lock()
	m.runners[r.ID()] = r
	m.mu.Unlock()
	m.changed()
}

// Detach — исполнитель ушёл; reason непуст — он упал, его задачи проваливаются
// (повтор по политике очереди), иначе задач у него уже нет.
func (m *Manager) Detach(id string, reason string) {
	m.mu.Lock()
	delete(m.runners, id)
	var lost []*running
	for jobID, job := range m.jobs {
		if job.runner == id {
			lost = append(lost, job)
			delete(m.jobs, jobID)
		}
	}
	m.signalIdle()
	m.mu.Unlock()
	for _, job := range lost {
		m.reliable(message.TypeJobFail, message.JobFail{
			JobRef:    job.ref,
			Code:      message.ErrWorkerCrashed,
			Message:   fmt.Sprintf("Исполнитель задачи завершился: %s", reason),
			Retryable: true,
		})
	}
	m.changed()
}

// ─── runtime.Capability ────────────────────────────────────────────────

func (m *Manager) Declare(caps *message.Capabilities) {
	caps.Jobs = &message.JobsCapability{Queues: m.capacity()}
}

func (m *Manager) Handles() []string {
	return []string{message.TypeJobAssign, message.TypeJobCancel, message.TypeJobStop}
}

func (m *Manager) Handle(_ context.Context, env message.Envelope) error {
	switch env.Type {
	case message.TypeJobAssign:
		var a message.JobAssign
		if err := env.Decode(&a); err != nil {
			return err
		}
		m.assign(a)
	case message.TypeJobCancel, message.TypeJobStop:
		var ref message.JobRef
		if err := env.Decode(&ref); err != nil {
			return err
		}
		m.signal(env.Type, ref)
	}
	return nil
}

// SetAccepting — drain/resume.
func (m *Manager) SetAccepting(accepting bool) {
	m.mu.Lock()
	m.accepting = accepting
	m.mu.Unlock()
}

// SetUnreported — источник задач с недоставленным итогом (outbox агента).
func (m *Manager) SetUnreported(fn func() []message.JobRef) { m.unreported = fn }

// RunningJobs — для hello: задачи, которые агент держит, — выполняющиеся и
// с недоставленным итогом (иначе сервер сочтёт их потерянными при сверке).
func (m *Manager) RunningJobs() []message.JobRef {
	m.mu.Lock()
	refs := make([]message.JobRef, 0, len(m.jobs))
	for _, job := range m.jobs {
		refs = append(refs, job.ref)
	}
	m.mu.Unlock()
	if m.unreported != nil {
		seen := map[message.JobRef]bool{}
		for _, ref := range refs {
			seen[ref] = true
		}
		for _, ref := range m.unreported() {
			if !seen[ref] {
				seen[ref] = true
				refs = append(refs, ref)
			}
		}
	}
	return refs
}

// ContributeStatus — свободные места очередей и выполняющиеся задачи. Очередь
// обслуживаемая, но без мест, — с нулём: сервер видит, что её знают.
func (m *Manager) ContributeStatus(st *message.Status) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if st.Capacity == nil {
		st.Capacity = map[string]int{}
	}
	for _, r := range m.runners {
		for q, limit := range r.Queues() {
			st.Capacity[q] += limit
			free := 0
			if m.accepting && r.Accepting() && !queuePaused(r, q) {
				free = max(limit-m.countLocked(r.ID(), q), 0)
			}
			st.Slots[q] += free
		}
	}
	for _, job := range m.jobs {
		st.Jobs = append(st.Jobs, message.StatusJob{
			JobID: job.ref.JobID, Attempt: job.ref.Attempt, Queue: job.queue,
			StartedAt: job.startedAt.UnixMilli(),
		})
	}
	sort.Slice(st.Jobs, func(i, j int) bool { return st.Jobs[i].StartedAt < st.Jobs[j].StartedAt })
}

// Stop — дождаться доработки задач (до срока ctx).
func (m *Manager) Stop(ctx context.Context) {
	m.SetAccepting(false)
	for {
		m.mu.Lock()
		if len(m.jobs) == 0 {
			m.mu.Unlock()
			return
		}
		if m.idle == nil {
			m.idle = make(chan struct{})
		}
		idle := m.idle
		m.mu.Unlock()
		select {
		case <-ctx.Done():
			m.log.Warn("задачи: остановка без доработки", "running", len(m.RunningJobs()))
			return
		case <-idle:
		}
	}
}

// ─── Reporter ──────────────────────────────────────────────────────────

func (m *Manager) Progress(p message.JobProgress) {
	if m.holds(p.JobRef) {
		m.sender.Stream(message.TypeJobProgress, p)
	}
}

func (m *Manager) Event(e message.JobEvent) {
	if m.holds(e.JobRef) {
		m.reliable(message.TypeJobEvent, e)
	}
}

// ErrNotHeld — задача уже не за агентом (отменена или чужая попытка).
var ErrNotHeld = errors.New("jobs: задача отменена или уже не за этим агентом")

func (m *Manager) URLs(ctx context.Context, req message.JobURLsRequest) (message.JobURLs, error) {
	if !m.holds(req.JobRef) {
		return message.JobURLs{}, ErrNotHeld
	}
	var urls message.JobURLs
	err := m.sender.Request(ctx, message.TypeJobURLs, req, &urls)
	return urls, err
}

func (m *Manager) Complete(c message.JobComplete) {
	if m.release(c.JobRef) {
		m.reliable(message.TypeJobComplete, c)
	}
}

func (m *Manager) Fail(f message.JobFail) {
	if m.release(f.JobRef) {
		m.reliable(message.TypeJobFail, f)
	}
}

// ─── внутреннее ────────────────────────────────────────────────────────

func (m *Manager) assign(a message.JobAssign) {
	m.mu.Lock()
	if _, ok := m.cancelled[a.Ref()]; ok {
		delete(m.cancelled, a.Ref())
		m.mu.Unlock()
		m.log.Info("задачи: назначение уже отменённой задачи пропущено", "jobId", a.JobID)
		return
	}
	if job, ok := m.jobs[a.JobID]; ok && job.ref == a.Ref() {
		// Повторная доставка (переподключение) — задача уже выполняется.
		m.mu.Unlock()
		m.sender.Stream(message.TypeJobAccept, a.Ref())
		return
	}
	runner, code := m.pickLocked(a.Queue)
	if runner == nil {
		m.mu.Unlock()
		m.log.Warn("задачи: отказ от назначения", "jobId", a.JobID, "queue", a.Queue, "code", code)
		m.reliable(message.TypeJobReject, message.JobReject{JobRef: a.Ref(), Code: code, Message: "Агент не может взять задачу очереди " + a.Queue})
		return
	}
	m.jobs[a.JobID] = &running{ref: a.Ref(), queue: a.Queue, runner: runner.ID(), startedAt: time.Now()}
	m.mu.Unlock()

	m.sender.Stream(message.TypeJobAccept, a.Ref())
	m.changed()
	if err := runner.Run(a); err != nil {
		m.Fail(message.JobFail{JobRef: a.Ref(), Code: message.ErrWorker, Message: "Задача не передана исполнителю: " + err.Error(), Retryable: true})
	}
}

// pickLocked — исполнитель очереди с наименьшей загрузкой.
func (m *Manager) pickLocked(queue string) (Runner, string) {
	if !m.accepting {
		return nil, message.ErrQueueBusy
	}
	served := false
	var best Runner
	bestFree := 0
	for _, r := range m.runners {
		limit, ok := r.Queues()[queue]
		if !ok {
			continue
		}
		served = true
		if !r.Accepting() || queuePaused(r, queue) {
			continue
		}
		free := limit - m.countLocked(r.ID(), queue)
		if free > bestFree {
			best, bestFree = r, free
		}
	}
	if best != nil {
		return best, ""
	}
	if served {
		return nil, message.ErrQueueBusy
	}
	return nil, message.ErrQueueNotServed
}

func (m *Manager) countLocked(runner, queue string) int {
	n := 0
	for _, job := range m.jobs {
		if job.runner == runner && job.queue == queue {
			n++
		}
	}
	return n
}

func (m *Manager) signal(typ string, ref message.JobRef) {
	m.mu.Lock()
	job, ok := m.jobs[ref.JobID]
	if !ok || job.ref != ref {
		if typ == message.TypeJobCancel {
			m.rememberCancelLocked(ref)
		}
		m.mu.Unlock()
		return
	}
	runner := m.runners[job.runner]
	if typ == message.TypeJobCancel {
		// Итог отменённой задачи серверу не нужен.
		delete(m.jobs, ref.JobID)
		m.signalIdle()
	}
	m.mu.Unlock()
	if runner == nil {
		return
	}
	if typ == message.TypeJobCancel {
		runner.Cancel(ref)
		m.changed()
	} else {
		runner.Stop(ref)
	}
}

// cancelledTTL — сколько помнить отмену задачи, которой ещё нет.
const cancelledTTL = 10 * time.Minute

func (m *Manager) rememberCancelLocked(ref message.JobRef) {
	now := time.Now()
	for r, at := range m.cancelled {
		if now.Sub(at) > cancelledTTL {
			delete(m.cancelled, r)
		}
	}
	m.cancelled[ref] = now
}

func (m *Manager) holds(ref message.JobRef) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	job, ok := m.jobs[ref.JobID]
	return ok && job.ref == ref
}

func (m *Manager) release(ref message.JobRef) bool {
	m.mu.Lock()
	job, ok := m.jobs[ref.JobID]
	if ok && job.ref == ref {
		delete(m.jobs, ref.JobID)
		m.signalIdle()
	}
	m.mu.Unlock()
	if ok {
		m.changed()
	}
	return ok && job.ref == ref
}

func (m *Manager) signalIdle() {
	if len(m.jobs) == 0 && m.idle != nil {
		close(m.idle)
		m.idle = nil
	}
}

func (m *Manager) reliable(typ string, data any) {
	if err := m.sender.Reliable(typ, data); err != nil {
		m.log.Error("задачи: итог не записан в outbox", "type", typ, "err", err)
	}
}

// capacity — очереди и суммарная параллельность исполнителей.
func (m *Manager) capacity() []message.QueueCapacity {
	m.mu.Lock()
	defer m.mu.Unlock()
	total := map[string]int{}
	for _, r := range m.runners {
		for q, n := range r.Queues() {
			total[q] += n
		}
	}
	out := make([]message.QueueCapacity, 0, len(total))
	for q, n := range total {
		out = append(out, message.QueueCapacity{Name: q, Concurrency: n})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}
