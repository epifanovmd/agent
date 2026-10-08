package jobs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Error — ошибка задачи с кодом; Retryable=false — без повторов.
type Error struct {
	Code      string
	Message   string
	Retryable bool
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Job — задача для обработчика на Go.
type Job struct {
	message.JobAssign
	reporter Reporter
	stop     chan struct{}
	eventSeq int64
	mu       sync.Mutex
}

// Progress — доля 0..1 и текст.
func (j *Job) Progress(value float64, text string) {
	j.reporter.Progress(message.JobProgress{JobRef: j.Ref(), Progress: &value, Text: &text})
}

// Log — строки лога задачи.
func (j *Job) Log(lines ...string) {
	j.reporter.Progress(message.JobProgress{JobRef: j.Ref(), Log: lines})
}

// Event — доменное событие (надёжно, по порядку).
func (j *Job) Event(typ string, data any) {
	raw, _ := json.Marshal(data)
	j.mu.Lock()
	j.eventSeq++
	seq := j.eventSeq
	j.mu.Unlock()
	j.reporter.Event(message.JobEvent{JobRef: j.Ref(), Seq: seq, Type: typ, Data: raw})
}

// StopRequested — штатная остановка: довести шаг и вернуть результат.
func (j *Job) StopRequested() <-chan struct{} { return j.stop }

// URLs — свежие подписанные ссылки на файлы задачи.
func (j *Job) URLs(ctx context.Context, inputs, outputs []string) (message.JobURLs, error) {
	return j.reporter.URLs(ctx, message.JobURLsRequest{JobRef: j.Ref(), Inputs: inputs, Outputs: outputs})
}

// HandlerFunc — обработчик задачи на Go; ctx отменяется при job.cancel.
type HandlerFunc func(ctx context.Context, job *Job) (any, error)

type funcQueue struct {
	fn          HandlerFunc
	concurrency int
}

type active struct {
	cancel context.CancelFunc
	job    *Job
}

// Funcs — исполнитель задач обработчиками на Go в процессе агента.
type Funcs struct {
	id       string
	reporter Reporter
	queues   map[string]funcQueue

	mu     sync.Mutex
	active map[string]*active
}

// NewFuncs — исполнитель с id (для status и логов).
func NewFuncs(id string, reporter Reporter) *Funcs {
	return &Funcs{id: id, reporter: reporter, queues: map[string]funcQueue{}, active: map[string]*active{}}
}

// Handle — обработчик очереди и сколько задач одновременно.
func (f *Funcs) Handle(queue string, concurrency int, fn HandlerFunc) {
	f.queues[queue] = funcQueue{fn: fn, concurrency: concurrency}
}

func (f *Funcs) ID() string      { return f.id }
func (f *Funcs) Accepting() bool { return true }

func (f *Funcs) Queues() map[string]int {
	out := make(map[string]int, len(f.queues))
	for q, h := range f.queues {
		out[q] = h.concurrency
	}
	return out
}

func (f *Funcs) Run(a message.JobAssign) error {
	h, ok := f.queues[a.Queue]
	if !ok {
		return fmt.Errorf("очередь %s не обслуживается", a.Queue)
	}
	ctx, cancel := context.WithCancel(context.Background())
	job := &Job{JobAssign: a, reporter: f.reporter, stop: make(chan struct{})}
	f.mu.Lock()
	f.active[a.JobID] = &active{cancel: cancel, job: job}
	f.mu.Unlock()

	go func() {
		defer func() {
			f.mu.Lock()
			delete(f.active, a.JobID)
			f.mu.Unlock()
			cancel()
		}()
		result, err := safeCall(ctx, h.fn, job)
		if ctx.Err() != nil {
			// Отменена: итог не нужен, только подтверждение, что место свободно.
			f.reporter.Fail(message.JobFail{JobRef: a.Ref(), Code: message.ErrCancelled, Message: "отменена"})
			return
		}
		if err != nil {
			var je *Error
			if errors.As(err, &je) {
				f.reporter.Fail(message.JobFail{JobRef: a.Ref(), Code: je.Code, Message: je.Message, Retryable: je.Retryable})
			} else {
				f.reporter.Fail(message.JobFail{JobRef: a.Ref(), Code: message.ErrWorker, Message: err.Error(), Retryable: true})
			}
			return
		}
		raw, _ := json.Marshal(result)
		f.reporter.Complete(message.JobComplete{JobRef: a.Ref(), Result: raw})
	}()
	return nil
}

func (f *Funcs) Cancel(ref message.JobRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if a, ok := f.active[ref.JobID]; ok && a.job.Ref() == ref {
		a.cancel()
	}
}

func (f *Funcs) Stop(ref message.JobRef) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if a, ok := f.active[ref.JobID]; ok && a.job.Ref() == ref {
		select {
		case <-a.job.stop:
		default:
			close(a.job.stop)
		}
	}
}

// safeCall — паника обработчика становится ошибкой задачи, а не падением агента.
func safeCall(ctx context.Context, fn HandlerFunc, job *Job) (result any, err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("паника обработчика: %v", r)
		}
	}()
	return fn(ctx, job)
}
