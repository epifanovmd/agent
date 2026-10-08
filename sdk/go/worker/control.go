package worker

import (
	"maps"
	"slices"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Context — контекст агента (worker.context): режим (run | cleanup), сведения
// об агенте, связь с сервером, действующая частота метрик, подписки на каналы
// воркера (Channels), уровень отправки лога. Агент шлёт его после worker.ready и при каждом изменении.
type Context = message.WorkerContext

// AutoInterval — интервал Telemetry «авто»: частота подписки на этот канал из
// контекста (Context().Channels), иначе Context().MetricsIntervalMs, пока
// неизвестна — 15 с; меняется вместе с контекстом.
const AutoInterval time.Duration = 0

// defaultInterval — интервал «авто», пока частота неизвестна.
const defaultInterval = 15 * time.Second

// Context — последний контекст агента; до первого worker.context — значения
// по умолчанию (mode run, online false, metricsIntervalMs 0).
func (w *Worker) Context() Context {
	w.mu.Lock()
	defer w.mu.Unlock()
	c := w.wctx
	c.Agent.Labels = maps.Clone(c.Agent.Labels)
	c.Channels = maps.Clone(c.Channels)
	return c
}

// OnContext — обработчик контекста агента: вызывается при каждом
// worker.context (по порядку, из цикла приёма — не блокировать). Можно
// вызывать и до, и после Run; паника — в лог.
func (w *Worker) OnContext(fn func(Context)) {
	if fn == nil {
		panic("worker: OnContext — нужен обработчик")
	}
	w.mu.Lock()
	w.onContext = append(w.onContext, fn)
	w.mu.Unlock()
}

// SetHealth — воркер сам сообщает агенту, в порядке ли он (worker.health):
// ok == false — status.workers[].health = degraded с причиной message, на
// сервере — alert workerDegraded; ok == true — снова в порядке (message —
// пояснение, необязательно). До worker.ready сообщение копится (см. control).
func (w *Worker) SetHealth(ok bool, msg string) error {
	return w.control(message.TypeWorkerHealth, message.WorkerHealth{OK: ok, Message: truncate(msg, 2000)})
}

// Pause — не брать новые задачи очередей queues (без queues — всех своих):
// агент перестаёт их выдавать, выданные доделываются. Пауза воркера
// независима от паузы с сервера (команда worker.pause).
func (w *Worker) Pause(queues ...string) error {
	return w.control(message.TypeWorkerPause, message.WorkerPause{Queues: slices.Clone(queues)})
}

// Resume — снова брать задачи очередей queues (без queues — всех), снятых
// Pause; паузу с сервера не снимает.
func (w *Worker) Resume(queues ...string) error {
	return w.control(message.TypeWorkerResume, message.WorkerResume{Queues: slices.Clone(queues)})
}

// RequestRestart — попросить агента заменить воркер штатно (как команда
// worker.restart: его способом, без статуса сбоя и без alert); reason — в
// журнал агента.
func (w *Worker) RequestRestart(reason string) error {
	return w.control(message.TypeWorkerRestart, message.WorkerRestartRequest{Reason: truncate(reason, 2000)})
}

// setContext — worker.context от агента: запомнить, разбудить опрос
// телеметрии с интервалом «авто», вызвать обработчики.
func (w *Worker) setContext(c Context) {
	if c.Mode == "" {
		c.Mode = message.WorkerModeRun
	}
	w.mu.Lock()
	w.wctx = c
	close(w.ctxChanged)
	w.ctxChanged = make(chan struct{})
	handlers := slices.Clone(w.onContext)
	w.mu.Unlock()
	for _, fn := range handlers {
		_, _ = w.safeCall("context", func() (any, error) {
			c := c
			c.Agent.Labels = maps.Clone(c.Agent.Labels)
			c.Channels = maps.Clone(c.Channels)
			fn(c)
			return nil, nil
		})
	}
}

// pollInterval — интервал опроса канала: свой или «авто» (AutoInterval);
// changed закрывается при новом контексте.
func (w *Worker) pollInterval(p poller) (interval time.Duration, changed <-chan struct{}) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if p.interval > 0 {
		return p.interval, nil
	}
	if ms := w.wctx.Channels[p.channel]; ms > 0 {
		return time.Duration(ms) * time.Millisecond, w.ctxChanged
	}
	if ms := w.wctx.MetricsIntervalMs; ms > 0 {
		return time.Duration(ms) * time.Millisecond, w.ctxChanged
	}
	return defaultInterval, w.ctxChanged
}
