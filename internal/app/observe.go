//go:build unix

package app

import (
	"context"
	"sync"
	"time"

	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/protocol"
)

// Частоты до первого hello и задержка внеочередного состояния; переменные — для тестов.
var (
	statusDebounce = 200 * time.Millisecond
	logEvery       = time.Second
)

// observer — частоты метрик и состояния (hello, watch) и сигналы циклам.
type observer struct {
	mu    sync.Mutex
	hello protocol.HelloReply
	watch *protocol.Watch
	timer *time.Timer
	// metricsKick — изменилась частота метрик; statusKick — изменилось состояние.
	metricsKick chan struct{}
	statusKick  chan struct{}
}

func (o *observer) init() {
	o.metricsKick = make(chan struct{}, 1)
	o.statusKick = make(chan struct{}, 1)
}

func kick(ch chan struct{}) {
	select {
	case ch <- struct{}{}:
	default:
	}
}

// metricsInterval — из watch, но не чаще MinWatchInterval и не реже, чем в hello (§7).
func (o *observer) metricsInterval() time.Duration {
	o.mu.Lock()
	defer o.mu.Unlock()
	base := protocol.DefaultMetricsEvery
	if o.hello.MetricsMs > 0 {
		base = time.Duration(o.hello.MetricsMs) * time.Millisecond
	}
	if o.watch != nil && o.watch.MetricsIntervalMs > 0 {
		base = min(base, max(time.Duration(o.watch.MetricsIntervalMs)*time.Millisecond, protocol.MinWatchInterval))
	}
	return base
}

// workerWatch — наблюдение для воркеров (§16): частота метрик, которой следует агент, и уровень
// журнала; наблюдения нет — пустое.
func (o *observer) workerWatch() protocol.Watch {
	every := o.metricsInterval()
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.watch == nil {
		return protocol.Watch{}
	}
	return protocol.Watch{MetricsIntervalMs: every.Milliseconds(), LogLevel: o.watch.LogLevel}
}

func (o *observer) stateInterval() time.Duration {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.hello.StateMs > 0 {
		return time.Duration(o.hello.StateMs) * time.Millisecond
	}
	return protocol.DefaultStateInterval
}

// setWatch — наблюдение из желаемого (§7): nil или наступивший until — снять.
func (a *App) setWatch(w *protocol.Watch) {
	o := &a.obs
	if w != nil && w.Until > 0 && time.UnixMilli(w.Until).Before(time.Now()) {
		w = nil
	}
	if w != nil && w.LogLevel != "" && protocol.LogLevelRank(w.LogLevel) < 0 {
		a.log.Warn("watch: незнакомый уровень журнала — пропущен", "logLevel", w.LogLevel)
		c := *w
		c.LogLevel = ""
		w = &c
	}
	o.mu.Lock()
	o.watch = w
	if o.timer != nil {
		o.timer.Stop()
		o.timer = nil
	}
	if w != nil && w.Until > 0 {
		o.timer = time.AfterFunc(time.Until(time.UnixMilli(w.Until)), func() { a.setWatch(nil) })
	}
	o.mu.Unlock()
	level := ""
	if w != nil {
		level = w.LogLevel
	}
	_ = a.forward.Override(level)
	kick(o.metricsKick)
	for _, name := range a.workers.Names() {
		a.pushWatch(name, false)
	}
}

// statusChanged — изменилось то, что видно в состоянии: внеочередное состояние.
func (a *App) statusChanged() { kick(a.obs.statusKick) }

// state — состояние агента и воркеров (§9).
func (a *App) state() protocol.State {
	applied, err := a.store.Applied()
	if err != nil {
		a.log.Warn("итоги настроек не прочитаны", "err", err)
	}
	ws := a.workers.Status()
	for i := range ws {
		ws[i].Configs = a.configsStatus(ws[i].Name, applied[ws[i].Name])
	}
	if ws == nil {
		ws = []protocol.WorkerState{}
	}
	return protocol.State{DesiredRev: a.currentDesired().Rev, Outbox: a.store.Len(), Update: a.latest.Load(), Workers: ws}
}

// stateLoop — состояние бэкенду раз в stateMs и при изменении.
func (a *App) stateLoop(ctx context.Context) {
	if a.backend == nil {
		return
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-a.obs.statusKick:
			// Несколько изменений подряд — одним состоянием.
			select {
			case <-ctx.Done():
				return
			case <-time.After(statusDebounce):
			}
		}
		a.backend.SetState(a.state())
		timer.Reset(a.obs.stateInterval())
	}
}

// metricsLoop — точка метрик раз в metricsInterval: узел и метрики из пульса воркеров (и без
// связи: последние точки ждут её в памяти).
func (a *App) metricsLoop(ctx context.Context) {
	if a.backend == nil {
		return
	}
	interval := a.obs.metricsInterval()
	timer := time.NewTimer(interval)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.obs.metricsKick:
			if next := a.obs.metricsInterval(); next != interval {
				interval = next
				timer.Reset(interval)
			}
			continue
		case <-timer.C:
		}
		m := protocol.Metrics{At: time.Now().UnixMilli(), Host: a.metrics.Collect(ctx)}
		if ws := a.workers.Metrics(); len(ws) > 0 {
			m.Workers = ws
		}
		if m.Host != nil || len(m.Workers) > 0 {
			a.backend.AddMetrics(m)
		}
		interval = a.obs.metricsInterval()
		timer.Reset(interval)
	}
}

// logLoop — записи журнала бэкенду пачками раз в logEvery (§9).
func (a *App) logLoop(ctx context.Context) {
	if a.backend == nil {
		return
	}
	t := time.NewTicker(logEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		a.backend.AddLogs(a.forward.Take())
	}
}

// ─── связь с бэкендом ──────────────────────────────────────────────────

// backendHandler — что приходит от бэкенда (backend.Handler).
type backendHandler struct{ a *App }

func (h backendHandler) Hello() protocol.Hello {
	a := h.a
	cfg := a.config()
	workers := a.workers.Info()
	if workers == nil {
		workers = []protocol.HelloWorker{}
	}
	return protocol.Hello{
		Agent: protocol.HelloAgent{Version: a.version, BootID: a.bootID, StartedAt: a.started.UnixMilli(),
			UpdateMode: cfg.Update.Mode, Install: InstallMode},
		Host: a.host, Name: cfg.Name, Labels: cfg.Labels, Workers: workers,
	}
}

// OnHello — hello принят: частоты, отметка «новая версия вышла на связь», итог agent.update.
func (h backendHandler) OnHello(r protocol.HelloReply) {
	a := h.a
	switch r.Status {
	case "", protocol.StatusActive:
		a.activate()
	case protocol.StatusPending:
		if !a.active() {
			a.log.Info("бэкенд ещё не подтвердил агента (pending) — воркеры не запускаются")
		}
	}
	a.obs.mu.Lock()
	a.obs.hello = r
	a.obs.mu.Unlock()
	kick(a.obs.metricsKick)
	if a.update.Marker != "" {
		update.Healthy(a.update)
	}
	a.finishAgentUpdate()
	a.statusChanged()
}

func (h backendHandler) OnDesired(d protocol.Desired) {
	if err := h.a.store.SaveDesired(d); err != nil {
		h.a.log.Error("желаемое не сохранено", "rev", d.Rev, "err", err)
	}
	h.a.log.Debug("желаемое", "rev", d.Rev)
	h.a.setDesired(d)
}

func (h backendHandler) OnCommand(cmd protocol.Command) { h.a.command(cmd) }

func (h backendHandler) Rev() int64 { return h.a.currentDesired().Rev }

func (h backendHandler) OnOnline(on bool) { h.a.statusChanged() }
