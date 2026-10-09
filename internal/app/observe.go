//go:build unix

package app

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
)

// Частоты до первого welcome и задержка внеочередного status; переменные —
// для тестов.
var (
	defaultMetricsInterval = time.Minute
	defaultStatusInterval  = time.Minute
	statusDebounce         = 200 * time.Millisecond
	logEvery               = message.LogBatchInterval
)

// observer — частоты метрик и status (welcome, watch) и сигналы циклам.
type observer struct {
	mu      sync.Mutex
	welcome message.Welcome
	watch   message.Watch
	timer   *time.Timer
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

func (o *observer) setWelcome(w message.Welcome) {
	o.mu.Lock()
	o.welcome = w
	o.mu.Unlock()
	kick(o.metricsKick)
	kick(o.statusKick)
}

// metricsInterval — из watch, но не чаще MinWatchInterval и не реже, чем в welcome (§9).
func (o *observer) metricsInterval() time.Duration {
	o.mu.Lock()
	defer o.mu.Unlock()
	base := defaultMetricsInterval
	if o.welcome.MetricsIntervalMs > 0 {
		base = time.Duration(o.welcome.MetricsIntervalMs) * time.Millisecond
	}
	if o.watch.MetricsIntervalMs > 0 {
		base = min(base, max(time.Duration(o.watch.MetricsIntervalMs)*time.Millisecond, message.MinWatchInterval))
	}
	return base
}

func (o *observer) statusInterval() time.Duration {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.welcome.StatusIntervalMs > 0 {
		return time.Duration(o.welcome.StatusIntervalMs) * time.Millisecond
	}
	return defaultStatusInterval
}

// setWatch — новый watch целиком заменяет прежний; {} или наступивший
// untilMs — снять (§9).
func (a *App) setWatch(w message.Watch) {
	o := &a.obs
	if !w.Empty() && w.UntilMs > 0 && time.UnixMilli(w.UntilMs).Before(time.Now()) {
		w = message.Watch{}
	}
	if w.LogLevel != "" && message.LogLevelRank(w.LogLevel) < 0 {
		a.log.Warn("watch: незнакомый уровень лога — пропущен", "logLevel", w.LogLevel)
		w.LogLevel = ""
	}
	o.mu.Lock()
	o.watch = w
	if o.timer != nil {
		o.timer.Stop()
		o.timer = nil
	}
	if !w.Empty() && w.UntilMs > 0 {
		o.timer = time.AfterFunc(time.Until(time.UnixMilli(w.UntilMs)), func() { a.setWatch(message.Watch{}) })
	}
	o.mu.Unlock()
	_ = a.forward.Override(w.LogLevel)
	kick(o.metricsKick)
}

// statusChanged — изменилось то, что видно в status: внеочередной status.
func (a *App) statusChanged() { kick(a.obs.statusKick) }

// status — состояние воркеров и outbox (§6).
func (a *App) status() message.Status {
	ws := a.workers.Status()
	for i := range ws {
		ws[i].Configs = a.configs.Status(ws[i].Name)
	}
	return message.Status{Workers: ws, Outbox: a.outbox.Len()}
}

func (a *App) sendStatus() { a.link.Stream(message.TypeStatus, a.status()) }

// statusLoop — status раз в statusIntervalMs и при изменении, пока есть связь.
func (a *App) statusLoop(ctx context.Context) {
	timer := time.NewTimer(a.obs.statusInterval())
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-a.obs.statusKick:
			// Несколько изменений подряд — одним сообщением.
			select {
			case <-ctx.Done():
				return
			case <-time.After(statusDebounce):
			}
		}
		if a.link.Connected() {
			a.sendStatus()
		}
		timer.Reset(a.obs.statusInterval())
	}
}

// metricsLoop — точка метрик раз в metricsInterval (и без связи: поток
// держит последние сообщения до подключения).
func (a *App) metricsLoop(ctx context.Context) {
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
		if m, ok := a.collect(ctx); ok {
			a.link.Stream(message.TypeMetrics, m)
		}
		interval = a.obs.metricsInterval()
		timer.Reset(interval)
	}
}

// collect — GET /metrics всех запущенных воркеров сразу (срок —
// lifecycle.probeTimeout воркера); ответ
// sysmetrics — host, остальных — workers как есть (§9).
func (a *App) collect(ctx context.Context) (message.Metrics, bool) {
	m := message.Metrics{CollectedAt: time.Now().UnixMilli()}
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, name := range a.workers.Running() {
		client, err := a.workers.Client(name)
		if err != nil {
			continue
		}
		timeout := message.ProbeTimeout
		if spec, ok := a.workers.Spec(name); ok {
			timeout = spec.Lifecycle.ProbeTimeout.Std()
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			raw, ok := probeMetrics(ctx, client, timeout)
			if !ok {
				return
			}
			mu.Lock()
			defer mu.Unlock()
			if name == config.SysmetricsWorker {
				var h message.HostMetrics
				if json.Unmarshal(raw, &h) == nil {
					m.Host = &h
				}
				return
			}
			if m.Workers == nil {
				m.Workers = map[string]json.RawMessage{}
			}
			m.Workers[name] = raw
		}()
	}
	wg.Wait()
	return m, m.Host != nil || len(m.Workers) > 0
}

// probeMetrics — GET /metrics: 2xx и JSON до 1 МБ; иначе воркера в точке нет.
func probeMetrics(ctx context.Context, client *http.Client, timeout time.Duration) (json.RawMessage, bool) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://worker"+message.MetricsPath, nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(resp.Body, message.MaxMetricsBytes+1))
	if err != nil || resp.StatusCode/100 != 2 || len(raw) > message.MaxMetricsBytes || !json.Valid(raw) {
		return nil, false
	}
	return raw, true
}

// logLoop — пачки лога серверу не чаще раза в секунду (§9).
func (a *App) logLoop(ctx context.Context) {
	t := time.NewTicker(logEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		if batch := a.forward.Take(); batch != nil {
			a.link.Stream(message.TypeLog, message.Log{Entries: batch})
		}
	}
}
