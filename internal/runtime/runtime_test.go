package runtime

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/sdk/go/message"
)

type fakeCap struct {
	slots    map[string]int
	jobs     []message.StatusJob
	degraded bool
	accept   bool
	handled  []string
}

func (f *fakeCap) Declare(caps *message.Capabilities) {
	caps.Jobs = &message.JobsCapability{Queues: []message.QueueCapacity{{Name: "q", Concurrency: 1}}}
}
func (f *fakeCap) Handles() []string { return []string{message.TypeJobAssign} }
func (f *fakeCap) Handle(_ context.Context, env message.Envelope) error {
	f.handled = append(f.handled, env.Type)
	return nil
}
func (f *fakeCap) ContributeStatus(st *message.Status) {
	for q, n := range f.slots {
		st.Slots[q] = n
	}
	st.Jobs = append(st.Jobs, f.jobs...)
	if f.degraded {
		st.State = message.StateDegraded
	}
}
func (f *fakeCap) RunningJobs() []message.JobRef { return []message.JobRef{{JobID: "j", Attempt: 1}} }
func (f *fakeCap) SetAccepting(a bool)           { f.accept = a }

func TestStatusStatesAndHello(t *testing.T) {
	rt := New(Info{Name: "n", Version: "v", Labels: map[string]string{"a": "b"}}, logx.Discard())
	c := &fakeCap{slots: map[string]int{"q": 2}}
	rt.Register(c)
	rt.SetOutbox(func() int { return 3 })

	if st := rt.Status(); st.State != message.StateIdle || st.Slots["q"] != 2 || st.Outbox != 3 {
		t.Fatalf("idle: %+v", st)
	}
	c.jobs = []message.StatusJob{{JobID: "j"}}
	if st := rt.Status(); st.State != message.StateBusy {
		t.Fatalf("busy: %+v", st)
	}
	c.degraded = true
	if st := rt.Status(); st.State != message.StateDegraded {
		t.Fatalf("degraded: %+v", st)
	}
	rt.Drain()
	if st := rt.Status(); st.State != message.StateDraining || st.Slots["q"] != 0 || c.accept {
		t.Fatalf("drain: %+v accept=%v", st, c.accept)
	}
	rt.Resume()
	rt.SetUpdating(true)
	if st := rt.Status(); st.State != message.StateUpdating || !c.accept {
		t.Fatalf("updating: %+v", st)
	}

	h := rt.Hello()
	if h.Agent.BootID == "" || h.Capabilities.Jobs == nil || len(h.Jobs) != 1 || h.Labels["a"] != "b" || h.Versions[0] != 1 {
		t.Fatalf("hello: %+v", h)
	}
}

func TestConfigFromWelcomeAndMessage(t *testing.T) {
	rt := New(Info{}, logx.Discard())
	rt.OnWelcome(message.Welcome{Config: message.SessionConfig{StatusIntervalMs: 1000, MetricsIntervalMs: 2000}})
	status, metrics := rt.intervals()
	if status.Milliseconds() != 1000 || metrics.Milliseconds() != 2000 {
		t.Fatalf("welcome.config: %v %v", status, metrics)
	}
	rt.applyConfig(message.SessionConfig{MetricsIntervalMs: 500})
	status, metrics = rt.intervals()
	if status.Milliseconds() != 1000 || metrics.Milliseconds() != 500 {
		t.Fatalf("частичный config: %v %v", status, metrics)
	}
}

type fakeSender struct {
	mu        sync.Mutex
	connected bool
	sent      []message.Envelope
}

func (f *fakeSender) Stream(typ string, data any) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.sent = append(f.sent, message.MustNew(typ, data))
}
func (f *fakeSender) Reliable(typ string, data any) error { f.Stream(typ, data); return nil }
func (f *fakeSender) Request(context.Context, string, any, any) error {
	return nil
}
func (f *fakeSender) Connected() bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.connected
}
func (f *fakeSender) setConnected(v bool) {
	f.mu.Lock()
	f.connected = v
	f.mu.Unlock()
}
func (f *fakeSender) of(typ string) []message.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []message.Envelope
	for _, e := range f.sent {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}

type fakeMetrics struct{ n atomic.Int64 }

func (f *fakeMetrics) Collect(context.Context) message.Metrics {
	return message.Metrics{CollectedAt: f.n.Add(1)}
}
func (f *fakeMetrics) Inventory(context.Context) message.Inventory {
	return message.Inventory{CollectedAt: time.Now().UnixMilli(), MemoryBytes: 42}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// Без связи метрики копятся и после welcome досылаются с backfill; смещение часов —
// у каждой отправки; inventory — после welcome.
func TestMetricsBacklogAndInventory(t *testing.T) {
	rt := New(Info{}, logx.Discard())
	sender := &fakeSender{}
	rt.SetSender(sender)
	rt.SetMetrics(&fakeMetrics{})
	rt.applyConfig(message.SessionConfig{MetricsIntervalMs: 10})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rt.metricsLoop(ctx)
	go rt.inventoryLoop(ctx)

	eventually(t, "точки без связи", func() bool { rt.mu.Lock(); defer rt.mu.Unlock(); return len(rt.backlog) >= 3 })
	sender.setConnected(true)
	// Часы сервера на минуту впереди: смещение — в каждой точке.
	rt.OnWelcome(message.Welcome{ServerTime: time.Now().Add(time.Minute).UnixMilli()})
	eventually(t, "досылка и свежие точки", func() bool {
		var m message.Metrics
		all := sender.of(message.TypeMetrics)
		if len(all) < 5 {
			return false
		}
		_ = all[len(all)-1].Decode(&m)
		return !m.Backfill
	})
	var first message.Metrics
	_ = sender.of(message.TypeMetrics)[0].Decode(&first)
	if !first.Backfill || first.CollectedAt != 1 || first.ClockOffsetMs == nil || *first.ClockOffsetMs < 59_000 {
		t.Fatalf("первая досланная точка: %+v", first)
	}
	eventually(t, "inventory после welcome", func() bool { return len(sender.of(message.TypeInventory)) == 1 })

	// Новый интервал применяется сразу, а не после прежнего таймера.
	rt.applyConfig(message.SessionConfig{MetricsIntervalMs: 60_000})
	time.Sleep(30 * time.Millisecond)
	before := len(sender.of(message.TypeMetrics))
	time.Sleep(60 * time.Millisecond)
	if after := len(sender.of(message.TypeMetrics)); after != before {
		t.Fatalf("после config 60 с метрики не идут каждые 10 мс: %d → %d", before, after)
	}
}

// telemetry.backlog: 0 — точки без связи не копятся; inventoryInterval: 0 —
// сведения об узле не отправляются даже после welcome; новые имя и метки —
// в следующем hello.
func TestTelemetrySettings(t *testing.T) {
	rt := New(Info{Name: "a"}, logx.Discard())
	sender := &fakeSender{}
	rt.SetSender(sender)
	rt.SetMetrics(&fakeMetrics{})
	rt.SetTelemetry(0, 0)
	rt.applyConfig(message.SessionConfig{MetricsIntervalMs: 5})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rt.metricsLoop(ctx)
	go rt.inventoryLoop(ctx)
	time.Sleep(60 * time.Millisecond)
	rt.mu.Lock()
	n := len(rt.backlog)
	rt.mu.Unlock()
	if n != 0 {
		t.Fatalf("backlog 0 — без накопления: %d", n)
	}
	sender.setConnected(true)
	rt.OnWelcome(message.Welcome{})
	eventually(t, "живые метрики", func() bool { return len(sender.of(message.TypeMetrics)) > 0 })
	time.Sleep(50 * time.Millisecond)
	if len(sender.of(message.TypeInventory)) != 0 {
		t.Fatal("inventoryInterval 0 — сведения не отправляются")
	}
	// Включили на ходу: сведения уходят по новому интервалу.
	rt.SetTelemetry(3, 20*time.Millisecond)
	eventually(t, "inventory после включения", func() bool { return len(sender.of(message.TypeInventory)) == 1 })

	rt.SetIdentity("b", map[string]string{"zone": "eu"})
	if h := rt.Hello(); h.Agent.Name != "b" || h.Labels["zone"] != "eu" {
		t.Fatalf("hello: %+v", h)
	}
}

// Порог лога из сводной подписки: из welcome (нет подписки — ""), из config
// (без subscription — без изменений, пустая — ""); ключ шифрования — в hello.
func TestLogLevelAndEncryptionKey(t *testing.T) {
	rt := New(Info{EncryptionKey: "a2V5"}, logx.Discard())
	if rt.Hello().Agent.EncryptionKey != "a2V5" {
		t.Fatal("encryptionKey не в hello")
	}
	levels := make(chan string, 10)
	rt.OnLogLevel(func(l string) { levels <- l })
	rt.OnWelcome(message.Welcome{Config: message.SessionConfig{Subscription: &message.Subscription{LogLevel: message.LogDebug}}})
	rt.OnWelcome(message.Welcome{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rt.dispatch(ctx)
	rt.OnMessage(message.MustNew(message.TypeConfig, message.SessionConfig{MetricsIntervalMs: 100}))
	rt.OnMessage(message.Envelope{Type: message.TypeConfig, Data: []byte(`{"subscription":{"logLevel":"info"}}`)})
	rt.OnMessage(message.Envelope{Type: message.TypeConfig, Data: []byte(`{"subscription":{}}`)})
	var got []string
	for len(got) < 4 {
		select {
		case l := <-levels:
			got = append(got, l)
		case <-time.After(3 * time.Second):
			t.Fatalf("пороги: %q", got)
		}
	}
	if want := []string{"debug", "", "info", ""}; !slices.Equal(got, want) {
		t.Fatalf("пороги: %q, ждали %q", got, want)
	}
}

// extendedMetrics — источник метрик с группами от сервера.
type extendedMetrics struct {
	fakeMetrics
	mu    sync.Mutex
	calls [][]string
}

func (e *extendedMetrics) SetExtra(groups []string) bool {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.calls = append(e.calls, slices.Clone(groups))
	return false
}

func (e *extendedMetrics) last() ([]string, int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.calls) == 0 {
		return nil, 0
	}
	return e.calls[len(e.calls)-1], len(e.calls)
}

// Группы метрик сводной подписки передаются источнику метрик; config без
// subscription их не меняет; пустая подписка и новый welcome без неё — снова
// только настройка агента.
func TestSubscriptionMetricGroups(t *testing.T) {
	rt := New(Info{}, logx.Discard())
	rt.SetSender(&fakeSender{})
	src := &extendedMetrics{}
	rt.SetMetrics(src)
	groups := []string{"sockets", "diskio"}
	rt.OnWelcome(message.Welcome{Config: message.SessionConfig{Subscription: &message.Subscription{Metrics: groups}}})
	if got, _ := src.last(); !slices.Equal(got, groups) {
		t.Fatalf("из welcome: %v", got)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rt.dispatch(ctx)
	rt.OnMessage(message.Envelope{Type: message.TypeConfig, Data: []byte(`{"metricsIntervalMs":1000}`)})
	rt.OnMessage(message.Envelope{Type: message.TypeConfig, Data: []byte(`{"subscription":{"metrics":["fds"]}}`)})
	eventually(t, "config.subscription.metrics", func() bool { got, n := src.last(); return n == 2 && slices.Equal(got, []string{"fds"}) })
	rt.OnMessage(message.Envelope{Type: message.TypeConfig, Data: []byte(`{"subscription":{}}`)})
	eventually(t, "пустая подписка", func() bool { got, n := src.last(); return n == 3 && len(got) == 0 })
	rt.OnWelcome(message.Welcome{})
	if got, n := src.last(); n != 4 || got != nil {
		t.Fatalf("welcome без подписки: %v (%d)", got, n)
	}
}

// Действующие частоты: настройка сервера, ужесточённая подпиской (не
// ослабленная); частота метрик — ещё и минимум по подпискам на каналы.
func TestSubscriptionIntervals(t *testing.T) {
	rt := New(Info{}, logx.Discard())
	rt.OnWelcome(message.Welcome{Config: message.SessionConfig{StatusIntervalMs: 15000, MetricsIntervalMs: 15000}})
	check := func(what string, status, metrics time.Duration) {
		t.Helper()
		if s, m := rt.intervals(); s != status || m != metrics {
			t.Fatalf("%s: status %v metrics %v, ждали %v %v", what, s, m, status, metrics)
		}
	}
	rt.setSubscription(message.Subscription{StatusIntervalMs: 1000, MetricsIntervalMs: 2000})
	check("подписка", time.Second, 2*time.Second)
	rt.setSubscription(message.Subscription{MetricsIntervalMs: 2000, Channels: map[string]int64{"example.app": 500, "example.db": 3000}})
	check("каналы", 15*time.Second, 500*time.Millisecond)
	rt.setSubscription(message.Subscription{StatusIntervalMs: 60_000, MetricsIntervalMs: 60_000})
	check("реже настройки", 15*time.Second, 15*time.Second)
	rt.setSubscription(message.Subscription{})
	check("пустая", 15*time.Second, 15*time.Second)
	rt.applyConfig(message.SessionConfig{MetricsIntervalMs: 300})
	rt.setSubscription(message.Subscription{Channels: map[string]int64{"example.app": 1000}})
	check("настройка чаще канала", 15*time.Second, 300*time.Millisecond)
	if sub := rt.Subscription(); sub.Channels["example.app"] != 1000 {
		t.Fatalf("Subscription: %+v", sub)
	}
}

// Подписка меняет частоту статуса и метрик сразу (не после прежнего таймера);
// пустая — обычная частота.
func TestSubscriptionLoops(t *testing.T) {
	rt := New(Info{}, logx.Discard())
	sender := &fakeSender{connected: true}
	rt.SetSender(sender)
	rt.SetMetrics(&fakeMetrics{})
	rt.applyConfig(message.SessionConfig{StatusIntervalMs: 60_000, MetricsIntervalMs: 60_000})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rt.dispatch(ctx)
	go rt.statusLoop(ctx)
	go rt.metricsLoop(ctx)
	time.Sleep(30 * time.Millisecond)
	if n := len(sender.of(message.TypeMetrics)) + len(sender.of(message.TypeStatus)); n != 0 {
		t.Fatalf("раньше обычной частоты: %d", n)
	}
	rt.OnMessage(message.MustNew(message.TypeConfig, message.SessionConfig{
		Subscription: &message.Subscription{StatusIntervalMs: 10, Channels: map[string]int64{"example.app": 10}},
	}))
	eventually(t, "частые status и metrics", func() bool {
		return len(sender.of(message.TypeMetrics)) >= 3 && len(sender.of(message.TypeStatus)) >= 3
	})
	rt.OnMessage(message.MustNew(message.TypeConfig, message.SessionConfig{Subscription: &message.Subscription{}}))
	eventually(t, "обычная частота", func() bool { s, m := rt.intervals(); return s == time.Minute && m == time.Minute })
	time.Sleep(50 * time.Millisecond)
	ms, ss := len(sender.of(message.TypeMetrics)), len(sender.of(message.TypeStatus))
	time.Sleep(100 * time.Millisecond)
	if ms2, ss2 := len(sender.of(message.TypeMetrics)), len(sender.of(message.TypeStatus)); ms2 != ms || ss2 != ss {
		t.Fatalf("после пустой подписки: metrics %d→%d status %d→%d", ms, ms2, ss, ss2)
	}
}

// Контекст воркеров: связь (welcome / разрыв), частота метрик и подписка
// (config) — с уведомлением WhenContextChanged.
func TestWorkerContextState(t *testing.T) {
	rt := New(Info{}, logx.Discard())
	var calls atomic.Int64
	rt.WhenContextChanged(func() { calls.Add(1) })
	if rt.Online() || rt.MetricsInterval() != 15*time.Second {
		t.Fatalf("до связи: online %v interval %v", rt.Online(), rt.MetricsInterval())
	}
	rt.OnWelcome(message.Welcome{Config: message.SessionConfig{MetricsIntervalMs: 15000}})
	if !rt.Online() || calls.Load() == 0 {
		t.Fatalf("после welcome: online %v calls %d", rt.Online(), calls.Load())
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go rt.dispatch(ctx)
	before := calls.Load()
	rt.OnMessage(message.MustNew(message.TypeConfig, message.SessionConfig{MetricsIntervalMs: 1000}))
	eventually(t, "частые метрики", func() bool { return rt.MetricsInterval() == time.Second && calls.Load() > before })
	before = calls.Load()
	rt.OnMessage(message.MustNew(message.TypeConfig, message.SessionConfig{
		Subscription: &message.Subscription{Channels: map[string]int64{"example.app": 500}},
	}))
	eventually(t, "подписка на канал", func() bool {
		return rt.Subscription().Channels["example.app"] == 500 && rt.MetricsInterval() == 500*time.Millisecond && calls.Load() > before
	})
	rt.OnMessage(message.MustNew(message.TypeConfig, message.SessionConfig{MetricsIntervalMs: 2000}))
	eventually(t, "config без subscription", func() bool {
		rt.mu.Lock()
		defer rt.mu.Unlock()
		return rt.cfg.MetricsIntervalMs == 2000
	})
	if rt.MetricsInterval() != 500*time.Millisecond {
		t.Fatalf("канал чаще настройки: %v", rt.MetricsInterval())
	}
	if rt.Subscription().Channels["example.app"] != 500 {
		t.Fatal("config без subscription сбросил подписку")
	}
	rt.OnWelcome(message.Welcome{}) // новая сессия без подписки — подписок нет
	if len(rt.Subscription().Channels) != 0 || rt.MetricsInterval() != 2*time.Second {
		t.Fatalf("welcome без подписки: %+v %v", rt.Subscription(), rt.MetricsInterval())
	}
	before = calls.Load()
	rt.OnDisconnect(nil)
	if rt.Online() || calls.Load() == before {
		t.Fatal("разрыв: online не сброшен или нет уведомления")
	}
	before = calls.Load()
	rt.OnDisconnect(nil) // уже без связи — без уведомления
	if calls.Load() != before {
		t.Fatal("повторный разрыв уведомил")
	}
}

// stopCap — этап остановки: blocking ждёт общего срока (задачи), остальные
// записывают, когда их позвали.
type stopCap struct {
	blocking bool
	called   chan time.Time
}

func (s *stopCap) Declare(*message.Capabilities)                  {}
func (s *stopCap) Handles() []string                              { return nil }
func (s *stopCap) Handle(context.Context, message.Envelope) error { return nil }
func (s *stopCap) Stop(ctx context.Context) {
	s.called <- time.Now()
	if s.blocking {
		<-ctx.Done()
	}
}

// Этапы остановки идут одновременно: ожидание задач не задерживает сигнал
// воркерам (иначе они получили бы SIGKILL сразу после SIGTERM).
func TestStopStagesConcurrent(t *testing.T) {
	rt := New(Info{Name: "a"}, logx.Discard())
	rt.SetSender(&fakeSender{})
	jobs := &stopCap{blocking: true, called: make(chan time.Time, 1)}
	workers := &stopCap{called: make(chan time.Time, 1)}
	rt.Register(jobs)
	rt.Register(workers)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = rt.Run(ctx, func(ctx context.Context) error { <-ctx.Done(); return nil }, 500*time.Millisecond)
		close(done)
	}()
	time.Sleep(20 * time.Millisecond)
	start := time.Now()
	cancel()
	if at := <-workers.called; at.Sub(start) > 200*time.Millisecond {
		t.Fatalf("воркеры позваны через %v — после ожидания задач", at.Sub(start))
	}
	<-done
}
