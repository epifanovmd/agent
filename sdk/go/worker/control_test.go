package worker

import (
	"context"
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// sameAsExample — data сообщения воркера совпадает с data образца.
func sameAsExample(t *testing.T, env message.Envelope, name string) {
	t.Helper()
	fix := example(t, name)
	var got map[string]any
	_ = json.Unmarshal(env.Data, &got)
	if env.Type != fix["type"] || !reflect.DeepEqual(got, fix["data"]) {
		t.Fatalf("%s: %s %s", name, env.Type, env.Data)
	}
}

// worker.context: до первого — значения по умолчанию; затем Context() и
// OnContext по образцу; паника обработчика — в лог; в режиме cleanup
// контекст приходит до worker.cleanup.
func TestContextFromAgent(t *testing.T) {
	w, a := newWorker(t, "report")
	w.Command("example.noop", func(context.Context, *Command) (any, error) { return nil, nil })
	if c := w.Context(); c.Mode != message.WorkerModeRun || c.Online || c.MetricsIntervalMs != 0 {
		t.Fatalf("по умолчанию: %+v", c)
	}
	got := make(chan Context, 4)
	w.OnContext(func(Context) { panic("сбой обработчика") })
	w.OnContext(func(c Context) { got <- c })
	modeAtCleanup := make(chan string, 1)
	w.Cleanup(func(context.Context) error {
		modeAtCleanup <- w.Context().Mode
		return nil
	})
	start(t, w, a)

	fix := example(t, "worker.context")
	a.send(fix["type"].(string), fix["data"], "")
	var c Context
	select {
	case c = <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("OnContext не вызван")
	}
	want := Context{
		Mode: message.WorkerModeRun,
		Agent: message.WorkerContextAgent{
			ID: fix["data"].(map[string]any)["agent"].(map[string]any)["id"].(string), Name: "node-01", Version: "1.1.0", Labels: map[string]string{"zone": "eu"},
		},
		Online: true, MetricsIntervalMs: 1000, Channels: map[string]int64{"example.app": 1000}, LogLevel: "warn",
	}
	if !reflect.DeepEqual(c, want) || !reflect.DeepEqual(w.Context(), want) {
		t.Fatalf("контекст: %+v\nждали %+v", c, want)
	}
	// Копия: изменение меток и каналов снаружи не меняет контекст воркера.
	c.Agent.Labels["zone"] = "us"
	c.Channels["example.app"] = 1
	if got := w.Context(); got.Agent.Labels["zone"] != "eu" || got.Channels["example.app"] != 1000 {
		t.Fatal("Context() отдаёт общие метки или каналы")
	}

	cleanup := example(t, "worker.context.cleanup")
	a.send(cleanup["type"].(string), cleanup["data"], "")
	a.send(message.TypeWorkerCleanup, struct{}{}, "c1")
	a.expect(message.TypeWorkerCleaned, nil)
	if mode := <-modeAtCleanup; mode != message.WorkerModeCleanup {
		t.Fatalf("режим при уборке: %q", mode)
	}
	if c := <-got; c.Mode != message.WorkerModeCleanup || c.Online {
		t.Fatalf("контекст cleanup: %+v", c)
	}
}

// SetHealth, Pause, Resume, RequestRestart — сообщения по образцам воркер → агент; до
// Run — ошибка.
func TestSelfControlMatchesExamples(t *testing.T) {
	w, a := newWorker(t, "report")
	w.Job("example.echo", 1, func(context.Context, *Job) (any, error) { return nil, nil })
	if err := w.SetHealth(false, "x"); err == nil {
		t.Fatal("SetHealth до Run")
	}
	start(t, w, a)

	steps := []struct {
		call func() error
		typ  string
		fix  string
		raw  string
	}{
		{func() error { return w.SetHealth(true, "") }, message.TypeWorkerHealth, "worker.health", ""},
		{func() error { return w.SetHealth(false, "example.db недоступна") }, message.TypeWorkerHealth, "worker.health.degraded", ""},
		{func() error { return w.SetHealth(true, "лишнее") }, message.TypeWorkerHealth, "", `{"ok":true}`},
		{func() error { return w.Pause("example.echo") }, message.TypeWorkerPause, "worker.pause", ""},
		{func() error { return w.Resume("example.echo") }, message.TypeWorkerResume, "worker.resume", ""},
		{func() error { return w.Pause() }, message.TypeWorkerPause, "", `{}`},
		{func() error { return w.Resume() }, message.TypeWorkerResume, "", `{}`},
		{func() error { return w.RequestRestart("новые настройки example.app") }, message.TypeWorkerRestart, "worker.restart", ""},
		{func() error { return w.RequestRestart("") }, message.TypeWorkerRestart, "", `{}`},
	}
	for i, s := range steps {
		if err := s.call(); err != nil {
			t.Fatalf("шаг %d: %v", i, err)
		}
		env := a.expect(s.typ, nil)
		if s.fix != "" {
			sameAsExample(t, env, s.fix)
		} else if string(env.Data) != s.raw {
			t.Fatalf("шаг %d: %s, ждали %s", i, env.Data, s.raw)
		}
	}
}

// Telemetry(AutoInterval): пока частота неизвестна — 15 с; контекст с
// metricsIntervalMs — опрос с этой частотой (сразу, если срок уже прошёл).
func TestTelemetryAutoInterval(t *testing.T) {
	w, a := newWorker(t, "tele")
	w.Telemetry("example.sys", AutoInterval, func() any { return 1 })
	start(t, w, a)
	a.expect(message.TypeTelemetry, nil)
	a.quiet(300 * time.Millisecond)

	a.send(message.TypeWorkerContext, message.WorkerContext{Mode: message.WorkerModeRun, MetricsIntervalMs: 50}, "")
	began := time.Now()
	for range 3 {
		a.expect(message.TypeTelemetry, nil)
	}
	if d := time.Since(began); d > 2*time.Second {
		t.Fatalf("опрос не с частотой метрик: %v", d)
	}
	// Частота снова неизвестна — 15 с.
	a.send(message.TypeWorkerContext, message.WorkerContext{Mode: message.WorkerModeRun}, "")
	time.Sleep(100 * time.Millisecond)
	for len(a.in) > 0 {
		<-a.in
	}
	a.quiet(300 * time.Millisecond)
}

// Telemetry(AutoInterval) с подпиской на канал: частота подписки из
// Context().Channels главнее metricsIntervalMs; подписка на чужой канал не
// влияет; подписка снята — снова частота метрик агента.
func TestTelemetryAutoIntervalChannel(t *testing.T) {
	w, a := newWorker(t, "tele")
	w.Telemetry("example.app", AutoInterval, func() any { return 1 })
	start(t, w, a)
	a.expect(message.TypeTelemetry, nil)

	// Подписка на другой канал — частота метрик агента (60 с): тишина.
	a.send(message.TypeWorkerContext, message.WorkerContext{
		Mode: message.WorkerModeRun, MetricsIntervalMs: 60_000, Channels: map[string]int64{"example.other": 50},
	}, "")
	a.quiet(300 * time.Millisecond)

	// Подписка на свой канал — его частота.
	a.send(message.TypeWorkerContext, message.WorkerContext{
		Mode: message.WorkerModeRun, MetricsIntervalMs: 60_000, Channels: map[string]int64{"example.app": 50},
	}, "")
	began := time.Now()
	for range 3 {
		a.expect(message.TypeTelemetry, nil)
	}
	if d := time.Since(began); d > 2*time.Second {
		t.Fatalf("опрос не с частотой подписки: %v", d)
	}

	// Подписка снята — частота метрик агента.
	a.send(message.TypeWorkerContext, message.WorkerContext{Mode: message.WorkerModeRun, MetricsIntervalMs: 60_000}, "")
	time.Sleep(100 * time.Millisecond)
	for len(a.in) > 0 {
		<-a.in
	}
	a.quiet(300 * time.Millisecond)
}
