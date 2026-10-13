//go:build unix

package worker_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/workerapi"
	"github.com/epifanovmd/agent/protocol"
	"github.com/epifanovmd/agent/worker"
)

// agent — сторона агента для теста: что воркер прислал через сокет.
type agent struct {
	mu       sync.Mutex
	reg      *protocol.Register
	opened   chan struct{}
	applied  []protocol.Applied
	results  []protocol.Result
	progress []protocol.Progress
	events   []protocol.WorkerEvent
	beats    atomic.Int32
	reply    protocol.RequestReply
}

func (a *agent) ByToken(token string) (string, bool) { return "w", token == "secret" }
func (a *agent) Register(_ string, reg protocol.Register) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.reg = &reg
	return nil
}
func (a *agent) Heartbeat(string, protocol.Health) error { a.beats.Add(1); return nil }
func (a *agent) Applied(_ string, items []protocol.Applied) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.applied = append(a.applied, items...)
	return nil
}
func (a *agent) Results(_ string, items []protocol.Result) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.results = append(a.results, items...)
	return nil
}
func (a *agent) Progress(_ string, p protocol.Progress) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.progress = append(a.progress, p)
	return nil
}
func (a *agent) Events(_ string, items []protocol.WorkerEvent) error {
	for _, e := range items {
		if e.Type != "w.done" {
			return protocol.NewError(protocol.CodeEventUndeclared, e.Type)
		}
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.events = append(a.events, items...)
	return nil
}
func (a *agent) Request(_ context.Context, _ string, r protocol.Request) protocol.RequestReply {
	if r.Type != "w.ask" {
		return protocol.RequestReply{Error: protocol.NewError(protocol.CodeRequestUndeclared, r.Type)}
	}
	return a.reply
}
func (a *agent) StreamOpened(string) error {
	select {
	case a.opened <- struct{}{}:
	default:
	}
	return nil
}

func (a *agent) result(t *testing.T, id string) protocol.Result {
	t.Helper()
	var r protocol.Result
	eventually(t, "итог "+id, func() bool {
		a.mu.Lock()
		defer a.mu.Unlock()
		for _, x := range a.results {
			if x.Re == id {
				r = x
				return true
			}
		}
		return false
	})
	return r
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Клиент воркера и сокет агента: регистрация, желаемое и итоги применения, команды с итогом,
// ходом и отменой, повтор команды, события, запросы, пульс и наблюдение; неверный токен — ошибка.
func TestWorkerAgainstSocket(t *testing.T) {
	dir, _ := os.MkdirTemp("", "w")
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "a.sock")
	ag := &agent{opened: make(chan struct{}, 1), reply: protocol.RequestReply{OK: true, Data: json.RawMessage(`{"x":1}`)}}
	srv, err := workerapi.Listen(sock, ag, logx.Discard())
	if err != nil {
		t.Fatal(err)
	}
	defer srv.Close()

	if _, err := worker.New(worker.Options{Socket: "", Token: ""}); !errors.Is(err, worker.ErrNoAgent) {
		t.Fatalf("без агента: %v", err)
	}
	var configured sync.Map
	var runs atomic.Int32
	var watched atomic.Value
	w, err := worker.New(worker.Options{
		Socket: sock, Token: "secret", Version: "1.0.0",
		Manifest: protocol.WorkerManifest{Configs: []protocol.ManifestConfig{{Key: "main"}},
			Commands: []protocol.ManifestCommand{{Type: "w.run"}, {Type: "w.wait"}}},
		Configure: func(_ context.Context, key string, data json.RawMessage) error {
			if string(data) == `"bad"` {
				return errors.New("плохое значение")
			}
			configured.Store(key, string(data))
			return nil
		},
		Commands: map[string]worker.CommandFunc{
			"w.run": func(ctx context.Context, c *worker.Call) (any, error) {
				runs.Add(1)
				_ = c.Progress(ctx, 0.5, "половина")
				return map[string]string{"id": c.ID}, nil
			},
			"w.wait": func(ctx context.Context, _ *worker.Call) (any, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		},
		Watch: func(v protocol.Watch) { watched.Store(v) },
		Log:   func(string, ...any) {},
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	select {
	case <-ag.opened:
	case <-time.After(10 * time.Second):
		t.Fatal("поток не открыт")
	}
	ag.mu.Lock()
	reg := ag.reg
	ag.mu.Unlock()
	if reg == nil || reg.Version != "1.0.0" || !reg.Manifest.DeclaresCommand("w.run") {
		t.Fatalf("регистрация: %+v", reg)
	}
	eventually(t, "пульс", func() bool { return ag.beats.Load() > 0 })
	// event: watch — пульс сразу и дальше с частотой метрик наблюдения; хук получает наблюдение.
	before := ag.beats.Load()
	srv.SendJSON("w", protocol.SSEWatch, protocol.Watch{MetricsIntervalMs: 1000, LogLevel: "debug"})
	eventually(t, "пульс раз в секунду", func() bool { return ag.beats.Load() >= before+3 })
	if v, _ := watched.Load().(protocol.Watch); v.LogLevel != "debug" {
		t.Fatalf("хук Watch: %+v", v)
	}

	srv.SendJSON("w", protocol.SSEDesired, protocol.WorkerDesiredEvent{Configs: map[string]protocol.ConfigValue{
		"main": {Version: 2, Data: json.RawMessage(`{"a":1}`)}}})
	eventually(t, "настройка применена", func() bool {
		ag.mu.Lock()
		defer ag.mu.Unlock()
		return len(ag.applied) == 1 && ag.applied[0].OK && ag.applied[0].Version == 2
	})
	if v, _ := configured.Load("main"); v != `{"a":1}` {
		t.Fatalf("Configure: %v", v)
	}
	// Та же версия — не применяется снова; неверное значение — ok: false.
	srv.SendJSON("w", protocol.SSEDesired, protocol.WorkerDesiredEvent{Configs: map[string]protocol.ConfigValue{
		"main": {Version: 2, Data: json.RawMessage(`{"a":1}`)}}})
	srv.SendJSON("w", protocol.SSEDesired, protocol.WorkerDesiredEvent{Configs: map[string]protocol.ConfigValue{
		"main": {Version: 3, Data: json.RawMessage(`"bad"`)}}})
	eventually(t, "неверное значение", func() bool {
		ag.mu.Lock()
		defer ag.mu.Unlock()
		return len(ag.applied) == 2 && !ag.applied[1].OK && ag.applied[1].Error.Code == protocol.CodeConfigRejected
	})

	srv.SendJSON("w", protocol.SSECommand, protocol.Command{ID: "c1", Type: "w.run"})
	if r := ag.result(t, "c1"); !r.OK || string(r.Result) != `{"id":"c1"}` {
		t.Fatalf("итог: %+v", r)
	}
	ag.mu.Lock()
	if len(ag.progress) != 1 || ag.progress[0].Re != "c1" || ag.progress[0].Progress != 0.5 {
		t.Fatalf("ход: %+v", ag.progress)
	}
	ag.results = nil
	ag.mu.Unlock()
	srv.SendJSON("w", protocol.SSECommand, protocol.Command{ID: "c1", Type: "w.run"})
	if r := ag.result(t, "c1"); !r.OK || runs.Load() != 1 {
		t.Fatalf("повтор — тот же итог без выполнения: %+v, выполнений %d", r, runs.Load())
	}

	srv.SendJSON("w", protocol.SSECommand, protocol.Command{ID: "c2", Type: "w.wait"})
	time.Sleep(100 * time.Millisecond)
	srv.SendJSON("w", protocol.SSECancel, protocol.Cancel{Re: "c2"})
	if r := ag.result(t, "c2"); r.OK || r.Error.Code != protocol.CodeCancelled {
		t.Fatalf("отмена: %+v", r)
	}
	srv.SendJSON("w", protocol.SSECommand, protocol.Command{ID: "c3", Type: "w.nope"})
	if r := ag.result(t, "c3"); r.OK || r.Error.Code != protocol.CodeUnknownCommand {
		t.Fatalf("незнакомая: %+v", r)
	}

	if err := w.Emit(ctx, "w.done", map[string]int{"n": 1}); err != nil {
		t.Fatal(err)
	}
	var ei *protocol.ErrorInfo
	if err := w.Emit(ctx, "w.other", nil); !errors.As(err, &ei) || ei.Code != protocol.CodeEventUndeclared {
		t.Fatalf("необъявленное событие: %v", err)
	}
	if data, err := w.Request(ctx, "w.ask", nil, time.Second); err != nil || string(data) != `{"x":1}` {
		t.Fatalf("запрос: %s %v", data, err)
	}
	if _, err := w.Request(ctx, "w.nope", nil, time.Second); !errors.As(err, &ei) || ei.Code != protocol.CodeRequestUndeclared {
		t.Fatalf("необъявленный запрос: %v", err)
	}

	bad, _ := worker.New(worker.Options{Socket: sock, Token: "wrong", Log: func(string, ...any) {}})
	if err := bad.Emit(ctx, "w.done", nil); !errors.As(err, &ei) || ei.Code != protocol.CodeUnauthorized {
		t.Fatalf("чужой токен: %v", err)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Run не завершился")
	}
}
