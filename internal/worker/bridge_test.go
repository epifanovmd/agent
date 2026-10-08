//go:build unix

package worker

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"os"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/commands"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/jobs"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/state"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// primitivesWorker — воркер без очередей: команды, домен, каналы, события.
func primitivesWorker() {
	conn, err := net.FileConn(os.NewFile(3, "ipc"))
	if err != nil {
		os.Exit(2)
	}
	var mu sync.Mutex
	send := func(env message.Envelope) {
		raw, _ := json.Marshal(env)
		mu.Lock()
		_, _ = conn.Write(append(raw, '\n'))
		mu.Unlock()
	}
	send(message.MustNew(message.TypeWorkerRegister, message.WorkerRegister{
		Name: "svc", Version: "1", SDK: "test",
		Queues:   []message.QueueCapacity{{Name: "bad/queue", Concurrency: 1}},
		Commands: []string{"svc.say", "svc.hang", "agent.evil", "svc bad"},
		Domains:  []string{"svc", "-svc"},
		Channels: []string{"svc", "host", "канал"},
	}))
	scanner := bufio.NewScanner(conn)
	for scanner.Scan() {
		var env message.Envelope
		_ = json.Unmarshal(scanner.Bytes(), &env)
		switch env.Type {
		case message.TypeWorkerDrain:
			os.Exit(0)
		case message.TypeWorkerReady:
			var ready message.WorkerReady
			_ = env.Decode(&ready)
			send(message.MustNew(message.TypeTelemetry, message.Telemetry{Channel: "svc", Data: json.RawMessage(`{"n":1}`)}))
			send(message.MustNew(message.TypeTelemetry, message.Telemetry{Channel: "host", Data: json.RawMessage(`{}`)}))
			raw, _ := json.Marshal(ready.Rejected)
			send(message.MustNew(message.TypeEvent, message.Event{Type: "started", Data: raw}))
		case message.TypeCmdRun:
			var run message.CommandRun
			_ = env.Decode(&run)
			if run.Name == "svc.say" {
				send(message.MustNew(message.TypeCmdOutput, message.CommandOutput{CommandID: run.CommandID, Chunk: "говорю\n"}))
				send(message.MustNew(message.TypeCmdDone, message.CommandDone{CommandID: run.CommandID, OK: true, Result: run.Args}))
			}
		case message.TypeCmdCancel:
			var ref message.CommandRef
			_ = env.Decode(&ref)
			raw, _ := json.Marshal(ref.CommandID)
			send(message.MustNew(message.TypeEvent, message.Event{Type: "cancelled", Data: raw}))
		case message.TypeStatePut:
			var put message.StatePut
			_ = env.Decode(&put)
			applied := message.StateApplied{Domain: put.Domain, Version: put.Version, OK: true, Report: put.Spec}
			if string(put.Spec) == `{"fail":true}` {
				// Не применилось — с отчётом (StateFailed в SDK).
				applied = message.StateApplied{Domain: put.Domain, Version: put.Version, Error: "порт занят", Report: json.RawMessage(`{"busy":8080}`)}
			}
			reply := message.MustNew(message.TypeStateApplied, applied)
			reply.Re = env.ID
			send(reply)
		}
	}
}

// syncBuffer — лог в память для проверки записей.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

type sink struct {
	mu       sync.Mutex
	declared []string
	latest   map[string]string
}

func (s *sink) Declare(ch string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.declared = append(s.declared, ch)
}
func (s *sink) Has(ch string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return ch == "host" || slices.Contains(s.declared, ch)
}
func (s *sink) Report(ch string, data json.RawMessage) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.latest == nil {
		s.latest = map[string]string{}
	}
	s.latest[ch] = string(data)
}
func (s *sink) Drop(ch string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.latest, ch)
}
func (s *sink) Undeclare(ch string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.declared = slices.DeleteFunc(s.declared, func(c string) bool { return c == ch })
	delete(s.latest, ch)
}
func (s *sink) get(ch string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.latest[ch]
}

func (r *recorder) all(typ string) []any {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]any(nil), r.msgs[typ]...)
}

func TestBridgeCommandsStateTelemetryEvents(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	rec := &recorder{}
	reg := commands.New(rec, logx.Discard())
	reg.Register("agent.logs", func(context.Context, json.RawMessage, io.Writer) (any, error) { return nil, nil })
	st := state.New(t.TempDir(), rec, logx.Discard())
	tel := &sink{}
	var changed atomic.Int32
	logs := &syncBuffer{}
	sup := New([]config.Worker{{
		Name: "svc", Command: []string{exe}, Replicas: 1, StopTimeout: config.Duration(5 * time.Second),
		Env: map[string]string{"TEST_WORKER": "primitives"},
	}}, jobs.New(rec, logx.Discard(), func() {}), slog.New(slog.NewTextHandler(logs, nil)), func() {}, "test")
	sup.SetBridge(Bridge{Commands: reg, State: st, Telemetry: tel, Events: rec, Changed: func() { changed.Add(1) }})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = st.Start(ctx) }()
	go func() { _ = sup.Start(ctx) }()

	waitFor(t, "команды воркера подключены", func() bool { return reg.Has("svc.say") && changed.Load() > 0 })
	if reg.Has("agent.evil") {
		t.Fatal("зарезервированное имя не должно подключаться")
	}
	waitFor(t, "телеметрия канала", func() bool { return tel.get("svc") == `{"n":1}` })
	if tel.get("host") != "" {
		t.Fatal("чужой канал host не должен приниматься от воркера")
	}
	waitFor(t, "событие", func() bool { return len(rec.all(message.TypeEvent)) == 1 })
	started := rec.all(message.TypeEvent)[0].(message.Event)
	if started.Source != "svc" || string(started.Data) != `["bad/queue","agent.evil","svc bad","-svc","host","канал"]` {
		t.Fatalf("событие и отклонённые имена: %+v %s", started, started.Data)
	}
	if reg.Has("svc bad") || st.Has("-svc") || tel.Has("канал") {
		t.Fatal("имя не по правилу не должно подключаться")
	}
	if log := logs.String(); !strings.Contains(log, "имена не по правилу") || !strings.Contains(log, "svc bad") {
		t.Fatalf("в логе нет записи о неверных именах:\n%s", log)
	}

	// Команда: вывод потоком, итог — результат воркера.
	_ = reg.Handle(ctx, message.MustNew(message.TypeCmdRun, message.CommandRun{CommandID: "c1", Name: "svc.say", Args: json.RawMessage(`{"x":1}`), TimeoutSec: 5}))
	waitFor(t, "итог команды", func() bool { return len(rec.all(message.TypeCmdDone)) == 1 })
	done := rec.all(message.TypeCmdDone)[0].(message.CommandDone)
	if !done.OK || string(done.Result) != `{"x":1}` {
		t.Fatalf("итог: %+v", done)
	}
	waitFor(t, "вывод команды", func() bool { return len(rec.all(message.TypeCmdOutput)) == 1 })

	// Срок истёк: TIMEOUT серверу, cmd.cancel воркеру.
	_ = reg.Handle(ctx, message.MustNew(message.TypeCmdRun, message.CommandRun{CommandID: "c2", Name: "svc.hang", TimeoutSec: 1}))
	waitFor(t, "таймаут команды", func() bool { return len(rec.all(message.TypeCmdDone)) == 2 })
	if hang := rec.all(message.TypeCmdDone)[1].(message.CommandDone); hang.OK || hang.Error.Code != "TIMEOUT" {
		t.Fatalf("таймаут: %+v", hang)
	}
	waitFor(t, "отмена дошла до воркера", func() bool { return len(rec.all(message.TypeEvent)) == 2 })

	// Состояние: снимок применяет воркер, после замены — снова.
	_ = st.Handle(ctx, message.MustNew(message.TypeStatePut, message.StatePut{Domain: "svc", Version: 1, Spec: json.RawMessage(`{"port":1}`)}))
	waitFor(t, "снимок применён", func() bool { return len(rec.all(message.TypeStateApplied)) == 1 })
	applied := rec.all(message.TypeStateApplied)[0].(message.StateApplied)
	if !applied.OK || string(applied.Report) != `{"port":1}` {
		t.Fatalf("применение: %+v", applied)
	}
	restart, stop := context.WithTimeout(ctx, 10*time.Second)
	defer stop()
	if err := sup.Restart(restart, "svc"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "новый экземпляр получил снимок", func() bool { return len(rec.all(message.TypeStateApplied)) == 2 })

	// Не применилось с отчётом: отчёт доходит до сервера вместе с ошибкой.
	_ = st.Handle(ctx, message.MustNew(message.TypeStatePut, message.StatePut{Domain: "svc", Version: 2, Spec: json.RawMessage(`{"fail":true}`)}))
	waitFor(t, "ошибка с отчётом", func() bool { return len(rec.all(message.TypeStateApplied)) == 3 })
	if failed := rec.all(message.TypeStateApplied)[2].(message.StateApplied); failed.OK || failed.Error != "порт занят" || string(failed.Report) != `{"busy":8080}` {
		t.Fatalf("ошибка с отчётом: %+v %s", failed, failed.Report)
	}

	stopCtx, done2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer done2()
	sup.Stop(stopCtx)
	waitFor(t, "данные канала сброшены", func() bool { return tel.get("svc") == "" })
}
