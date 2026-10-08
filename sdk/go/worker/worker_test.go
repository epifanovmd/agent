package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"encoding/pem"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epifanovmd/agent/sdk/go/internal/examples"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// fakeAgent — сторона агента в канале IPC (net.Pipe).
type fakeAgent struct {
	t    *testing.T
	conn net.Conn
	in   chan message.Envelope
}

func newFake(t *testing.T) (*fakeAgent, net.Conn) {
	agentSide, workerSide := net.Pipe()
	a := &fakeAgent{t: t, conn: agentSide, in: make(chan message.Envelope, 256)}
	go func() {
		defer close(a.in)
		sc := bufio.NewScanner(agentSide)
		sc.Buffer(make([]byte, 64*1024), 16<<20)
		for sc.Scan() {
			var env message.Envelope
			if err := json.Unmarshal(sc.Bytes(), &env); err != nil {
				t.Errorf("воркер прислал не JSON: %s", sc.Bytes())
				continue
			}
			a.in <- env
		}
	}()
	t.Cleanup(func() { agentSide.Close() })
	return a, workerSide
}

func (a *fakeAgent) send(typ string, data any, id string) {
	a.t.Helper()
	env := message.MustNew(typ, data)
	env.ID = id
	raw, _ := json.Marshal(env)
	if _, err := a.conn.Write(append(raw, '\n')); err != nil {
		a.t.Fatalf("агент → воркер: %v", err)
	}
}

// next — следующее сообщение воркера (пропуская типы skip).
func (a *fakeAgent) next(skip ...string) message.Envelope {
	a.t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case env, ok := <-a.in:
			if !ok {
				a.t.Fatal("канал закрыт")
			}
			if contains(skip, env.Type) {
				continue
			}
			return env
		case <-timeout:
			a.t.Fatal("нет сообщения от воркера")
		}
	}
}

// expect — следующее сообщение типа typ (пропуская skip).
func (a *fakeAgent) expect(typ string, v any, skip ...string) message.Envelope {
	a.t.Helper()
	env := a.next(skip...)
	if env.Type != typ {
		a.t.Fatalf("ждали %s, пришло %s: %s", typ, env.Type, env.Data)
	}
	if v != nil {
		if err := env.Decode(v); err != nil {
			a.t.Fatal(err)
		}
	}
	return env
}

// quiet — воркер ничего не прислал за d (кроме skip).
func (a *fakeAgent) quiet(d time.Duration, skip ...string) {
	a.t.Helper()
	timeout := time.After(d)
	for {
		select {
		case env, ok := <-a.in:
			if !ok || contains(skip, env.Type) {
				continue
			}
			a.t.Fatalf("лишнее сообщение %s: %s", env.Type, env.Data)
		case <-timeout:
			return
		}
	}
}

func contains(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

// start — воркер против фейкового агента: регистрация и worker.ready.
func start(t *testing.T, w *Worker, a *fakeAgent) (stop func() error) {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background()) }()
	a.expect(message.TypeWorkerRegister, nil)
	a.send(message.TypeWorkerReady, message.WorkerReady{AgentVersion: "test"}, "")
	var once sync.Once
	var result error
	wait := func() error {
		once.Do(func() {
			select {
			case result = <-done:
			case <-time.After(10 * time.Second):
				t.Fatal("Run не вернулся")
			}
		})
		return result
	}
	t.Cleanup(func() {
		a.conn.Close()
		_ = wait()
	})
	return wait
}

func newWorker(t *testing.T, name string) (*Worker, *fakeAgent) {
	a, conn := newFake(t)
	return New(name, "2.0.0", WithConn(conn), WithLogger(quietLog()), WithoutSignals()), a
}

// example — конверт образца sdk/spec/examples по имени.
func example(t *testing.T, name string) map[string]any {
	t.Helper()
	var env map[string]any
	if err := json.Unmarshal(examples.Message(t, name), &env); err != nil {
		t.Fatal(err)
	}
	return env
}

func TestRegisterMatchesExample(t *testing.T) {
	w, a := newWorker(t, "report")
	w.Job("example.convert", 2, func(context.Context, *Job) (any, error) { return nil, nil })
	w.Command("example.app.reload", func(context.Context, *Command) (any, error) { return nil, nil })
	w.State("example.app", func(context.Context, int64, json.RawMessage) (any, error) { return nil, nil })
	w.Channel("example.app")
	go func() { _ = w.Run(context.Background()) }()
	env := a.expect(message.TypeWorkerRegister, nil)

	var got map[string]any
	_ = json.Unmarshal(env.Data, &got)
	want := example(t, "worker.register")["data"].(map[string]any)
	if !strings.HasPrefix(got["sdk"].(string), "go/") {
		t.Fatalf("sdk: %v", got["sdk"])
	}
	got["sdk"], want["sdk"] = nil, nil
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("worker.register:\n got %v\nwant %v", got, want)
	}
	if env.Type != example(t, "worker.register")["type"] {
		t.Fatal(env.Type)
	}
}

func TestRegisterQueuesAlwaysPresent(t *testing.T) {
	w, a := newWorker(t, "svc")
	w.Command("example.ping", func(context.Context, *Command) (any, error) { return nil, nil })
	go func() { _ = w.Run(context.Background()) }()
	env := a.expect(message.TypeWorkerRegister, nil)
	var got map[string]any
	_ = json.Unmarshal(env.Data, &got)
	if q, ok := got["queues"].([]any); !ok || len(q) != 0 {
		t.Fatalf("queues: %v", got["queues"])
	}
	for _, key := range []string{"domains", "channels"} {
		if _, ok := got[key]; ok {
			t.Fatalf("пустой список %s не отправляется: %s", key, env.Data)
		}
	}
}

func TestReadyRejected(t *testing.T) {
	w, a := newWorker(t, "svc")
	w.Command("agent.reboot", func(context.Context, *Command) (any, error) { return nil, nil })
	go func() { _ = w.Run(context.Background()) }()
	a.expect(message.TypeWorkerRegister, nil)
	a.send(message.TypeWorkerReady, message.WorkerReady{AgentVersion: "t", Rejected: []string{"agent.reboot"}}, "")
	deadline := time.Now().Add(5 * time.Second)
	for len(w.Rejected()) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("rejected не получены")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func assign(id string, attempt int) message.JobAssign {
	return message.JobAssign{JobID: id, Attempt: attempt, Queue: "example.q", Data: json.RawMessage(`{"n":2}`), LeaseSeconds: 60}
}

func TestJobComplete(t *testing.T) {
	w, a := newWorker(t, "jobs")
	w.Job("example.q", 1, func(_ context.Context, job *Job) (any, error) {
		for i := 0; i < 20; i++ {
			job.Progress(float64(i)/20, "шаг")
		}
		job.Log("строка")
		if err := job.Event("stage", map[string]int{"n": 1}); err != nil {
			return nil, err
		}
		_ = job.Event("stage", map[string]int{"n": 2})
		var data struct{ N int }
		_ = json.Unmarshal(job.Data, &data)
		return map[string]int{"double": data.N * 2}, nil
	})
	start(t, w, a)
	a.send(message.TypeJobAssign, assign("j1", 0), "")

	progress := 0
	var seqs []int64
	for {
		env := a.next()
		switch env.Type {
		case message.TypeJobProgress:
			progress++
		case message.TypeJobEvent:
			var e message.JobEvent
			_ = env.Decode(&e)
			seqs = append(seqs, e.Seq)
		case message.TypeJobComplete:
			var c message.JobComplete
			_ = env.Decode(&c)
			if c.JobID != "j1" || string(c.Result) != `{"double":4}` {
				t.Fatalf("job.complete: %s", env.Data)
			}
			if progress == 0 || progress > 3 {
				t.Fatalf("прогресс не схлопнут: %d сообщений", progress)
			}
			if !reflect.DeepEqual(seqs, []int64{1, 2}) {
				t.Fatalf("seq событий: %v", seqs)
			}
			return
		default:
			t.Fatalf("лишнее: %s", env.Type)
		}
	}
}

func TestJobFailures(t *testing.T) {
	w, a := newWorker(t, "jobs")
	w.Job("example.q", 3, func(_ context.Context, job *Job) (any, error) {
		switch job.ID {
		case "coded":
			return nil, Fail("BAD_INPUT", "плохо", false)
		case "plain":
			return nil, errors.New("сломалось")
		default:
			panic("паника")
		}
	})
	start(t, w, a)
	want := map[string]message.JobFail{
		"coded": {Code: "BAD_INPUT", Message: "плохо", Retryable: false},
		"plain": {Code: CodeWorkerError, Message: "сломалось", Retryable: true},
		"panic": {Code: CodeWorkerError, Retryable: true},
	}
	for id := range want {
		a.send(message.TypeJobAssign, message.JobAssign{JobID: id, Queue: "example.q"}, "")
	}
	for range want {
		var f message.JobFail
		a.expect(message.TypeJobFail, &f)
		exp := want[f.JobID]
		if f.Code != exp.Code || f.Retryable != exp.Retryable || (exp.Message != "" && f.Message != exp.Message) {
			t.Fatalf("%s: %+v", f.JobID, f)
		}
	}
}

func TestJobCancelAndStop(t *testing.T) {
	w, a := newWorker(t, "jobs")
	var cancelled atomic.Bool
	w.Job("example.q", 2, func(ctx context.Context, job *Job) (any, error) {
		select {
		case <-ctx.Done():
			cancelled.Store(true)
			return nil, ctx.Err()
		case <-job.StopRequested():
			return map[string]bool{"stopped": true}, nil
		}
	})
	start(t, w, a)
	a.send(message.TypeJobAssign, assign("c", 0), "")
	a.send(message.TypeJobAssign, assign("s", 1), "")
	time.Sleep(50 * time.Millisecond)
	a.send(message.TypeJobCancel, message.JobRef{JobID: "c", Attempt: 0}, "")
	a.send(message.TypeJobStop, message.JobRef{JobID: "s", Attempt: 0}, "") // чужая попытка — мимо
	a.quiet(200 * time.Millisecond)
	a.send(message.TypeJobStop, message.JobRef{JobID: "s", Attempt: 1}, "")
	var c message.JobComplete
	a.expect(message.TypeJobComplete, &c)
	if c.JobID != "s" || string(c.Result) != `{"stopped":true}` {
		t.Fatalf("job.complete: %+v", c)
	}
	a.quiet(200 * time.Millisecond)
	if !cancelled.Load() {
		t.Fatal("ctx отменённой задачи не отменён")
	}
}

func TestDrain(t *testing.T) {
	w, a := newWorker(t, "jobs")
	release := make(chan struct{})
	w.Job("example.q", 2, func(context.Context, *Job) (any, error) {
		<-release
		return "ok", nil
	})
	wait := start(t, w, a)
	a.send(message.TypeJobAssign, assign("running", 0), "")
	time.Sleep(50 * time.Millisecond)
	a.send(message.TypeWorkerDrain, struct{}{}, "")
	a.send(message.TypeJobAssign, assign("late", 0), "")
	var f message.JobFail
	a.expect(message.TypeJobFail, &f)
	if f.JobID != "late" || f.Code != CodeWorkerStopping || !f.Retryable {
		t.Fatalf("задача после drain: %+v", f)
	}
	close(release)
	var c message.JobComplete
	a.expect(message.TypeJobComplete, &c)
	if c.JobID != "running" {
		t.Fatalf("доработка: %+v", c)
	}
	if err := wait(); err != nil {
		t.Fatalf("Run после drain: %v", err)
	}
}

func TestChannelClosedCancelsWork(t *testing.T) {
	w, a := newWorker(t, "jobs")
	cancelled := make(chan struct{})
	w.Job("example.q", 1, func(ctx context.Context, _ *Job) (any, error) {
		<-ctx.Done()
		close(cancelled)
		return nil, ctx.Err()
	})
	wait := start(t, w, a)
	a.send(message.TypeJobAssign, assign("j", 0), "")
	time.Sleep(50 * time.Millisecond)
	a.conn.Close()
	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("задача не отменена после закрытия канала")
	}
	if err := wait(); err != nil {
		t.Fatalf("Run: %v", err)
	}
}

func TestCommands(t *testing.T) {
	w, a := newWorker(t, "cmds")
	w.Command("example.echo", func(_ context.Context, cmd *Command) (any, error) {
		var args struct{ Text string }
		_ = json.Unmarshal(cmd.Args, &args)
		_, _ = io.WriteString(cmd, "привет ")
		_, _ = cmd.Write([]byte("мир\n")[:3]) // половина символа «м» — дописывается следующим Write
		_, _ = cmd.Write([]byte("мир\n")[3:])
		return map[string]string{"echo": args.Text}, nil
	})
	w.Command("example.fail", func(context.Context, *Command) (any, error) {
		return nil, CommandError("NOT_FOUND", "нет")
	})
	w.Command("example.panic", func(context.Context, *Command) (any, error) { panic("ой") })
	w.Command("example.slow", func(ctx context.Context, _ *Command) (any, error) {
		<-ctx.Done()
		return "поздно", nil
	})
	start(t, w, a)

	a.send(message.TypeCmdRun, message.CommandRun{CommandID: "c1", Name: "example.echo", Args: json.RawMessage(`{"text":"x"}`), TimeoutSec: 5}, "")
	var out strings.Builder
	for {
		env := a.next()
		if env.Type == message.TypeCmdOutput {
			var o message.CommandOutput
			_ = env.Decode(&o)
			out.WriteString(o.Chunk)
			continue
		}
		var done message.CommandDone
		_ = env.Decode(&done)
		if env.Type != message.TypeCmdDone || !done.OK || string(done.Result) != `{"echo":"x"}` {
			t.Fatalf("итог: %s %s", env.Type, env.Data)
		}
		break
	}
	if out.String() != "привет мир\n" {
		t.Fatalf("вывод: %q", out.String())
	}

	for name, code := range map[string]string{"example.fail": "NOT_FOUND", "example.panic": CodeCommandFailed, "example.nope": CodeCommandUnknown} {
		a.send(message.TypeCmdRun, message.CommandRun{CommandID: name, Name: name, TimeoutSec: 5}, "")
		var done message.CommandDone
		a.expect(message.TypeCmdDone, &done)
		if done.OK || done.Error == nil || done.Error.Code != code {
			t.Fatalf("%s: %+v", name, done)
		}
	}

	a.send(message.TypeCmdRun, message.CommandRun{CommandID: "slow", Name: "example.slow", TimeoutSec: 1}, "")
	time.Sleep(50 * time.Millisecond)
	a.send(message.TypeCmdCancel, message.CommandRef{CommandID: "slow"}, "")
	a.quiet(300 * time.Millisecond)
}

func TestLongOutputChunks(t *testing.T) {
	w, a := newWorker(t, "cmds")
	text := strings.Repeat("я", chunkMax) // 128 КБ
	w.Command("example.big", func(_ context.Context, cmd *Command) (any, error) {
		_, err := io.WriteString(cmd, text)
		return nil, err
	})
	start(t, w, a)
	a.send(message.TypeCmdRun, message.CommandRun{CommandID: "b", Name: "example.big"}, "")
	var got strings.Builder
	for {
		env := a.next()
		if env.Type == message.TypeCmdDone {
			break
		}
		var o message.CommandOutput
		_ = env.Decode(&o)
		if len(o.Chunk) > chunkMax {
			t.Fatalf("кусок %d байт", len(o.Chunk))
		}
		got.WriteString(o.Chunk)
	}
	if got.String() != text {
		t.Fatal("вывод искажён")
	}
}

func TestState(t *testing.T) {
	w, a := newWorker(t, "state")
	w.State("example.kv", func(_ context.Context, version int64, spec json.RawMessage) (any, error) {
		if version == 13 {
			return nil, errors.New("не применить")
		}
		if version == 14 {
			return nil, StateFailed("порт занят", map[string]int{"port": 8080})
		}
		var m map[string]any
		_ = json.Unmarshal(spec, &m)
		return map[string]int{"keys": len(m)}, nil
	})
	start(t, w, a)
	put := example(t, "state.put@agent")
	data, _ := json.Marshal(put["data"])
	var p message.StatePut
	_ = json.Unmarshal(data, &p)
	p.Domain = "example.kv"
	a.send(message.TypeStatePut, p, put["id"].(string))
	var applied message.StateApplied
	env := a.expect(message.TypeStateApplied, &applied)
	if env.Re != put["id"] || !applied.OK || applied.Version != 12 || string(applied.Report) != `{"keys":1}` {
		t.Fatalf("state.applied: re=%s %s", env.Re, env.Data)
	}
	a.send(message.TypeStatePut, message.StatePut{Domain: "example.kv", Version: 13, Spec: json.RawMessage(`{}`)}, "p2")
	applied = message.StateApplied{}
	env = a.expect(message.TypeStateApplied, &applied)
	if env.Re != "p2" || applied.OK || applied.Error == "" || applied.Report != nil {
		t.Fatalf("ошибка применения: %s", env.Data)
	}
	// StateFailed — ok: false, ошибка и отчёт вместе.
	a.send(message.TypeStatePut, message.StatePut{Domain: "example.kv", Version: 14, Spec: json.RawMessage(`{}`)}, "p3")
	applied = message.StateApplied{}
	env = a.expect(message.TypeStateApplied, &applied)
	if env.Re != "p3" || applied.OK || applied.Error != "порт занят" || string(applied.Report) != `{"port":8080}` {
		t.Fatalf("ошибка с отчётом: %s", env.Data)
	}
}

func TestTelemetryAndEvents(t *testing.T) {
	w, a := newWorker(t, "tele")
	var calls atomic.Int32
	w.Telemetry("example.sys", 20*time.Millisecond, func() any {
		if calls.Add(1) == 1 {
			panic("сбой источника") // в лог, не падение
		}
		return map[string]int{"n": 1}
	})
	w.Channel("example.manual")
	if err := w.Report("example.nope", 1); err == nil {
		t.Fatal("необъявленный канал принят")
	}
	start(t, w, a)
	var tel message.Telemetry
	a.expect(message.TypeTelemetry, &tel)
	if tel.Channel != "example.sys" || string(tel.Data) != `{"n":1}` {
		t.Fatalf("телеметрия: %+v", tel)
	}
	if err := w.Report("example.manual", map[string]bool{"up": true}); err != nil {
		t.Fatal(err)
	}
	for tel.Channel != "example.manual" {
		a.expect(message.TypeTelemetry, &tel)
	}
	if string(tel.Data) != `{"up":true}` {
		t.Fatalf("Report: %s", tel.Data)
	}
	if err := w.Event("sys.started", map[string]int{"pid": 1}); err != nil {
		t.Fatal(err)
	}
	var e message.Event
	a.expect(message.TypeEvent, &e, message.TypeTelemetry)
	if e.Type != "sys.started" || string(e.Data) != `{"pid":1}` || e.Source != "" {
		t.Fatalf("событие: %+v", e)
	}
	// Схема — как у образца event@worker.
	want := example(t, "event@worker")["data"].(map[string]any)
	if _, ok := want["type"]; !ok {
		t.Fatal("образец события без type")
	}
}

func TestUploadRetriesWithFreshURL(t *testing.T) {
	uploadRetryDelay = 10 * time.Millisecond
	t.Cleanup(func() { uploadRetryDelay = 5 * time.Second })
	var mu sync.Mutex
	stored := map[string]string{}
	var puts atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/in/source":
			_, _ = io.WriteString(rw, "вход")
		case r.Method == http.MethodPut && r.URL.Path == "/out/stale":
			puts.Add(1)
			rw.WriteHeader(http.StatusForbidden) // ссылка истекла
		case r.Method == http.MethodPut && r.URL.Path == "/out/fresh":
			puts.Add(1)
			body, _ := io.ReadAll(r.Body)
			mu.Lock()
			stored[r.Header.Get("Content-Type")] = string(body)
			mu.Unlock()
		default:
			rw.WriteHeader(http.StatusNotFound)
		}
	}))
	defer srv.Close()

	w, a := newWorker(t, "files")
	w.Job("example.q", 1, func(ctx context.Context, job *Job) (any, error) {
		path, err := job.InputPath(ctx, "source")
		if err != nil {
			return nil, err
		}
		in, _ := os.ReadFile(path)
		if !reflectEqual(job.Inputs(), []string{"source"}) || !reflectEqual(job.Outputs(), []string{"result"}) {
			return nil, errors.New("имена файлов")
		}
		return nil, job.Upload(ctx, "result", append(in, " → выход"...))
	})
	start(t, w, a)
	a.send(message.TypeJobAssign, message.JobAssign{
		JobID: "f", Queue: "example.q",
		Inputs:  map[string]string{"source": srv.URL + "/in/source"},
		Outputs: map[string]message.OutputURL{"result": {URL: srv.URL + "/out/stale", ContentType: "text/plain"}},
	}, "")
	var req message.JobURLsRequest
	env := a.expect(message.TypeJobURLs, &req)
	if env.ID == "" || req.JobID != "f" || !reflectEqual(req.Outputs, []string{"result"}) {
		t.Fatalf("job.urls: %s %s", env.ID, env.Data)
	}
	reply := message.MustNew(message.TypeJobURLs, message.JobURLs{
		Outputs:   map[string]message.OutputURL{"result": {URL: srv.URL + "/out/fresh", ContentType: "text/plain"}},
		ExpiresAt: time.Now().Add(time.Hour).UnixMilli(),
	})
	reply.Re = env.ID
	raw, _ := json.Marshal(reply)
	_, _ = a.conn.Write(append(raw, '\n'))
	var c message.JobComplete
	a.expect(message.TypeJobComplete, &c)
	mu.Lock()
	defer mu.Unlock()
	if stored["text/plain"] != "вход → выход" || puts.Load() != 2 {
		t.Fatalf("загрузка: %v, PUT: %d", stored, puts.Load())
	}
}

func reflectEqual(a, b []string) bool { return reflect.DeepEqual(a, b) }

func TestRunErrors(t *testing.T) {
	if err := New("x", "1", WithLogger(quietLog())).Run(context.Background()); err == nil {
		t.Fatal("без объявлений — ошибка")
	}
	t.Setenv("AGENT_IPC_FD", "")
	w := New("x", "1", WithLogger(quietLog()), WithoutSignals())
	w.Channel("example.c")
	if err := w.Run(context.Background()); err == nil || !strings.Contains(err.Error(), "AGENT_IPC_FD") {
		t.Fatalf("без канала: %v", err)
	}
	if err := w.Event("x", nil); err == nil {
		t.Fatal("событие до Run")
	}
}

func TestRunRejectsInvalidNames(t *testing.T) {
	w := New("x", "1", WithConn(nopConn{}), WithLogger(quietLog()), WithoutSignals())
	nop := func(context.Context, *Job) (any, error) { return nil, nil }
	w.Job("example.ok", 1, nop)
	w.Job("bad/queue", 1, nop)
	w.Command("example cmd", func(context.Context, *Command) (any, error) { return nil, nil })
	w.State("-example", func(context.Context, int64, json.RawMessage) (any, error) { return nil, nil })
	w.Telemetry("канал", time.Second, func() any { return nil })
	w.Channel(strings.Repeat("a", 65))
	err := w.Run(context.Background())
	if err == nil {
		t.Fatal("имена не по правилу — ждали ошибку Run")
	}
	for _, name := range []string{"bad/queue", "example cmd", "-example", "канал", strings.Repeat("a", 65)} {
		if !strings.Contains(err.Error(), name) {
			t.Errorf("в ошибке нет %q: %v", name, err)
		}
	}
	if strings.Contains(err.Error(), "example.ok") {
		t.Errorf("верное имя в ошибке: %v", err)
	}
}

// nopConn — транспорт, которым Run пользоваться не должен.
type nopConn struct{}

func (nopConn) Read([]byte) (int, error)    { return 0, io.EOF }
func (nopConn) Write(p []byte) (int, error) { return len(p), nil }
func (nopConn) Close() error                { return nil }

func TestContextStopsRun(t *testing.T) {
	w, a := newWorker(t, "ctx")
	w.Channel("example.c")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- w.Run(ctx) }()
	a.expect(message.TypeWorkerRegister, nil)
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run не остановлен ctx")
	}
}

func TestStoppingSignal(t *testing.T) {
	w := New("s", "1")
	select {
	case <-w.Stopping():
		t.Fatal("до drain — не уходит")
	default:
	}
	w.Drain()
	w.Drain() // повторно — без паники
	select {
	case <-w.Stopping():
	case <-time.After(time.Second):
		t.Fatal("после Drain — сигнал")
	}
}

// worker.cleanup (a2w) → worker.cleaned (w2a) по образцам: успех, ошибка,
// паника; без обработчика — успех сразу.
func TestCleanupMatchesExamples(t *testing.T) {
	req := example(t, "worker.cleanup")
	okFix := example(t, "worker.cleaned")
	failFix := example(t, "worker.cleaned.failed")
	keys := func(m map[string]any) []string {
		out := make([]string, 0, len(m))
		for k := range m {
			out = append(out, k)
		}
		slices.Sort(out)
		return out
	}

	w, a := newWorker(t, "cleanup")
	w.Command("example.noop", func(context.Context, *Command) (any, error) { return nil, nil })
	var mode atomic.Int32
	w.Cleanup(func(ctx context.Context) error {
		switch mode.Load() {
		case 1:
			return errors.New("правило не снято")
		case 2:
			panic("сбой")
		}
		return nil
	})
	start(t, w, a)
	id := req["id"].(string)
	a.send(req["type"].(string), req["data"], id)
	env := a.expect(message.TypeWorkerCleaned, nil)
	var got map[string]any
	_ = json.Unmarshal(env.Data, &got)
	if env.Type != okFix["type"] || env.Re != okFix["re"] || !reflect.DeepEqual(got, okFix["data"]) {
		t.Fatalf("worker.cleaned: re=%s %s", env.Re, env.Data)
	}
	for m := int32(1); m <= 2; m++ {
		mode.Store(m)
		a.send(message.TypeWorkerCleanup, struct{}{}, "again")
		env = a.expect(message.TypeWorkerCleaned, nil)
		got = nil
		_ = json.Unmarshal(env.Data, &got)
		if env.Re != "again" || got["ok"] != false || got["error"] == "" ||
			!reflect.DeepEqual(keys(got), keys(failFix["data"].(map[string]any))) {
			t.Fatalf("ошибка уборки (%d): %s", m, env.Data)
		}
	}

	bare, b := newWorker(t, "bare")
	bare.Command("example.noop", func(context.Context, *Command) (any, error) { return nil, nil })
	start(t, bare, b)
	b.send(message.TypeWorkerCleanup, struct{}{}, "c1")
	var cleaned message.WorkerCleaned
	if env := b.expect(message.TypeWorkerCleaned, &cleaned); env.Re != "c1" || !cleaned.OK {
		t.Fatalf("без обработчика — ok: %s", env.Data)
	}
}

// AGENT_SERVER_CA_FILE: файлы задач с сервера на своём корневом сертификате.
func TestDownloadServerCA(t *testing.T) {
	srv := httptest.NewTLSServer(http.HandlerFunc(func(rw http.ResponseWriter, r *http.Request) {
		_, _ = io.WriteString(rw, "вход")
	}))
	defer srv.Close()
	dir := t.TempDir()
	caFile := filepath.Join(dir, "ca.pem")
	if err := os.WriteFile(caFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: srv.Certificate().Raw}), 0o600); err != nil {
		t.Fatal(err)
	}
	reset := func() {
		httpClient = sync.OnceValues(func() (*http.Client, error) {
			return newHTTPClient(os.Getenv("AGENT_SERVER_CA_FILE"))
		})
	}
	t.Cleanup(reset)

	// Без переменной — только системные корни: сертификат не принят.
	t.Setenv("AGENT_SERVER_CA_FILE", "")
	reset()
	if err := download(t.Context(), srv.URL+"/f", filepath.Join(dir, "none")); err == nil {
		t.Fatal("скачано без CA сервера")
	}

	t.Setenv("AGENT_SERVER_CA_FILE", caFile)
	reset()
	path := filepath.Join(dir, "in")
	if err := download(t.Context(), srv.URL+"/f", path); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(path); string(got) != "вход" {
		t.Fatalf("содержимое: %q", got)
	}

	// Файл без PEM — ошибка с причиной.
	bad := filepath.Join(dir, "bad.pem")
	_ = os.WriteFile(bad, []byte("нет"), 0o600)
	if _, err := newHTTPClient(bad); err == nil || !strings.Contains(err.Error(), "AGENT_SERVER_CA_FILE") {
		t.Fatalf("плохой CA: %v", err)
	}
}
