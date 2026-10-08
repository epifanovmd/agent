//go:build unix

package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"net"
	"os"
	"os/signal"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/jobs"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// TestMain — тестовый бинарь служит и воркером: TEST_WORKER=1.
func TestMain(m *testing.M) {
	switch os.Getenv("TEST_WORKER") {
	case "1":
		fakeWorker()
		return
	case "primitives":
		primitivesWorker()
		return
	}
	os.Exit(m.Run())
}

// fakeWorker — воркер на IPC: echo-очередь; WORKER_CRASH=1 — падает на задаче.
func fakeWorker() {
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
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM)
	go func() {
		<-term
		os.Exit(0)
	}()
	version := os.Getenv("WORKER_VERSION")
	if version == "" {
		version = os.Getenv("AGENT_WORKER_RELEASE_VERSION") // воркер из выпуска
	}
	// WORKER_PING: answer — отвечает на worker.ping, silent — нет.
	ping := os.Getenv("WORKER_PING")
	var cmds []string
	if c := os.Getenv("WORKER_COMMANDS"); c != "" {
		cmds = strings.Split(c, ",")
	}
	send(message.MustNew(message.TypeWorkerRegister, message.WorkerRegister{
		Name: "echo", Version: version, SDK: "test", Ping: ping != "",
		Queues:   []message.QueueCapacity{{Name: "echo", Concurrency: 2}, {Name: "hidden", Concurrency: 1}},
		Commands: cmds,
	}))
	// IPC_LOG — записывать каждое сообщение агента строкой «тип данные».
	var ipcLog *os.File
	if path := os.Getenv("IPC_LOG"); path != "" {
		ipcLog, _ = os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	}
	scanner := bufio.NewScanner(conn)
	var pending *message.JobAssign
	for scanner.Scan() {
		var env message.Envelope
		_ = json.Unmarshal(scanner.Bytes(), &env)
		if ipcLog != nil {
			_, _ = ipcLog.WriteString(env.Type + " " + string(env.Data) + "\n")
		}
		switch env.Type {
		case message.TypeWorkerDrain:
			os.Exit(0)
		case message.TypeWorkerPing:
			if ping == "answer" {
				pong := message.MustNew(message.TypeWorkerPong, struct{}{})
				pong.Re = env.ID
				send(pong)
			}
		case message.TypeStatePut:
			// WORKER_STATE: ok (по умолчанию) | silent (не отвечает).
			if os.Getenv("WORKER_STATE") == "silent" {
				continue
			}
			var put message.StatePut
			_ = env.Decode(&put)
			reply := message.MustNew(message.TypeStateApplied, message.StateApplied{Domain: put.Domain, Version: put.Version, OK: true})
			reply.Re = env.ID
			send(reply)
		case message.TypeWorkerCleanup:
			// WORKER_CLEANUP: ok (по умолчанию) | fail | silent (не отвечает).
			reply := message.WorkerCleaned{OK: true}
			switch os.Getenv("WORKER_CLEANUP") {
			case "silent":
				continue
			case "fail":
				reply = message.WorkerCleaned{Error: "правило не снято"}
			}
			if mark := os.Getenv("CLEANUP_MARK"); mark != "" {
				_ = os.WriteFile(mark, []byte("cleaned"), 0o600)
			}
			out := message.MustNew(message.TypeWorkerCleaned, reply)
			out.Re = env.ID
			send(out)
		case message.TypeJobAssign:
			if os.Getenv("WORKER_CRASH") == "1" {
				os.Exit(1)
			}
			if os.Getenv("WORKER_HANG") == "1" {
				continue // задача «зависла»: ни итога, ни реакции на отмену
			}
			var a message.JobAssign
			_ = env.Decode(&a)
			p := 0.5
			send(message.MustNew(message.TypeJobProgress, message.JobProgress{JobRef: a.Ref(), Progress: &p}))
			req := message.MustNew(message.TypeJobURLs, message.JobURLsRequest{JobRef: a.Ref()})
			req.ID = "u1"
			pending = &a
			send(req)
		case message.TypeJobURLs:
			// Итог — после ответа на запрос ссылок, как в SDK.
			if env.Re == "u1" && pending != nil {
				send(message.MustNew(message.TypeJobComplete, message.JobComplete{JobRef: pending.Ref(), Result: pending.Data}))
				pending = nil
			}
		}
	}
}

type recorder struct {
	mu   sync.Mutex
	msgs map[string][]any
}

func (r *recorder) add(typ string, data any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.msgs == nil {
		r.msgs = map[string][]any{}
	}
	r.msgs[typ] = append(r.msgs[typ], data)
}
func (r *recorder) Stream(typ string, data any) { r.add(typ, data) }
func (r *recorder) Reliable(typ string, data any) error {
	r.add(typ, data)
	return nil
}
func (r *recorder) Request(_ context.Context, typ string, _ any, out any) error {
	r.add(typ, nil)
	return json.Unmarshal([]byte(`{"inputs":{},"outputs":{},"expiresAt":7}`), out)
}
func (r *recorder) count(typ string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.msgs[typ])
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func setup(t *testing.T, env map[string]string) (*Supervisor, *jobs.Manager, *recorder, context.CancelFunc) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	env["TEST_WORKER"] = "1"
	rec := &recorder{}
	manager := jobs.New(rec, logx.Discard(), func() {})
	spec := config.Worker{
		Name: "echo", Command: []string{exe}, Env: env, Replicas: 1,
		Queues: []string{"echo"}, StopTimeout: config.Duration(5 * time.Second),
	}
	sup := New([]config.Worker{spec}, manager, logx.Discard(), func() {}, "test")
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = sup.Start(ctx) }()
	return sup, manager, rec, cancel
}

func slots(m *jobs.Manager) map[string]int {
	st := message.Status{Slots: map[string]int{}}
	m.ContributeStatus(&st)
	return st.Slots
}

func assign(m *jobs.Manager, id string) {
	_ = m.Handle(context.Background(), message.MustNew(message.TypeJobAssign, message.JobAssign{
		JobID: id, Queue: "echo", Data: json.RawMessage(`{"text":"hi"}`),
	}))
}

func TestRegisterRunAndQueueFilter(t *testing.T) {
	sup, manager, rec, cancel := setup(t, map[string]string{"WORKER_VERSION": "1.0"})
	defer cancel()
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	if _, ok := slots(manager)["hidden"]; ok {
		t.Fatal("очередь вне фильтра конфигурации не должна обслуживаться")
	}

	assign(manager, "j1")
	waitFor(t, "итог задачи", func() bool { return rec.count(message.TypeJobComplete) == 1 })
	if rec.count(message.TypeJobProgress) != 1 || rec.count(message.TypeJobURLs) != 1 {
		t.Fatalf("прогресс и запрос ссылок: %v", rec.msgs)
	}

	st := message.Status{Slots: map[string]int{}}
	sup.ContributeStatus(&st)
	if len(st.Workers) != 1 || st.Workers[0].State != "running" || st.Workers[0].Version != "1.0" {
		t.Fatalf("статус воркера: %+v", st.Workers)
	}

	stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	sup.Stop(stopCtx)
}

func TestCrashFailsJobAndRestarts(t *testing.T) {
	_, manager, rec, cancel := setup(t, map[string]string{"WORKER_CRASH": "1"})
	defer cancel()
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	assign(manager, "crash")
	waitFor(t, "провал задачи упавшего воркера", func() bool { return rec.count(message.TypeJobFail) == 1 })
	rec.mu.Lock()
	fail := rec.msgs[message.TypeJobFail][0].(message.JobFail)
	rec.mu.Unlock()
	if fail.Code != message.ErrWorkerCrashed || !fail.Retryable {
		t.Fatalf("код: %+v", fail)
	}
	waitFor(t, "перезапуск", func() bool { return slots(manager)["echo"] == 2 })
}

func TestRestartWithoutDowntime(t *testing.T) {
	sup, manager, _, cancel := setup(t, map[string]string{})
	defer cancel()
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := sup.Restart(ctx, "echo"); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "один исполнитель после замены", func() bool { return slots(manager)["echo"] == 2 })
	sup.mu.Lock()
	id := sup.workers["echo"].slots[0].current.id
	sup.mu.Unlock()
	if id != "echo#2" {
		t.Fatalf("текущий экземпляр: %s", id)
	}
	if err := sup.Restart(ctx, "nope"); err != ErrUnknownWorker {
		t.Fatalf("неизвестный воркер: %v", err)
	}
}

// stop-first: старый экземпляр уходит до запуска нового — два экземпляра
// одновременно не живут (воркер с портом или интерфейсом); замена работает.
func TestRestartStopFirst(t *testing.T) {
	exe, _ := os.Executable()
	rec := &recorder{}
	manager := jobs.New(rec, logx.Discard(), func() {})
	spec := config.Worker{
		Name: "echo", Command: []string{exe}, Env: map[string]string{"TEST_WORKER": "1"}, Replicas: 1,
		Queues: []string{"echo"}, StopTimeout: config.Duration(5 * time.Second), Restart: config.RestartStopFirst,
	}
	sup := New([]config.Worker{spec}, manager, logx.Discard(), func() {}, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = sup.Start(ctx) }()
	capacity := func() int {
		st := message.Status{Slots: map[string]int{}}
		manager.ContributeStatus(&st)
		return st.Capacity["echo"]
	}
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })

	var peak atomic.Int32
	done := make(chan struct{})
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			if c := int32(capacity()); c > peak.Load() {
				peak.Store(c)
			}
			time.Sleep(time.Millisecond)
		}
	}()
	restartCtx, stop := context.WithTimeout(ctx, 15*time.Second)
	defer stop()
	if err := sup.Restart(restartCtx, "echo"); err != nil {
		t.Fatal(err)
	}
	close(done)
	if p := peak.Load(); p > 2 {
		t.Fatalf("stop-first: одновременно жили два экземпляра (ёмкость %d)", p)
	}
	sup.mu.Lock()
	id := sup.workers["echo"].slots[0].current.id
	sup.mu.Unlock()
	if id != "echo#2" {
		t.Fatalf("после замены — новый экземпляр: %s", id)
	}
	waitFor(t, "новый экземпляр берёт задачи", func() bool { return slots(manager)["echo"] == 2 })
	assign(manager, "after")
	waitFor(t, "задача выполнена новым экземпляром", func() bool { return rec.count(message.TypeJobComplete) == 1 })
	st := message.Status{Slots: map[string]int{}}
	sup.ContributeStatus(&st)
	if st.State == message.StateDegraded {
		t.Fatalf("замена stop-first — не сбой: %+v", st)
	}
}

// Apply — перечитанные настройки на ходу: replicas меняются, новый воркер
// запускается, изменённый заменяется, удалённый уходит.
func TestApplyScalesAddsReplacesRemoves(t *testing.T) {
	sup, manager, _, cancel := setup(t, map[string]string{"WORKER_VERSION": "1.0"})
	defer cancel()
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	exe, _ := os.Executable()
	spec := func(name, version string, replicas int, queues ...string) config.Worker {
		return config.Worker{
			Name: name, Command: []string{exe}, Replicas: replicas, Queues: queues,
			Env:         map[string]string{"TEST_WORKER": "1", "WORKER_VERSION": version},
			StopTimeout: config.Duration(5 * time.Second),
		}
	}
	status := func() map[string]message.StatusWorker {
		st := message.Status{Slots: map[string]int{}}
		sup.ContributeStatus(&st)
		out := map[string]message.StatusWorker{}
		for _, w := range st.Workers {
			out[w.Name] = w
		}
		return out
	}

	// replicas 1 → 3 и новый воркер.
	if removed := sup.Apply([]config.Worker{spec("echo", "1.0", 3, "echo"), spec("extra", "1.0", 1, "hidden")}); len(removed) != 0 {
		t.Fatalf("никто не удалён: %v", removed)
	}
	waitFor(t, "три экземпляра и новый воркер", func() bool {
		s := slots(manager)
		return s["echo"] == 6 && s["hidden"] == 1 && status()["echo"].Instances == 3
	})

	// replicas 3 → 1, настройки echo изменились — замена; extra удалён.
	removed := sup.Apply([]config.Worker{spec("echo", "2.0", 1, "echo")})
	if len(removed) != 1 || removed[0] != "extra" {
		t.Fatalf("удалён extra: %v", removed)
	}
	waitFor(t, "один экземпляр новой версии, extra ушёл", func() bool {
		s, st := slots(manager), status()
		_, hidden := s["hidden"]
		_, extra := st["extra"]
		return s["echo"] == 2 && !hidden && !extra && st["echo"].Instances == 1 && st["echo"].Version == "2.0"
	})
	if names := sup.Names(); len(names) != 1 || names[0] != "echo" {
		t.Fatalf("воркеры: %v", names)
	}
	stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	sup.Stop(stopCtx)
}
