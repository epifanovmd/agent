//go:build unix

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/commands"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/jobs"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// startSpec — супервизор с одним воркером spec; остановка — в конце теста.
func startSpec(t *testing.T, spec config.Worker) (*Supervisor, *jobs.Manager, *recorder) {
	t.Helper()
	rec := &recorder{}
	manager := jobs.New(rec, logx.Discard(), func() {})
	sup := New([]config.Worker{spec}, manager, logx.Discard(), func() {}, "test")
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = sup.Start(ctx) }()
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		sup.Stop(stopCtx)
		cancel()
	})
	return sup, manager, rec
}

func echoSpec(env map[string]string) config.Worker {
	exe, _ := os.Executable()
	env["TEST_WORKER"] = "1"
	return config.Worker{
		Name: "echo", Command: []string{exe}, Env: env, Replicas: 1,
		Queues: []string{"echo"}, StopTimeout: config.Duration(5 * time.Second),
	}
}

// worker.ping: отвечающий воркер работает дальше; зависший (три запроса
// подряд без ответа) перезапускается как упавший.
func TestPing(t *testing.T) {
	e, w := pingEvery, pingWait
	t.Cleanup(func() { pingEvery, pingWait = e, w }) // после остановки воркеров
	pingEvery, pingWait = 30*time.Millisecond, 30*time.Millisecond

	sup, manager, _ := startSpec(t, echoSpec(map[string]string{"WORKER_PING": "answer"}))
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	first := current(sup, "echo")
	time.Sleep(400 * time.Millisecond)
	if current(sup, "echo") != first || first.hasExited() {
		t.Fatal("отвечающий воркер не перезапускается")
	}

	hung, hmanager, _ := startSpec(t, echoSpec(map[string]string{"WORKER_PING": "silent"}))
	waitFor(t, "регистрация", func() bool { return slots(hmanager)["echo"] == 2 })
	inst := current(hung, "echo")
	waitFor(t, "зависший завершён", inst.hasExited)
	if !strings.Contains(inst.exitReason(), "worker.ping") {
		t.Fatalf("причина: %s", inst.exitReason())
	}
	waitFor(t, "новая копия", func() bool { c := current(hung, "echo"); return c != nil && c != inst })
}

// Отменённая задача не завершилась за срок — копия заменяется новой.
func TestStuckCancelReplacesInstance(t *testing.T) {
	sup, manager, _ := startSpec(t, echoSpec(map[string]string{"WORKER_HANG": "1"}))
	manager.SetCancelTimeout(100 * time.Millisecond)
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	inst := current(sup, "echo")
	assign(manager, "stuck")
	_ = manager.Handle(context.Background(), message.MustNew(message.TypeJobCancel, message.JobRef{JobID: "stuck"}))
	if slots(manager)["echo"] != 1 {
		t.Fatal("место отменённой занято до срока")
	}
	waitFor(t, "замена копии", func() bool {
		c := current(sup, "echo")
		return c != inst && c.registered() && slots(manager)["echo"] == 2
	})
}

// Снимок раздела получают все копии воркера; применён — когда все ответили.
func TestStateToAllReplicas(t *testing.T) {
	log := filepath.Join(t.TempDir(), "ipc")
	spec := echoSpec(map[string]string{"IPC_LOG": log})
	spec.Replicas = 2
	sup, manager, _ := startSpec(t, spec)
	waitFor(t, "две копии", func() bool { return slots(manager)["echo"] == 4 })
	proxy := domainProxy{s: sup, worker: "echo", domain: "example.kv"}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := proxy.Apply(ctx, 1, json.RawMessage(`{}`)); err != nil {
		t.Fatal(err)
	}
	puts := 0
	for _, line := range ipcLines(t, log) {
		if strings.HasPrefix(line, message.TypeStatePut+" ") {
			puts++
		}
	}
	if puts != 2 {
		t.Fatalf("state.put получили копий: %d", puts)
	}
}

// Повторная регистрация с меньшим набором команд снимает лишние (сервер
// узнаёт новым hello — Narrowed).
func TestReRegisterPrunesNames(t *testing.T) {
	rec := &recorder{}
	reg := commands.New(rec, logx.Discard())
	var narrowed atomic.Int32
	manager := jobs.New(rec, logx.Discard(), func() {})
	sup := New([]config.Worker{echoSpec(map[string]string{"WORKER_COMMANDS": "example.a,example.b"})}, manager, logx.Discard(), func() {}, "test")
	sup.SetBridge(Bridge{Commands: reg, Narrowed: func() { narrowed.Add(1) }})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = sup.Start(ctx) }()
	defer func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		sup.Stop(stopCtx)
	}()
	waitFor(t, "команды подключены", func() bool { return reg.Has("example.a") && reg.Has("example.b") })
	fromWorker(t, current(sup, "echo"), message.TypeWorkerRegister, message.WorkerRegister{
		Name: "echo", Version: "2", Queues: []message.QueueCapacity{{Name: "echo", Concurrency: 2}}, Commands: []string{"example.a"},
	})
	if !reg.Has("example.a") || reg.Has("example.b") || narrowed.Load() != 1 {
		t.Fatalf("после регистрации: a=%v b=%v narrowed=%d", reg.Has("example.a"), reg.Has("example.b"), narrowed.Load())
	}
}

// maxRestarts: падающий воркер после предела остаётся остановленным
// (degraded); worker.restart запускает его снова.
func TestMaxRestarts(t *testing.T) {
	spec := config.Worker{
		Name: "bad", Command: []string{"/bin/sh", "-c", "exit 3"}, Replicas: 1, StopTimeout: config.Duration(time.Second),
		MaxRestarts: 2, Backoff: config.Backoff{Min: config.Duration(10 * time.Millisecond), Max: config.Duration(20 * time.Millisecond)},
	}
	sup, _, _ := startSpec(t, spec)
	waitFor(t, "остановлен", func() bool {
		w, st := workerStatus(sup)
		return w.State == "stopped" && st.State == message.StateDegraded && strings.Contains(st.Message, "maxRestarts")
	})
	sup.mu.Lock()
	gen := sup.workers["bad"].generation
	sup.mu.Unlock()
	if gen != 3 {
		t.Fatalf("запусков: %d", gen)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = sup.Restart(ctx, "bad")
	waitFor(t, "снова запускается", func() bool {
		sup.mu.Lock()
		defer sup.mu.Unlock()
		return sup.workers["bad"].generation > gen
	})
}

// Остановка: сигнал получает вся группа процессов — потомок воркера тоже.
func TestStopKillsChildren(t *testing.T) {
	exe, _ := os.Executable()
	pidFile := filepath.Join(t.TempDir(), "child")
	spec := config.Worker{
		Name: "echo", Command: []string{"/bin/sh", "-c", "sleep 300 & echo $! > " + pidFile + "; exec " + exe},
		Env: map[string]string{"TEST_WORKER": "1"}, Replicas: 1, StopTimeout: config.Duration(5 * time.Second),
	}
	rec := &recorder{}
	manager := jobs.New(rec, logx.Discard(), func() {})
	sup := New([]config.Worker{spec}, manager, logx.Discard(), func() {}, "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = sup.Start(ctx) }()
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	raw, _ := os.ReadFile(pidFile)
	pid, err := strconv.Atoi(strings.TrimSpace(string(raw)))
	if err != nil {
		t.Fatal(err)
	}
	stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
	defer stop()
	sup.Stop(stopCtx)
	waitFor(t, "потомок завершён", func() bool {
		var ws syscall.WaitStatus
		_, _ = syscall.Wait4(pid, &ws, syscall.WNOHANG, nil)
		return errors.Is(syscall.Kill(pid, 0), syscall.ESRCH)
	})
}

// Пауза сервера хранится в файле и переживает перезапуск агента.
func TestServerPausePersists(t *testing.T) {
	path := filepath.Join(t.TempDir(), "worker-pause.json")
	spec := echoSpec(map[string]string{})
	first := New([]config.Worker{spec}, jobs.New(&recorder{}, logx.Discard(), func() {}), logx.Discard(), func() {}, "test")
	first.SetPauseFile(path)
	if err := first.Pause("echo", []string{"echo"}); err != nil {
		t.Fatal(err)
	}
	second := New([]config.Worker{spec}, jobs.New(&recorder{}, logx.Discard(), func() {}), logx.Discard(), func() {}, "test")
	second.SetPauseFile(path)
	if !second.Paused("echo") {
		t.Fatal("пауза не пережила перезапуск")
	}
	_ = second.Resume("echo", nil)
	third := New([]config.Worker{spec}, jobs.New(&recorder{}, logx.Discard(), func() {}), logx.Discard(), func() {}, "test")
	third.SetPauseFile(path)
	if third.Paused("echo") {
		t.Fatal("снятая пауза осталась")
	}
}

// Вывод воркера: stdout — info, stderr — warn; длинная строка обрезается.
func TestLineLog(t *testing.T) {
	var buf bytes.Buffer
	log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
	out := &lineLog{log: log, level: slog.LevelInfo}
	errs := &lineLog{log: log, level: slog.LevelWarn}
	_, _ = io.WriteString(out, "обычная\nвторая")
	_, _ = io.WriteString(errs, "ошибка\n")
	long := strings.Repeat("ж", lineMax) // 2·lineMax байт без перевода строки
	_, _ = io.WriteString(out, "\n"+long)
	_, _ = io.WriteString(out, long+"\nпосле\n")
	out.Flush()
	text := buf.String()
	for _, want := range []string{`level=INFO msg=обычная`, `level=INFO msg=вторая`, `level=WARN msg=ошибка`, truncatedMark, `msg=после`} {
		if !strings.Contains(text, want) {
			t.Fatalf("нет %q в\n%.300s", want, text)
		}
	}
	for _, line := range strings.Split(text, "\n") {
		if len(line) > lineMax+200 {
			t.Fatalf("строка %d байт", len(line))
		}
	}
}

// worker.restart во время worker.update — WORKER_UPDATE_IN_PROGRESS.
func TestRestartDuringUpdate(t *testing.T) {
	sup, manager, _, _ := releaseSetup(t, "1.0.0", "")
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	release, started := make(chan struct{}), make(chan struct{})
	go func() {
		_, _ = sup.Update(context.Background(), "echo", "2.0.0", func(string) error {
			close(started)
			<-release
			return errors.New("отменено тестом")
		}, &bytes.Buffer{})
	}()
	<-started
	defer close(release)
	if err := sup.Restart(context.Background(), "echo"); cmdCode(err) != message.ErrWorkerUpdateInProgress {
		t.Fatalf("перезапуск во время обновления: %v", err)
	}
}

// Сборка — архив: каталог current, запуск ./run в нём; прежняя сборка-файл
// — в previous; откат возвращает её.
func TestReleaseArchive(t *testing.T) {
	exe, _ := os.Executable()
	sup, manager, dir, _ := releaseSetup(t, "1.0.0", "")
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	archive := func(script string) func(string) error {
		return func(dst string) error {
			if err := os.Mkdir(dst, 0o755); err != nil {
				return err
			}
			return os.WriteFile(filepath.Join(dst, "run"), []byte(script), 0o755)
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := sup.Update(ctx, "echo", "2.0.0", archive("#!/bin/sh\npwd > started\nexec "+exe+"\n"), &bytes.Buffer{}); err != nil {
		t.Fatal(err)
	}
	if !isDir(filepath.Join(dir, "current")) || isDir(filepath.Join(dir, "previous")) {
		t.Fatal("current — каталог, previous — прежний файл")
	}
	if raw, _ := os.ReadFile(filepath.Join(dir, "current", "started")); !strings.Contains(string(raw), "current") {
		t.Fatalf("запуск не в каталоге сборки: %q", raw)
	}
	waitFor(t, "версия 2.0.0", func() bool { w, _ := workerStatus(sup); return w.Version == "2.0.0" })

	defer func(d time.Duration) { updateWait = d }(updateWait)
	updateWait = 2 * time.Second
	_, err := sup.Update(ctx, "echo", "3.0.0", archive("#!/bin/sh\nexit 3\n"), &bytes.Buffer{})
	if cmdCode(err) != message.ErrWorkerUpdateFailed {
		t.Fatalf("сломанный архив: %v", err)
	}
	if readVersion(filepath.Join(dir, "version")) != "2.0.0" || !isDir(filepath.Join(dir, "current")) {
		entries, _ := os.ReadDir(dir)
		t.Fatalf("откат на каталог 2.0.0: %v; %v %v", err, readVersion(filepath.Join(dir, "version")), entries)
	}
	waitFor(t, "работает 2.0.0", func() bool {
		w, _ := workerStatus(sup)
		return w.State == "running" && w.Version == "2.0.0"
	})
}
