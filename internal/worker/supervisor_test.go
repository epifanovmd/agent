//go:build unix

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/message"
)

// fast — короткие сроки опроса для тестов.
func fast(t *testing.T) {
	t.Helper()
	oldBusy, oldTail, oldAdopt := busyPoll, tailEvery, adoptPoll
	busyPoll, tailEvery, adoptPoll = 50*time.Millisecond, 20*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { busyPoll, tailEvery, adoptPoll = oldBusy, oldTail, oldAdopt })
}

// life — жизнь тестового воркера: короткие сроки.
func life() config.Lifecycle {
	l := config.DefaultLifecycle()
	l.Backoff = config.Backoff{Min: config.Duration(20 * time.Millisecond), Max: config.Duration(50 * time.Millisecond)}
	interval := config.Duration(50 * time.Millisecond)
	l.Health.Interval, l.Health.Timeout = &interval, config.Duration(200*time.Millisecond)
	l.ProbeTimeout = config.Duration(200 * time.Millisecond)
	l.StartTimeout, l.StopTimeout, l.UpdateHealthyTimeout = config.Duration(5*time.Second), config.Duration(2*time.Second), config.Duration(2*time.Second)
	return l
}

// spec — тестовый воркер name в режиме mode; события — в файл events.
func spec(name, mode, events string) config.Worker {
	return config.Worker{
		Name: name, Command: []string{os.Args[0]},
		Env:       map[string]string{"TEST_WORKER": mode, "TEST_EVENTS": events},
		Lifecycle: life(),
	}
}

type harness struct {
	dir     string
	sup     *Supervisor
	journal *logx.Journal
	started chan string
	adopted chan string
	changes atomic.Int32
	cancel  context.CancelFunc
	done    chan struct{}
}

// tempDir — короткий каталог (путь сокета ограничен по длине).
func tempDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "w")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

func start(t *testing.T, specs ...config.Worker) *harness {
	t.Helper()
	return startIn(t, tempDir(t), specs...)
}

// startIn — воркеры с каталогами сокетов и данных в dir (тот же dir —
// следующий запуск «агента»).
func startIn(t *testing.T, dir string, specs ...config.Worker) *harness {
	t.Helper()
	fast(t)
	h := &harness{dir: dir, journal: logx.NewJournal(100), started: make(chan string, 16), adopted: make(chan string, 16), done: make(chan struct{})}
	log, _ := logx.New(&bytes.Buffer{}, h.journal, logx.Options{Level: "debug"})
	h.sup = New(specs, Options{
		RunDir: filepath.Join(dir, "run"), DataDir: dir, AgentSocket: filepath.Join(dir, "agent.sock"), AgentVersion: "1.0.0-test", Log: log,
		OnStarted: func(name string) { h.started <- name },
		OnAdopted: func(name string) { h.adopted <- name },
		OnChange:  func() { h.changes.Add(1) },
	})
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		h.sup.Run(ctx)
		close(h.done)
	}()
	t.Cleanup(h.stop)
	return h
}

func (h *harness) stop() {
	h.cancel()
	<-h.done
}

func (h *harness) status(name string) message.WorkerStatus {
	for _, st := range h.sup.Status() {
		if st.Name == name {
			return st
		}
	}
	return message.WorkerStatus{}
}

func (h *harness) get(t *testing.T, name, path string, v any) {
	t.Helper()
	client, err := h.sup.Client(name)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Get("http://worker" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(v); err != nil {
		t.Fatal(err)
	}
}

// Запуск (§12): переменные окружения, сокет в своём каталоге 0700, ожидание
// сокета, OnStarted, GET /health в status, вывод — в журнал воркера.
func TestStartEnvHealthOutput(t *testing.T) {
	events := filepath.Join(t.TempDir(), "events")
	h := start(t, spec("echo", "ok", events))
	if name := <-h.started; name != "echo" {
		t.Fatal(name)
	}
	var env map[string]string
	h.get(t, "echo", "/env", &env)
	sock := env[message.EnvWorkerSocket]
	if env[message.EnvWorker] != "echo" || env[message.EnvVersion] != "1.0.0-test" || !strings.HasSuffix(env[message.EnvSocket], "agent.sock") ||
		env[message.EnvWorkerToken] != h.sup.Token("echo") || len(env[message.EnvWorkerToken]) < 32 || filepath.Base(sock) != SocketName {
		t.Fatalf("окружение: %v", env)
	}
	if info, err := os.Stat(filepath.Dir(sock)); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("каталог сокета: %v %v", info.Mode(), err)
	}
	if name, ok := h.sup.ByToken(env[message.EnvWorkerToken]); !ok || name != "echo" {
		t.Fatal("токен воркера не узнан")
	}
	if _, ok := h.sup.ByToken(""); ok {
		t.Fatal("пустой токен")
	}
	eventually(t, "health в status", func() bool {
		st := h.status("echo")
		return st.State == message.WorkerRunning && st.Health != nil && st.Health.OK && string(st.Health.Info) == `{"version":""}`
	})
	eventually(t, "вывод воркера в журнале", func() bool {
		lines := h.journal.Tail("echo", 10)
		return len(lines) > 0 && lines[0].Msg == "воркер слушает сокет" && lines[0].Level == "info"
	})
	if _, err := h.sup.Client("nope"); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
}

// Падение (§13): перезапуск с растущей паузой, state backoff, счётчик
// restarts, stderr — warn; maxRestarts — воркер остаётся остановленным до
// worker.restart.
func TestCrashBackoffAndMaxRestarts(t *testing.T) {
	events := filepath.Join(t.TempDir(), "events")
	crash := spec("crash", "crash", events)
	crash.Lifecycle.MaxRestarts = 3
	h := start(t, crash)
	eventually(t, "остановлен после maxRestarts", func() bool {
		st := h.status("crash")
		return st.State == message.WorkerStopped && st.Restarts == 4
	})
	time.Sleep(200 * time.Millisecond)
	if st := h.status("crash"); st.Restarts != 4 {
		t.Fatalf("после maxRestarts не перезапускается: %+v", st)
	}
	lines := h.journal.Tail("crash", 10)
	if len(lines) == 0 || lines[0].Msg != "падаю" || lines[0].Level != "warn" {
		t.Fatalf("stderr: %+v", lines)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.sup.Restart(ctx, "crash", false); err == nil {
		t.Fatal("падающий воркер не запустился — ошибка")
	}
	eventually(t, "worker.restart запускает снова", func() bool { return h.status("crash").Restarts > 4 })
}

// Замена (§13): сначала уходит прежний процесс, потом запускается новый;
// копия всегда одна.
func TestRestartStopFirst(t *testing.T) {
	events := filepath.Join(t.TempDir(), "events")
	h := start(t, spec("echo", "ok", events))
	<-h.started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.sup.Restart(ctx, "echo", false); err != nil {
		t.Fatal(err)
	}
	<-h.started
	got := readEvents(events)
	if len(got) != 3 || !strings.HasPrefix(got[0], "start ") || got[1] != "stop "+strings.TrimPrefix(got[0], "start ") ||
		!strings.HasPrefix(got[2], "start ") || got[2] == got[0] {
		t.Fatalf("порядок: %v", got)
	}
	if st := h.status("echo"); st.State != message.WorkerRunning || st.Restarts != 0 {
		t.Fatalf("замена — не падение: %+v", st)
	}
	if err := h.sup.Restart(ctx, "nope", false); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
}

func alive(pid int) bool { return syscall.Kill(pid, 0) == nil }

func childPid(t *testing.T, path string) int {
	t.Helper()
	var pid int
	eventually(t, "pid потомка", func() bool {
		raw, _ := os.ReadFile(path)
		pid, _ = strconv.Atoi(strings.TrimSpace(string(raw)))
		return pid > 0
	})
	return pid
}

// keepChildren: false — остановка завершает всю группу процессов; true —
// потомки переживают перезапуск воркера.
func TestKeepChildren(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(fmt.Sprint(keep), func(t *testing.T) {
			dir := t.TempDir()
			w := spec("parent", "children", filepath.Join(dir, "events"))
			w.Env["TEST_CHILD_PID"] = filepath.Join(dir, "child")
			w.Lifecycle.KeepChildren = keep
			h := start(t, w)
			<-h.started
			pid := childPid(t, filepath.Join(dir, "child"))
			defer syscall.Kill(pid, syscall.SIGKILL)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := h.sup.Restart(ctx, "parent", false); err != nil {
				t.Fatal(err)
			}
			if keep {
				time.Sleep(100 * time.Millisecond)
				if !alive(pid) {
					t.Fatal("keepChildren: потомок должен пережить перезапуск")
				}
				return
			}
			eventually(t, "потомок завершён", func() bool {
				var ws syscall.WaitStatus
				_, _ = syscall.Wait4(pid, &ws, syscall.WNOHANG, nil)
				return !alive(pid) || zombie(pid)
			})
		})
	}
}

// zombie — процесс завершился, но не убран родителем (в контейнере его
// убирает init).
func zombie(pid int) bool {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	fields := strings.Fields(string(raw))
	return len(fields) > 2 && fields[2] == "Z"
}

// Нет ответа на GET /health три раза подряд — воркер завис и перезапускается.
func TestHealthMissesRestart(t *testing.T) {
	events := filepath.Join(t.TempDir(), "events")
	h := start(t, spec("deaf", "deaf", events))
	eventually(t, "перезапуск зависшего", func() bool { return h.status("deaf").Restarts >= 1 && len(readEvents(events)) >= 2 })
}

// Не вышел за stopTimeout после SIGTERM — SIGKILL.
func TestStopTimeoutKill(t *testing.T) {
	events := filepath.Join(t.TempDir(), "events")
	w := spec("stubborn", "stubborn", events)
	w.Lifecycle.StopTimeout = config.Duration(300 * time.Millisecond)
	h := start(t, w)
	<-h.started
	began := time.Now()
	h.stop()
	if d := time.Since(began); d < 300*time.Millisecond || d > 5*time.Second {
		t.Fatalf("остановка: %v", d)
	}
	if st := h.status("stubborn"); st.State != message.WorkerStopped {
		t.Fatalf("состояние: %+v", st)
	}
}

// Apply: новый воркер запускается, удалённый останавливается, изменённый
// перезапускается.
func TestApply(t *testing.T) {
	dir := t.TempDir()
	a, b := spec("a", "ok", filepath.Join(dir, "a")), spec("b", "ok", filepath.Join(dir, "b"))
	h := start(t, a)
	<-h.started
	b2 := b
	h.sup.Apply([]config.Worker{b2})
	if name := <-h.started; name != "b" {
		t.Fatal(name)
	}
	eventually(t, "a остановлен", func() bool {
		ev := readEvents(filepath.Join(dir, "a"))
		return len(ev) == 2 && strings.HasPrefix(ev[1], "stop ")
	})
	if h.sup.Has("a") || len(h.sup.Status()) != 1 {
		t.Fatal("a удалён из настроек")
	}
	b3 := b
	b3.Env = map[string]string{"TEST_WORKER": "ok", "TEST_EVENTS": b.Env["TEST_EVENTS"], "X": "1"}
	h.sup.Apply([]config.Worker{b3})
	<-h.started
	if ev := readEvents(filepath.Join(dir, "b")); len(ev) != 3 {
		t.Fatalf("b перезапущен: %v", ev)
	}
}

// Манифест (§12): GET /manifest после запуска — в status, version — из
// манифеста (у воркера из выпуска — версия сборки, манифест как есть); новый
// запуск — новый манифест.
func TestManifest(t *testing.T) {
	dir := t.TempDir()
	described := spec("described", "ok", filepath.Join(dir, "described"))
	described.Env["TEST_MANIFEST"] = `{"version":"3.1.0","extra":true,"routes":[{"method":"POST","path":"/echo"}]}`
	rel := config.Worker{Name: "report", Release: true, ReleaseDir: filepath.Join(dir, "workers", "report"),
		Lifecycle: life(), Env: map[string]string{"TEST_MANIFEST": `{"version":"9.9.9"}`}}
	_ = os.MkdirAll(rel.ReleaseDir, 0o755)
	writeBuild(t, rel.Current(), releaseScript("ok", "1.0.0", filepath.Join(dir, "report")))
	_ = os.WriteFile(filepath.Join(rel.ReleaseDir, config.ReleaseVersion), []byte("1.0.0\n"), 0o644)
	h := start(t, described, rel)

	eventually(t, "манифест в status", func() bool {
		st := h.status("described")
		return st.Manifest != nil && st.Version == "3.1.0" && len(st.Manifest.Routes) == 1 && st.Manifest.Routes[0].Path == "/echo"
	})
	eventually(t, "у воркера из выпуска — версия сборки", func() bool {
		st := h.status("report")
		return st.Version == "1.0.0" && st.Manifest != nil && st.Manifest.Version == "9.9.9"
	})

	changes := h.changes.Load()
	next := described
	next.Env = map[string]string{"TEST_WORKER": "ok", "TEST_EVENTS": described.Env["TEST_EVENTS"],
		"TEST_MANIFEST": `{"version":"3.2.0"}`}
	h.sup.Apply([]config.Worker{next, rel})
	eventually(t, "новый манифест после перезапуска", func() bool {
		st := h.status("described")
		return st.Manifest != nil && st.Version == "3.2.0" && st.Manifest.Routes == nil
	})
	if h.changes.Load() <= changes {
		t.Fatal("изменение манифеста — внеочередной status")
	}
}

// Регистрация (§12): без GET /manifest, с неверным манифестом или без
// GET /health воркер — invalid с причиной, настроек (OnStarted) и запросов
// (Client → ErrInvalid) не получает, процесс не перезапускается; ответы
// стали корректными — повтор проверки регистрирует его: running, OnStarted.
func TestRegistration(t *testing.T) {
	dir := t.TempDir()
	missing := spec("missing", "ok", filepath.Join(dir, "missing"))
	missing.Env["TEST_MANIFEST_FILE"] = filepath.Join(dir, "manifest.json")
	broken := spec("broken", "ok", filepath.Join(dir, "broken"))
	broken.Env["TEST_MANIFEST"] = `{"routes":[{"method":"get","path":"echo"}]}`
	nohealth := spec("nohealth", "nohealth", filepath.Join(dir, "nohealth"))
	h := start(t, missing, broken, nohealth)

	for name, want := range map[string]string{
		"missing":  "GET /manifest: HTTP 404",
		"broken":   "GET /manifest: version: обязательно",
		"nohealth": "GET /health: HTTP 404",
	} {
		eventually(t, name+" — invalid", func() bool {
			st := h.status(name)
			return st.State == message.WorkerInvalid && strings.Contains(st.Message, want)
		})
		if _, err := h.sup.Client(name); !errors.Is(err, ErrInvalid) {
			t.Fatalf("%s: Client: %v", name, err)
		}
	}
	if st := h.status("broken"); st.Manifest != nil || st.Version != "" {
		t.Fatalf("неверный манифест не хранится: %+v", st)
	}
	if st := h.status("nohealth"); st.Manifest == nil || st.Manifest.Version != "1.0.0" {
		t.Fatalf("корректный манифест хранится и у invalid: %+v", st)
	}
	eventually(t, "предупреждение в логе", func() bool {
		for _, e := range h.journal.Tail(message.LogSourceAgent, 200) {
			if strings.HasPrefix(e.Msg, "воркер не зарегистрирован") && e.Attrs["worker"] == "missing" {
				return true
			}
		}
		return false
	})
	select {
	case name := <-h.started:
		t.Fatalf("незарегистрированный %s получил настройки", name)
	default:
	}

	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(`{"version":"2.0.0","events":[{"type":"item.done"}]}`), 0o600); err != nil {
		t.Fatal(err)
	}
	eventually(t, "missing — зарегистрирован", func() bool {
		st := h.status("missing")
		return st.State == message.WorkerRunning && st.Message == "" && st.Version == "2.0.0"
	})
	select {
	case name := <-h.started:
		if name != "missing" {
			t.Fatalf("OnStarted: %s", name)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("после регистрации нет OnStarted")
	}
	if _, err := h.sup.Client("missing"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if m := h.sup.WaitManifest(ctx, "missing"); !m.DeclaresEvent("item.done") {
		t.Fatalf("WaitManifest: %+v", m)
	}
	if ev := readEvents(filepath.Join(dir, "missing")); len(ev) != 1 {
		t.Fatalf("invalid-воркер не перезапускается: %v", ev)
	}
}

// Уборка (§13): воркер запускается, получает POST /cleanup и останавливается.
func TestCleanup(t *testing.T) {
	fast(t)
	dir := t.TempDir()
	run := tempDir(t)
	sup := New([]config.Worker{spec("echo", "ok", filepath.Join(dir, "events")), spec("crash", "crash", "")},
		Options{RunDir: run, DataDir: run, Log: logx.Discard()})
	res := sup.Cleanup(context.Background())
	if len(res) != 2 || res[0].Err != nil || res[1].Err == nil {
		t.Fatalf("итог: %+v", res)
	}
	got := readEvents(filepath.Join(dir, "events"))
	if len(got) != 3 || !strings.HasPrefix(got[1], "cleanup ") || !strings.HasPrefix(got[2], "stop ") {
		t.Fatalf("порядок: %v", got)
	}
}

// release — тестовый воркер из выпуска: сборка — скрипт, запускающий
// тестовый файл в режиме mode с версией version.
func releaseScript(mode, version, events string) string {
	return fmt.Sprintf("#!/bin/sh\nTEST_WORKER=%s TEST_VERSION=%s TEST_EVENTS=%s exec %s\n", mode, version, events, os.Args[0])
}

func writeBuild(t *testing.T, path, script string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

// worker.update (§11): новая сборка здорова — она остаётся, итог {version,
// previous}; не здорова за срок — возврат прежней, UPDATE_FAILED; уже
// обновляется — BUSY; не из выпуска — WORKER_NOT_RELEASED.
func TestUpdateAndRollback(t *testing.T) {
	dir := t.TempDir()
	events := filepath.Join(dir, "events")
	rel := config.Worker{Name: "report", Release: true, ReleaseDir: filepath.Join(dir, "workers", "report"),
		Lifecycle: life()}
	_ = os.MkdirAll(rel.ReleaseDir, 0o755)
	writeBuild(t, rel.Current(), releaseScript("ok", "1.0.0", events))
	_ = os.WriteFile(filepath.Join(rel.ReleaseDir, config.ReleaseVersion), []byte("1.0.0\n"), 0o644)
	h := start(t, rel, spec("plain", "ok", filepath.Join(dir, "plain")))
	<-h.started
	<-h.started
	if st := h.status("report"); st.Version != "1.0.0" || !st.Release {
		t.Fatalf("версия: %+v", st)
	}
	ctx := context.Background()

	res, err := h.sup.Update(ctx, "report", "1.1.0", false, func(dst string) error {
		writeBuild(t, dst, releaseScript("ok", "1.1.0", events))
		return nil
	})
	if err != nil || res != (message.UpdateResult{Version: "1.1.0", Previous: "1.0.0"}) {
		t.Fatalf("обновление: %+v %v", res, err)
	}
	if st := h.status("report"); st.Version != "1.1.0" {
		t.Fatalf("после обновления: %+v", st)
	}
	var health message.Health
	h.get(t, "report", "/health", &health)
	if string(health.Info) != `{"version":"1.1.0"}` {
		t.Fatalf("работает новая сборка: %s", health.Info)
	}

	// Новая сборка не здорова — возврат 1.1.0.
	_, err = h.sup.Update(ctx, "report", "1.2.0", false, func(dst string) error {
		writeBuild(t, dst, releaseScript("unhealthy", "1.2.0", events))
		return nil
	})
	var ei *message.ErrorInfo
	if !errors.As(err, &ei) || ei.Code != message.CodeUpdateFailed || !strings.Contains(ei.Message, "возвращена прежняя сборка 1.1.0") {
		t.Fatalf("откат: %v", err)
	}
	eventually(t, "прежняя сборка работает", func() bool {
		var h2 message.Health
		client, err := h.sup.Client("report")
		if err != nil {
			return false
		}
		resp, err := client.Get("http://worker/health")
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		_ = json.NewDecoder(resp.Body).Decode(&h2)
		return h2.OK && string(h2.Info) == `{"version":"1.1.0"}`
	})
	if st := h.status("report"); st.Version != "1.1.0" {
		t.Fatalf("версия после отката: %+v", st)
	}

	// Ошибка загрузки — как есть; одновременное обновление — BUSY.
	inFetch, block := make(chan struct{}), make(chan struct{})
	var wg sync.WaitGroup
	var first error
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, first = h.sup.Update(ctx, "report", "1.3.0", false, func(string) error {
			close(inFetch)
			<-block
			return message.NewError(message.CodeUpdateNotVerified, "нет ключа")
		})
	}()
	<-inFetch
	if _, err := h.sup.Update(ctx, "report", "1.3.0", false, nil); !errors.As(err, &ei) || ei.Code != message.CodeBusy {
		t.Fatalf("уже обновляется: %v", err)
	}
	close(block)
	wg.Wait()
	if !errors.As(first, &ei) || ei.Code != message.CodeUpdateNotVerified {
		t.Fatalf("ошибка загрузки — как есть: %v", first)
	}
	if _, err := h.sup.Update(ctx, "plain", "1", false, nil); !errors.As(err, &ei) || ei.Code != message.CodeWorkerNotReleased {
		t.Fatalf("не из выпуска: %v", err)
	}
	if _, err := h.sup.Update(ctx, "nope", "1", false, nil); !errors.As(err, &ei) || ei.Code != message.CodeWorkerUnknown {
		t.Fatalf("неизвестный: %v", err)
	}
}

// Сборка-архив: каталог current, без command — запускается ./run.
func TestReleaseArchiveRun(t *testing.T) {
	dir := t.TempDir()
	rel := config.Worker{Name: "packed", Release: true, ReleaseDir: filepath.Join(dir, "workers", "packed"),
		Lifecycle: life()}
	_ = os.MkdirAll(rel.Current(), 0o755)
	writeBuild(t, filepath.Join(rel.Current(), config.ReleaseRun), releaseScript("ok", "2.0.0", filepath.Join(dir, "events")))
	h := start(t, rel)
	<-h.started
	var health message.Health
	h.get(t, "packed", "/health", &health)
	if !health.OK {
		t.Fatalf("%+v", health)
	}
	// Нет сборки — воркер не запускается, ошибка в журнале агента.
	missing := config.Worker{Name: "missing", Release: true, ReleaseDir: filepath.Join(dir, "workers", "missing")}
	if _, _, err := argv(missing); err == nil || !strings.Contains(err.Error(), "agent install --worker missing") {
		t.Fatal(err)
	}
}

// Строка вывода длиннее предела обрезается с пометкой.
func TestLineLogTruncates(t *testing.T) {
	j := logx.NewJournal(10)
	log, _ := logx.New(&bytes.Buffer{}, j, logx.Options{})
	l := &lineLog{log: log.With(logx.OutputKey, "w"), level: 0}
	_, _ = l.Write([]byte(strings.Repeat("я", lineMax) + "\nвторая\nхвост"))
	l.Flush()
	got := j.Tail("w", 10)
	if len(got) != 3 || !strings.HasSuffix(got[0].Msg, truncatedMark) || len(got[0].Msg) > lineMax+len(truncatedMark) ||
		got[1].Msg != "вторая" || got[2].Msg != "хвост" {
		t.Fatalf("%d %+v", len(got), got[1:])
	}
}
