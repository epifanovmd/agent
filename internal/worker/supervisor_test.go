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
	"github.com/epifanovmd/agent/internal/workerapi"
	"github.com/epifanovmd/agent/protocol"
)

// fast — короткие сроки опроса для тестов.
func fast(t *testing.T) {
	t.Helper()
	oldBusy, oldTail, oldAdopt := busyPoll, tailEvery, adoptPoll
	busyPoll, tailEvery, adoptPoll = 50*time.Millisecond, 20*time.Millisecond, 20*time.Millisecond
	t.Cleanup(func() { busyPoll, tailEvery, adoptPoll = oldBusy, oldTail, oldAdopt })
}

// life — остановка тестового воркера: короткий срок.
func life() config.Lifecycle {
	return config.Lifecycle{StopTimeout: config.Duration(2 * time.Second)}
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

// sockAgent — сокет агента для тестов: регистрация и пульс — в Supervisor.
type sockAgent struct{ sup *Supervisor }

func (a sockAgent) ByToken(token string) (string, bool) { return a.sup.ByToken(token) }
func (a sockAgent) Register(name string, reg protocol.Register) error {
	return asInfo(a.sup.Register(name, reg))
}
func (a sockAgent) Heartbeat(name string, h protocol.Health) error {
	return asInfo(a.sup.Heartbeat(name, h))
}
func (sockAgent) Applied(string, []protocol.Applied) error    { return nil }
func (sockAgent) Results(string, []protocol.Result) error     { return nil }
func (sockAgent) Progress(string, protocol.Progress) error    { return nil }
func (sockAgent) Events(string, []protocol.WorkerEvent) error { return nil }
func (sockAgent) StreamOpened(string) error                   { return nil }
func (sockAgent) Request(context.Context, string, protocol.Request) protocol.RequestReply {
	return protocol.RequestReply{}
}

func asInfo(err error) error {
	if err != nil {
		return protocol.NewError(protocol.CodeWorkerInvalid, err.Error())
	}
	return nil
}

// startIn — воркеры с каталогом данных и сокетом агента в dir (тот же dir — следующий запуск
// «агента»).
func startIn(t *testing.T, dir string, specs ...config.Worker) *harness {
	t.Helper()
	fast(t)
	h := &harness{dir: dir, journal: logx.NewJournal(100), started: make(chan string, 16), done: make(chan struct{})}
	log, _ := logx.New(&bytes.Buffer{}, h.journal, logx.Options{Level: "debug"})
	sock := filepath.Join(dir, "agent.sock")
	h.sup = New(specs, Options{
		DataDir: dir, AgentSocket: sock, Log: log,
		OnRegistered: func(name string) { h.started <- name },
		OnChange:     func() { h.changes.Add(1) },
	})
	srv, err := workerapi.Listen(sock, sockAgent{h.sup}, log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() {
		h.sup.Run(ctx)
		srv.Close()
		close(h.done)
	}()
	t.Cleanup(h.stop)
	return h
}

func (h *harness) stop() {
	h.cancel()
	<-h.done
}

func (h *harness) status(name string) protocol.WorkerState {
	for _, st := range h.sup.Status() {
		if st.Name == name {
			return st
		}
	}
	return protocol.WorkerState{}
}

// pid — процесс воркера по его последней регистрации.
func (h *harness) pid(t *testing.T, name string) int {
	t.Helper()
	m := h.sup.Manifest(name)
	if m == nil {
		t.Fatalf("%s не зарегистрирован", name)
	}
	pid, err := strconv.Atoi(strings.TrimPrefix(m.Description, "pid "))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// env — окружение воркера из его каталога данных.
func (h *harness) env(t *testing.T, name string) map[string]string {
	t.Helper()
	var env map[string]string
	eventually(t, "окружение воркера", func() bool {
		raw, err := os.ReadFile(filepath.Join(StateDir(h.dir, name), "env.json"))
		return err == nil && json.Unmarshal(raw, &env) == nil
	})
	return env
}

// Запуск (§11): переменные окружения, каталог данных воркера 0700, регистрация — OnRegistered,
// пульс — в состоянии, метрики пульса — в Metrics, вывод — в журнал воркера.
func TestStartEnvHealthOutput(t *testing.T) {
	events := filepath.Join(t.TempDir(), "events")
	w := spec("echo", "ok", events)
	w.Env["TEST_VERSION"] = "1.2.3"
	h := start(t, w)
	if name := <-h.started; name != "echo" {
		t.Fatal(name)
	}
	env := h.env(t, "echo")
	if env[protocol.EnvWorkerName] != "echo" || env[protocol.EnvAgentSocket] != filepath.Join(h.dir, "agent.sock") ||
		env[protocol.EnvWorkerToken] != h.sup.Token("echo") || len(env[protocol.EnvWorkerToken]) < 32 ||
		env[protocol.EnvStateDir] != StateDir(h.dir, "echo") {
		t.Fatalf("окружение: %v", env)
	}
	if info, err := os.Stat(StateDir(h.dir, "echo")); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("каталог данных: %v", err)
	}
	if name, ok := h.sup.ByToken(env[protocol.EnvWorkerToken]); !ok || name != "echo" {
		t.Fatal("токен воркера не узнан")
	}
	if _, ok := h.sup.ByToken(""); ok {
		t.Fatal("пустой токен")
	}
	eventually(t, "пульс в состоянии", func() bool {
		st := h.status("echo")
		return st.State == protocol.WorkerRunning && st.Health != nil && st.Health.OK && st.Version == "1.2.3" && st.Health.Metrics == nil
	})
	eventually(t, "метрики из пульса", func() bool { return string(h.sup.Metrics()["echo"]) == `{"n":1}` })
	if err := h.sup.Ready("echo"); err != nil {
		t.Fatal(err)
	}
	if err := h.sup.Ready("nope"); !errors.Is(err, ErrUnknown) {
		t.Fatal(err)
	}
	eventually(t, "вывод воркера в журнале", func() bool {
		lines := h.journal.Tail("echo", 10)
		return len(lines) > 0 && lines[0].Msg == "воркер запущен" && lines[0].Level == "info"
	})
}

// Падение (§11): перезапуск с растущей паузой, state backoff, счётчик restarts без предела,
// stderr — warn.
func TestCrashBackoff(t *testing.T) {
	events := filepath.Join(t.TempDir(), "events")
	h := start(t, spec("crash", "crash", events))
	eventually(t, "перезапуски", func() bool { return h.status("crash").Restarts >= 4 })
	lines := h.journal.Tail("crash", 10)
	if len(lines) == 0 || lines[0].Msg != "падаю" || lines[0].Level != "warn" {
		t.Fatalf("stderr: %+v", lines)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.sup.Restart(ctx, "crash", false); err == nil {
		t.Fatal("падающий воркер не запустился — ошибка")
	}
}

// Замена (§11): сначала уходит прежний процесс, потом запускается новый; копия всегда одна;
// итог — после регистрации нового.
func TestRestartStopFirst(t *testing.T) {
	events := filepath.Join(t.TempDir(), "events")
	h := start(t, spec("echo", "ok", events))
	<-h.started
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.sup.Restart(ctx, "echo", false); err != nil {
		t.Fatal(err)
	}
	if st := h.status("echo"); st.State != protocol.WorkerRunning || st.Restarts != 0 {
		t.Fatalf("после замены — зарегистрирован, не падение: %+v", st)
	}
	<-h.started
	got := readEvents(events)
	if len(got) != 3 || !strings.HasPrefix(got[0], "start ") || got[1] != "stop "+strings.TrimPrefix(got[0], "start ") ||
		!strings.HasPrefix(got[2], "start ") || got[2] == got[0] {
		t.Fatalf("порядок: %v", got)
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

// Остановка завершает всю группу процессов воркера: потомки не переживают его перезапуск.
func TestChildrenStopped(t *testing.T) {
	dir := t.TempDir()
	w := spec("parent", "children", filepath.Join(dir, "events"))
	w.Env["TEST_CHILD_PID"] = filepath.Join(dir, "child")
	h := start(t, w)
	<-h.started
	pid := childPid(t, filepath.Join(dir, "child"))
	defer syscall.Kill(pid, syscall.SIGKILL)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.sup.Restart(ctx, "parent", false); err != nil {
		t.Fatal(err)
	}
	eventually(t, "потомок завершён", func() bool {
		var ws syscall.WaitStatus
		_, _ = syscall.Wait4(pid, &ws, syscall.WNOHANG, nil)
		return !alive(pid) || zombie(pid)
	})
}

// zombie — процесс завершился, но не убран родителем (в контейнере его убирает init).
func zombie(pid int) bool {
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err != nil {
		return false
	}
	fields := strings.Fields(string(raw))
	return len(fields) > 2 && fields[2] == "Z"
}

// Пульса нет healthSilence — воркер нездоров; ещё healthRestart — перезапуск (§16).
func TestHeartbeatSilenceRestart(t *testing.T) {
	dir := t.TempDir()
	events, hang := filepath.Join(dir, "events"), filepath.Join(dir, "hang")
	w := spec("echo", "ok", events)
	w.Env["TEST_HANG"] = hang
	h := start(t, w)
	<-h.started
	_ = os.WriteFile(hang, []byte(strconv.Itoa(h.pid(t, "echo"))), 0o600)
	eventually(t, "нездоров", func() bool {
		st := h.status("echo")
		return st.Health != nil && !st.Health.OK && strings.HasPrefix(st.Health.Message, "нет пульса")
	})
	eventually(t, "перезапуск зависшего", func() bool { return h.status("echo").Restarts >= 1 && len(readEvents(events)) >= 2 })
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
	if st := h.status("stubborn"); st.State != protocol.WorkerStopped {
		t.Fatalf("состояние: %+v", st)
	}
}

// Apply: новый воркер запускается, удалённый останавливается, изменённый перезапускается.
func TestApply(t *testing.T) {
	dir := t.TempDir()
	a, b := spec("a", "ok", filepath.Join(dir, "a")), spec("b", "ok", filepath.Join(dir, "b"))
	h := start(t, a)
	<-h.started
	h.sup.Apply([]config.Worker{b})
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
	b2 := b
	b2.Env = map[string]string{"TEST_WORKER": "ok", "TEST_EVENTS": b.Env["TEST_EVENTS"], "X": "1"}
	h.sup.Apply([]config.Worker{b2})
	<-h.started
	if ev := readEvents(filepath.Join(dir, "b")); len(ev) != 3 {
		t.Fatalf("b перезапущен: %v", ev)
	}
}

// Регистрация (§13): манифест и версия — из регистрации (у воркера со сборкой — версия сборки);
// новый запуск — новая регистрация; изменение — внеочередное состояние.
func TestRegisterVersion(t *testing.T) {
	dir := t.TempDir()
	described := spec("described", "ok", filepath.Join(dir, "described"))
	described.Env["TEST_VERSION"] = "3.1.0"
	rel := config.Worker{Name: "report", Release: true, ReleaseDir: filepath.Join(dir, "workers", "report"), Lifecycle: life()}
	_ = os.MkdirAll(rel.ReleaseDir, 0o755)
	writeBuild(t, rel.Current(), releaseScript("ok", "9.9.9", filepath.Join(dir, "report")))
	_ = os.WriteFile(filepath.Join(rel.ReleaseDir, config.ReleaseVersion), []byte("1.0.0\n"), 0o644)
	h := start(t, described, rel)
	eventually(t, "версии", func() bool {
		return h.status("described").Version == "3.1.0" && h.status("report").Version == "1.0.0"
	})
	info := h.sup.Info()
	if len(info) != 2 || info[0].Manifest == nil || !info[1].Release || info[1].Version != "1.0.0" {
		t.Fatalf("hello: %+v", info)
	}
	changes := h.changes.Load()
	next := described
	next.Env = map[string]string{"TEST_WORKER": "ok", "TEST_EVENTS": described.Env["TEST_EVENTS"], "TEST_VERSION": "3.2.0"}
	h.sup.Apply([]config.Worker{next, rel})
	eventually(t, "новая регистрация после перезапуска", func() bool { return h.status("described").Version == "3.2.0" })
	if h.changes.Load() <= changes {
		t.Fatal("изменение — внеочередное состояние")
	}
}

// Не зарегистрировался за registerTimeout — invalid с причиной, процесс работает; пульс
// незарегистрированного не принимается; поздняя регистрация — running.
func TestRegisterTimeout(t *testing.T) {
	dir := t.TempDir()
	h := start(t, spec("silent", "noregister", filepath.Join(dir, "events")))
	eventually(t, "invalid", func() bool {
		st := h.status("silent")
		return st.State == protocol.WorkerInvalid && strings.Contains(st.Message, "не зарегистрировался")
	})
	if err := h.sup.Ready("silent"); !errors.Is(err, ErrNotRegistered) {
		t.Fatal(err)
	}
	if err := h.sup.Heartbeat("silent", protocol.Health{OK: true}); !errors.Is(err, ErrNotRegistered) {
		t.Fatal(err)
	}
	if ev := readEvents(filepath.Join(dir, "events")); len(ev) != 1 {
		t.Fatalf("invalid-воркер не перезапускается: %v", ev)
	}
	if err := h.sup.Register("silent", protocol.Register{Version: "1"}); err != nil {
		t.Fatal(err)
	}
	if st := h.status("silent"); st.State != protocol.WorkerRunning || st.Message != "" {
		t.Fatalf("поздняя регистрация: %+v", st)
	}
}

// releaseScript — тестовый воркер со сборкой: скрипт, запускающий тестовый файл в режиме mode
// с версией version.
func releaseScript(mode, version, events string) string {
	return fmt.Sprintf("#!/bin/sh\nTEST_WORKER=%s TEST_VERSION=%s TEST_EVENTS=%s exec %s\n", mode, version, events, os.Args[0])
}

func writeBuild(t *testing.T, path, script string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
}

// worker.update (§10): новая сборка зарегистрировалась с ok: true — она остаётся, итог
// {version, previous}; нездорова за срок — возврат прежней, UPDATE_FAILED; уже обновляется —
// BUSY; без release: true — WORKER_NOT_RELEASED.
func TestUpdateAndRollback(t *testing.T) {
	dir := t.TempDir()
	events := filepath.Join(dir, "events")
	rel := config.Worker{Name: "report", Release: true, ReleaseDir: filepath.Join(dir, "workers", "report"), Lifecycle: life()}
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
	before := h.pid(t, "report")

	res, err := h.sup.Update(ctx, "report", "1.1.0", false, func(dst string) error {
		writeBuild(t, dst, releaseScript("ok", "1.1.0", events))
		return nil
	})
	if err != nil || res != (protocol.UpdateResult{Version: "1.1.0", Previous: "1.0.0"}) {
		t.Fatalf("обновление: %+v %v", res, err)
	}
	if st := h.status("report"); st.Version != "1.1.0" || h.pid(t, "report") == before {
		t.Fatalf("после обновления: %+v", st)
	}

	// Новая сборка нездорова — возврат 1.1.0.
	_, err = h.sup.Update(ctx, "report", "1.2.0", false, func(dst string) error {
		writeBuild(t, dst, releaseScript("unhealthy", "1.2.0", events))
		return nil
	})
	var ei *protocol.ErrorInfo
	if !errors.As(err, &ei) || ei.Code != protocol.CodeUpdateFailed || !strings.Contains(ei.Message, "возвращена прежняя сборка 1.1.0") {
		t.Fatalf("откат: %v", err)
	}
	eventually(t, "прежняя сборка работает", func() bool {
		st := h.status("report")
		return st.State == protocol.WorkerRunning && st.Health != nil && st.Health.OK
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
			return protocol.NewError(protocol.CodeUpdateNotVerified, "нет ключа")
		})
	}()
	<-inFetch
	if _, err := h.sup.Update(ctx, "report", "1.3.0", false, nil); !errors.As(err, &ei) || ei.Code != protocol.CodeBusy {
		t.Fatalf("уже обновляется: %v", err)
	}
	close(block)
	wg.Wait()
	if !errors.As(first, &ei) || ei.Code != protocol.CodeUpdateNotVerified {
		t.Fatalf("ошибка загрузки — как есть: %v", first)
	}
	if _, err := h.sup.Update(ctx, "plain", "1", false, nil); !errors.As(err, &ei) || ei.Code != protocol.CodeWorkerNotReleased {
		t.Fatalf("без release: true: %v", err)
	}
	if _, err := h.sup.Update(ctx, "nope", "1", false, nil); !errors.As(err, &ei) || ei.Code != protocol.CodeWorkerUnknown {
		t.Fatalf("неизвестный: %v", err)
	}
}

// Сборка-архив: каталог current, без command — запускается ./run.
func TestReleaseArchiveRun(t *testing.T) {
	dir := t.TempDir()
	rel := config.Worker{Name: "packed", Release: true, ReleaseDir: filepath.Join(dir, "workers", "packed"), Lifecycle: life()}
	_ = os.MkdirAll(rel.Current(), 0o755)
	writeBuild(t, filepath.Join(rel.Current(), config.ReleaseRun), releaseScript("ok", "2.0.0", filepath.Join(dir, "events")))
	h := start(t, rel)
	if name := <-h.started; name != "packed" {
		t.Fatal(name)
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

// ./setup (§11): выполняется до первого запуска один раз — повторный запуск воркера его не
// вызывает, изменённый setup или файл зависимостей (requirements.txt) — снова; вывод — в журнал воркера; ошибка — воркер не
// запускается (повтор с паузой); срок — lifecycle.setupTimeout.
func TestSetup(t *testing.T) {
	dir := t.TempDir()
	runs := filepath.Join(dir, "runs")
	rel := config.Worker{Name: "packed", Release: true, ReleaseDir: filepath.Join(dir, "workers", "packed"), Lifecycle: life()}
	_ = os.MkdirAll(rel.Current(), 0o755)
	writeBuild(t, filepath.Join(rel.Current(), config.ReleaseRun), releaseScript("ok", "1.0.0", filepath.Join(dir, "events")))
	setup := filepath.Join(rel.Current(), config.Setup)
	writeBuild(t, setup, "#!/bin/sh\necho ставлю\necho \"$WORKER_STATE_DIR\" >> "+runs+"\n")
	h := start(t, rel)
	<-h.started
	if got := readEvents(runs); len(got) != 1 || got[0] != StateDir(h.dir, "packed") {
		t.Fatalf("setup до запуска, с окружением воркера: %v", got)
	}
	if !journalHasMsg(h, "packed", "ставлю") {
		t.Fatal("вывод setup — в журнале воркера")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.sup.Restart(ctx, "packed", true); err != nil {
		t.Fatal(err)
	}
	if got := readEvents(runs); len(got) != 1 {
		t.Fatalf("повторный запуск — без setup: %v", got)
	}
	writeBuild(t, setup, "#!/bin/sh\necho second >> "+runs+"\n")
	if err := h.sup.Restart(ctx, "packed", true); err != nil {
		t.Fatal(err)
	}
	if got := readEvents(runs); len(got) != 2 || got[1] != "second" {
		t.Fatalf("изменённый setup — снова: %v", got)
	}
	// Изменился файл зависимостей — снова; другой файл рядом — нет.
	_ = os.WriteFile(filepath.Join(rel.Current(), "requirements.txt"), []byte("x==1\n"), 0o644)
	_ = os.WriteFile(filepath.Join(rel.Current(), "notes.txt"), []byte("x\n"), 0o644)
	if err := h.sup.Restart(ctx, "packed", true); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(rel.Current(), "notes.txt"), []byte("y\n"), 0o644)
	if err := h.sup.Restart(ctx, "packed", true); err != nil {
		t.Fatal(err)
	}
	if got := readEvents(runs); len(got) != 3 {
		t.Fatalf("requirements.txt изменён — setup снова, notes.txt — нет: %v", got)
	}

	writeBuild(t, setup, "#!/bin/sh\necho сломано >&2\nexit 3\n")
	if err := h.sup.Restart(ctx, "packed", true); err == nil || !strings.Contains(err.Error(), "./setup") {
		t.Fatalf("ошибка setup: %v", err)
	}
	eventually(t, "повторы после ошибки setup", func() bool { return h.status("packed").Restarts >= 2 })
	if _, err := os.Stat(filepath.Join(rel.Current(), setupMark)); err != nil {
		t.Fatal("отметка прежнего успешного setup на месте")
	}

	slow := filepath.Join(t.TempDir(), "slow")
	_ = os.MkdirAll(slow, 0o755)
	writeBuild(t, filepath.Join(slow, config.Setup), "#!/bin/sh\nsleep 30\n")
	began := time.Now()
	err := runSetup(context.Background(), "slow", slow, nil, 200*time.Millisecond, logx.Discard())
	if err == nil || !strings.Contains(err.Error(), "setupTimeout") || time.Since(began) > 10*time.Second {
		t.Fatalf("срок setup: %v за %s", err, time.Since(began))
	}
	if err := os.Chmod(filepath.Join(slow, config.Setup), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := runSetup(context.Background(), "slow", slow, nil, time.Second, logx.Discard()); err == nil || !strings.Contains(err.Error(), "chmod") {
		t.Fatalf("неисполняемый setup: %v", err)
	}
}

func journalHasMsg(h *harness, name, text string) bool {
	for _, e := range h.journal.Tail(name, 100) {
		if e.Msg == text {
			return true
		}
	}
	return false
}

// ./cleanup (agent stop-workers --cleanup): выполняется в каталоге воркера с его окружением;
// воркер без cleanup пропускается; ошибка — в итоге.
func TestCleanup(t *testing.T) {
	dir := t.TempDir()
	done := filepath.Join(dir, "done")
	good := filepath.Join(dir, "good")
	bad := filepath.Join(dir, "bad")
	none := filepath.Join(dir, "none")
	for _, d := range []string{good, bad, none} {
		_ = os.MkdirAll(d, 0o755)
	}
	writeBuild(t, filepath.Join(good, config.Cleanup), "#!/bin/sh\necho \"$AGENT_WORKER_NAME $WORKER_STATE_DIR\" > "+done+"\n")
	writeBuild(t, filepath.Join(bad, config.Cleanup), "#!/bin/sh\nexit 2\n")
	specs := []config.Worker{
		{Name: "good", Command: []string{"x"}, Dir: good},
		{Name: "bad", Command: []string{"x"}, Dir: bad},
		{Name: "none", Command: []string{"x"}, Dir: none},
	}
	res := Cleanup(context.Background(), specs, dir, logx.Discard())
	if len(res) != 2 || res[0].Worker != "good" || res[0].Err != nil || res[1].Worker != "bad" || res[1].Err == nil {
		t.Fatalf("итоги: %+v", res)
	}
	if raw, _ := os.ReadFile(done); strings.TrimSpace(string(raw)) != "good "+StateDir(dir, "good") {
		t.Fatalf("окружение: %q", raw)
	}
}
