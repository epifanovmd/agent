//go:build unix

package worker

import (
	"context"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
)

// call — запрос к запущенному воркеру: тело ответа.
func (h *harness) call(t *testing.T, name, method, path string) string {
	t.Helper()
	client, err := h.sup.Client(name)
	if err != nil {
		t.Fatal(err)
	}
	req, _ := http.NewRequest(method, "http://worker"+path, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return string(raw)
}

func (h *harness) pid(t *testing.T, name string) int {
	t.Helper()
	pid, err := strconv.Atoi(h.call(t, name, http.MethodGet, "/pid"))
	if err != nil {
		t.Fatal(err)
	}
	return pid
}

// journalHas — в журнале воркера есть строка text.
func (h *harness) journalHas(name, text string) bool {
	for _, e := range h.journal.Tail(name, 100) {
		if e.Msg == text {
			return true
		}
	}
	return false
}

// Вывод воркера — в файлах <dataDir>/logs (stdout и stderr отдельно), агент
// читает их в журнал с уровнями; большой файл переносится в .1 и очищается.
func TestOutputFiles(t *testing.T) {
	w := spec("echo", "ok", "")
	w.Logs = config.Logs{MaxSize: 2048}
	h := start(t, w)
	<-h.started
	files := LogFiles(filepath.Join(h.dir, LogsDir), "echo")
	eventually(t, "строка запуска в файле и журнале", func() bool {
		raw, _ := os.ReadFile(files[0])
		return strings.Contains(string(raw), "воркер слушает сокет") && h.journalHas("echo", "воркер слушает сокет")
	})
	h.call(t, "echo", http.MethodPost, "/print?stderr=1&text=ошибка")
	eventually(t, "stderr — warn", func() bool {
		raw, _ := os.ReadFile(files[1])
		for _, e := range h.journal.Tail("echo", 100) {
			if e.Msg == "ошибка" && e.Level == "warn" {
				return strings.Contains(string(raw), "ошибка")
			}
		}
		return false
	})
	for i := range 40 {
		h.call(t, "echo", http.MethodPost, "/print?text="+strings.Repeat("x", 100)+strconv.Itoa(i))
	}
	eventually(t, "файл перенесён в .1 и очищен", func() bool {
		st, err := os.Stat(files[0])
		_, err1 := os.Stat(files[0] + ".1")
		return err == nil && err1 == nil && st.Size() <= 2048
	})
	eventually(t, "все строки в журнале", func() bool { return h.journalHas("echo", strings.Repeat("x", 100)+"39") })
}

// Агент перезапускается (ExitRestart, onAgentRestart: keep): воркер
// работает дальше; следующий агент подхватывает тот же процесс с тем же
// токеном, читает его вывод (и написанное, пока агента не было), следит за
// ним; остановка агента без SetExit останавливает воркер.
func TestAdoptAfterAgentRestart(t *testing.T) {
	dir := tempDir(t)
	events := filepath.Join(dir, "events")
	w := spec("echo", "ok", events)
	h1 := startIn(t, dir, w)
	<-h1.started
	pid, token := h1.pid(t, "echo"), h1.sup.Token("echo")
	h1.sup.SetExit(ExitRestart)
	h1.stop()
	if !alive(pid) || len(readEvents(events)) != 1 {
		t.Fatalf("воркер должен работать дальше: %v", readEvents(events))
	}
	st, ok := readState(filepath.Join(dir, ProcessesDir), "echo")
	if !ok || st.PID != pid || st.Token != token || st.Start == "" {
		t.Fatalf("файл процесса: %+v", st)
	}
	// Пока агента нет, воркер пишет в свой файл.
	client := unixClient(st.Socket)
	resp, err := client.Post("http://worker/print?text=без+агента", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()

	h2 := startIn(t, dir, w)
	if name := <-h2.adopted; name != "echo" {
		t.Fatal(name)
	}
	if got := h2.pid(t, "echo"); got != pid || h2.sup.Token("echo") != token {
		t.Fatalf("подхвачен другой процесс: %d %q", got, h2.sup.Token("echo"))
	}
	eventually(t, "вывод без агента — в журнале нового агента", func() bool { return h2.journalHas("echo", "без агента") })
	eventually(t, "health подхваченного", func() bool {
		st := h2.status("echo")
		return st.State == message.WorkerRunning && st.Health != nil && st.Health.OK
	})
	select {
	case name := <-h2.started:
		t.Fatalf("подхваченный воркер запущен заново: %s", name)
	default:
	}
	h2.stop()
	eventually(t, "остановка агента — воркер остановлен", func() bool { return !alive(pid) || zombie(pid) })
	if _, ok := readState(filepath.Join(dir, ProcessesDir), "echo"); ok {
		t.Fatal("файл процесса остался")
	}
}

// onAgentRestart: restart и onAgentStop: stop — воркер останавливается
// вместе с агентом; onAgentStop: keep — работает дальше.
func TestExitPolicies(t *testing.T) {
	for _, c := range []struct {
		name  string
		exit  Exit
		tune  func(*config.Lifecycle)
		alive bool
	}{
		{"restart-keep", ExitRestart, func(*config.Lifecycle) {}, true},
		{"restart-restart", ExitRestart, func(l *config.Lifecycle) { l.OnAgentRestart = config.RestartWorker }, false},
		{"stop-keep", ExitStop, func(*config.Lifecycle) {}, true},
		{"stop-stop", ExitStop, func(l *config.Lifecycle) { l.OnAgentStop = config.StopWorker }, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			w := spec("echo", "ok", "")
			c.tune(&w.Lifecycle)
			h := start(t, w)
			<-h.started
			pid := h.pid(t, "echo")
			defer syscall.Kill(pid, syscall.SIGKILL)
			h.sup.SetExit(c.exit)
			h.stop()
			time.Sleep(100 * time.Millisecond)
			if got := alive(pid) && !zombie(pid); got != c.alive {
				t.Fatalf("жив: %v, ждали %v", got, c.alive)
			}
		})
	}
}

// Файл процесса с pid, выданным другому процессу (время запуска другое), —
// не подхват: воркер запускается заново, чужой процесс не трогается.
// Процесс воркера, которого нет в настройках, останавливается.
func TestAdoptRejectsReusedPID(t *testing.T) {
	dir := tempDir(t)
	stranger := exec.Command("sleep", "30")
	orphan := exec.Command("sleep", "30")
	for _, c := range []*exec.Cmd{stranger, orphan} {
		if err := c.Start(); err != nil {
			t.Fatal(err)
		}
		defer c.Process.Kill()
		go func() { _ = c.Wait() }()
	}
	states := filepath.Join(dir, ProcessesDir)
	_ = writeState(states, "echo", procState{PID: stranger.Process.Pid, Start: "не то", Socket: filepath.Join(dir, "x.sock"), Token: "t"})
	start, _ := procStart(orphan.Process.Pid)
	_ = writeState(states, "gone", procState{PID: orphan.Process.Pid, Start: start, StopTimeout: config.Duration(time.Second), KeepChildren: true})

	h := startIn(t, dir, spec("echo", "ok", ""))
	<-h.started
	if h.pid(t, "echo") == stranger.Process.Pid || h.sup.Token("echo") == "t" {
		t.Fatal("подхвачен чужой процесс")
	}
	eventually(t, "процесс удалённого воркера остановлен", func() bool { _, ok := procStart(orphan.Process.Pid); return !ok })
	if !alive(stranger.Process.Pid) {
		t.Fatal("чужой процесс завершён")
	}
	if _, ok := readState(states, "gone"); ok {
		t.Fatal("файл удалённого воркера остался")
	}
}

// restartAsync — worker.restart в фоне; итог — в канале, отсрочка (pending) — в deferred.
func restartAsync(h *harness, name string, force bool) chan error {
	done, _ := restartDeferred(h, name, force)
	return done
}

func restartDeferred(h *harness, name string, force bool) (chan error, chan string) {
	done, deferred := make(chan error, 1), make(chan string, 2)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		done <- h.sup.Restart(WithDeferred(ctx, func(p string) { deferred <- p }), name, force)
	}()
	return done, deferred
}

// busy: замена ждёт, пока воркер занят (status pending: restart), и идёт,
// когда он освободился; force — сразу; busy.timeout — предел ожидания.
func TestBusyDefersReplace(t *testing.T) {
	dir := t.TempDir()
	busy := filepath.Join(dir, "busy")
	_ = os.WriteFile(busy, nil, 0o600)
	w := spec("echo", "ok", filepath.Join(dir, "events"))
	w.Env["TEST_BUSY"] = busy
	h := start(t, w)
	<-h.started
	pid := h.pid(t, "echo")

	done, deferred := restartDeferred(h, "echo", false)
	eventually(t, "замена ждёт", func() bool {
		st := h.status("echo")
		return st.Pending == message.PendingRestart && st.Health != nil && st.Health.Busy
	})
	select {
	case p := <-deferred:
		if p != message.PendingRestart {
			t.Fatalf("отсрочка: %q", p)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("контекст замены не узнал об отсрочке")
	}
	time.Sleep(300 * time.Millisecond)
	if h.pid(t, "echo") != pid {
		t.Fatal("занятый воркер заменён")
	}
	_ = os.Remove(busy)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if h.pid(t, "echo") == pid || h.status("echo").Pending != "" {
		t.Fatalf("после окончания работы — замена: %+v", h.status("echo"))
	}

	// force — сразу.
	_ = os.WriteFile(busy, nil, 0o600)
	eventually(t, "занят", func() bool { st := h.status("echo"); return st.Health != nil && st.Health.Busy })
	pid = h.pid(t, "echo")
	forced, forcedDeferred := restartDeferred(h, "echo", true)
	if err := <-forced; err != nil || h.pid(t, "echo") == pid {
		t.Fatalf("force: %v", err)
	}
	if len(forcedDeferred) != 0 || len(deferred) != 0 {
		t.Fatal("force не откладывается; об отсрочке сообщается один раз")
	}

	// busy.timeout — дольше не ждать.
	next := w
	next.Lifecycle = life()
	next.Lifecycle.Busy.Timeout = config.Duration(300 * time.Millisecond)
	h.sup.Apply([]config.Worker{next}) // меняется только lifecycle — без замены
	eventually(t, "занят", func() bool { st := h.status("echo"); return st.Health != nil && st.Health.Busy })
	pid = h.pid(t, "echo")
	began := time.Now()
	if err := <-restartAsync(h, "echo", false); err != nil || h.pid(t, "echo") == pid || time.Since(began) < 300*time.Millisecond {
		t.Fatalf("busy.timeout: %v %v", err, time.Since(began))
	}
}

// Занятый воркер завис (нет ответа на GET /health) — ожидание кончается,
// замена идёт; новый процесс — один.
func TestBusyHangRestarts(t *testing.T) {
	dir := t.TempDir()
	busy, hang := filepath.Join(dir, "busy"), filepath.Join(dir, "hang")
	_ = os.WriteFile(busy, nil, 0o600)
	w := spec("echo", "ok", filepath.Join(dir, "events"))
	w.Env["TEST_BUSY"], w.Env["TEST_HANG"] = busy, hang
	h := start(t, w)
	<-h.started
	done := restartAsync(h, "echo", false)
	eventually(t, "замена ждёт", func() bool { return h.status("echo").Pending == message.PendingRestart })
	_ = os.WriteFile(hang, []byte(strconv.Itoa(h.pid(t, "echo"))), 0o600)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	time.Sleep(200 * time.Millisecond)
	starts := 0
	for _, e := range readEvents(filepath.Join(dir, "events")) {
		if strings.HasPrefix(e, "start ") {
			starts++
		}
	}
	if starts != 2 {
		t.Fatalf("запусков: %d (лишняя замена после перезапуска зависшего)", starts)
	}
}

// lifecycle.restart: on-failure — выход с кодом 0 без перезапуска; always —
// перезапуск; never — падение без перезапуска.
func TestRestartPolicy(t *testing.T) {
	for _, c := range []struct {
		mode, policy string
		restarted    bool
	}{
		{"exit0", config.RestartFailure, false},
		{"exit0", config.RestartAlways, true},
		{"crash", config.RestartNever, false},
		{"crash", config.RestartFailure, true},
	} {
		t.Run(c.mode+"-"+c.policy, func(t *testing.T) {
			w := spec("w", c.mode, "")
			w.Lifecycle.Restart = c.policy
			h := start(t, w)
			if c.mode == "exit0" {
				<-h.started
			}
			if c.restarted {
				eventually(t, "перезапуск", func() bool { return h.status("w").Restarts >= 1 })
				return
			}
			eventually(t, "остановлен", func() bool {
				st := h.status("w")
				return st.State == message.WorkerStopped && st.Restarts == 0 && h.journalHas(message.LogSourceAgent, "воркер не перезапускается (lifecycle.restart: "+c.policy+") — до worker.restart или изменения настроек")
			})
			time.Sleep(200 * time.Millisecond)
			if st := h.status("w"); st.State != message.WorkerStopped || st.Restarts != 0 {
				t.Fatalf("без перезапуска: %+v", st)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := h.sup.Restart(ctx, "w", false); c.mode == "exit0" && err != nil {
				t.Fatalf("worker.restart запускает снова: %v", err)
			}
		})
	}
}

// health.failures: 0 — не отвечающий воркер не перезапускается; interval: 0
// — GET /health не вызывается.
func TestHealthDisabled(t *testing.T) {
	deaf := spec("deaf", "deaf", "")
	zero := 0
	deaf.Lifecycle.Health.Failures = &zero
	off := spec("off", "ok", "")
	var none config.Duration
	off.Lifecycle.Health.Interval = &none
	h := start(t, deaf, off)
	<-h.started
	time.Sleep(600 * time.Millisecond)
	// Не отвечает — не зарегистрирован (invalid), но и не перезапускается.
	if st := h.status("deaf"); st.Restarts != 0 || st.State != message.WorkerInvalid {
		t.Fatalf("failures: 0: %+v", st)
	}
	if st := h.status("off"); st.Health != nil {
		t.Fatalf("interval: 0: %+v", st)
	}
}

// StopProcesses (agent stop-workers): процессы, оставшиеся после остановки
// агента, останавливаются.
func TestStopProcesses(t *testing.T) {
	dir := tempDir(t)
	h := startIn(t, dir, spec("echo", "ok", ""))
	<-h.started
	pid := h.pid(t, "echo")
	h.sup.SetExit(ExitStop)
	h.stop()
	if names := StopProcesses(dir); len(names) != 1 || names[0] != "echo" {
		t.Fatalf("остановлены: %v", names)
	}
	eventually(t, "процесс завершён", func() bool { return !alive(pid) || zombie(pid) })
}

// Занятый воркер, который перестал отвечать, перезапускается как зависший:
// busy не защищает от перезапуска.
func TestBusyHangKilled(t *testing.T) {
	dir := t.TempDir()
	busy, hang := filepath.Join(dir, "busy"), filepath.Join(dir, "hang")
	_ = os.WriteFile(busy, nil, 0o600)
	w := spec("echo", "ok", filepath.Join(dir, "events"))
	w.Env["TEST_BUSY"], w.Env["TEST_HANG"] = busy, hang
	h := start(t, w)
	<-h.started
	eventually(t, "занят", func() bool { st := h.status("echo"); return st.Health != nil && st.Health.Busy })
	_ = os.WriteFile(hang, []byte(strconv.Itoa(h.pid(t, "echo"))), 0o600)
	eventually(t, "зависший перезапущен", func() bool { return h.status("echo").Restarts == 1 })
}
