//go:build unix

package worker

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/protocol"
)

// say — вывод тестового воркера через его файл TEST_CMD (строки «out …» и «err …»).
func say(t *testing.T, w config.Worker, lines ...string) {
	t.Helper()
	path := w.Env["TEST_CMD"]
	eventually(t, "воркер прочитал прежние строки", func() bool { _, err := os.Stat(path); return os.IsNotExist(err) })
	if err := os.WriteFile(path+".tmp", []byte(strings.Join(lines, "\n")), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(path+".tmp", path); err != nil {
		t.Fatal(err)
	}
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

// Вывод воркера — в файлах <dataDir>/logs (stdout и stderr отдельно), агент читает их в журнал
// с уровнями; большой файл переносится в .1 и очищается.
func TestOutputFiles(t *testing.T) {
	// Прежнее значение — после остановки воркера (очистки идут в обратном порядке).
	prev := logMaxSize
	t.Cleanup(func() { logMaxSize = prev })
	logMaxSize = 2048
	w := spec("echo", "ok", "")
	w.Env["TEST_CMD"] = filepath.Join(t.TempDir(), "cmd")
	h := start(t, w)
	<-h.started
	files := LogFiles(filepath.Join(h.dir, LogsDir), "echo")
	eventually(t, "строка запуска в файле и журнале", func() bool {
		raw, _ := os.ReadFile(files[0])
		return strings.Contains(string(raw), "воркер запущен") && h.journalHas("echo", "воркер запущен")
	})
	say(t, w, "err ошибка")
	eventually(t, "stderr — warn", func() bool {
		raw, _ := os.ReadFile(files[1])
		for _, e := range h.journal.Tail("echo", 100) {
			if e.Msg == "ошибка" && e.Level == "warn" {
				return strings.Contains(string(raw), "ошибка")
			}
		}
		return false
	})
	var lines []string
	for i := range 40 {
		lines = append(lines, "out "+strings.Repeat("x", 100)+strconv.Itoa(i))
	}
	say(t, w, lines...)
	eventually(t, "файл перенесён в .1 и очищен", func() bool {
		st, err := os.Stat(files[0])
		_, err1 := os.Stat(files[0] + ".1")
		return err == nil && err1 == nil && st.Size() <= 2048
	})
	eventually(t, "все строки в журнале", func() bool { return h.journalHas("echo", strings.Repeat("x", 100)+"39") })
}

// Агент перезапускается (ExitRestart): воркер работает дальше; следующий агент подхватывает тот
// же процесс с тем же токеном, читает его вывод (и написанное, пока агента не было), ждёт
// повторной регистрации; остановка агента без SetExit останавливает воркер.
func TestAdoptAfterAgentRestart(t *testing.T) {
	dir := tempDir(t)
	events := filepath.Join(dir, "events")
	w := spec("echo", "ok", events)
	w.Env["TEST_CMD"] = filepath.Join(dir, "cmd")
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
	say(t, w, "out без агента")

	h2 := startIn(t, dir, w)
	if name := <-h2.started; name != "echo" {
		t.Fatal(name)
	}
	if got := h2.pid(t, "echo"); got != pid || h2.sup.Token("echo") != token {
		t.Fatalf("подхвачен другой процесс: %d %q", got, h2.sup.Token("echo"))
	}
	eventually(t, "вывод без агента — в журнале нового агента", func() bool { return h2.journalHas("echo", "без агента") })
	eventually(t, "пульс подхваченного", func() bool {
		st := h2.status("echo")
		return st.State == protocol.WorkerRunning && st.Health != nil && st.Health.OK
	})
	if ev := readEvents(events); len(ev) != 1 {
		t.Fatalf("подхваченный воркер запущен заново: %v", ev)
	}
	h2.stop()
	eventually(t, "остановка агента — воркер остановлен", func() bool { return !alive(pid) || zombie(pid) })
	if _, ok := readState(filepath.Join(dir, ProcessesDir), "echo"); ok {
		t.Fatal("файл процесса остался")
	}
}

// Перезапуск и остановка агента (ExitRestart, ExitStop) — воркер работает дальше; без SetExit —
// останавливается вместе с агентом.
func TestExitPolicies(t *testing.T) {
	for _, c := range []struct {
		name  string
		exit  Exit
		alive bool
	}{
		{"restart", ExitRestart, true},
		{"stop", ExitStop, true},
		{"shutdown", ExitShutdown, false},
	} {
		t.Run(c.name, func(t *testing.T) {
			h := start(t, spec("echo", "ok", ""))
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

// Файл процесса с pid, выданным другому процессу (время запуска другое), — не подхват: воркер
// запускается заново, чужой процесс не трогается. Процесс воркера, которого нет в настройках,
// останавливается.
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
	_ = writeState(states, "echo", procState{PID: stranger.Process.Pid, Start: "не то", Token: "t"})
	start, _ := procStart(orphan.Process.Pid)
	_ = writeState(states, "gone", procState{PID: orphan.Process.Pid, Start: start, StopTimeout: config.Duration(time.Second)})

	h := startIn(t, dir, spec("echo", "ok", ""))
	<-h.started
	if h.pid(t, "echo") == stranger.Process.Pid || h.sup.Token("echo") == "t" {
		t.Fatal("подхвачен чужой процесс")
	}
	eventually(t, "процесс удалённого воркера остановлен", func() bool { _, ok := procStart(orphan.Process.Pid); return !ok })
	if !alive(stranger.Process.Pid) {
		t.Fatal("чужой процесс завершён")
	}
	eventually(t, "файл удалённого воркера удалён", func() bool { _, ok := readState(states, "gone"); return !ok })
}

// restartAsync — worker.restart в фоне; итог — в канале.
func restartAsync(h *harness, name string, force bool) chan error {
	done, _ := restartDeferred(h, name, force)
	return done
}

// restartDeferred — worker.restart в фоне; итог и отсрочка (pending) — в каналах.
func restartDeferred(h *harness, name string, force bool) (chan error, chan string) {
	done, deferred := make(chan error, 1), make(chan string, 2)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		done <- h.sup.Restart(WithDeferred(ctx, func(p string) { deferred <- p }), name, force)
	}()
	return done, deferred
}

// busy (§16): замена ждёт, пока воркер занят (pending: restart), и идёт, когда он освободился;
// force — сразу; busyTimeout — предел ожидания.
func TestBusyDefersReplace(t *testing.T) {
	dir := t.TempDir()
	busy := filepath.Join(dir, "busy")
	_ = os.WriteFile(busy, nil, 0o600)
	w := spec("echo", "ok", filepath.Join(dir, "events"))
	w.Env["TEST_BUSY"] = busy
	h := start(t, w)
	<-h.started
	eventually(t, "занят", func() bool { st := h.status("echo"); return st.Health != nil && st.Health.Busy })
	pid := h.pid(t, "echo")

	done, deferred := restartDeferred(h, "echo", false)
	eventually(t, "замена ждёт", func() bool { return h.status("echo").Pending == protocol.PendingRestart })
	select {
	case p := <-deferred:
		if p != protocol.PendingRestart {
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

	// busyTimeout — дольше не ждать.
	defer func(d time.Duration) { busyTimeout = d }(busyTimeout)
	busyTimeout = 300 * time.Millisecond
	eventually(t, "занят", func() bool { st := h.status("echo"); return st.Health != nil && st.Health.Busy })
	pid = h.pid(t, "echo")
	began := time.Now()
	if err := <-restartAsync(h, "echo", false); err != nil || h.pid(t, "echo") == pid || time.Since(began) < 300*time.Millisecond {
		t.Fatalf("busyTimeout: %v %v", err, time.Since(began))
	}
}

// Занятый воркер завис (нет пульса) — ожидание кончается, замена идёт; новый процесс — один.
func TestBusyHangRestarts(t *testing.T) {
	dir := t.TempDir()
	busy, hang := filepath.Join(dir, "busy"), filepath.Join(dir, "hang")
	_ = os.WriteFile(busy, nil, 0o600)
	w := spec("echo", "ok", filepath.Join(dir, "events"))
	w.Env["TEST_BUSY"], w.Env["TEST_HANG"] = busy, hang
	h := start(t, w)
	<-h.started
	eventually(t, "занят", func() bool { st := h.status("echo"); return st.Health != nil && st.Health.Busy })
	done := restartAsync(h, "echo", false)
	eventually(t, "замена ждёт", func() bool { return h.status("echo").Pending == protocol.PendingRestart })
	_ = os.WriteFile(hang, []byte(strconv.Itoa(h.pid(t, "echo"))), 0o600)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	time.Sleep(time.Second)
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

// Выход с кодом 0 — без перезапуска (до worker.restart); падение — перезапуск.
func TestRestartAfterExit(t *testing.T) {
	for _, c := range []struct {
		mode      string
		restarted bool
	}{{"exit0", false}, {"crash", true}} {
		t.Run(c.mode, func(t *testing.T) {
			h := start(t, spec("w", c.mode, ""))
			if c.restarted {
				eventually(t, "перезапуск", func() bool { return h.status("w").Restarts >= 1 })
				return
			}
			eventually(t, "остановлен", func() bool {
				st := h.status("w")
				return st.State == protocol.WorkerStopped && st.Restarts == 0 &&
					h.journalHas(protocol.LogSourceAgent, "воркер завершился сам (exit 0) — не перезапускается до worker.restart или изменения настроек")
			})
			time.Sleep(200 * time.Millisecond)
			if st := h.status("w"); st.State != protocol.WorkerStopped || st.Restarts != 0 {
				t.Fatalf("без перезапуска: %+v", st)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := h.sup.Restart(ctx, "w", false); err != nil {
				t.Fatalf("worker.restart запускает снова: %v", err)
			}
		})
	}
}

// StopProcesses (agent stop-workers): процессы, оставшиеся после остановки агента,
// останавливаются.
func TestStopProcesses(t *testing.T) {
	dir := tempDir(t)
	h := startIn(t, dir, spec("echo", "ok", ""))
	<-h.started
	pid := h.pid(t, "echo")
	h.sup.SetExit(ExitStop)
	h.stop()
	if names, failed := StopProcesses(dir); len(names) != 1 || names[0] != "echo" || len(failed) != 0 {
		t.Fatalf("остановлены: %v", names)
	}
	eventually(t, "процесс завершён", func() bool { return !alive(pid) || zombie(pid) })
}

// Занятый воркер без пульса перезапускается как зависший: busy не защищает от перезапуска.
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
