//go:build unix

package integration

import (
	"context"
	"net/http"
	"os"
	"slices"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/test/testserver"
)

// Действия worker.restart и agent.logs; health и манифест воркера в status; ошибки действий.
func TestWorkerActions(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	n := s.start(s.config(s.worker("w")))
	a := s.running("w")
	eventually(t, "health воркера в status", func() bool {
		b, _ := s.server.Agent(agentName)
		w, _ := b.Worker("w")
		return w.Health != nil && w.Health.OK && len(w.Health.Info) > 0
	})
	eventually(t, "манифест воркера в status, версия — из манифеста", func() bool {
		b, _ := s.server.Agent(agentName)
		w, _ := b.Worker("w")
		m := w.Manifest
		return m != nil && w.Version == "0.1.0" && len(m.Configs) == 2 && string(m.Configs[0].Schema) == `{"type":"object"}` &&
			len(m.Routes) > 1 && m.Routes[0].Path == "/echo" && len(m.Events) == 4
	})

	pid := s.pid(a.ID, "w")
	if err := s.server.Action("restartWorker", a.ID, nil, "w"); err != nil {
		t.Fatal(err)
	}
	if next := s.pid(a.ID, "w"); next == pid {
		t.Fatalf("после worker.restart тот же процесс %s", pid)
	}
	if err := s.server.Action("restartWorker", a.ID, nil, "nope"); errorCode(err) != "WORKER_UNKNOWN" {
		t.Fatalf("restart неизвестного воркера: %v", err)
	}
	if err := s.server.Action("updateWorker", a.ID, nil, "w"); errorCode(err) == "" {
		t.Fatal("updateWorker без сборок на сервере выполнен")
	}

	n.direct("w", http.MethodPost, "/print?text=строка-для-журнала")
	var entries []testserver.LogEntry
	defer func() {
		if t.Failed() {
			t.Logf("agent.logs: %+v", entries)
		}
	}()
	eventually(t, "agent.logs воркера", func() bool {
		entries = nil
		if err := s.server.Action("agentLogs", a.ID, &entries, map[string]any{"worker": "w", "lines": 50}); err != nil {
			t.Fatal(err)
		}
		return slices.ContainsFunc(entries, func(e testserver.LogEntry) bool { return e.Msg == "строка-для-журнала" })
	})
	if len(entries) > 50 {
		t.Fatalf("строк %d при lines 50", len(entries))
	}
	var own []testserver.LogEntry
	if err := s.server.Action("agentLogs", a.ID, &own, map[string]any{}); err != nil {
		t.Fatal(err)
	}
	for _, e := range own {
		if e.Source != message.LogSourceAgent {
			t.Fatalf("журнал агента: запись от %s", e.Source)
		}
	}
}

// Зависший воркер (нет ответа на GET /health три раза подряд) перезапускается; упавший —
// перезапускается с растущей паузой, restarts растёт.
func TestWorkerHealthAndCrash(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	n := s.start(s.config(s.worker("w")))
	a := s.running("w")
	pid := s.pid(a.ID, "w")

	n.direct("w", http.MethodPost, "/hang")
	within(t, 60*time.Second, "зависший воркер перезапущен", func() bool {
		b, _ := s.server.Agent(agentName)
		w, _ := b.Worker("w")
		if w.State != message.WorkerRunning || w.Restarts < 1 {
			return false
		}
		res, err := s.server.Fetch(a.ID, "w", "/pid", testserver.FetchInit{})
		return err == nil && res.Body != pid
	})

	// Падение: процесс воркера убит — агент запускает новый.
	pid = s.pid(a.ID, "w")
	p, _ := strconv.Atoi(pid)
	if err := syscall.Kill(p, syscall.SIGKILL); err != nil {
		t.Fatal(err)
	}
	eventually(t, "упавший воркер перезапущен", func() bool {
		res, err := s.server.Fetch(a.ID, "w", "/pid", testserver.FetchInit{})
		b, _ := s.server.Agent(agentName)
		w, _ := b.Worker("w")
		return err == nil && res.Body != pid && w.Restarts >= 2
	})
}

// keepChildren: с true дочерние процессы воркера переживают его перезапуск, без него —
// завершаются вместе с ним.
func TestKeepChildren(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	keep := s.worker("keep")
	keep.Lifecycle.KeepChildren = true
	s.start(s.config(keep, s.worker("plain")))
	s.running("keep")
	a := s.running("plain")

	child := func(w string) int {
		res := s.fetch(a.ID, w, "/child", testserver.FetchInit{Method: "POST"})
		pid, err := strconv.Atoi(res.Body)
		if err != nil {
			t.Fatalf("%s /child: %+v", w, res)
		}
		return pid
	}
	alive := func(pid int) bool {
		if syscall.Kill(pid, 0) != nil {
			return false
		}
		// Завершившийся, но не прибранный процесс — не живой.
		raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
		return err != nil || !containsState(string(raw), "Z")
	}
	kept, plain := child("keep"), child("plain")
	t.Cleanup(func() { _ = syscall.Kill(kept, syscall.SIGKILL) })
	for _, w := range []string{"keep", "plain"} {
		if err := s.server.Action("restartWorker", a.ID, nil, w); err != nil {
			t.Fatal(err)
		}
	}
	eventually(t, "потомок воркера без keepChildren завершён", func() bool { return !alive(plain) })
	if !alive(kept) {
		t.Fatal("потомок воркера с keepChildren завершён при перезапуске")
	}
}

// containsState — состояние процесса в /proc/<pid>/stat (поле после имени в скобках).
func containsState(stat, state string) bool {
	for i := len(stat) - 1; i >= 0; i-- {
		if stat[i] == ')' {
			return len(stat) > i+2 && string(stat[i+2]) == state
		}
	}
	return false
}

// Удаление агента (agent uninstall вызывает agent cleanup): без связи каждый воркер
// запускается, получает POST /cleanup и останавливается.
func TestCleanup(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	cfg := s.config(s.worker("a"), s.worker("b"))
	n := s.start(cfg)
	s.running("a")
	n.stop()
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
	defer cancel()
	results, err := app.Cleanup(ctx, cfg, "it")
	if err != nil {
		t.Fatal(err)
	}
	if len(results) != 2 {
		t.Fatalf("итоги уборки: %+v", results)
	}
	for _, r := range results {
		if r.Err != nil {
			t.Fatalf("%s: %v", r.Worker, r.Err)
		}
		if got := s.lines(r.Worker, "cleanup"); len(got) != 1 {
			t.Fatalf("%s: POST /cleanup не вызван: %q", r.Worker, got)
		}
	}
}
