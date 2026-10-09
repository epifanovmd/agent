//go:build unix

package integration

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/test/testserver"
)

// progress — события долгой работы id воркера: шаги (без повторов) и pid процесса, итог.
func progress(a testserver.Agent, id string) (steps map[int]bool, pids map[int]bool, done bool) {
	steps, pids = map[int]bool{}, map[int]bool{}
	for _, e := range a.Events {
		var d struct {
			ID   string `json:"id"`
			Step int    `json:"step"`
			PID  int    `json:"pid"`
		}
		if json.Unmarshal(e.Data, &d) != nil || d.ID != id {
			continue
		}
		pids[d.PID] = true
		switch e.Type {
		case "example.progress":
			steps[d.Step] = true
		case "example.done":
			done = true
		}
	}
	return steps, pids, done
}

// Долгая работа воркера переживает остановку и запуск агента (SIGTERM, менеджер запускает
// его снова) и самообновление агента: процесс воркера тот же, события прогресса идут дальше
// (написанные без агента доходят после его запуска), итог дошёл.
func TestLongWorkSurvivesAgent(t *testing.T) {
	t.Parallel()
	kit := newReleaseKit(t)
	kit.write()
	s := newStand(t, func(c *testserver.Config) {
		c.ReleasesDir, c.PublicKey = kit.dir, kit.pub
		c.Options = map[string]any{"offlineGraceMs": 0}
	})
	dir := t.TempDir()
	bin := filepath.Join(dir, "agent")
	if err := os.WriteFile(bin, testBinary(t, "1.0.0"), 0o755); err != nil {
		t.Fatal(err)
	}
	exe, _ := os.Executable()
	state := s.stateDir("w")
	_ = os.MkdirAll(state, 0o755)
	data := filepath.Join(dir, "data")
	conf := filepath.Join(dir, "agent.yaml")
	writeFile(t, conf, fmt.Sprintf(`server: {url: %q}
dataDir: %q
name: %s
enroll: {token: %s}
log: {level: error}
telemetry: {metrics: []}
update: {mode: self, publicKey: %q}
workers:
  - name: w
    command: [%q]
    env: {IT_WORKER: "1", IT_STATE: %q}
    lifecycle: {stopTimeout: 5s}
`, s.http.URL, data, agentName, token, kit.pub, exe, state))
	r := runAgent(t, bin, conf, data)
	a := s.running("w")
	pid := s.pid(a.ID, "w")
	boot := a.BootID

	s.fetch(a.ID, "w", "/work?id=long&steps=40&ms=250", testserver.FetchInit{Method: "POST"})
	eventually(t, "работа идёт", func() bool {
		b, _ := s.server.Agent(agentName)
		steps, _, _ := progress(b, "long")
		return len(steps) >= 3
	})

	// Остановка агента: воркер работает дальше, агент запускается снова и подхватывает его.
	r.signal(syscall.SIGTERM)
	within(t, 60*time.Second, "агент запущен снова", func() bool {
		b, ok := s.server.Agent(agentName)
		return ok && b.Online && b.BootID != "" && b.BootID != boot
	})
	if got := s.pid(a.ID, "w"); got != pid {
		t.Fatalf("воркер запущен заново после перезапуска агента: %s → %s", pid, got)
	}

	// Самообновление агента во время работы.
	kit.setAgent("1.1.0", testBinary(t, "1.1.0"))
	var res testserver.UpdateResult
	if err := s.server.Action("updateAgent", a.ID, &res, map[string]any{"timeoutMs": 120_000}); err != nil || res.Version != "1.1.0" {
		t.Fatalf("agent.update: %+v %v", res, err)
	}
	if got := s.pid(a.ID, "w"); got != pid {
		t.Fatalf("воркер запущен заново после обновления агента: %s → %s", pid, got)
	}

	within(t, 60*time.Second, "работа закончена", func() bool {
		b, _ := s.server.Agent(agentName)
		_, _, done := progress(b, "long")
		return done
	})
	b, _ := s.server.Agent(agentName)
	steps, pids, _ := progress(b, "long")
	if len(steps) != 40 || len(pids) != 1 {
		t.Fatalf("шаги %d из 40, процессов %d", len(steps), len(pids))
	}
	if w, _ := b.Worker("w"); w.Restarts != 0 || w.State != message.WorkerRunning {
		t.Fatalf("воркер: %+v", w)
	}
}

// worker.restart во время долгой работы ждёт её окончания (status.workers[].pending:
// restart), итог — после замены; force — сразу.
func TestRestartWaitsForBusy(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	w := s.worker("w")
	interval := config.Duration(200 * time.Millisecond)
	w.Lifecycle.Health.Interval, w.Lifecycle.Health.Timeout = &interval, interval
	s.start(s.config(w))
	a := s.running("w")
	pid := s.pid(a.ID, "w")

	s.fetch(a.ID, "w", "/work?id=a&steps=8&ms=250", testserver.FetchInit{Method: "POST"})
	eventually(t, "воркер занят", func() bool {
		b, _ := s.server.Agent(agentName)
		w, _ := b.Worker("w")
		return w.Health != nil && w.Health.Busy
	})
	done := make(chan error, 1)
	go func() { done <- s.server.Action("restartWorker", a.ID, nil, "w") }()
	eventually(t, "замена ждёт", func() bool {
		b, _ := s.server.Agent(agentName)
		w, _ := b.Worker("w")
		return w.Pending == message.PendingRestart
	})
	if got := s.pid(a.ID, "w"); got != pid {
		t.Fatal("занятый воркер заменён")
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	b, _ := s.server.Agent(agentName)
	if _, _, finished := progress(b, "a"); !finished {
		t.Fatal("замена прошла до окончания работы")
	}
	next := s.pid(a.ID, "w")
	if next == pid {
		t.Fatal("после окончания работы воркер не заменён")
	}

	// force — не ждать.
	s.fetch(a.ID, "w", "/work?id=b&steps=40&ms=250", testserver.FetchInit{Method: "POST"})
	eventually(t, "воркер занят", func() bool {
		b, _ := s.server.Agent(agentName)
		w, _ := b.Worker("w")
		return w.Health != nil && w.Health.Busy
	})
	began := time.Now()
	if err := s.server.Action("restartWorker", a.ID, nil, "w", map[string]any{"force": true}); err != nil {
		t.Fatal(err)
	}
	if time.Since(began) > 8*time.Second || s.pid(a.ID, "w") == next {
		t.Fatalf("force: замена через %s", time.Since(began))
	}
	b, _ = s.server.Agent(agentName)
	if _, _, finished := progress(b, "b"); finished {
		t.Fatal("force: работа не прервана")
	}
}
