//go:build unix

package integration

import (
	"context"
	"encoding/json"
	"os"
	"slices"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/sdk/go/message"
	"github.com/epifanovmd/agent/sdk/go/worker"
	"github.com/epifanovmd/agent/test/testserver"
)

// controlWorker — воркер ctl (IT_WORKER_KIND=control) на sdk/go/worker:
// очередь ctl.echo и команды, через которые тест управляет им самим —
// контекст (ctl.context, с pid), здоровье, пауза, просьба о замене.
func controlWorker() {
	w := worker.New("ctl", "it")
	w.Job("ctl.echo", 1, func(_ context.Context, job *worker.Job) (any, error) {
		return map[string]string{"echo": string(job.Data)}, nil
	})
	w.Command("ctl.context", func(context.Context, *worker.Command) (any, error) {
		return map[string]any{"pid": os.Getpid(), "context": w.Context()}, nil
	})
	// Канал показателей с частотой «авто»: подписка на него — в контексте воркера.
	var polls atomic.Int64
	w.Telemetry("example.ctl", worker.AutoInterval, func() any {
		return map[string]int64{"polls": polls.Add(1)}
	})
	w.Command("ctl.health", func(_ context.Context, cmd *worker.Command) (any, error) {
		var h message.WorkerHealth
		_ = json.Unmarshal(cmd.Args, &h)
		return nil, w.SetHealth(h.OK, h.Message)
	})
	w.Command("ctl.pause", func(_ context.Context, cmd *worker.Command) (any, error) {
		var pause bool
		_ = json.Unmarshal(cmd.Args, &pause)
		if pause {
			return nil, w.Pause()
		}
		return nil, w.Resume()
	})
	w.Command("ctl.restart", func(context.Context, *worker.Command) (any, error) {
		return nil, w.RequestRestart("тест")
	})
	if err := w.Run(context.Background()); err != nil {
		os.Exit(1)
	}
}

// run — команда агенту it-agent; итог — result успешной команды.
func (s *stand) run(name string, args any) json.RawMessage {
	s.t.Helper()
	cmd, err := s.server.Command(testserver.CommandRequest{Name: name, Args: data(args)})
	if err != nil {
		s.t.Fatal(err)
	}
	return s.await(cmd.ID)
}

func (s *stand) await(id string) json.RawMessage {
	s.t.Helper()
	var c testserver.Command
	eventually(s.t, "команда "+id, func() bool {
		c, _ = s.server.CommandSnapshot(id)
		if c.Status == testserver.CommandFailed {
			s.t.Fatalf("команда %s: %+v", c.Name, c.Error)
		}
		return c.Status == testserver.CommandSucceeded
	})
	return c.Result
}

type ctlState struct {
	PID     int                   `json:"pid"`
	Context message.WorkerContext `json:"context"`
}

func (s *stand) ctl() ctlState {
	s.t.Helper()
	var st ctlState
	if err := json.Unmarshal(s.run("ctl.context", nil), &st); err != nil {
		s.t.Fatal(err)
	}
	return st
}

// ctlStatus — status.workers[ctl] и status агента на сервере.
func (s *stand) ctlStatus() (message.StatusWorker, message.Status) {
	a, ok := s.server.Agent("it-agent")
	if !ok || a.Status == nil {
		return message.StatusWorker{}, message.Status{}
	}
	for _, w := range a.Status.Workers {
		if w.Name == "ctl" {
			return w, *a.Status
		}
	}
	return message.StatusWorker{}, *a.Status
}

// Управление воркером: контекст агента (worker.context) и его изменения,
// здоровье воркера в status, пауза от сервера и от воркера (складываются),
// замена по просьбе воркера — без сбоя.
func TestWorkerControl(t *testing.T) {
	s := newStand(t)
	exe, _ := os.Executable()
	s.agent("ws", []config.Worker{{
		Name: "ctl", Command: []string{exe}, Env: map[string]string{"IT_WORKER": "1", "IT_WORKER_KIND": "control"},
		Replicas: 1, StopTimeout: config.Duration(10 * time.Second),
	}}, func(c *config.Config) { c.Labels = map[string]string{"zone": "eu"} })

	var agent testserver.Agent
	eventually(t, "агент объявил команды воркера и worker.pause", func() bool {
		a, ok := s.server.Agent("it-agent")
		agent = a
		return ok && a.Online && a.Capabilities != nil && a.Capabilities.Commands != nil &&
			slices.Contains(a.Capabilities.Commands.Names, "ctl.context") &&
			slices.Contains(a.Capabilities.Commands.Names, message.CommandWorkerPause)
	})

	// Контекст: агент на связи, его id, имя, версия, метки, частота метрик, порог лога.
	var st ctlState
	eventually(t, "контекст воркера", func() bool {
		st = s.ctl()
		return st.Context.Online && st.Context.Agent.ID == agent.ID
	})
	c := st.Context
	if c.Mode != message.WorkerModeRun || c.Agent.Name != "it-agent" || c.Agent.Version != "it" ||
		c.Agent.Labels["zone"] != "eu" || c.MetricsIntervalMs != 200 || c.LogLevel != "warn" || c.Channels != nil {
		t.Fatalf("контекст: %+v", c)
	}
	// Здоровье: degraded с причиной — в status.workers и status.state.
	s.run("ctl.health", message.WorkerHealth{OK: false, Message: "example.db недоступна"})
	eventually(t, "воркер degraded", func() bool {
		w, st := s.ctlStatus()
		return w.Health == message.WorkerHealthDegraded && w.Message == "example.db недоступна" &&
			st.State == message.StateDegraded
	})
	s.run("ctl.health", message.WorkerHealth{OK: true})
	eventually(t, "воркер снова в порядке", func() bool {
		w, st := s.ctlStatus()
		return w.Health == message.WorkerHealthOK && st.State != message.StateDegraded
	})

	// Пауза сервера и пауза воркера складываются.
	cmd, err := s.server.Agents().PauseWorker(agent.ID, "ctl")
	if err != nil {
		t.Fatal(err)
	}
	s.await(cmd.ID)
	eventually(t, "пауза сервера в status", func() bool {
		w, st := s.ctlStatus()
		return w.Paused && st.Slots["ctl.echo"] == 0
	})
	s.run("ctl.pause", true)
	cmd, err = s.server.Agents().ResumeWorker(agent.ID, "ctl")
	if err != nil {
		t.Fatal(err)
	}
	s.await(cmd.ID)
	job := s.server.Enqueue(testserver.EnqueueRequest{Queue: "ctl.echo", Data: data("x")})
	time.Sleep(700 * time.Millisecond)
	if w, st := s.ctlStatus(); !w.Paused || st.Slots["ctl.echo"] != 0 {
		t.Fatalf("пауза воркера снята возобновлением сервера: %+v %v", w, st.Slots)
	}
	if j, _ := s.server.Job(job.ID); j.Status == testserver.JobCompleted {
		t.Fatal("задача выдана воркеру на паузе")
	}
	s.run("ctl.pause", false)
	s.waitJob(job.ID, testserver.JobCompleted)
	if w, _ := s.ctlStatus(); w.Paused {
		eventually(t, "пауза снята", func() bool { w, _ := s.ctlStatus(); return !w.Paused })
	}

	// Замена по просьбе воркера: новый процесс, без degraded и backoff.
	before := s.ctl().PID
	s.run("ctl.restart", nil)
	deadline := time.Now().Add(15 * time.Second)
	for {
		if w, st := s.ctlStatus(); st.State == message.StateDegraded || w.State == "backoff" {
			t.Fatalf("замена по просьбе — не сбой: %s %q, воркер %s", st.State, st.Message, w.State)
		}
		if pid := s.ctl().PID; pid != before {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("воркер не заменён")
		}
		time.Sleep(50 * time.Millisecond)
	}
	if c := s.ctl().Context; !c.Online || c.Agent.ID != agent.ID {
		t.Fatalf("контекст нового экземпляра: %+v", c)
	}
}
