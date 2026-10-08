//go:build unix

package worker

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// ipcLines — сообщения агента, полученные тестовым воркером (IPC_LOG).
func ipcLines(t *testing.T, path string) []string {
	t.Helper()
	raw, _ := os.ReadFile(path)
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

// contexts — полученные воркером worker.context по порядку.
func contexts(t *testing.T, path string) []message.WorkerContext {
	t.Helper()
	var out []message.WorkerContext
	for _, line := range ipcLines(t, path) {
		data, ok := strings.CutPrefix(line, message.TypeWorkerContext+" ")
		if !ok {
			continue
		}
		var c message.WorkerContext
		if err := json.Unmarshal([]byte(data), &c); err != nil {
			t.Fatal(err)
		}
		out = append(out, c)
	}
	return out
}

func supStatus(sup *Supervisor) message.Status {
	_, st := workerStatus(sup)
	return st
}

func current(sup *Supervisor, name string) *instance {
	sup.mu.Lock()
	defer sup.mu.Unlock()
	return sup.workers[name].slots[0].current
}

// fromWorker — сообщение воркера агенту (как если бы пришло по IPC).
func fromWorker(t *testing.T, inst *instance, typ string, data any) {
	t.Helper()
	if err := inst.handle(message.MustNew(typ, data)); err != nil {
		t.Fatal(err)
	}
}

// worker.context: сразу после worker.ready (до всего остального), затем при
// изменениях — не чаще раза в 100 мс, одинаковый повторно не шлётся;
// channels — только каналы этого воркера.
func TestWorkerContext(t *testing.T) {
	log := filepath.Join(t.TempDir(), "ipc")
	sup, manager, _, cancel := setup(t, map[string]string{"IPC_LOG": log})
	defer cancel()
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	waitFor(t, "первый контекст", func() bool { return len(contexts(t, log)) == 1 })
	lines := ipcLines(t, log)
	if !strings.HasPrefix(lines[0], message.TypeWorkerReady+" ") || !strings.HasPrefix(lines[1], message.TypeWorkerContext+" ") {
		t.Fatalf("порядок: %v", lines)
	}
	first := contexts(t, log)[0]
	if first.Mode != message.WorkerModeRun || first.Agent.Version != "test" || first.Online {
		t.Fatalf("контекст по умолчанию: %+v", first)
	}

	// Экземпляр принял канал example.app: из подписок на каналы видит только его.
	inst := current(sup, "echo")
	inst.mu.Lock()
	inst.accepted[owned{kindChannel, "example.app"}] = true
	inst.mu.Unlock()
	next := message.WorkerContext{
		Agent:  message.WorkerContextAgent{ID: "a1", Name: "node-01", Version: "test", Labels: map[string]string{"zone": "eu"}},
		Online: true, MetricsIntervalMs: 1000, LogLevel: "info",
		Channels: map[string]int64{"example.app": 1000, "example.other": 500},
	}
	start := time.Now()
	for n := range 20 {
		c := next
		c.MetricsIntervalMs = int64(1000 + n)
		sup.SetContext(c)
		time.Sleep(2 * time.Millisecond)
	}
	final := next
	final.MetricsIntervalMs = 1019
	waitFor(t, "последний контекст", func() bool {
		got := contexts(t, log)
		return got[len(got)-1].MetricsIntervalMs == 1019
	})
	got := contexts(t, log)
	// 20 изменений за ~40 мс — не больше трёх сообщений (по одному на 100 мс).
	if len(got) > 4 {
		t.Fatalf("контекст чаще раза в 100 мс: %d сообщений за %s", len(got)-1, time.Since(start))
	}
	last := got[len(got)-1]
	final.Mode = message.WorkerModeRun
	if last.Agent.ID != "a1" || last.Agent.Labels["zone"] != "eu" || !last.Online || last.LogLevel != "info" ||
		len(last.Channels) != 1 || last.Channels["example.app"] != 1000 || last.Mode != "run" {
		t.Fatalf("контекст: %+v", last)
	}

	// То же самое — не шлётся.
	sup.SetContext(final)
	time.Sleep(300 * time.Millisecond)
	if n := len(contexts(t, log)); n != len(got) {
		t.Fatalf("одинаковый контекст отправлен повторно: %d → %d", len(got), n)
	}
}

// worker.health: не в порядке — status.workers[].health/message и агент
// degraded с причиной; снова в порядке — degraded снят.
func TestWorkerHealthStatus(t *testing.T) {
	sup, manager, _, cancel := setup(t, map[string]string{})
	defer cancel()
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	if st := supStatus(sup); st.Workers[0].Health != "" || st.State != "" {
		t.Fatalf("до worker.health: %+v", st)
	}
	inst := current(sup, "echo")
	fromWorker(t, inst, message.TypeWorkerHealth, message.WorkerHealth{OK: false, Message: "example.db недоступна"})
	st := supStatus(sup)
	w := st.Workers[0]
	if w.Health != message.WorkerHealthDegraded || w.Message != "example.db недоступна" {
		t.Fatalf("status.workers: %+v", w)
	}
	if st.State != message.StateDegraded || !strings.Contains(st.Message, "echo") || !strings.Contains(st.Message, "example.db недоступна") {
		t.Fatalf("status: %s %q", st.State, st.Message)
	}
	fromWorker(t, inst, message.TypeWorkerHealth, message.WorkerHealth{OK: true})
	st = supStatus(sup)
	if st.Workers[0].Health != message.WorkerHealthOK || st.Workers[0].Message != "" || st.State != "" {
		t.Fatalf("после ok: %+v", st)
	}
}

// Пауза от воркера и от сервера складываются: места — 0, пока стоит хоть
// одна; ёмкость не меняется; задача на паузе не выдаётся.
func TestWorkerPauseResume(t *testing.T) {
	sup, manager, rec, cancel := setup(t, map[string]string{})
	defer cancel()
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	inst := current(sup, "echo")
	paused := func() bool { return supStatus(sup).Workers[0].Paused }

	fromWorker(t, inst, message.TypeWorkerPause, message.WorkerPause{Queues: []string{"echo"}})
	st := message.Status{Slots: map[string]int{}}
	manager.ContributeStatus(&st)
	if st.Slots["echo"] != 0 || st.Capacity["echo"] != 2 || !paused() {
		t.Fatalf("пауза воркера: slots %v capacity %v paused %v", st.Slots, st.Capacity, paused())
	}
	assign(manager, "p1")
	waitFor(t, "задача на паузе отклонена", func() bool { return rec.count(message.TypeJobReject) == 1 })

	// Сервер ставит паузу на всё; воркер снимает свою — пауза сервера остаётся.
	if err := sup.Pause("echo", nil); err != nil {
		t.Fatal(err)
	}
	fromWorker(t, inst, message.TypeWorkerResume, message.WorkerPause{})
	if slots(manager)["echo"] != 0 || !paused() {
		t.Fatal("пауза сервера снята возобновлением воркера")
	}
	// Воркер снова на паузе; сервер снимает свою — пауза воркера остаётся.
	fromWorker(t, inst, message.TypeWorkerPause, message.WorkerPause{})
	if err := sup.Resume("echo", nil); err != nil {
		t.Fatal(err)
	}
	if slots(manager)["echo"] != 0 || !paused() {
		t.Fatal("пауза воркера снята возобновлением сервера")
	}
	fromWorker(t, inst, message.TypeWorkerResume, message.WorkerPause{Queues: []string{"echo"}})
	if slots(manager)["echo"] != 2 || paused() {
		t.Fatalf("после возобновления: %v paused %v", slots(manager), paused())
	}
	if err := sup.Pause("nope", nil); err != ErrUnknownWorker {
		t.Fatalf("неизвестный воркер: %v", err)
	}

	// Пауза сервера переживает замену экземпляра.
	if err := sup.Pause("echo", []string{"echo"}); err != nil {
		t.Fatal(err)
	}
	ctx, stop := context.WithTimeout(context.Background(), 10*time.Second)
	defer stop()
	if err := sup.Restart(ctx, "echo"); err != nil {
		t.Fatal(err)
	}
	if current(sup, "echo") == inst || slots(manager)["echo"] != 0 || !paused() {
		t.Fatal("пауза сервера не пережила замену")
	}
}

// worker.restart от воркера — штатная замена: новый экземпляр, без backoff и degraded.
func TestWorkerRestartRequest(t *testing.T) {
	sup, manager, _, cancel := setup(t, map[string]string{})
	defer cancel()
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	inst := current(sup, "echo")
	bad := make(chan string, 1)
	done := make(chan struct{})
	defer close(done)
	go func() {
		for {
			select {
			case <-done:
				return
			default:
			}
			st := supStatus(sup)
			if st.State == message.StateDegraded || st.Workers[0].State == "backoff" {
				select {
				case bad <- st.State + " " + st.Workers[0].State:
				default:
				}
			}
			time.Sleep(time.Millisecond)
		}
	}()
	fromWorker(t, inst, message.TypeWorkerRestart, message.WorkerRestartRequest{Reason: "новые настройки"})
	fromWorker(t, inst, message.TypeWorkerRestart, message.WorkerRestartRequest{}) // повтор — не вторая замена
	waitFor(t, "новый экземпляр", func() bool {
		next := current(sup, "echo")
		return next != inst && next.registered() && inst.hasExited()
	})
	time.Sleep(200 * time.Millisecond)
	if id := current(sup, "echo").id; id != "echo#2" {
		t.Fatalf("замена одна: %s", id)
	}
	select {
	case s := <-bad:
		t.Fatalf("просьба о замене — не сбой: %s", s)
	default:
	}
}

func TestPauseSet(t *testing.T) {
	var p pauseSet
	if p.any(nil) || p.paused("a") {
		t.Fatal("пусто")
	}
	p.pause([]string{"a"})
	if !p.paused("a") || p.paused("b") || !p.any([]string{"a", "b"}) || p.any([]string{"b"}) {
		t.Fatalf("a: %+v", p)
	}
	p.pause(nil)
	p.resume([]string{"a"})
	if p.paused("a") || !p.paused("b") || p.any([]string{"a"}) || !p.any([]string{"a", "b"}) {
		t.Fatalf("все, кроме a: %+v", p)
	}
	p.pause([]string{"a"})
	if !p.paused("a") {
		t.Fatal("a снова на паузе")
	}
	p.resume(nil)
	if p.any(nil) {
		t.Fatal("снято всё")
	}
}
