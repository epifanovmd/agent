package server

import (
	"slices"
	"testing"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

func statusWith(state, msg string, workers ...message.StatusWorker) message.Envelope {
	if workers == nil {
		workers = []message.StatusWorker{}
	}
	return streamEnv(message.TypeStatus, message.Status{
		State: state, Message: msg, Slots: map[string]int{}, Jobs: []message.StatusJob{}, Workers: workers,
	})
}

type alertWant struct {
	typ    string
	active bool
	sub    string
}

func expectAlerts(t *testing.T, got []Alert, agentID, name string, want ...alertWant) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("уведомлений %d, ждали %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		al := got[i]
		if al.Type != w.typ || al.Active != w.active || al.Domain+al.Worker != w.sub ||
			al.AgentID != agentID || al.AgentName != name || al.At == 0 {
			t.Fatalf("уведомление %d: %+v, ждали %+v", i, al, w)
		}
	}
}

// Уведомления о проблемах: каждое начало и конец — одно событие; Alerts —
// активные.
func TestAlerts(t *testing.T) {
	var alerts collector[Alert]
	agents := newTestAgents(t, Options{OnAlert: alerts.add, OfflineGrace: 50 * time.Millisecond})
	id, _ := enroll(t, agents, "node")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))

	// degraded и workerDown (backoff — сбой; starting — норма).
	handle(agents, ss, statusWith(message.StateDegraded, "воркеры перезапускаются",
		message.StatusWorker{Name: "w1", State: "backoff"}, message.StatusWorker{Name: "w2", State: "starting"}))
	got := alerts.take()
	expectAlerts(t, got, id, "node", alertWant{AlertDegraded, true, ""}, alertWant{AlertWorkerDown, true, "w1"})
	if got[0].Message != "воркеры перезапускаются" {
		t.Fatalf("message: %q", got[0].Message)
	}
	// Повтор той же проблемы (другой status) — без событий.
	handle(agents, ss, statusWith(message.StateDegraded, "воркеры перезапускаются",
		message.StatusWorker{Name: "w1", State: "backoff"}, message.StatusWorker{Name: "w2", State: "running"}))
	expectAlerts(t, alerts.take(), id, "node")
	if act := mustAlerts(t, agents); len(act) != 2 {
		t.Fatalf("активные: %+v", act)
	}
	// Воркер пропал из списка, агент в норме — обе закончились.
	handle(agents, ss, statusWith(message.StateIdle, ""))
	expectAlerts(t, alerts.take(), id, "node", alertWant{AlertDegraded, false, ""}, alertWant{AlertWorkerDown, false, "w1"})

	// stateFailed по разделу; закончилась — ok этого раздела.
	handle(agents, ss, reliableEnv(message.TypeStateApplied, message.StateApplied{Domain: "d", Version: 1, OK: false, Error: "не применилось"}))
	handle(agents, ss, reliableEnv(message.TypeStateApplied, message.StateApplied{Domain: "d", Version: 2, OK: false, Error: "снова"}))
	handle(agents, ss, reliableEnv(message.TypeStateApplied, message.StateApplied{Domain: "e", Version: 1, OK: true}))
	got = alerts.take()
	expectAlerts(t, got, id, "node", alertWant{AlertStateFailed, true, "d"})
	if got[0].Message != "не применилось" {
		t.Fatalf("message: %q", got[0].Message)
	}
	if act := mustAlerts(t, agents); len(act) != 1 || act[0].Domain != "d" || !act[0].Active {
		t.Fatalf("активные: %+v", act)
	}
	handle(agents, ss, reliableEnv(message.TypeStateApplied, message.StateApplied{Domain: "d", Version: 3, OK: true}))
	expectAlerts(t, alerts.take(), id, "node", alertWant{AlertStateFailed, false, "d"})

	// offline — после отсрочки; снова на связи — конец.
	agents.mu.Lock()
	agents.closeSession(ss, message.CloseNormal)
	agents.unlock()
	if got := alerts.take(); len(got) != 0 {
		t.Fatalf("offline до отсрочки: %+v", got)
	}
	eventually(t, "offline", func() bool { return len(mustAlerts(t, agents)) == 1 })
	expectAlerts(t, alerts.take(), id, "node", alertWant{AlertOffline, true, ""})
	ss = open(t, agents, id, helloEnv("b", message.Capabilities{}))
	expectAlerts(t, alerts.take(), id, "node", alertWant{AlertOffline, false, ""})
	if act := mustAlerts(t, agents); len(act) != 0 {
		t.Fatalf("активные: %+v", act)
	}

	// Отзыв заканчивает проблемы агента.
	handle(agents, ss, statusWith(message.StateDegraded, "плохо"))
	alerts.take()
	if err := agents.Revoke(id); err != nil {
		t.Fatal(err)
	}
	expectAlerts(t, alerts.take(), id, "node", alertWant{AlertDegraded, false, ""})

	// Остановка Agents — без offline.
	id2, _ := enroll(t, agents, "other")
	open(t, agents, id2, helloEnv("b", message.Capabilities{}))
	agents.Close()
	if got := alerts.take(); len(got) != 0 {
		t.Fatalf("уведомления при остановке: %+v", got)
	}
}

// Тексты уведомлений: одинаковые во всех SDK; конец проблемы — с текстом её
// начала.
func TestAlertMessages(t *testing.T) {
	var alerts collector[Alert]
	agents := newTestAgents(t, Options{OnAlert: alerts.add, OfflineGrace: time.Millisecond})
	id, _ := enroll(t, agents, "node")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))
	messages := func() [][2]string {
		var out [][2]string
		for _, al := range alerts.take() {
			state := "начало"
			if !al.Active {
				state = "конец"
			}
			out = append(out, [2]string{al.Type + " " + state, al.Message})
		}
		return out
	}
	expect := func(want ...[2]string) {
		t.Helper()
		if got := messages(); !slices.Equal(got, want) {
			t.Fatalf("уведомления %v, ждали %v", got, want)
		}
	}

	handle(agents, ss, statusWith(message.StateDegraded, "",
		message.StatusWorker{Name: "w1", State: "failed"},
		message.StatusWorker{Name: "w2", State: "running", Health: message.WorkerHealthDegraded},
		message.StatusWorker{Name: "w3", State: "running", Health: message.WorkerHealthDegraded, Message: "нет базы"}))
	expect(
		[2]string{"degraded начало", "Агент не в порядке"},
		[2]string{"workerDown начало", "Воркер w1: failed"},
		[2]string{"workerDegraded начало", "Воркер w2 не в порядке"},
		[2]string{"workerDegraded начало", "нет базы"},
	)
	handle(agents, ss, statusWith(message.StateIdle, ""))
	expect(
		[2]string{"degraded конец", "Агент не в порядке"},
		[2]string{"workerDegraded конец", "Воркер w2 не в порядке"},
		[2]string{"workerDegraded конец", "нет базы"},
		[2]string{"workerDown конец", "Воркер w1: failed"},
	)

	handle(agents, ss, reliableEnv(message.TypeStateApplied, message.StateApplied{Domain: "d", Version: 1, OK: false, Error: "нет файла"}))
	handle(agents, ss, reliableEnv(message.TypeStateApplied, message.StateApplied{Domain: "d", Version: 2, OK: true}))
	expect([2]string{"stateFailed начало", "нет файла"}, [2]string{"stateFailed конец", "нет файла"})

	agents.mu.Lock()
	agents.closeSession(ss, message.CloseNormal)
	agents.unlock()
	eventually(t, "offline", func() bool { return len(mustAlerts(t, agents)) == 1 })
	expect([2]string{"offline начало", "Агент без связи"})
	ss = open(t, agents, id, helloEnv("b", message.Capabilities{}))
	expect([2]string{"offline конец", "Агент без связи"})

	handle(agents, ss, statusWith(message.StateDegraded, "плохо"))
	if err := agents.Revoke(id); err != nil {
		t.Fatal(err)
	}
	expect([2]string{"degraded начало", "плохо"}, [2]string{"degraded конец", "плохо"})
}

// Процесс с сессией агента упал: другой процесс по сверке видит online без
// сессии и без вестей дольше OfflineAfter — offline, change agent, alert
// offline; повторно — ничего. Процесс с живой сессией агента не трогает.
func TestOfflineAfterWithoutSession(t *testing.T) {
	shared := NewMemoryStore()
	var alerts collector[Alert]
	var changes collector[Change]
	local := newTestAgents(t, Options{Store: shared, OfflineAfter: time.Minute})
	remote := newTestAgents(t, Options{Store: shared, OfflineAfter: time.Minute, OnAlert: alerts.add, OnChange: changes.add})
	if def := newTestAgents(t, Options{}).opts.OfflineAfter; def != 50*time.Second {
		t.Fatalf("OfflineAfter по умолчанию: %v", def)
	}
	id, _ := enroll(t, local, "node")
	open(t, local, id, helloEnv("b", message.Capabilities{}))
	sweep := func(a *Agents) {
		a.mu.Lock()
		a.sweep()
		a.unlock()
	}

	// Вести свежие — online.
	sweep(remote)
	if ag, _ := remote.Agent(id); !ag.Online {
		t.Fatal("свежий агент снят с связи")
	}
	expectAlerts(t, alerts.take(), id, "node")

	// Вестей нет 2 мин (процесс local «упал»).
	ag, _ := shared.GetAgent(id)
	ag.LastSeenAt = now() - 2*time.Minute.Milliseconds()
	if _, err := shared.UpdateAgent(ag); err != nil {
		t.Fatal(err)
	}
	sweep(local) // сессия здесь — не трогаем
	if ag, _ := local.Agent(id); !ag.Online {
		t.Fatal("процесс с сессией снял агента")
	}
	changes.take()
	sweep(remote)
	if ag, _ := remote.Agent(id); ag.Online {
		t.Fatal("агент без вестей остался online")
	}
	expectAlerts(t, alerts.take(), id, "node", alertWant{AlertOffline, true, ""})
	if !slices.Contains(changes.take(), Change{Kind: ChangeAgent, ID: id}) {
		t.Fatal("нет change agent")
	}
	sweep(remote)
	expectAlerts(t, alerts.take(), id, "node")
}

// mustAlerts — активные проблемы (Agents.Alerts) без ошибки.
func mustAlerts(t *testing.T, agents *Agents) []Alert {
	t.Helper()
	list, err := agents.Alerts()
	if err != nil {
		t.Fatal(err)
	}
	return list
}
