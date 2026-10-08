package server

import (
	"slices"
	"sort"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Активные уведомления о проблемах агента хранятся в его записи
// (Agent.Alerts) и меняются той же условной записью, что и их причина
// (status, state.applied, переход в offline, отзыв). Событие OnAlert — только
// у процесса, который записал изменение.

// alertEvents — начала и концы проблем одного изменения записи; уходят в
// OnAlert после успешной записи (queueAlerts).
type alertEvents []Alert

// alertSub — раздел (stateFailed) или воркер (workerDown, workerDegraded) проблемы.
func alertSub(al Alert) string { return al.Domain + al.Worker }

// set — проблема в записи агента началась (active) или закончилась: событие —
// только при смене. Конец проблемы — с текстом её начала, msg тогда не нужен.
func (ev *alertEvents) set(agent *Agent, typ, sub string, active bool, msg string) {
	i := slices.IndexFunc(agent.Alerts, func(al Alert) bool { return al.Type == typ && alertSub(al) == sub })
	if (i >= 0) == active {
		return
	}
	if active {
		al := Alert{Type: typ, AgentID: agent.ID, AgentName: agent.Name, Active: true, Message: msg, At: now()}
		switch typ {
		case AlertStateFailed:
			al.Domain = sub
		case AlertWorkerDown, AlertWorkerDegraded:
			al.Worker = sub
		}
		agent.Alerts = append(agent.Alerts, al)
		*ev = append(*ev, al)
		return
	}
	al := agent.Alerts[i]
	agent.Alerts = slices.Delete(agent.Alerts, i, i+1)
	if len(agent.Alerts) == 0 {
		agent.Alerts = nil
	}
	al.Active, al.AgentName, al.At = false, agent.Name, now()
	*ev = append(*ev, al)
}

// queueAlerts — события записанного изменения — в OnAlert (при снятии блокировки). Под a.mu.
func (a *Agents) queueAlerts(ev alertEvents) {
	if a.opts.OnAlert != nil {
		a.alertQ = append(a.alertQ, ev...)
	}
}

// workerFailed — воркер в сбое: упал и перезапускается с паузой (backoff)
// или в ошибке; running, starting, stopping, stopped — норма.
func workerFailed(state string) bool {
	switch state {
	case "backoff", "failed", "crashed", "error":
		return true
	}
	return false
}

// statusAlerts — degraded, workerDown и workerDegraded по новому status (в записи агента).
func (ev *alertEvents) statusAlerts(agent *Agent, st *message.Status) {
	if st.State == message.StateDegraded {
		msg := st.Message
		if msg == "" {
			msg = "Агент не в порядке"
		}
		ev.set(agent, AlertDegraded, "", true, msg)
	} else {
		ev.set(agent, AlertDegraded, "", false, "")
	}
	down, degraded := map[string]bool{}, map[string]bool{}
	for _, w := range st.Workers {
		if workerFailed(w.State) {
			down[w.Name] = true
			ev.set(agent, AlertWorkerDown, w.Name, true, "Воркер "+w.Name+": "+w.State)
		}
		if w.Health == message.WorkerHealthDegraded {
			degraded[w.Name] = true
			msg := w.Message
			if msg == "" {
				msg = "Воркер " + w.Name + " не в порядке"
			}
			ev.set(agent, AlertWorkerDegraded, w.Name, true, msg)
		}
	}
	for _, al := range sortedAlerts(agent.Alerts) {
		switch {
		case al.Type == AlertWorkerDown && !down[al.Worker]:
			ev.set(agent, AlertWorkerDown, al.Worker, false, "")
		case al.Type == AlertWorkerDegraded && !degraded[al.Worker]:
			ev.set(agent, AlertWorkerDegraded, al.Worker, false, "")
		}
	}
}

// clearAlerts — закончить все проблемы агента (отозван).
func (ev *alertEvents) clearAlerts(agent *Agent) {
	for _, al := range sortedAlerts(agent.Alerts) {
		ev.set(agent, al.Type, alertSub(al), false, "")
	}
}

// sortedAlerts — копия проблем по типу и разделу (воркеру).
func sortedAlerts(list []Alert) []Alert {
	out := slices.Clone(list)
	sort.Slice(out, func(i, j int) bool {
		if out[i].Type != out[j].Type {
			return out[i].Type < out[j].Type
		}
		return alertSub(out[i]) < alertSub(out[j])
	})
	return out
}

// Alerts — активные проблемы агентов по записям в Store (видны всем
// процессам с общим Store), по времени начала.
func (a *Agents) Alerts() ([]Alert, error) {
	list, err := a.store.ListAgents()
	if err != nil {
		return nil, err
	}
	out := []Alert{}
	for _, agent := range list {
		for _, al := range agent.Alerts {
			if al.Active {
				al.AgentName = agent.Name
				out = append(out, al)
			}
		}
	}
	sort.Slice(out, func(i, j int) bool {
		x, y := out[i], out[j]
		switch {
		case x.At != y.At:
			return x.At < y.At
		case x.AgentID != y.AgentID:
			return x.AgentID < y.AgentID
		case x.Type != y.Type:
			return x.Type < y.Type
		}
		return alertSub(x) < alertSub(y)
	})
	return out, nil
}
