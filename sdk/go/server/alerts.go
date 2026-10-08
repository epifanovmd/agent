package server

import (
	"sort"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// alertKey — проблема: тип, агент, раздел (stateFailed) или воркер
// (workerDown, workerDegraded).
type alertKey struct{ typ, agentID, sub string }

// setAlert — проблема началась (active) или закончилась: событие — только при
// смене (активные держатся в памяти процесса). Конец проблемы — с текстом её
// начала, msg тогда не нужен. Под a.mu.
func (a *Agents) setAlert(agent *Agent, typ, sub string, active bool, msg string) {
	key := alertKey{typ, agent.ID, sub}
	started, was := a.alerts[key]
	if was == active {
		return
	}
	if !active {
		msg = started.Message
	}
	al := Alert{Type: typ, AgentID: agent.ID, AgentName: agent.Name, Active: active, Message: msg, At: now()}
	switch typ {
	case AlertStateFailed:
		al.Domain = sub
	case AlertWorkerDown, AlertWorkerDegraded:
		al.Worker = sub
	}
	if active {
		a.alerts[key] = al
	} else {
		delete(a.alerts, key)
	}
	if a.opts.OnAlert != nil {
		a.alertQ = append(a.alertQ, al)
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

// statusAlerts — degraded, workerDown и workerDegraded по новому status. Под a.mu.
func (a *Agents) statusAlerts(agent *Agent, st *message.Status) {
	if st.State == message.StateDegraded {
		msg := st.Message
		if msg == "" {
			msg = "Агент не в порядке"
		}
		a.setAlert(agent, AlertDegraded, "", true, msg)
	} else {
		a.setAlert(agent, AlertDegraded, "", false, "")
	}
	down, degraded := map[string]bool{}, map[string]bool{}
	for _, w := range st.Workers {
		if workerFailed(w.State) {
			down[w.Name] = true
			a.setAlert(agent, AlertWorkerDown, w.Name, true, "Воркер "+w.Name+": "+w.State)
		}
		if w.Health == message.WorkerHealthDegraded {
			degraded[w.Name] = true
			msg := w.Message
			if msg == "" {
				msg = "Воркер " + w.Name + " не в порядке"
			}
			a.setAlert(agent, AlertWorkerDegraded, w.Name, true, msg)
		}
	}
	for _, key := range a.agentAlerts(agent.ID) {
		switch {
		case key.typ == AlertWorkerDown && !down[key.sub]:
			a.setAlert(agent, AlertWorkerDown, key.sub, false, "")
		case key.typ == AlertWorkerDegraded && !degraded[key.sub]:
			a.setAlert(agent, AlertWorkerDegraded, key.sub, false, "")
		}
	}
}

// clearAlerts — закончить все проблемы агента (отозван). Под a.mu.
func (a *Agents) clearAlerts(agent *Agent) {
	for _, key := range a.agentAlerts(agent.ID) {
		a.setAlert(agent, key.typ, key.sub, false, "")
	}
}

// agentAlerts — ключи активных проблем агента по порядку.
func (a *Agents) agentAlerts(agentID string) []alertKey {
	var keys []alertKey
	for key := range a.alerts {
		if key.agentID == agentID {
			keys = append(keys, key)
		}
	}
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].typ != keys[j].typ {
			return keys[i].typ < keys[j].typ
		}
		return keys[i].sub < keys[j].sub
	})
	return keys
}

// Alerts — активные проблемы агентов (этого процесса), по времени начала.
func (a *Agents) Alerts() []Alert {
	a.mu.Lock()
	defer a.mu.Unlock()
	out := make([]Alert, 0, len(a.alerts))
	for _, al := range a.alerts {
		out = append(out, al)
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
		return x.Domain+x.Worker < y.Domain+y.Worker
	})
	return out
}
