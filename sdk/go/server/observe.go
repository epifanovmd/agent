package server

import (
	"github.com/epifanovmd/agent/sdk/go/message"
)

// addMetrics — точка metrics: в историю — с прореживанием
// (MetricsStoreInterval), в OnMetrics — каждая; «текущие» метрики агента — последняя
// по времени точка без backfill (досланные их не перезаписывают).
func (a *Agents) addMetrics(agent *Agent, m message.Metrics) error {
	at := metricsAt(m, now())
	point := MetricsPoint{At: at, Backfill: m.Backfill, Metrics: m}
	key := storedKey(agent.ID, m.Backfill)
	if a.keepMetrics(key, at) {
		if err := a.store.AddMetrics(agent.ID, point); err != nil {
			return err
		}
		a.stored[key] = at
	}
	if a.opts.OnMetrics != nil {
		a.points = append(a.points, agentPoint{agentID: agent.ID, point: point})
	}
	if !m.Backfill && (agent.Metrics == nil || at >= agent.MetricsAt) {
		agent.Metrics, agent.MetricsAt = &m, at
	}
	return nil
}

// keepMetrics — сохранять ли точку в историю (MetricsStoreInterval): не
// раньше последней сохранённой точки того же рода + интервал. Последняя — в
// памяти процесса: первая точка после запуска сохраняется всегда.
func (a *Agents) keepMetrics(key string, at int64) bool {
	iv := a.opts.MetricsStoreInterval
	if iv < 0 {
		return true
	}
	last, ok := a.stored[key]
	return !ok || at >= last+iv.Milliseconds()
}

// storedKey — досланные точки прореживаются отдельно от живых: их время раньше
// уже сохранённых живых, и общее правило отбросило бы всю историю без связи.
func storedKey(agentID string, backfill bool) string {
	if backfill {
		return agentID + "\x00backfill"
	}
	return agentID
}

// metricsAt — время точки по часам сервера (§6.2): collectedAt +
// clockOffsetMs; без смещения — received.
func metricsAt(m message.Metrics, received int64) int64 {
	if m.ClockOffsetMs != nil && m.CollectedAt > 0 {
		return m.CollectedAt + *m.ClockOffsetMs
	}
	return received
}

// Metrics — история метрик агента: точки с At строго позже since (мс; 0 —
// все), по возрастанию At.
func (a *Agents) Metrics(agentID string, since int64) ([]MetricsPoint, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	points, err := a.store.ListMetrics(agentID, since)
	if points == nil && err == nil {
		points = []MetricsPoint{}
	}
	return points, err
}

// Revoke — отозвать учётные данные агента: Agent.Revoked, сессия
// закрывается кодом 4401, дальше WebSocket и HTTP sync отвечают 401. Агенту
// нужна новая регистрация.
func (a *Agents) Revoke(agentID string) error { return a.revoke("", agentID) }

func (a *Agents) revoke(actor, agentID string) error {
	a.mu.Lock()
	defer a.unlock()
	agent, err := a.store.GetAgent(agentID)
	if err != nil {
		return protoErr("AGENT_NOT_FOUND", "Агент не найден")
	}
	a.dropRevoked(agentID)
	agent.Revoked, agent.Online, agent.Subscriptions, agent.PendingSecretHash = true, false, nil, ""
	if err := a.store.UpdateAgent(agent); err != nil {
		return err
	}
	a.emit(ChangeAgent, agentID)
	a.audit(actor, AuditAgentRevoke, agentID, agentID, nil)
	a.clearAlerts(agent)
	a.log.Info("учётные данные агента отозваны", "agent", agent.Name)
	return nil
}

// Refresh — перечитать хранилище и доставить агенту (пусто — всем агентам на
// связи с этим процессом) то, что появилось в нём мимо этого процесса: ожидающие команды,
// новые снимки состояния, задачи, подписки (Subscribe); агент отозван (Revoke в
// другом процессе) — сессия закрывается кодом 4401. Для бэкенда из
// нескольких процессов с общим Store: процесс, изменивший данные, уведомляет
// остальные (например, Postgres NOTIFY), каждый вызывает Refresh — доставит
// тот, у кого сессия агента.
func (a *Agents) Refresh(agentID string) {
	a.mu.Lock()
	defer a.unlock()
	close(a.kick) // ждущие Call — перечитать итог: его мог сохранить другой процесс
	a.kick = make(chan struct{})
	for _, ss := range a.sortedSessions() {
		if agentID != "" && ss.agentID != agentID {
			continue
		}
		agent, err := a.store.GetAgent(ss.agentID)
		if err != nil {
			a.log.Error("агент не прочитан", "agent", ss.agentID, "err", err)
			continue
		}
		if agent.Revoked {
			a.dropRevoked(ss.agentID)
			continue
		}
		a.deliver(ss)
		a.applySubscription(ss, agent)
	}
	a.dispatchJobs()
}

// dropRevoked — учётные данные агента отозваны: сессия этого процесса
// закрывается кодом 4401, отложенный переход в offline не нужен. Под a.mu.
func (a *Agents) dropRevoked(agentID string) {
	if t := a.offline[agentID]; t != nil {
		t.Stop()
		delete(a.offline, agentID)
	}
	if ss := a.sessions[agentID]; ss != nil {
		delete(a.sessions, agentID)
		ss.close(message.CloseUnauthorized)
	}
}
