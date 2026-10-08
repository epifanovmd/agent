package server

import (
	"errors"
	"maps"
	"slices"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// currentMetrics — «текущие» метрики в записи агента: последняя по времени
// точка без backfill (досланные их не перезаписывают).
func currentMetrics(agent *Agent, m *message.Metrics, at int64) {
	if !m.Backfill && (agent.Metrics == nil || at >= agent.MetricsAt) {
		agent.Metrics, agent.MetricsAt = m, at
	}
}

// storeMetrics — точка metrics: в историю — с прореживанием
// (MetricsStoreInterval), в OnMetrics — каждая. Под a.mu.
func (a *Agents) storeMetrics(agentID string, point MetricsPoint) error {
	key := storedKey(agentID, point.Backfill)
	if a.keepMetrics(key, point.At) {
		if err := a.store.AddMetrics(agentID, point); err != nil {
			return err
		}
		a.stored[key] = point.At
	}
	if a.opts.OnMetrics != nil {
		a.points = append(a.points, agentPoint{agentID: agentID, point: point})
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
	var ev alertEvents
	agent, _, err := a.mutateAgent(agentID, func(ag *Agent) bool {
		ev = nil
		ev.clearAlerts(ag)
		ag.Revoked, ag.Online, ag.Subscriptions, ag.PendingSecretHash = true, false, nil, ""
		return true
	})
	if errors.Is(err, ErrNotFound) {
		return protoErr("AGENT_NOT_FOUND", "Агент не найден")
	}
	if err != nil {
		return err
	}
	a.dropRevoked(agentID)
	a.emit(ChangeAgent, agentID)
	a.audit(actor, AuditAgentRevoke, agentID, agentID, nil)
	a.queueAlerts(ev)
	a.log.Info("учётные данные агента отозваны", "agent", agent.Name)
	return nil
}

// DeleteAgent — удалить запись отозванного агента (Revoke — сначала) и его
// историю метрик; задачи, команды, события и снимки состояния остаются. Нет
// агента — AGENT_NOT_FOUND, не отозван — AGENT_NOT_REVOKED.
func (a *Agents) DeleteAgent(agentID string) error { return a.deleteAgent("", agentID) }

func (a *Agents) deleteAgent(actor, agentID string) error {
	agent, err := a.store.GetAgent(agentID)
	if errors.Is(err, ErrNotFound) {
		return protoErr("AGENT_NOT_FOUND", "Агент не найден")
	}
	if err != nil {
		return err
	}
	if !agent.Revoked {
		return protoErr("AGENT_NOT_REVOKED", "Агент не отозван: сначала Revoke")
	}
	a.mu.Lock()
	defer a.unlock()
	deleted, err := a.store.DeleteAgent(agentID)
	if err != nil {
		return err
	}
	if !deleted {
		return protoErr("AGENT_NOT_FOUND", "Агент не найден")
	}
	a.dropRevoked(agentID)
	delete(a.stored, storedKey(agentID, false))
	delete(a.stored, storedKey(agentID, true))
	a.emit(ChangeAgent, agentID)
	a.audit(actor, AuditAgentDelete, agentID, agentID, nil)
	a.log.Info("агент удалён", "agent", agent.Name)
	return nil
}

// Refresh — перечитать хранилище и доставить агенту (пусто — всем агентам на
// связи с этим процессом) то, что появилось в нём мимо этого процесса: ожидающие команды,
// новые снимки состояния, задачи, подписки (Subscribe), отмену отправленных в
// сессии команд (CancelCommand — cmd.cancel); агент отозван (Revoke в
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
		a.cancelSent(ss)
		a.deliver(ss)
		a.applySubscription(ss, agent)
	}
	a.dispatchJobs()
}

// cancelSent — команды, отправленные в сессии и завершённые мимо этого
// процесса: отменённым — cmd.cancel (по одному разу); завершённые больше не
// отслеживаются. Под a.mu.
func (a *Agents) cancelSent(ss *session) {
	for _, id := range slices.Sorted(maps.Keys(ss.sent)) {
		cmd, err := a.store.GetCommand(id)
		switch {
		case errors.Is(err, ErrNotFound):
			delete(ss.sent, id)
		case err != nil:
			a.log.Error("команда не прочитана", "command", id, "err", err)
		case cmd.Finished():
			delete(ss.sent, id)
			if cmd.Status == CommandCancelled {
				ss.send(message.TypeCmdCancel, message.CommandRef{CommandID: id}, "")
			}
		}
	}
}

// PruneOptions — уборка (Agents.Prune): удалить завершённые задачи и команды,
// завершившиеся раньше чем JobsOlderThan и CommandsOlderThan назад, и события
// старше EventsOlderThan; 0 — этот вид записей не трогать.
type PruneOptions struct {
	JobsOlderThan     time.Duration
	CommandsOlderThan time.Duration
	EventsOlderThan   time.Duration
}

// Prune — удалить старые завершённые задачи, команды и события
// (Store.Prune); возвращает число удалённых записей. Сами Agents уборку не
// запускают — её вызывает приложение (например, раз в сутки).
func (a *Agents) Prune(opts PruneOptions) (int, error) {
	ts := time.Now()
	before := func(d time.Duration) int64 {
		if d <= 0 {
			return 0
		}
		return ts.Add(-d).UnixMilli()
	}
	return a.store.Prune(PruneBefore{
		Jobs: before(opts.JobsOlderThan), Commands: before(opts.CommandsOlderThan), Events: before(opts.EventsOlderThan),
	})
}

// dropRevoked — учётные данные агента отозваны: сессия этого процесса
// закрывается кодом 4401, отложенный переход в offline не нужен. Под a.mu.
func (a *Agents) dropRevoked(agentID string) {
	if t := a.offline[agentID]; t != nil {
		t.timer.Stop()
		delete(a.offline, agentID)
	}
	if ss := a.sessions[agentID]; ss != nil {
		delete(a.sessions, agentID)
		ss.close(message.CloseUnauthorized)
	}
}
