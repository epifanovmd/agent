package server

import (
	"github.com/epifanovmd/agent/sdk/go/message"
)

// agentLog — пачка записей лога агента, ждущая OnLog.
type agentLog struct {
	agentID string
	entries []message.LogEntry
}

// receiveLog — сообщение log (поток): записи — в OnLog (SDK их не хранит). Под a.mu.
func (a *Agents) receiveLog(agent *Agent, env message.Envelope) *message.Error {
	var l message.LogBatch
	if err := env.Decode(&l); err != nil {
		return invalid(env.Type, err)
	}
	a.storeAgent(agent) // lastSeenAt
	if a.opts.OnLog != nil && len(l.Entries) > 0 {
		a.logs = append(a.logs, agentLog{agentID: agent.ID, entries: l.Entries})
	}
	return nil
}
