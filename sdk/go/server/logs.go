package server

import (
	"github.com/epifanovmd/agent/sdk/go/message"
)

// agentLog — пачка записей лога агента, ждущая OnLog.
type agentLog struct {
	agentID string
	entries []message.LogEntry
}

// receiveLog — записи сообщения log (поток) — в OnLog (SDK их не хранит). Под a.mu.
func (a *Agents) receiveLog(agentID string, entries []message.LogEntry) {
	if a.opts.OnLog != nil && len(entries) > 0 {
		a.logs = append(a.logs, agentLog{agentID: agentID, entries: entries})
	}
}
