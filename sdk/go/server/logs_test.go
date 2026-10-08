package server

import (
	"testing"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Сообщение log (поток): записи — в OnLog, ack по seq, повтор seq — без OnLog.
func TestOnLog(t *testing.T) {
	type batch struct {
		agentID string
		entries []message.LogEntry
	}
	var got collector[batch]
	agents := newTestAgents(t, Options{OnLog: func(id string, e []message.LogEntry) { got.add(batch{id, e}) }})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))
	entries := []message.LogEntry{
		{At: 1, Level: message.LogWarn, Source: message.LogSourceAgent, Msg: "нет связи", Attrs: map[string]any{"try": float64(2)}},
		{At: 2, Level: message.LogError, Source: "report", Msg: "упал"},
	}
	env := streamEnv(message.TypeLog, message.LogBatch{Entries: entries})
	out := handle(agents, ss, env)
	var ack message.Ack
	if len(out) != 1 || out[0].Type != message.TypeAck || out[0].Decode(&ack) != nil || ack.Seq != env.Seq {
		t.Fatalf("ждали ack{seq}: %+v", out)
	}
	b := got.take()
	if len(b) != 1 || b[0].agentID != id || len(b[0].entries) != 2 || b[0].entries[1].Source != "report" ||
		b[0].entries[0].Attrs["try"] != float64(2) {
		t.Fatalf("OnLog: %+v", b)
	}
	handle(agents, ss, env) // повтор
	if b := got.take(); len(b) != 0 {
		t.Fatalf("повтор — в OnLog: %+v", b)
	}
	if bad := handle(agents, ss, streamEnv(message.TypeLog, "x")); len(ofType(bad, message.TypeError)) != 1 {
		t.Fatalf("некорректное log: %+v", bad)
	}
}
