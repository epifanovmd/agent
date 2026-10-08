package server

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// PauseWorker/ResumeWorker: cmd.run worker.pause/worker.resume по образцу,
// срок 30 с, аудит worker.pause/worker.resume с actor; ошибки — по коду.
func TestPauseResumeWorker(t *testing.T) {
	var audits collector[AuditEntry]
	agents := newTestAgents(t, Options{OnAudit: audits.add})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", commandsCaps(message.CommandWorkerPause, message.CommandWorkerResume)))

	cmd, err := agents.By("ivan").PauseWorker(id, "report", "example.echo")
	if err != nil || cmd.Name != message.CommandWorkerPause || cmd.Actor != "ivan" || cmd.TimeoutSec != 30 {
		t.Fatalf("PauseWorker: %+v %v", cmd, err)
	}
	runs := ofType(take(agents, ss), message.TypeCmdRun)
	if len(runs) != 1 {
		t.Fatalf("cmd.run: %+v", runs)
	}
	var got, want map[string]any
	_ = json.Unmarshal(runs[0].Data, &got)
	_ = json.Unmarshal(exampleEnv(t, "cmd.run.workerPause").Data, &want)
	got["commandId"], want["commandId"] = nil, nil
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("cmd.run worker.pause:\n got %v\nwant %v", got, want)
	}

	cmd2, err := agents.ResumeWorker(id, "report")
	if err != nil || cmd2.Name != message.CommandWorkerResume || cmd2.Actor != "" || string(cmd2.Args) != `{"name":"report"}` {
		t.Fatalf("ResumeWorker: %+v %s %v", cmd2, cmd2.Args, err)
	}
	entries := audits.take()
	if len(entries) != 2 {
		t.Fatalf("аудит: %+v", entries)
	}
	p, r := entries[0], entries[1]
	if p.Action != AuditWorkerPause || p.Actor != "ivan" || p.AgentID != id || p.Target != id ||
		p.Details["worker"] != "report" || p.Details["commandId"] != cmd.ID ||
		!reflect.DeepEqual(p.Details["queues"], []string{"example.echo"}) {
		t.Fatalf("аудит pause: %+v", p)
	}
	if r.Action != AuditWorkerResume || r.Actor != "" || r.Details["worker"] != "report" || r.Details["commandId"] != cmd2.ID {
		t.Fatalf("аудит resume: %+v", r)
	}
	if _, ok := r.Details["queues"]; ok {
		t.Fatalf("без очередей — без queues: %+v", r.Details)
	}

	other, _ := enroll(t, agents, "old")
	open(t, agents, other, helloEnv("b", commandsCaps("x.run")))
	revoked, _ := enroll(t, agents, "gone")
	if err := agents.Revoke(revoked); err != nil {
		t.Fatal(err)
	}
	audits.take()
	for name, c := range map[string]struct {
		err  error
		code string
	}{
		"нет агента":     {err1(agents.PauseWorker("nobody", "report")), "AGENT_NOT_FOUND"},
		"имя воркера":    {err1(agents.PauseWorker(id, "a b")), "MESSAGE_INVALID"},
		"имя очереди":    {err1(agents.ResumeWorker(id, "report", "a;b")), "MESSAGE_INVALID"},
		"не объявлена":   {err1(agents.PauseWorker(other, "report")), "COMMAND_NOT_SUPPORTED"},
		"агент отозван":  {err1(agents.ResumeWorker(revoked, "report")), "AGENT_REVOKED"},
		"actor и ошибки": {err1(agents.By("ivan").ResumeWorker(other, "report")), "COMMAND_NOT_SUPPORTED"},
	} {
		if protoCode(c.err) != c.code {
			t.Errorf("%s: %v, ждали %s", name, c.err, c.code)
		}
	}
	if left := audits.take(); len(left) != 0 {
		t.Fatalf("аудит при ошибках: %+v", left)
	}
}

func err1(_ *Command, err error) error { return err }

// workerDegraded: начало — health degraded (message — причина), повтор — без
// событий, конец — health ok или воркер пропал из status.
func TestWorkerDegradedAlert(t *testing.T) {
	var alerts collector[Alert]
	agents := newTestAgents(t, Options{OnAlert: alerts.add})
	id, _ := enroll(t, agents, "node")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))
	degraded := message.StatusWorker{Name: "report", State: "running", Health: message.WorkerHealthDegraded, Message: "example.db недоступна"}

	handle(agents, ss, statusWith(message.StateDegraded, "воркер report не в порядке", degraded))
	got := alerts.take()
	expectAlerts(t, got, id, "node", alertWant{AlertDegraded, true, ""}, alertWant{AlertWorkerDegraded, true, "report"})
	if got[1].Message != "example.db недоступна" || got[1].Worker != "report" {
		t.Fatalf("workerDegraded: %+v", got[1])
	}
	handle(agents, ss, statusWith(message.StateDegraded, "воркер report не в порядке", degraded))
	expectAlerts(t, alerts.take(), id, "node")

	ok := degraded
	ok.Health, ok.Message = message.WorkerHealthOK, ""
	handle(agents, ss, statusWith(message.StateIdle, "", ok))
	expectAlerts(t, alerts.take(), id, "node", alertWant{AlertDegraded, false, ""}, alertWant{AlertWorkerDegraded, false, "report"})

	// Без причины — своё сообщение; воркер пропал — конец.
	degraded.Message = ""
	handle(agents, ss, statusWith(message.StateIdle, "", degraded))
	got = alerts.take()
	expectAlerts(t, got, id, "node", alertWant{AlertWorkerDegraded, true, "report"})
	if got[0].Message == "" {
		t.Fatal("сообщение без причины пустое")
	}
	handle(agents, ss, statusWith(message.StateIdle, ""))
	expectAlerts(t, alerts.take(), id, "node", alertWant{AlertWorkerDegraded, false, "report"})
}

// Адрес агента: IP без порта из RemoteAddr; X-Forwarded-For — только при
// TrustProxy (первый адрес списка).
func TestAgentAddress(t *testing.T) {
	for _, c := range []struct {
		trust      bool
		remote     string
		xff        string
		want       string
		descriptor string
	}{
		{false, "10.0.0.1:5000", "203.0.113.7", "10.0.0.1", "без доверия прокси"},
		{false, "[::1]:80", "", "::1", "IPv6"},
		{true, "10.0.0.1:5000", "203.0.113.7, 10.0.0.2", "203.0.113.7", "первый из списка"},
		{true, "10.0.0.1:5000", " [2001:db8::1]:443 ", "2001:db8::1", "с портом"},
		{true, "10.0.0.1:5000", "", "10.0.0.1", "без заголовка"},
	} {
		a := &Agents{opts: Options{TrustProxy: c.trust}}
		r := httptest.NewRequest(http.MethodGet, "/", nil)
		r.RemoteAddr = c.remote
		if c.xff != "" {
			r.Header.Set("X-Forwarded-For", c.xff)
		}
		if got := a.agentAddress(r); got != c.want {
			t.Errorf("%s: %q, ждали %q", c.descriptor, got, c.want)
		}
	}

	// WebSocket без TrustProxy: заголовок не учитывается.
	s := newStand(t)
	id, auth := s.enroll("ws")
	ws := s.dial(auth)
	ws.send(helloEnv("b", message.Capabilities{}))
	ws.expect(message.TypeWelcome, nil)
	if a, _ := s.agents.Agent(id); a.Address != "127.0.0.1" {
		t.Fatalf("адрес WebSocket: %q", a.Address)
	}

	// HTTP sync с TrustProxy: адрес — из X-Forwarded-For при hello.
	p := standWith(t, Options{TrustProxy: true})
	id2, auth2 := p.enroll("http")
	raw, _ := json.Marshal(map[string]any{"messages": []message.Envelope{helloEnv("b", message.Capabilities{})}})
	req, _ := http.NewRequestWithContext(context.Background(), http.MethodPost, p.srv.URL+message.SyncPath, strings.NewReader(string(raw)))
	req.Header.Set("Authorization", auth2)
	req.Header.Set("X-Forwarded-For", "203.0.113.7, 10.0.0.2")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if a, _ := p.agents.Agent(id2); resp.StatusCode != http.StatusOK || a.Address != "203.0.113.7" {
		t.Fatalf("адрес HTTP sync: %d %q", resp.StatusCode, a.Address)
	}
}
