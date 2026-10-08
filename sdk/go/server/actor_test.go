package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// standWith — стенд HTTP со своими опциями (токен регистрации — tok).
func standWith(t *testing.T, opts Options) *stand {
	opts.EnrollToken = "tok"
	agents := newTestAgents(t, opts)
	srv := httptest.NewServer(agents.Handler())
	t.Cleanup(srv.Close)
	return &stand{t: t, agents: agents, srv: srv}
}

// collector — потокобезопасный сбор уведомлений.
type collector[T any] struct {
	mu    sync.Mutex
	items []T
}

func (c *collector[T]) add(v T) {
	c.mu.Lock()
	c.items = append(c.items, v)
	c.mu.Unlock()
}

func (c *collector[T]) take() []T {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := c.items
	c.items = nil
	return out
}

func protoCode(err error) string {
	var pe *message.Error
	if errors.As(err, &pe) {
		return pe.Code
	}
	return ""
}

func commandsCaps(names ...string) message.Capabilities {
	return message.Capabilities{Commands: &message.CommandsCapability{Names: names}}
}

// RotateKey: cmd.run agent.rotateKey; cmd.done с secretHash — ожидающий
// секрет, ack и закрытие 1012; подключение с новым секретом — он основной,
// старый перестаёт действовать.
func TestRotateKey(t *testing.T) {
	s := newStand(t)
	id, auth := s.enroll("a")
	a := s.dial(auth)
	a.send(helloEnv("boot", commandsCaps(message.CommandRotateKey)))
	a.expect(message.TypeWelcome, nil)

	cmd, err := s.agents.RotateKey(id)
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Name != "agent.rotateKey" || cmd.TimeoutSec != 60 || cmd.AgentID != id {
		t.Fatalf("команда: %+v", cmd)
	}
	var run message.CommandRun
	a.expect(message.TypeCmdRun, &run)
	if run.CommandID != cmd.ID || run.Name != message.CommandRotateKey {
		t.Fatalf("cmd.run: %+v", run)
	}

	newSecret := "new-secret"
	sum := sha256.Sum256([]byte(newSecret))
	done := reliableEnv(message.TypeCmdDone, message.CommandDone{
		CommandID: cmd.ID, OK: true,
		Result: json.RawMessage(`{"secretHash":"` + hex.EncodeToString(sum[:]) + `"}`),
	})
	a.send(done)
	var ack message.Ack
	a.expect(message.TypeAck, &ack)
	if len(ack.IDs) != 1 || ack.IDs[0] != done.ID {
		t.Fatalf("ack: %+v", ack)
	}
	select {
	case err := <-a.done:
		if websocket.CloseStatus(err) != message.CloseRestart {
			t.Fatalf("закрытие: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("сессия не закрыта")
	}
	got, _ := s.agents.CommandByID(cmd.ID)
	if got.Status != CommandSucceeded {
		t.Fatalf("статус команды: %s", got.Status)
	}
	agent, _ := s.agents.Agent(id)
	if agent.PendingSecretHash != hex.EncodeToString(sum[:]) {
		t.Fatalf("ожидающий секрет: %q", agent.PendingSecretHash)
	}
	raw, _ := json.Marshal(agent)
	if strings.Contains(string(raw), agent.PendingSecretHash) {
		t.Fatalf("секрет наружу: %s", raw)
	}

	// Пока агент не подключился с новым — действует и старый.
	if _, ok := s.agents.authenticate(auth); !ok {
		t.Fatal("старый секрет до перехода отклонён")
	}
	newAuth := "Agent " + id + "." + newSecret
	b := s.dial(newAuth)
	b.send(helloEnv("boot", commandsCaps(message.CommandRotateKey)))
	b.expect(message.TypeWelcome, nil)
	agent, _ = s.agents.Agent(id)
	if agent.PendingSecretHash != "" || agent.SecretHash != hex.EncodeToString(sum[:]) {
		t.Fatalf("после перехода: secret=%q pending=%q", agent.SecretHash, agent.PendingSecretHash)
	}
	if _, ok := s.agents.authenticate(auth); ok {
		t.Fatal("старый секрет действует после перехода")
	}
	if _, ok := s.agents.authenticate(newAuth); !ok {
		t.Fatal("новый секрет отклонён")
	}
}

func TestRotateKeyErrors(t *testing.T) {
	agents := newTestAgents(t, Options{})
	plain, _ := enroll(t, agents, "plain")
	open(t, agents, plain, helloEnv("b", commandsCaps("x.run")))
	if _, err := agents.RotateKey(plain); protoCode(err) != "COMMAND_NOT_SUPPORTED" {
		t.Fatalf("не объявил: %v", err)
	}
	if _, err := agents.RotateKey("nobody"); protoCode(err) != "AGENT_NOT_FOUND" {
		t.Fatalf("нет агента: %v", err)
	}
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", commandsCaps(message.CommandRotateKey)))

	// Итог без корректного secretHash — ожидающего нет, сессия жива.
	cmd, err := agents.RotateKey(id)
	if err != nil {
		t.Fatal(err)
	}
	handle(agents, ss, reliableEnv(message.TypeCmdDone, message.CommandDone{CommandID: cmd.ID, OK: true, Result: json.RawMessage(`{"secretHash":"xyz"}`)}))
	if a, _ := agents.Agent(id); a.PendingSecretHash != "" || ss.closed {
		t.Fatalf("некорректный hash: pending=%q closed=%v", a.PendingSecretHash, ss.closed)
	}

	// Revoke очищает ожидающий; отозванному — AGENT_REVOKED.
	cmd, _ = agents.RotateKey(id)
	hash := strings.Repeat("ab", 32)
	handle(agents, ss, reliableEnv(message.TypeCmdDone, message.CommandDone{CommandID: cmd.ID, OK: true, Result: json.RawMessage(`{"secretHash":"` + hash + `"}`)}))
	agents.mu.Lock()
	closed, code := ss.closed, ss.code
	agents.mu.Unlock()
	if !closed || code != message.CloseRestart {
		t.Fatalf("сессия: closed=%v code=%d", closed, code)
	}
	if a, _ := agents.Agent(id); a.PendingSecretHash != hash {
		t.Fatalf("pending: %q", a.PendingSecretHash)
	}
	if err := agents.Revoke(id); err != nil {
		t.Fatal(err)
	}
	if a, _ := agents.Agent(id); a.PendingSecretHash != "" {
		t.Fatalf("pending после Revoke: %q", a.PendingSecretHash)
	}
	if _, err := agents.RotateKey(id); protoCode(err) != "AGENT_REVOKED" {
		t.Fatalf("отозван: %v", err)
	}
}

// RotateKey по HTTP sync: ответ с ack, затем сессия недействительна.
func TestRotateKeyHTTPSync(t *testing.T) {
	s := newStand(t)
	id, auth := s.enroll("a")
	ctx := context.Background()
	_, resp, _ := s.sync(ctx, auth, nil, 0, helloEnv("boot", commandsCaps(message.CommandRotateKey)))
	sid := resp.SessionID
	cmd, err := s.agents.RotateKey(id)
	if err != nil {
		t.Fatal(err)
	}
	if _, resp, _ = s.sync(ctx, auth, &sid, 0); len(ofType(resp.Messages, message.TypeCmdRun)) != 1 {
		t.Fatalf("нет cmd.run: %+v", resp.Messages)
	}
	done := reliableEnv(message.TypeCmdDone, message.CommandDone{
		CommandID: cmd.ID, OK: true, Result: json.RawMessage(`{"secretHash":"` + strings.Repeat("0f", 32) + `"}`),
	})
	code, resp, _ := s.sync(ctx, auth, &sid, 0, done)
	if code != http.StatusOK || len(ofType(resp.Messages, message.TypeAck)) != 1 {
		t.Fatalf("ответ на cmd.done: %d %+v", code, resp.Messages)
	}
	if code, _, _ := s.sync(ctx, auth, &sid, 0); code != http.StatusConflict {
		t.Fatalf("сессия после смены ключа: %d", code)
	}
}

// Ограничение неудачных регистраций: после limit неудач — 429 даже с верным
// токеном, пока окно не пройдёт; удачные не считаются.
func TestEnrollRateLimit(t *testing.T) {
	s := standWith(t, Options{EnrollFailureLimit: 3, EnrollFailureWindow: 500 * time.Millisecond})
	s.enroll("ok1")
	s.enroll("ok2")
	for i := range 3 {
		resp, data := s.post(message.EnrollPath, "", map[string]any{"token": "bad", "name": "x"})
		if resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("попытка %d: %d %s", i, resp.StatusCode, data)
		}
	}
	resp, data := s.post(message.EnrollPath, "", map[string]any{"token": "tok", "name": "x"})
	var e message.Error
	_ = json.Unmarshal(data, &e)
	if resp.StatusCode != http.StatusTooManyRequests || e.Code != "ENROLL_RATE_LIMITED" {
		t.Fatalf("после лимита: %d %s", resp.StatusCode, data)
	}
	if ra, err := strconv.Atoi(resp.Header.Get("Retry-After")); err != nil || ra < 1 {
		t.Fatalf("Retry-After: %q", resp.Header.Get("Retry-After"))
	}
	time.Sleep(600 * time.Millisecond)
	s.enroll("after")

	// Без ограничения — только 401.
	s = standWith(t, Options{EnrollFailureLimit: -1})
	for i := range 15 {
		if resp, _ := s.post(message.EnrollPath, "", map[string]any{"token": "bad", "name": "x"}); resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("без ограничения, попытка %d: %d", i, resp.StatusCode)
		}
	}

	// По умолчанию — 10 за минуту.
	s = standWith(t, Options{})
	for range 10 {
		s.post(message.EnrollPath, "", map[string]any{"token": "bad", "name": "x"})
	}
	if resp, _ := s.post(message.EnrollPath, "", map[string]any{"token": "tok", "name": "x"}); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("по умолчанию: %d", resp.StatusCode)
	}
}

func TestClientAddr(t *testing.T) {
	for addr, want := range map[string]string{"10.0.0.1:5000": "10.0.0.1", "[::1]:80": "::1", "sock": "sock", "": "*"} {
		r := httptest.NewRequest(http.MethodPost, "/", nil)
		r.RemoteAddr = addr
		r.Header.Set("X-Forwarded-For", "1.2.3.4")
		if got := clientAddr(r); got != want {
			t.Fatalf("%q: %q, ждали %q", addr, got, want)
		}
	}
}

// By: actor в Job, Command, DesiredState и записях аудита; без By — пустой.
func TestActorAndAudit(t *testing.T) {
	var audits collector[AuditEntry]
	agents := newTestAgents(t, Options{OnAudit: audits.add})
	id, _ := enroll(t, agents, "a")
	open(t, agents, id, helloEnv("b", commandsCaps("x.run", message.CommandRotateKey)))
	ivan := agents.By("ivan")
	if ivan.Name() != "ivan" {
		t.Fatal(ivan.Name())
	}

	job, err := ivan.Enqueue(JobRequest{Queue: "q"})
	if err != nil || job.Actor != "ivan" {
		t.Fatalf("Enqueue: %+v %v", job, err)
	}
	plain, _ := agents.Enqueue(JobRequest{Queue: "q"})
	if plain.Actor != "" {
		t.Fatalf("без By: %q", plain.Actor)
	}
	if err := ivan.StopJob(job.ID); err != nil {
		t.Fatal(err)
	}
	if err := ivan.CancelJob(plain.ID); err != nil {
		t.Fatal(err)
	}
	cmd, err := ivan.Command(CommandRequest{AgentID: id, Name: "x.run"})
	if err != nil || cmd.Actor != "ivan" {
		t.Fatalf("Command: %+v %v", cmd, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := ivan.Call(ctx, CommandRequest{AgentID: id, Name: "x.run"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("Call: %v", err)
	}
	st, err := ivan.SetState("d", map[string]int{"v": 1}, "")
	if err != nil || st.Actor != "ivan" {
		t.Fatalf("SetState: %+v %v", st, err)
	}
	if got, _ := agents.States(); len(got) != 1 || got[0].Actor != "ivan" {
		t.Fatalf("States: %+v", got)
	}
	if _, err := ivan.SetState("d", map[string]int{"v": 2}, id); err != nil {
		t.Fatal(err)
	}
	if _, err := ivan.RollbackState("d", st.Version, ""); err != nil {
		t.Fatal(err)
	}
	if _, err := ivan.DeleteState("d", id); err != nil {
		t.Fatal(err)
	}
	if _, err := ivan.RotateKey(id); err != nil {
		t.Fatal(err)
	}
	if err := ivan.Revoke(id); err != nil {
		t.Fatal(err)
	}

	got := audits.take()
	want := []struct{ action, agentID, target string }{
		{AuditJobEnqueue, "", job.ID},
		{AuditJobEnqueue, "", plain.ID},
		{AuditJobStop, "", job.ID}, // в очереди: StopJob отменяет, действие — stop
		{AuditJobCancel, "", plain.ID},
		{AuditCommand, id, cmd.ID},
		{AuditCommand, id, ""},
		{AuditStateSet, "", "d"},
		{AuditStateSet, id, "d"},
		{AuditStateRollback, "", "d"},
		{AuditStateDelete, id, "d"},
		{AuditAgentRotateKey, id, id},
		{AuditAgentRevoke, id, id},
	}
	if len(got) != len(want) {
		t.Fatalf("записей %d, ждали %d: %+v", len(got), len(want), got)
	}
	for i, w := range want {
		e := got[i]
		wantActor := "ivan"
		if i == 1 {
			wantActor = ""
		}
		if e.Action != w.action || e.AgentID != w.agentID || (w.target != "" && e.Target != w.target) ||
			e.Actor != wantActor || e.At == 0 {
			t.Fatalf("запись %d: %+v, ждали %+v", i, e, w)
		}
	}
	if got[0].Details["queue"] != "q" || got[4].Details["name"] != "x.run" || got[8].Details["fromVersion"] != st.Version {
		t.Fatalf("details: %+v %+v %+v", got[0].Details, got[4].Details, got[8].Details)
	}
	// StopJob задачи в работе — job.stop.
	id2, _ := enroll(t, agents, "b")
	ss := open(t, agents, id2, helloEnv("b", message.Capabilities{}))
	handle(agents, ss, status(map[string]int{"q2": 1}))
	running, _ := agents.Enqueue(JobRequest{Queue: "q2"})
	if j, _ := agents.Job(running.ID); j.Status != JobRunning {
		t.Fatalf("задача не выдана: %s", j.Status)
	}
	audits.take()
	if err := agents.By("petr").StopJob(running.ID); err != nil {
		t.Fatal(err)
	}
	if got := audits.take(); len(got) != 1 || got[0].Action != AuditJobStop || got[0].Actor != "petr" || got[0].AgentID != id2 {
		t.Fatalf("job.stop: %+v", got)
	}
	// Ошибка — без записи.
	if _, err := agents.By("x").SetState("", nil, ""); err == nil {
		t.Fatal("пустой domain")
	}
	if got := audits.take(); len(got) != 0 {
		t.Fatalf("аудит ошибки: %+v", got)
	}
}

func TestUpdateAgentAudit(t *testing.T) {
	var audits collector[AuditEntry]
	s := releaseStandWith(t, Options{OnAudit: audits.add})
	id, auth := s.enroll("a")
	a := s.dial(auth)
	h := message.Hello{
		Versions: []int{1}, Agent: message.HelloAgent{Name: "a", Version: "1.0.0", BootID: "b"},
		Host: message.Host{OS: "linux", Arch: "amd64"},
		Capabilities: message.Capabilities{
			Commands: &message.CommandsCapability{Names: []string{"agent.update"}},
			Update:   &message.UpdateCapability{Mode: "self"},
		},
		Jobs: []message.JobRef{},
	}
	a.send(message.MustNew(message.TypeHello, h))
	a.expect(message.TypeWelcome, nil)
	cmd, err := s.agents.By("ivan").UpdateAgent(id)
	if err != nil || cmd.Actor != "ivan" {
		t.Fatalf("UpdateAgent: %+v %v", cmd, err)
	}
	got := audits.take()
	if len(got) != 1 || got[0].Action != AuditAgentUpdate || got[0].Target != id || got[0].Actor != "ivan" ||
		got[0].Details["commandId"] != cmd.ID || got[0].Details["version"] != "2.0.0" {
		t.Fatalf("аудит: %+v", got)
	}
}

// История состояния: от новых к старым, лимит, не удаляется DeleteState;
// откат — новая версия с тем же spec; нет версии — STATE_VERSION_NOT_FOUND.
func TestStateHistoryAndRollback(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{State: &message.StateCapability{Domains: map[string]*int64{"d": nil}}}))
	var versions []int64
	for i := 1; i <= 3; i++ {
		st, err := agents.SetState("d", map[string]int{"v": i}, "")
		if err != nil {
			t.Fatal(err)
		}
		versions = append(versions, st.Version)
	}
	agents.SetState("d", map[string]int{"own": 1}, id)
	take(agents, ss)

	h, err := agents.StateHistory("d", "", 0)
	if err != nil || len(h) != 3 || h[0].Version != versions[2] || h[2].Version != versions[0] {
		t.Fatalf("история: %+v %v", h, err)
	}
	if h, _ := agents.StateHistory("d", "", 2); len(h) != 2 || h[1].Version != versions[1] {
		t.Fatalf("лимит: %+v", h)
	}
	if h, _ := agents.StateHistory("d", id, 0); len(h) != 1 || string(h[0].Spec) != `{"own":1}` {
		t.Fatalf("личная: %+v", h)
	}
	if h, _ := agents.StateHistory("none", "", 0); h == nil || len(h) != 0 {
		t.Fatalf("пустая: %#v", h)
	}

	if _, err := agents.DeleteState("d", id); err != nil {
		t.Fatal(err)
	}
	take(agents, ss)
	if h, _ := agents.StateHistory("d", id, 0); len(h) != 1 {
		t.Fatalf("история после удаления: %+v", h)
	}
	st, err := agents.RollbackState("d", versions[0], "")
	if err != nil {
		t.Fatal(err)
	}
	cur, _ := agents.States()
	if string(st.Spec) != `{"v":1}` || st.Version <= versions[2] || len(cur) != 1 || cur[0].Version != st.Version {
		t.Fatalf("откат: %+v текущие %+v", st, cur)
	}
	if puts := ofType(take(agents, ss), message.TypeStatePut); len(puts) != 1 {
		t.Fatalf("откат не доставлен: %+v", puts)
	}
	// 3 + переизданный общий при удалении личного + откат.
	if h, _ := agents.StateHistory("d", "", 0); len(h) != 5 || h[0].Version != st.Version {
		t.Fatalf("история после отката: %+v", h)
	}
	if _, err := agents.RollbackState("d", 12345, ""); protoCode(err) != "STATE_VERSION_NOT_FOUND" {
		t.Fatalf("нет версии: %v", err)
	}
	if _, err := agents.RollbackState("d", versions[0], "nobody"); protoCode(err) != "AGENT_NOT_FOUND" {
		t.Fatalf("нет агента: %v", err)
	}
}

func TestMemoryStoreStateHistoryKeep(t *testing.T) {
	s := NewMemoryStore()
	for i := range 60 {
		if _, err := s.SetState("d", "", json.RawMessage(strconv.Itoa(i)), "a"); err != nil {
			t.Fatal(err)
		}
	}
	h, _ := s.ListStateHistory("d", "", 0)
	if len(h) != 50 || string(h[0].Spec) != "59" || string(h[49].Spec) != "10" || h[0].Actor != "a" {
		t.Fatalf("история: %d %s…%s", len(h), h[0].Spec, h[len(h)-1].Spec)
	}
	if h, _ := s.ListStateHistory("d", "x", 0); len(h) != 0 {
		t.Fatalf("чужой ключ: %+v", h)
	}
}
