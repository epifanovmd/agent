package server

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// sameAudit — запись аудита с этими полями (без подробностей).
func sameAudit(e AuditEntry, actor, action, agentID, target string) bool {
	return e.At != 0 && e.Actor == actor && e.Action == action && e.AgentID == agentID && e.Target == target && e.Details == nil
}

// cancelOf — id команд в сообщениях cmd.cancel.
func cancelOf(out []message.Envelope) []string {
	var ids []string
	for _, env := range ofType(out, message.TypeCmdCancel) {
		var ref message.CommandRef
		_ = env.Decode(&ref)
		ids = append(ids, ref.CommandID)
	}
	return ids
}

// Отмена команды: ждущая и не отправленная — без сообщения; отправленная и
// выполняющаяся — cmd.cancel; поздний итог не меняет cancelled; аудит
// command.cancel; ждущий Call просыпается; повтор — COMMAND_NOT_ACTIVE, нет
// команды — COMMAND_NOT_FOUND.
func TestCancelCommand(t *testing.T) {
	var audits collector[AuditEntry]
	var changes collector[Change]
	agents := newTestAgents(t, Options{OnAudit: audits.add, OnChange: changes.add})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", commandsCaps("x.run")))

	// Ждущая, агент её не объявил — не отправлена.
	idle, err := agents.Command(CommandRequest{AgentID: id, Name: "y.run"})
	if err != nil {
		t.Fatal(err)
	}
	changes.take()
	got, err := agents.By("ivan").CancelCommand(idle.ID)
	if err != nil || got.Status != CommandCancelled || got.Error == nil || got.Error.Code != "CANCELLED" || got.FinishedAt == 0 {
		t.Fatalf("ждущая: %+v %v", got, err)
	}
	if out := take(agents, ss); len(out) != 0 {
		t.Fatalf("ждущей — сообщения: %+v", out)
	}
	if a := audits.take(); len(a) != 2 || !sameAudit(a[1], "ivan", AuditCommandCancel, id, idle.ID) {
		t.Fatalf("аудит: %+v", a)
	}
	if c := changes.take(); len(c) != 1 || c[0] != (Change{Kind: ChangeCommand, ID: idle.ID}) {
		t.Fatalf("уведомления: %+v", c)
	}

	// Отправленная (cmd.run ушёл) — cmd.cancel; поздний итог CANCELLED — ack, итог прежний.
	sent, _ := agents.Command(CommandRequest{AgentID: id, Name: "x.run"})
	if out := ofType(take(agents, ss), message.TypeCmdRun); len(out) != 1 {
		t.Fatalf("cmd.run: %+v", out)
	}
	if _, err := agents.CancelCommand(sent.ID); err != nil {
		t.Fatal(err)
	}
	if ids := cancelOf(take(agents, ss)); len(ids) != 1 || ids[0] != sent.ID {
		t.Fatalf("cmd.cancel: %v", ids)
	}
	done := reliableEnv(message.TypeCmdDone, message.CommandDone{CommandID: sent.ID, Error: &message.CommandError{Code: "CANCELLED", Message: "прервана"}})
	if out := handle(agents, ss, done); len(out) != 1 || out[0].Type != message.TypeAck {
		t.Fatalf("поздний итог: %+v", out)
	}
	if cur, _ := agents.CommandByID(sent.ID); cur.Status != CommandCancelled || cur.Error.Message != "Команду отменили" {
		t.Fatalf("итог изменился: %+v", cur)
	}

	// Выполняющаяся: cmd.cancel; поздний успешный итог не учитывается; Call просыпается.
	result := make(chan *Command, 1)
	go func() {
		cmd, _ := agents.Call(context.Background(), CommandRequest{AgentID: id, Name: "x.run"})
		result <- cmd
	}()
	var run message.CommandRun
	eventually(t, "cmd.run", func() bool {
		out := ofType(take(agents, ss), message.TypeCmdRun)
		return len(out) == 1 && out[0].Decode(&run) == nil
	})
	handle(agents, ss, streamEnv(message.TypeCmdAccept, message.CommandRef{CommandID: run.CommandID}))
	if cur, _ := agents.CommandByID(run.CommandID); cur.Status != CommandRunning {
		t.Fatalf("не выполняется: %+v", cur)
	}
	if _, err := agents.CancelCommand(run.CommandID); err != nil {
		t.Fatal(err)
	}
	select {
	case cmd := <-result:
		if cmd == nil || cmd.Status != CommandCancelled {
			t.Fatalf("Call: %+v", cmd)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Call не проснулся")
	}
	if ids := cancelOf(take(agents, ss)); len(ids) != 1 || ids[0] != run.CommandID {
		t.Fatalf("cmd.cancel выполняющейся: %v", ids)
	}
	handle(agents, ss, reliableEnv(message.TypeCmdDone, message.CommandDone{CommandID: run.CommandID, OK: true}))
	if cur, _ := agents.CommandByID(run.CommandID); cur.Status != CommandCancelled {
		t.Fatalf("поздний успех учтён: %+v", cur)
	}

	if _, err := agents.CancelCommand(run.CommandID); protoCode(err) != "COMMAND_NOT_ACTIVE" {
		t.Fatalf("повтор: %v", err)
	}
	if _, err := agents.CancelCommand("нет"); protoCode(err) != "COMMAND_NOT_FOUND" {
		t.Fatalf("нет команды: %v", err)
	}
}

// Сессия агента в другом процессе: отмена там без сообщения, процесс с
// сессией шлёт cmd.cancel при Refresh — один раз.
func TestCancelCommandOtherProcess(t *testing.T) {
	_, a1, a2 := sharedPair(t, Options{})
	id, _ := enroll(t, a1, "a")
	ss := open(t, a1, id, helloEnv("b", commandsCaps("x.run")))
	cmd, _ := a1.Command(CommandRequest{AgentID: id, Name: "x.run"})
	take(a1, ss)
	if _, err := a2.CancelCommand(cmd.ID); err != nil {
		t.Fatal(err)
	}
	if out := take(a1, ss); len(out) != 0 {
		t.Fatalf("до Refresh: %+v", out)
	}
	a1.Refresh("")
	if ids := cancelOf(take(a1, ss)); len(ids) != 1 || ids[0] != cmd.ID {
		t.Fatalf("Refresh: %v", ids)
	}
	a1.Refresh("")
	if ids := cancelOf(take(a1, ss)); len(ids) != 0 {
		t.Fatalf("повторный Refresh: %v", ids)
	}
}

// Выполняющаяся команда, принятая в прошлой сессии (у другого процесса):
// отмену в процессе без сессии доставляет при Refresh процесс новой сессии
// агента — один раз.
func TestCancelCommandRunningFromPreviousSession(t *testing.T) {
	_, a1, a2 := sharedPair(t, Options{})
	id, _ := enroll(t, a1, "a")
	ss := open(t, a1, id, helloEnv("b", commandsCaps("x.run")))
	cmd, _ := a1.Command(CommandRequest{AgentID: id, Name: "x.run"})
	take(a1, ss)
	handle(a1, ss, streamEnv(message.TypeCmdAccept, message.CommandRef{CommandID: cmd.ID}))
	a1.mu.Lock()
	a1.closeSession(ss, message.CloseNormal)
	a1.unlock()

	ss2 := open(t, a2, id, helloEnv("b", commandsCaps("x.run")))
	take(a2, ss2)
	if _, err := a1.CancelCommand(cmd.ID); err != nil {
		t.Fatal(err)
	}
	a2.Refresh("")
	if ids := cancelOf(take(a2, ss2)); len(ids) != 1 || ids[0] != cmd.ID {
		t.Fatalf("Refresh: %v", ids)
	}
	a2.Refresh("")
	if ids := cancelOf(take(a2, ss2)); len(ids) != 0 {
		t.Fatalf("повторный Refresh: %v", ids)
	}
}

// conflictStore — хранилище, где условная запись команды всегда не проходит
// (запись всё время меняет кто-то другой).
type conflictStore struct{ *MemoryStore }

func (conflictStore) UpdateCommand(*Command) (bool, error) { return false, nil }

// Повторы условной записи исчерпаны — ошибка STORE_CONFLICT.
func TestStoreConflict(t *testing.T) {
	agents := newTestAgents(t, Options{Store: conflictStore{NewMemoryStore()}})
	id, _ := enroll(t, agents, "a")
	cmd, err := agents.Command(CommandRequest{AgentID: id, Name: "x.run"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := agents.CancelCommand(cmd.ID); protoCode(err) != "STORE_CONFLICT" {
		t.Fatalf("CancelCommand: %v", err)
	}
}

// Удаление агента — только отозванного: запись и история метрик удаляются,
// задачи остаются; аудит agent.delete.
func TestDeleteAgent(t *testing.T) {
	var audits collector[AuditEntry]
	var changes collector[Change]
	agents := newTestAgents(t, Options{OnAudit: audits.add, OnChange: changes.add, MetricsStoreInterval: -1})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{}))
	job, _ := agents.Enqueue(JobRequest{Queue: "q", AgentID: id})

	if err := agents.DeleteAgent("нет"); protoCode(err) != "AGENT_NOT_FOUND" {
		t.Fatalf("нет агента: %v", err)
	}
	if err := agents.DeleteAgent(id); protoCode(err) != "AGENT_NOT_REVOKED" {
		t.Fatalf("не отозван: %v", err)
	}
	if err := agents.Revoke(id); err != nil {
		t.Fatal(err)
	}
	audits.take()
	changes.take()
	if err := agents.By("ivan").DeleteAgent(id); err != nil {
		t.Fatal(err)
	}
	if _, err := agents.Agent(id); !errors.Is(err, ErrNotFound) {
		t.Fatalf("запись осталась: %v", err)
	}
	if points, _ := agents.Metrics(id, 0); len(points) != 0 {
		t.Fatalf("метрики остались: %d", len(points))
	}
	if j, err := agents.Job(job.ID); err != nil || j.ID != job.ID {
		t.Fatalf("задача удалена: %v", err)
	}
	if a := audits.take(); len(a) != 1 || !sameAudit(a[0], "ivan", AuditAgentDelete, id, id) {
		t.Fatalf("аудит: %+v", a)
	}
	if c := changes.take(); len(c) != 1 || c[0] != (Change{Kind: ChangeAgent, ID: id}) {
		t.Fatalf("уведомления: %+v", c)
	}
	if err := agents.DeleteAgent(id); protoCode(err) != "AGENT_NOT_FOUND" {
		t.Fatalf("повтор: %v", err)
	}
}

// postEnroll — POST enroll с сырым телом и заголовками.
func (s *stand) postEnroll(body []byte, header http.Header) *http.Response {
	s.t.Helper()
	req, _ := http.NewRequest(http.MethodPost, s.srv.URL+message.EnrollPath, bytes.NewReader(body))
	for k, v := range header {
		req.Header[k] = v
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp
}

func enrollBody(t *testing.T, v any) []byte {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Запрос регистрации: тело не больше 64 КБ (413), name и метки по правилам
// (400 MESSAGE_INVALID); каждая такая попытка — в счёт неудач. Хук Enroll
// получает name, labels и host.
func TestEnrollRequestChecks(t *testing.T) {
	var seen collector[EnrollInfo]
	s := standWith(t, Options{EnrollFailureLimit: -1, Enroll: func(token string, info EnrollInfo) (map[string]string, bool) {
		seen.add(info)
		return nil, token == "tok"
	}})
	labels := func(n int) map[string]string {
		m := map[string]string{}
		for i := range n {
			m["k"+strings.Repeat("x", i)] = "v"
		}
		return m
	}
	for name, c := range map[string]struct {
		body []byte
		code int
	}{
		"больше 64 КБ":        {enrollBody(t, map[string]any{"token": "tok", "name": "a", "host": map[string]string{"x": strings.Repeat("x", 65<<10)}}), http.StatusRequestEntityTooLarge},
		"без name":            {enrollBody(t, map[string]any{"token": "tok"}), http.StatusBadRequest},
		"без token":           {enrollBody(t, map[string]any{"name": "a"}), http.StatusBadRequest},
		"name длиннее 128":    {enrollBody(t, map[string]any{"token": "tok", "name": strings.Repeat("я", 129)}), http.StatusBadRequest},
		"меток больше 64":     {enrollBody(t, map[string]any{"token": "tok", "name": "a", "labels": labels(65)}), http.StatusBadRequest},
		"пустой ключ метки":   {enrollBody(t, map[string]any{"token": "tok", "name": "a", "labels": map[string]string{"": "v"}}), http.StatusBadRequest},
		"длинное значение":    {enrollBody(t, map[string]any{"token": "tok", "name": "a", "labels": map[string]string{"k": strings.Repeat("v", 257)}}), http.StatusBadRequest},
		"метка не строка":     {enrollBody(t, map[string]any{"token": "tok", "name": "a", "labels": map[string]any{"k": 1}}), http.StatusBadRequest},
		"name 128 и 64 метки": {enrollBody(t, map[string]any{"token": "tok", "name": strings.Repeat("я", 128), "labels": labels(64)}), http.StatusCreated},
		"длина метки 256":     {enrollBody(t, map[string]any{"token": "tok", "name": "a", "labels": map[string]string{strings.Repeat("k", 256): strings.Repeat("v", 256)}}), http.StatusCreated},
		"неверный токен":      {enrollBody(t, map[string]any{"token": "bad", "name": "a"}), http.StatusUnauthorized},
	} {
		if resp := s.postEnroll(c.body, nil); resp.StatusCode != c.code {
			t.Fatalf("%s: %d, ждали %d", name, resp.StatusCode, c.code)
		}
	}

	// Хук — с name, labels и host.
	seen.take()
	body := enrollBody(t, map[string]any{"token": "tok", "name": "node-01", "labels": map[string]string{"zone": "eu"},
		"host": map[string]string{"os": "linux"}})
	if resp := s.postEnroll(body, nil); resp.StatusCode != http.StatusCreated {
		t.Fatalf("регистрация: %d", resp.StatusCode)
	}
	if got := seen.take(); len(got) != 1 || got[0].Name != "node-01" || got[0].Labels["zone"] != "eu" || string(got[0].Host) != `{"os":"linux"}` {
		t.Fatalf("хук: %+v", got)
	}

	// Слишком большое тело — в счёт неудач.
	limited := standWith(t, Options{EnrollFailureLimit: 1})
	big := enrollBody(t, map[string]any{"token": "tok", "name": strings.Repeat("x", 70<<10)})
	if resp := limited.postEnroll(big, nil); resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Fatalf("большое тело: %d", resp.StatusCode)
	}
	if resp := limited.postEnroll(enrollBody(t, map[string]any{"token": "tok", "name": "a"}), nil); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("после большого тела: %d", resp.StatusCode)
	}
}

// Неудачные регистрации считаются по адресу клиента: при TrustProxy — по
// первому адресу X-Forwarded-For, без него — по адресу сокета.
func TestEnrollRateLimitByClientAddress(t *testing.T) {
	bad := func(t *testing.T) []byte { return enrollBody(t, map[string]any{"token": "bad", "name": "x"}) }
	xff := func(addr string) http.Header { return http.Header{"X-Forwarded-For": {addr + ", 10.0.0.1"}} }

	proxied := standWith(t, Options{TrustProxy: true, EnrollFailureLimit: 2})
	for range 2 {
		proxied.postEnroll(bad(t), xff("203.0.113.1"))
	}
	if resp := proxied.postEnroll(bad(t), xff("203.0.113.1")); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("тот же адрес за прокси: %d", resp.StatusCode)
	}
	if resp := proxied.postEnroll(bad(t), xff("203.0.113.2")); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("другой адрес за прокси: %d", resp.StatusCode)
	}

	direct := standWith(t, Options{EnrollFailureLimit: 2})
	direct.postEnroll(bad(t), xff("203.0.113.1"))
	direct.postEnroll(bad(t), xff("203.0.113.2"))
	if resp := direct.postEnroll(bad(t), xff("203.0.113.3")); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("без TrustProxy X-Forwarded-For учтён: %d", resp.StatusCode)
	}
}

// Адрес сервера из запроса: Host и TLS; X-Forwarded-Host и X-Forwarded-Proto —
// только при TrustProxy.
func TestRequestBase(t *testing.T) {
	plain := newTestAgents(t, Options{})
	proxied := newTestAgents(t, Options{TrustProxy: true})
	r := httptest.NewRequest(http.MethodGet, "/", nil)
	r.Host = "server.example:8080"
	if got := plain.requestBase(r); got != "http://server.example:8080" {
		t.Fatal(got)
	}
	r.TLS = &tls.ConnectionState{}
	if got := plain.requestBase(r); got != "https://server.example:8080" {
		t.Fatal(got)
	}
	r.TLS = nil
	r.Header.Set("X-Forwarded-Host", "agents.example.com, inner.example")
	r.Header.Set("X-Forwarded-Proto", "https")
	if got := plain.requestBase(r); got != "http://server.example:8080" {
		t.Fatalf("без TrustProxy: %s", got)
	}
	if got := proxied.requestBase(r); got != "https://agents.example.com" {
		t.Fatalf("с TrustProxy: %s", got)
	}
}

// Команда без agentId отозванному агенту не поручается, даже если он объявил её.
func TestCommandSkipsRevoked(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	open(t, agents, id, helloEnv("b", commandsCaps("x.run")))
	if err := agents.Revoke(id); err != nil {
		t.Fatal(err)
	}
	if _, err := agents.Command(CommandRequest{Name: "x.run"}); protoCode(err) != "COMMAND_NOT_SUPPORTED" {
		t.Fatalf("отозванному: %v", err)
	}
}
