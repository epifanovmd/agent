package agentsock

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/outbox"
	"github.com/epifanovmd/agent/internal/requests"
)

type agent struct {
	mu     sync.Mutex
	events []message.Event
	full   bool
	broken bool
	// asked — запросы к серверу; reqErr — ответ на них ошибкой.
	asked  []message.RequestPost
	reqErr error
}

// DeclaredRequest — у воркера report объявлен только report.recipients.
func (a *agent) DeclaredRequest(_ context.Context, name, typ string) bool {
	return name == "report" && typ == "report.recipients"
}

func (a *agent) Request(_ context.Context, name string, p message.RequestPost) (json.RawMessage, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.asked = append(a.asked, p)
	if a.reqErr != nil {
		return nil, a.reqErr
	}
	return json.RawMessage(`{"worker":"` + name + `","echo":` + string(p.Data) + `}`), nil
}

func (a *agent) ByToken(token string) (string, bool) {
	if token == "worker-token-example" {
		return "report", true
	}
	return "", false
}

// Declared — в манифесте воркера report объявлено только report.sent.
func (a *agent) Declared(_ context.Context, name, typ string) bool {
	return name == "report" && typ == "report.sent"
}

func (a *agent) Event(e message.Event) error {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.full {
		return outbox.ErrFull
	}
	if a.broken {
		return errors.New("outbox: диск недоступен")
	}
	a.events = append(a.events, e)
	return nil
}

func (a *agent) Config(name, key string) (message.ConfigValue, bool) {
	if name == "report" && key == "main" {
		return message.ConfigValue{Version: 42, Data: json.RawMessage(`{"a":1}`)}, true
	}
	return message.ConfigValue{}, false
}

func (a *agent) Context() message.Context {
	return message.Context{Agent: message.ContextAgent{ID: "id-1", Name: "node-01", Version: "1.0.0"}, Online: true}
}

func client(t *testing.T, a Agent) (*http.Client, string) {
	dir, _ := os.MkdirTemp("", "s")
	t.Cleanup(func() { os.RemoveAll(dir) })
	path := filepath.Join(dir, "agent.sock")
	srv, err := Listen(path, a, logx.Discard())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(srv.Close)
	return &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", path)
	}}}, path
}

func call(t *testing.T, c *http.Client, method, path, token, body string) (int, message.ErrorInfo, []byte) {
	t.Helper()
	req, _ := http.NewRequest(method, "http://agent"+path, strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var buf bytes.Buffer
	_, _ = buf.ReadFrom(resp.Body)
	var e message.ErrorInfo
	_ = json.Unmarshal(buf.Bytes(), &e)
	return resp.StatusCode, e, buf.Bytes()
}

const token = "worker-token-example"

// Сокет агента (§12): права 0666, токен воркера обязателен; POST /events →
// 202, агент добавляет worker и at; тип не из манифеста — 400
// EVENT_UNDECLARED; GET /config/{key}; GET /context.
func TestSocket(t *testing.T) {
	a := &agent{}
	c, path := client(t, a)
	if info, err := os.Stat(path); err != nil || info.Mode().Perm() != 0o666 {
		t.Fatalf("права сокета: %v %v", info.Mode(), err)
	}
	for _, tok := range []string{"", "чужой"} {
		if code, e, _ := call(t, c, "GET", "/context", tok, ""); code != 401 || e.Code != message.CodeUnauthorized {
			t.Fatalf("без токена: %d %+v", code, e)
		}
	}
	if code, _, _ := call(t, c, "POST", "/events", token, `{"type":"report.sent","data":{"items":12}}`); code != 202 {
		t.Fatalf("событие: %d", code)
	}
	if len(a.events) != 1 || a.events[0].Worker != "report" || a.events[0].Type != "report.sent" || a.events[0].At == 0 ||
		string(a.events[0].Data) != `{"items":12}` {
		t.Fatalf("событие: %+v", a.events)
	}
	for body, code := range map[string]string{
		`{"type":"Report"}`: message.CodeMessageInvalid,
		`не json`:           message.CodeMessageInvalid,
		`{"type":"x","data":"` + strings.Repeat("я", message.MaxEventDataBytes/2) + `"}`: message.CodeBodyTooLarge,
	} {
		if _, e, _ := call(t, c, "POST", "/events", token, body); e.Code != code {
			t.Errorf("%.40s: %+v", body, e)
		}
	}
	if status, e, _ := call(t, c, "POST", "/events", token, `{"type":"report.lost"}`); status != 400 || e.Code != message.CodeEventUndeclared {
		t.Fatalf("необъявленное событие: %d %+v", status, e)
	}
	if len(a.events) != 1 {
		t.Fatalf("необъявленное событие не записывается: %+v", a.events)
	}
	a.full = true
	if status, e, _ := call(t, c, "POST", "/events", token, `{"type":"report.sent"}`); status != 503 || e.Code != message.CodeOutboxFull {
		t.Fatalf("outbox полон: %d %+v", status, e)
	}
	a.full, a.broken = false, true
	if status, e, _ := call(t, c, "POST", "/events", token, `{"type":"report.sent"}`); status != 500 || e.Code != message.CodeInternal {
		t.Fatalf("outbox не записан: %d %+v", status, e)
	}
	a.broken = false
	status, _, raw := call(t, c, "GET", "/config/main", token, "")
	var v message.ConfigValue
	if status != 200 || json.Unmarshal(raw, &v) != nil || v.Version != 42 || string(v.Data) != `{"a":1}` {
		t.Fatalf("настройки: %d %s", status, raw)
	}
	if status, e, _ := call(t, c, "GET", "/config/limits", token, ""); status != 404 || e.Code != message.CodeNotFound {
		t.Fatalf("нет ключа: %d %+v", status, e)
	}
	status, _, raw = call(t, c, "GET", "/context", token, "")
	var ctx message.Context
	if status != 200 || json.Unmarshal(raw, &ctx) != nil || ctx.Agent.ID != "id-1" || !ctx.Online {
		t.Fatalf("контекст: %d %s", status, raw)
	}
	if status, e, _ := call(t, c, "GET", "/nope", token, ""); status != 404 || e.Code != message.CodeNotFound {
		t.Fatalf("нет пути: %d %+v", status, e)
	}
}

// POST /requests (§12): объявленный тип — ответ сервера 200 {data}; не по
// правилу, не JSON, больше 1 МБ, не из манифеста — отказ без запроса к
// серверу; ошибка запроса (нет связи, отказ сервера) — её статус и код.
func TestSocketRequests(t *testing.T) {
	a := &agent{}
	c, _ := client(t, a)
	status, _, raw := call(t, c, "POST", "/requests", token, `{"type":"report.recipients","data":{"report":"daily"},"timeoutMs":5000}`)
	var reply message.RequestReply
	if status != 200 || json.Unmarshal(raw, &reply) != nil || string(reply.Data) != `{"worker":"report","echo":{"report":"daily"}}` {
		t.Fatalf("запрос: %d %s", status, raw)
	}
	if len(a.asked) != 1 || a.asked[0].TimeoutMs != 5000 {
		t.Fatalf("запрос к серверу: %+v", a.asked)
	}
	for body, want := range map[string]struct {
		status int
		code   string
	}{
		`{"type":"Report"}`:       {400, message.CodeMessageInvalid},
		`не json`:                 {400, message.CodeMessageInvalid},
		`{"type":"report.other"}`: {400, message.CodeRequestUndeclared},
		`{"type":"report.recipients","data":"` + strings.Repeat("x", message.MaxRequestBytes) + `"}`: {413, message.CodeBodyTooLarge},
	} {
		if status, e, _ := call(t, c, "POST", "/requests", token, body); status != want.status || e.Code != want.code {
			t.Errorf("%.40s: %d %+v", body, status, e)
		}
	}
	if len(a.asked) != 1 {
		t.Fatalf("отклонённые запросы не уходят серверу: %+v", a.asked)
	}
	a.reqErr = &requests.Error{Status: 503, Info: message.ErrorInfo{Code: message.CodeAgentOffline, Message: "нет связи"}}
	if status, e, _ := call(t, c, "POST", "/requests", token, `{"type":"report.recipients"}`); status != 503 || e.Code != message.CodeAgentOffline {
		t.Fatalf("без связи: %d %+v", status, e)
	}
	a.reqErr = &requests.Error{Status: 422, Info: message.ErrorInfo{Code: "REQUEST_INVALID", Message: "нет поля report"}}
	if status, e, _ := call(t, c, "POST", "/requests", token, `{"type":"report.recipients"}`); status != 422 || e.Code != "REQUEST_INVALID" {
		t.Fatalf("отказ сервера: %d %+v", status, e)
	}
}
