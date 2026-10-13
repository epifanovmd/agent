package backend

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/backoff"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/sse"
	"github.com/epifanovmd/agent/internal/store"
	"github.com/epifanovmd/agent/protocol"
)

type fakeAuth struct{ renewed int }

func (a *fakeAuth) Authorization() string       { return "Agent a.s" }
func (a *fakeAuth) Accepted(string)             {}
func (a *fakeAuth) Fallback() bool              { return false }
func (a *fakeAuth) Renew(context.Context) error { a.renewed++; return nil }

type handler struct {
	mu       sync.Mutex
	desired  []int64
	commands []string
	online   []bool
}

func (h *handler) Hello() protocol.Hello       { return protocol.Hello{Name: "n"} }
func (h *handler) OnHello(protocol.HelloReply) {}
func (h *handler) Rev() int64                  { return 0 }
func (h *handler) OnDesired(d protocol.Desired) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.desired = append(h.desired, d.Rev)
}
func (h *handler) OnCommand(c protocol.Command) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.commands = append(h.commands, c.ID)
}
func (h *handler) OnOnline(on bool) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.online = append(h.online, on)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// Связь: hello, поток с желаемым и командами (after — seq последней), очередь событий:
// принятые и отклонённые уходят из очереди, ошибка протокола (400) — пачка не повторяется,
// 503 — повтор; состояние, метрики и журнал доходят; запрос воркера; отзыв — связь прекращена.
func TestClient(t *testing.T) {
	retry = backoff.Policy{Min: 10 * time.Millisecond, Max: 20 * time.Millisecond}
	var mu sync.Mutex
	var afters []string
	calls := map[string]int{}
	var stateBody, metricsBody, logsBody string
	revoked := false
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, _ := io.ReadAll(r.Body)
		mu.Lock()
		path := strings.TrimPrefix(r.URL.Path, protocol.BackendPrefix)
		calls[path]++
		n, rev := calls[path], revoked
		mu.Unlock()
		if r.Header.Get("Authorization") != "Agent a.s" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if rev {
			w.WriteHeader(http.StatusForbidden)
			_ = json.NewEncoder(w).Encode(protocol.NewError(protocol.CodeAgentRevoked, "отозван"))
			return
		}
		switch path {
		case protocol.PathHello:
			_ = json.NewEncoder(w).Encode(protocol.HelloReply{StateMs: 1000})
		case protocol.PathStream:
			mu.Lock()
			afters = append(afters, r.URL.Query().Get("after"))
			mu.Unlock()
			out, _ := sse.NewWriter(w)
			_ = out.Send(sse.Event{Type: protocol.SSEDesired, Data: `{"rev":5}`})
			_ = out.Send(sse.Event{Type: protocol.SSECommand, Data: `{"seq":7,"id":"c7","type":"agent.logs"}`})
			if n == 1 {
				return // поток закрыт бэкендом — агент переподключается
			}
			<-r.Context().Done()
		case protocol.PathEvents:
			var items []protocol.Event
			_ = json.Unmarshal(raw, &items)
			switch {
			case n == 1:
				w.WriteHeader(http.StatusServiceUnavailable)
			case items[0].ID == "bad":
				w.WriteHeader(http.StatusBadRequest)
			default:
				_ = json.NewEncoder(w).Encode(protocol.Accepted{Accepted: []string{"e1"}, Rejected: []protocol.Rejection{{ID: "e2", Reason: "нет"}}})
			}
		case protocol.PathState:
			mu.Lock()
			stateBody = string(raw)
			mu.Unlock()
		case protocol.PathMetrics:
			mu.Lock()
			metricsBody = string(raw)
			mu.Unlock()
		case protocol.PathLogs:
			mu.Lock()
			logsBody = string(raw)
			mu.Unlock()
		case protocol.PathRequests:
			_ = json.NewEncoder(w).Encode(protocol.RequestReply{OK: true, Data: json.RawMessage(`{"ok":1}`)})
		}
	}))
	defer srv.Close()
	st, err := store.Open(t.TempDir(), 0)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	h := &handler{}
	c := New(Options{URL: srv.URL, HTTP: srv.Client(), Auth: &fakeAuth{}, Store: st, Log: logx.Discard()}, h)
	if r := c.Request(context.Background(), protocol.Request{Type: "x"}); r.Error == nil || r.Error.Code != protocol.CodeAgentOffline {
		t.Fatalf("запрос без связи: %+v", r)
	}
	_ = st.AppendEvents([]protocol.Event{{ID: "e1", Type: "t"}, {ID: "e2", Type: "t"}})
	c.SetState(protocol.State{DesiredRev: 5})
	c.AddMetrics(protocol.Metrics{At: 1})
	c.AddLogs([]protocol.LogEntry{{Msg: "m"}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		c.Run(ctx)
		close(done)
	}()

	eventually(t, "команда и seq", func() bool { return st.LastSeq() == 7 })
	eventually(t, "переподключение с after=7", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(afters) >= 2 && afters[0] == "0" && afters[len(afters)-1] == "7"
	})
	eventually(t, "очередь пуста после 503 и ответа", func() bool { return st.Len() == 0 })
	eventually(t, "состояние, метрики, журнал", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return strings.Contains(stateBody, `"desiredRev":5`) && strings.HasPrefix(metricsBody, `[{"at":1`) && strings.Contains(logsBody, `"msg":"m"`)
	})
	_ = st.AppendEvents([]protocol.Event{{ID: "bad", Type: "t"}})
	eventually(t, "пачка с ошибкой протокола не повторяется", func() bool { return st.Len() == 0 })
	if r := c.Request(ctx, protocol.Request{Type: "x"}); !r.OK || string(r.Data) != `{"ok":1}` {
		t.Fatalf("запрос: %+v", r)
	}
	h.mu.Lock()
	if len(h.desired) == 0 || h.desired[0] != 5 || h.commands[0] != "c7" || !h.online[0] {
		t.Fatalf("обработчик: %+v", h)
	}
	h.mu.Unlock()

	mu.Lock()
	revoked = true
	mu.Unlock()
	c.Reconnect()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("отозванный агент не прекратил связь")
	}
	if !c.Revoked() || c.Online() {
		t.Fatalf("отозван: %v, на связи: %v", c.Revoked(), c.Online())
	}
}
