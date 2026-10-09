package requests

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

// session — соединение для тестов: отправленные запросы — в канал.
type session struct {
	ctx    context.Context
	cancel context.CancelFunc
	sent   chan message.Envelope
}

func newSession() *session {
	ctx, cancel := context.WithCancel(context.Background())
	return &session{ctx: ctx, cancel: cancel, sent: make(chan message.Envelope, 128)}
}

func (s *session) Send(env message.Envelope) error {
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	s.sent <- env
	return nil
}
func (s *session) Context() context.Context { return s.ctx }

func reply(t *testing.T, b *Broker, re string, r message.RequestResult) {
	t.Helper()
	env := message.MustNew(message.TypeRequestResult, r)
	env.Re = re
	if err := b.Result(env); err != nil {
		t.Fatal(err)
	}
}

func code(err error) (int, string) {
	var e *Error
	if errors.As(err, &e) {
		return e.Status, e.Info.Code
	}
	return 0, ""
}

// Нет связи — сразу AGENT_OFFLINE; запрос уходит сообщением request (id,
// worker, type, data, срок); ответ ok — data, отказ — 422 с кодом сервера.
func TestRequestOfflineOKRejected(t *testing.T) {
	var cur Session
	var mu sync.Mutex
	b := New(func() Session { mu.Lock(); defer mu.Unlock(); return cur })
	if st, c := code(func() error {
		_, err := b.Do(context.Background(), "report", message.RequestPost{Type: "a.b"})
		return err
	}()); st != 503 || c != message.CodeAgentOffline {
		t.Fatalf("без связи: %d %s", st, c)
	}
	s := newSession()
	mu.Lock()
	cur = s
	mu.Unlock()

	type res struct {
		data json.RawMessage
		err  error
	}
	done := make(chan res, 1)
	go func() {
		data, err := b.Do(context.Background(), "report", message.RequestPost{Type: "report.recipients", Data: json.RawMessage(`{"report":"daily"}`)})
		done <- res{data, err}
	}()
	env := <-s.sent
	var req message.Request
	if err := env.Decode(&req); err != nil || env.Type != message.TypeRequest || env.ID == "" ||
		req.Worker != "report" || req.Type != "report.recipients" || string(req.Data) != `{"report":"daily"}` ||
		req.TimeoutMs != message.DefaultRequestTimeout.Milliseconds() {
		t.Fatalf("request: %+v %+v", env, req)
	}
	reply(t, b, env.ID, message.RequestResult{OK: true, Data: json.RawMessage(`["ops@example.com"]`)})
	if r := <-done; r.err != nil || string(r.data) != `["ops@example.com"]` {
		t.Fatalf("ответ: %s %v", r.data, r.err)
	}
	if err := b.Result(env); !errors.Is(err, ErrUnexpected) {
		t.Fatalf("повтор ответа: %v", err)
	}

	go func() {
		_, err := b.Do(context.Background(), "report", message.RequestPost{Type: "report.recipients"})
		done <- res{nil, err}
	}()
	env = <-s.sent
	reply(t, b, env.ID, message.RequestResult{Error: &message.ErrorInfo{Code: "REQUEST_INVALID", Message: "нет поля report"}})
	if st, c := code((<-done).err); st != http.StatusUnprocessableEntity || c != "REQUEST_INVALID" {
		t.Fatalf("отказ: %d %s", st, c)
	}
}

// Срок истёк — 504 TIMEOUT (срок не больше 5 мин); связь оборвалась —
// 503 DISCONNECTED; воркер ушёл — ошибка ctx; больше 64 сразу — 503 BUSY.
func TestRequestTimeoutDisconnectBusy(t *testing.T) {
	if Timeout(0) != message.DefaultRequestTimeout || Timeout(int64(time.Hour/time.Millisecond)) != message.MaxRequestTimeout ||
		Timeout(1500) != 1500*time.Millisecond {
		t.Fatal("срок запроса")
	}
	s := newSession()
	b := New(func() Session { return s })
	start := time.Now()
	_, err := b.Do(context.Background(), "w", message.RequestPost{Type: "a", TimeoutMs: 100})
	if st, c := code(err); st != http.StatusGatewayTimeout || c != message.CodeTimeout || time.Since(start) > 2*time.Second {
		t.Fatalf("срок: %d %s", st, c)
	}
	<-s.sent

	ctx, cancel := context.WithCancel(context.Background())
	errs := make(chan error, message.MaxRequests+1)
	for range message.MaxRequests {
		go func() {
			_, err := b.Do(ctx, "w", message.RequestPost{Type: "a"})
			errs <- err
		}()
	}
	for range message.MaxRequests {
		<-s.sent
	}
	if _, err := b.Do(ctx, "w", message.RequestPost{Type: "a"}); func() string { _, c := code(err); return c }() != message.CodeBusy {
		t.Fatalf("сверх предела: %v", err)
	}
	cancel()
	for range message.MaxRequests {
		if err := <-errs; !errors.Is(err, context.Canceled) {
			t.Fatalf("воркер ушёл: %v", err)
		}
	}

	go func() {
		_, err := b.Do(context.Background(), "w", message.RequestPost{Type: "a"})
		errs <- err
	}()
	<-s.sent
	s.cancel()
	if st, c := code(<-errs); st != http.StatusServiceUnavailable || c != message.CodeDisconnected {
		t.Fatalf("разрыв: %d %s", st, c)
	}
}
