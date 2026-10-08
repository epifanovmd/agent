package link

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/epifanovmd/agent/internal/backoff"
	"github.com/epifanovmd/agent/internal/identity"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/outbox"
	"github.com/epifanovmd/agent/internal/stream"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// fakeServer — минимальный сервер: принимает WebSocket, отвечает welcome,
// подтверждает потоковые и надёжные сообщения, отвечает на job.urls.
type fakeServer struct {
	t       *testing.T
	srv     *httptest.Server
	auth    atomic.Value
	mu      sync.Mutex
	got     []message.Envelope
	conns   atomic.Int32
	noAck   atomic.Bool
	closeWS chan int
}

func newFakeServer(t *testing.T) *fakeServer {
	f := &fakeServer{t: t, closeWS: make(chan int, 1)}
	f.auth.Store("Agent a.s")
	f.srv = httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(f.srv.Close)
	return f
}

func (f *fakeServer) serve(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != f.auth.Load().(string) {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{message.WSChannel}})
	if err != nil {
		return
	}
	f.conns.Add(1)
	ctx := r.Context()
	send := func(env message.Envelope) {
		raw, _ := json.Marshal(env)
		_ = c.Write(ctx, websocket.MessageText, raw)
	}
	go func() {
		select {
		case code := <-f.closeWS:
			_ = c.Close(websocket.StatusCode(code), "test")
		case <-ctx.Done():
		}
	}()
	for {
		_, raw, err := c.Read(ctx)
		if err != nil {
			return
		}
		var env message.Envelope
		_ = json.Unmarshal(raw, &env)
		f.mu.Lock()
		f.got = append(f.got, env)
		f.mu.Unlock()
		switch {
		case env.Type == message.TypeHello:
			send(message.MustNew(message.TypeWelcome, message.Welcome{Version: 1, AgentID: "a", SessionID: "s"}))
		case env.Type == message.TypeJobURLs:
			reply := message.MustNew(message.TypeJobURLs, message.JobURLs{ExpiresAt: 42})
			reply.Re = env.ID
			send(reply)
		case f.noAck.Load():
		case env.Seq > 0:
			send(message.MustNew(message.TypeAck, message.Ack{Seq: env.Seq}))
		case env.ID != "":
			send(message.MustNew(message.TypeAck, message.Ack{IDs: []string{env.ID}}))
		}
	}
}

func (f *fakeServer) received(typ string) []message.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	var out []message.Envelope
	for _, env := range f.got {
		if env.Type == typ {
			out = append(out, env)
		}
	}
	return out
}

type testHandler struct {
	welcomes atomic.Int32
}

func (h *testHandler) Hello() message.Hello {
	return message.Hello{Versions: message.Versions, Agent: message.HelloAgent{Name: "t", Version: "1", BootID: "b"}, Jobs: []message.JobRef{}}
}
func (h *testHandler) OnWelcome(message.Welcome)  { h.welcomes.Add(1) }
func (h *testHandler) OnMessage(message.Envelope) {}
func (h *testHandler) OnDisconnect(error)         {}

type testAuth struct {
	value   atomic.Value
	renewed atomic.Int32
	next    string
}

func (a *testAuth) Authorization() string { return a.value.Load().(string) }
func (a *testAuth) Fallback() bool        { return false }
func (a *testAuth) Accepted(string)       {}
func (a *testAuth) Renew(context.Context) error {
	a.renewed.Add(1)
	a.value.Store(a.next)
	return nil
}

func newLink(t *testing.T, f *fakeServer, auth *testAuth, transport string) (*Link, *testHandler, *outbox.Outbox, *stream.Buffer) {
	ob, err := outbox.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if auth == nil {
		auth = &testAuth{}
		auth.value.Store("Agent a.s")
	}
	buf := stream.New(100)
	h := &testHandler{}
	l := New(Options{
		ServerURL: f.srv.URL,
		Transport: transport,
		Auth:      auth,
		Outbox:    ob,
		Stream:    buf,
		Log:       logx.Discard(),
		Backoff:   backoff.Policy{Min: 10 * time.Millisecond, Max: 50 * time.Millisecond},
	}, h)
	return l, h, ob, buf
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestHandshakeAcksAndRequest(t *testing.T) {
	f := newFakeServer(t)
	l, h, ob, buf := newLink(t, f, nil, "ws")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)

	eventually(t, "welcome", func() bool { return h.welcomes.Load() == 1 && l.Connected() })
	if l.Mode() != "ws" {
		t.Fatalf("mode %q", l.Mode())
	}

	l.Stream(message.TypeStatus, message.Status{State: message.StateIdle})
	if err := l.Reliable(message.TypeJobComplete, message.JobComplete{}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "ack потока и outbox", func() bool { return len(buf.Unacked()) == 0 && ob.Len() == 0 })

	var urls message.JobURLs
	if err := l.Request(ctx, message.TypeJobURLs, message.JobURLsRequest{}, &urls); err != nil || urls.ExpiresAt != 42 {
		t.Fatalf("запрос: %v %+v", err, urls)
	}
}

func TestResendAfterReconnect(t *testing.T) {
	f := newFakeServer(t)
	f.noAck.Store(true)
	l, h, ob, _ := newLink(t, f, nil, "ws")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	eventually(t, "welcome", func() bool { return h.welcomes.Load() == 1 })

	l.Stream(message.TypeStatus, message.Status{State: message.StateBusy})
	_ = l.Reliable(message.TypeJobFail, message.JobFail{Code: "X"})
	eventually(t, "отправлено", func() bool { return len(f.received(message.TypeJobFail)) == 1 })

	f.noAck.Store(false)
	f.closeWS <- message.CloseRestart
	eventually(t, "переподключение", func() bool { return h.welcomes.Load() == 2 })
	eventually(t, "досылка", func() bool {
		return len(f.received(message.TypeJobFail)) == 2 && len(f.received(message.TypeStatus)) == 2 && ob.Len() == 0
	})
	statuses := f.received(message.TypeStatus)
	if statuses[0].Seq != statuses[1].Seq {
		t.Fatalf("досылка с тем же seq: %d != %d", statuses[0].Seq, statuses[1].Seq)
	}
}

func TestUnauthorizedRenews(t *testing.T) {
	f := newFakeServer(t)
	f.auth.Store("Agent new.s")
	auth := &testAuth{next: "Agent new.s"}
	auth.value.Store("Agent old.s")
	l, h, _, _ := newLink(t, f, auth, "ws")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	eventually(t, "сессия с новыми учётными данными", func() bool { return h.welcomes.Load() == 1 })
	if auth.renewed.Load() != 1 {
		t.Fatalf("renew: %d", auth.renewed.Load())
	}
}

// keysAuth — Auth поверх identity.Keys (как в агенте).
type keysAuth struct {
	*identity.Keys
	renewed atomic.Int32
}

func (a *keysAuth) Accepted(authz string) { _ = a.Keys.Accepted(authz) }
func (a *keysAuth) Renew(context.Context) error {
	a.renewed.Add(1)
	return errors.New("нет токена")
}

func newKeysAuth(t *testing.T, creds identity.Credentials) (*keysAuth, *identity.Store) {
	store := identity.NewStore(t.TempDir())
	if err := store.Save(creds); err != nil {
		t.Fatal(err)
	}
	return &keysAuth{Keys: identity.NewKeys(store, creds)}, store
}

func runKeysLink(t *testing.T, f *fakeServer, auth *keysAuth) *testHandler {
	ob, err := outbox.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	h := &testHandler{}
	l := New(Options{
		ServerURL: f.srv.URL, Transport: "ws", Auth: auth, Outbox: ob, Stream: stream.New(100),
		Log:     logx.Discard(),
		Backoff: backoff.Policy{Min: 10 * time.Millisecond, Max: 50 * time.Millisecond},
	}, h)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go l.Run(ctx)
	return h
}

// Ожидающий секрет принят — он становится основным, прежний удалён из файла.
func TestPendingSecretPromoted(t *testing.T) {
	f := newFakeServer(t)
	f.auth.Store("Agent a.new")
	auth, store := newKeysAuth(t, identity.Credentials{AgentID: "a", Secret: "s", PendingSecret: "new"})
	h := runKeysLink(t, f, auth)
	eventually(t, "сессия с новым секретом", func() bool { return h.welcomes.Load() == 1 })
	got, _, _ := store.Load()
	if got != (identity.Credentials{AgentID: "a", Secret: "new"}) {
		t.Fatalf("файл: %+v", got)
	}
	if auth.renewed.Load() != 0 {
		t.Fatal("повторной регистрации быть не должно")
	}
}

// Ожидающий отклонён (401) — подключение с прежним, без повторной регистрации.
func TestPendingSecretRejectedFallsBack(t *testing.T) {
	f := newFakeServer(t)
	creds := identity.Credentials{AgentID: "a", Secret: "s", PendingSecret: "new"}
	auth, store := newKeysAuth(t, creds)
	h := runKeysLink(t, f, auth)
	eventually(t, "сессия с прежним секретом", func() bool { return h.welcomes.Load() == 1 })
	if auth.renewed.Load() != 0 {
		t.Fatal("повторной регистрации быть не должно")
	}
	if got, _, _ := store.Load(); got != creds {
		t.Fatalf("ожидающий должен остаться: %+v", got)
	}
	if auth.Session() != "Agent a.s" {
		t.Fatalf("заголовок сессии: %s", auth.Session())
	}
}

// Отклонены оба секрета — повторная регистрация.
func TestBothSecretsRejectedRenews(t *testing.T) {
	f := newFakeServer(t)
	f.auth.Store("Agent other.x")
	auth, _ := newKeysAuth(t, identity.Credentials{AgentID: "a", Secret: "s", PendingSecret: "new"})
	runKeysLink(t, f, auth)
	eventually(t, "повторная регистрация", func() bool { return auth.renewed.Load() >= 1 })
}

func TestFallbackStatus(t *testing.T) {
	for status, want := range map[int]bool{404: true, 400: true, 426: true, 401: false, 403: false, 429: false, 502: false, 101: false} {
		if got := fallbackStatus(status); got != want {
			t.Errorf("%d: %v", status, got)
		}
	}
}

// fakeSyncServer — HTTP sync: hello → welcome, ack всего присланного.
func TestHTTPSync(t *testing.T) {
	var mu sync.Mutex
	var got []message.Envelope
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req syncRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		mu.Lock()
		got = append(got, req.Messages...)
		mu.Unlock()
		var out []message.Envelope
		var ack message.Ack
		for _, env := range req.Messages {
			switch {
			case env.Type == message.TypeHello:
				out = append(out, message.MustNew(message.TypeWelcome, message.Welcome{Version: 1, SessionID: "s"}))
			case env.Seq > 0:
				ack.Seq = env.Seq
			case env.ID != "":
				ack.IDs = append(ack.IDs, env.ID)
			}
		}
		if ack.Seq > 0 || len(ack.IDs) > 0 {
			out = append(out, message.MustNew(message.TypeAck, ack))
		}
		if len(req.Messages) == 0 && req.WaitSeconds > 0 {
			select {
			case <-r.Context().Done():
				return
			case <-time.After(200 * time.Millisecond):
			}
		}
		_ = json.NewEncoder(w).Encode(syncResponse{SessionID: "s", Messages: out})
	}))
	defer srv.Close()

	f := &fakeServer{srv: srv}
	l, h, ob, buf := newLink(t, f, nil, "http")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	eventually(t, "welcome по HTTP", func() bool { return h.welcomes.Load() == 1 && l.Mode() == "http" })

	l.Stream(message.TypeStatus, message.Status{State: message.StateIdle})
	_ = l.Reliable(message.TypeCmdDone, message.CommandDone{OK: true})
	eventually(t, "ack по HTTP", func() bool { return len(buf.Unacked()) == 0 && ob.Len() == 0 })
}

// HTTP sync не отменяет запросы в полёте: доставка, забранная сервером для
// ожидающего long-poll, не теряется, когда у агента появляется исходящее.
func TestHTTPSyncNeverCancelsInFlight(t *testing.T) {
	var cancelled atomic.Int32
	var mu sync.Mutex
	var pending []message.Envelope
	wake := make(chan struct{}, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req syncRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		var out []message.Envelope
		for _, env := range req.Messages {
			switch {
			case env.Type == message.TypeHello:
				out = append(out, message.MustNew(message.TypeWelcome, message.Welcome{Version: 1, SessionID: "s"}))
			case env.ID != "":
				out = append(out, message.MustNew(message.TypeAck, message.Ack{IDs: []string{env.ID}}))
				// Сообщение агента порождает доставку — её заберёт ожидающий long-poll.
				mu.Lock()
				pending = append(pending, message.MustNew(message.TypeJobStop, message.JobRef{JobID: env.ID}))
				mu.Unlock()
				select {
				case wake <- struct{}{}:
				default:
				}
			}
		}
		if len(req.Messages) == 0 && req.WaitSeconds > 0 {
			select {
			case <-wake:
			case <-time.After(2 * time.Second):
			case <-r.Context().Done():
				cancelled.Add(1)
				return
			}
			mu.Lock()
			out, pending = append(out, pending...), nil
			mu.Unlock()
		}
		_ = json.NewEncoder(w).Encode(syncResponse{SessionID: "s", Messages: out})
	}))
	defer srv.Close()

	got := make(chan message.Envelope, 10)
	f := &fakeServer{srv: srv}
	l, h, _, _ := newLink(t, f, nil, "http")
	l.handler = &recordingHandler{testHandler: h, got: got}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go l.Run(ctx)
	eventually(t, "welcome", func() bool { return h.welcomes.Load() == 1 })
	time.Sleep(100 * time.Millisecond) // long-poll уже ждёт

	for range 3 {
		_ = l.Reliable(message.TypeJobComplete, message.JobComplete{})
		select {
		case env := <-got:
			if env.Type != message.TypeJobStop {
				t.Fatalf("неожиданное %s", env.Type)
			}
		case <-time.After(3 * time.Second):
			t.Fatal("доставка, забранная long-poll, потеряна")
		}
	}
	if n := cancelled.Load(); n != 0 {
		t.Fatalf("отменено запросов в полёте: %d", n)
	}
}

type recordingHandler struct {
	*testHandler
	got chan message.Envelope
}

func (h *recordingHandler) OnMessage(env message.Envelope) { h.got <- env }

// Несколько адресов: первый недоступен — связь через второй; закрытие кодом
// и отказ 4xx адрес не меняют.
func TestServerURLsRoundRobin(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // порт закрыт: соединение отклоняется
	f := newFakeServer(t)
	for _, transport := range []string{"ws"} { // HTTP sync — в test/integration
		t.Run(transport, func(t *testing.T) {
			ob, err := outbox.Open(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			auth := &testAuth{}
			auth.value.Store("Agent a.s")
			h := &testHandler{}
			l := New(Options{
				ServerURL: deadURL, ServerURLs: []string{f.srv.URL, deadURL}, Transport: transport, Auth: auth,
				Outbox: ob, Stream: stream.New(100), Log: logx.Discard(),
				Backoff: backoff.Policy{Min: 10 * time.Millisecond, Max: 50 * time.Millisecond},
			}, h)
			if len(l.urls) != 2 || l.ServerURL() != deadURL {
				t.Fatalf("адреса: %v", l.urls)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			go l.Run(ctx)
			eventually(t, "сессия через второй адрес", func() bool { return l.Connected() })
			if l.ServerURL() != f.srv.URL {
				t.Fatalf("адрес: %s", l.ServerURL())
			}
		})
	}
	for _, c := range []struct {
		err  error
		want bool
	}{
		{errors.New("connection refused"), true},
		{&DialError{Status: 502}, true},
		{&DialError{Status: 401}, false},
		{&CloseError{Code: message.CloseRestart}, false},
		{errRenew, false},
	} {
		if got := switchable(c.err); got != c.want {
			t.Errorf("switchable(%v) = %v", c.err, got)
		}
	}
}

// Перегрузка сервера: 4429 с секундами в reason, HTTP 429/503 с Retry-After —
// пауза не меньше указанной и не больше maxRetryAfter.
func TestRetryAfter(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "7")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer srv.Close()
	_, err := dialWS(context.Background(), srv.Client(), srv.URL, "Agent a.s")
	var de *DialError
	if !errors.As(err, &de) || de.Status != http.StatusTooManyRequests || de.RetryAfter != 7*time.Second {
		t.Fatalf("отказ: %#v", err)
	}
	cases := []struct {
		err  error
		want time.Duration
	}{
		{err, 7 * time.Second},
		{&CloseError{Code: message.CloseOverloaded, Reason: "30"}, 30 * time.Second},
		{&CloseError{Code: message.CloseOverloaded, Reason: "100000"}, maxRetryAfter},
		{&CloseError{Code: message.CloseOverloaded, Reason: "скоро"}, 0},
		{&DialError{Status: http.StatusServiceUnavailable, RetryAfter: 2 * time.Second}, 2 * time.Second},
		{&DialError{Status: http.StatusBadGateway, RetryAfter: 2 * time.Second}, 0},
		{&CloseError{Code: message.CloseRestart, Reason: "5"}, 0},
	}
	for _, c := range cases {
		if got := retryAfter(c.err); got != c.want {
			t.Errorf("retryAfter(%v) = %v, нужно %v", c.err, got, c.want)
		}
	}
	if d := parseRetryAfter(time.Now().Add(time.Minute).UTC().Format(http.TimeFormat)); d < 58*time.Second || d > time.Minute {
		t.Fatalf("дата HTTP: %v", d)
	}
}
