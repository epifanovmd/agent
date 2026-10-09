package link

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
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
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/outbox"
	"github.com/epifanovmd/agent/internal/stream"
)

// fakeServer — сервер для тестов связи: проверяет ключ и канал agent.v2,
// отвечает welcome на hello, подтверждает поток и важные (если не noAck),
// записывает всё полученное.
type fakeServer struct {
	t       *testing.T
	srv     *httptest.Server
	auth    atomic.Value
	mu      sync.Mutex
	got     []message.Envelope
	conns   atomic.Int32
	noAck   atomic.Bool
	closeWS chan int
	// reject — ответ error на важные сообщения (nil — ack).
	reject atomic.Pointer[message.Error]
	// deaf — после welcome не читать (на ping нет pong) в первом соединении.
	deaf atomic.Bool
	// send — сообщения сервера в текущее соединение.
	send chan message.Envelope
}

func newFakeServer(t *testing.T) *fakeServer {
	f := &fakeServer{t: t, closeWS: make(chan int, 1), send: make(chan message.Envelope, 16)}
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
	if r.Header.Get("Sec-WebSocket-Protocol") != message.Subprotocol {
		w.WriteHeader(http.StatusUpgradeRequired)
		return
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{message.Subprotocol}})
	if err != nil {
		return
	}
	n := f.conns.Add(1)
	ctx, cancel := context.WithCancel(r.Context())
	defer cancel()
	write := func(env message.Envelope) {
		raw, _ := json.Marshal(env)
		_ = c.Write(ctx, websocket.MessageText, raw)
	}
	go func() {
		for {
			select {
			case code := <-f.closeWS:
				_ = c.Close(websocket.StatusCode(code), "test")
				return
			case env := <-f.send:
				write(env)
			case <-ctx.Done():
				return
			}
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
			write(message.MustNew(message.TypeWelcome, message.Welcome{ServerTime: 1, MetricsIntervalMs: 60000, StatusIntervalMs: 60000}))
			if n == 1 && f.deaf.Load() {
				// Не читаем: ping агента остаётся без pong.
				select {
				case <-ctx.Done():
				case <-time.After(3 * time.Second):
				}
				return
			}
		case f.noAck.Load():
		case env.Seq > 0:
			write(message.MustNew(message.TypeAck, message.Ack{Seq: env.Seq}))
		case env.ID != "" && f.reject.Load() != nil:
			e := message.MustNew(message.TypeError, f.reject.Load())
			e.Re = env.ID
			write(e)
		case env.ID != "":
			write(message.MustNew(message.TypeAck, message.Ack{IDs: []string{env.ID}}))
		}
	}
}

func (f *fakeServer) all() []message.Envelope {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]message.Envelope(nil), f.got...)
}

func (f *fakeServer) received(typ string) []message.Envelope {
	var out []message.Envelope
	for _, env := range f.all() {
		if env.Type == typ {
			out = append(out, env)
		}
	}
	return out
}

type testHandler struct {
	welcomes atomic.Int32
	mu       sync.Mutex
	msgs     []message.Envelope
	// onWelcome — что сделать в OnWelcome (например, отправить status).
	onWelcome func(*Session)
	onMessage func(*Session, message.Envelope)
}

func (h *testHandler) Hello() message.Hello {
	return message.Hello{Agent: message.HelloAgent{Version: "1", BootID: "b"}}
}
func (h *testHandler) OnWelcome(s *Session, _ message.Welcome) {
	h.welcomes.Add(1)
	if h.onWelcome != nil {
		h.onWelcome(s)
	}
}
func (h *testHandler) OnMessage(s *Session, env message.Envelope) {
	h.mu.Lock()
	h.msgs = append(h.msgs, env)
	h.mu.Unlock()
	if h.onMessage != nil {
		h.onMessage(s, env)
	}
}
func (h *testHandler) OnDisconnect(error) {}

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

func newLink(t *testing.T, f *fakeServer, auth *testAuth, h *testHandler) (*Link, *outbox.Outbox, *stream.Buffer) {
	ob, err := outbox.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if auth == nil {
		auth = &testAuth{}
		auth.value.Store("Agent a.s")
	}
	buf := stream.New(message.StreamBacklog)
	l := New(Options{
		ServerURLs: []string{f.srv.URL},
		Auth:       auth,
		Outbox:     ob,
		Stream:     buf,
		Log:        logx.Discard(),
		Backoff:    backoff.Policy{Min: 10 * time.Millisecond, Max: 50 * time.Millisecond},
	}, h)
	return l, ob, buf
}

func run(t *testing.T, l *Link) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go func() { _ = l.Run(ctx) }()
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

func important(typ string) message.Envelope { return message.MustNew(typ, struct{}{}) }

// hello — первое сообщение; сообщения сервера приходят агенту с соединением,
// ответ через Session уходит в него; поток и важные подтверждаются.
func TestHandshakeAckAndSession(t *testing.T) {
	f := newFakeServer(t)
	h := &testHandler{onMessage: func(s *Session, env message.Envelope) {
		reply := message.MustNew(message.TypeFetchEnd, message.FetchEnd{})
		reply.Re = env.ID
		_ = s.Send(reply)
	}}
	l, ob, buf := newLink(t, f, nil, h)
	run(t, l)
	eventually(t, "welcome", func() bool { return h.welcomes.Load() == 1 && l.Connected() })
	if got := f.all(); got[0].Type != message.TypeHello {
		t.Fatalf("первое сообщение: %s", got[0].Type)
	}
	l.Stream(message.TypeStatus, message.Status{})
	if err := l.Important(important(message.TypeEvent)); err != nil {
		t.Fatal(err)
	}
	eventually(t, "ack потока и outbox", func() bool { return len(buf.Unacked()) == 0 && ob.Len() == 0 })

	req := message.MustNew(message.TypeFetch, message.Fetch{Worker: "echo", Method: "GET", Path: "/"})
	req.ID = "f1"
	f.send <- req
	eventually(t, "ответ в соединение", func() bool {
		ends := f.received(message.TypeFetchEnd)
		return len(ends) == 1 && ends[0].Re == "f1"
	})
	if ev := f.received(message.TypeEvent); len(ev) != 1 || ev[0].ID == "" {
		t.Fatalf("важное — с id: %+v", ev)
	}
}

// После welcome (§4): сначала outbox по порядку, затем поток с первого
// неподтверждённого seq, затем то, что агент шлёт в OnWelcome (status).
func TestOrderAfterWelcome(t *testing.T) {
	f := newFakeServer(t)
	f.noAck.Store(true)
	var l *Link
	h := &testHandler{onWelcome: func(*Session) { l.Stream(message.TypeStatus, message.Status{Outbox: 9}) }}
	l, _, _ = newLink(t, f, nil, h)
	// Без связи: два важных и два сообщения потока.
	l.Stream(message.TypeMetrics, message.Metrics{CollectedAt: 1})
	_ = l.Important(important(message.TypeConfigApplied))
	l.Stream(message.TypeLog, message.Log{})
	_ = l.Important(important(message.TypeActionResult))
	run(t, l)
	eventually(t, "всё отправлено", func() bool { return len(f.all()) >= 6 })
	var types []string
	for _, env := range f.all()[:6] {
		types = append(types, env.Type)
	}
	want := []string{message.TypeHello, message.TypeConfigApplied, message.TypeActionResult, message.TypeMetrics, message.TypeLog, message.TypeStatus}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("порядок: %v, нужно %v", types, want)
		}
	}
	got := f.all()
	if got[3].Seq != 1 || got[4].Seq != 2 || got[5].Seq != 3 {
		t.Fatalf("seq: %d %d %d", got[3].Seq, got[4].Seq, got[5].Seq)
	}
}

// Разрыв: неподтверждённые важные и поток досылаются в новом соединении с
// теми же id и seq.
func TestResendAfterReconnect(t *testing.T) {
	f := newFakeServer(t)
	f.noAck.Store(true)
	h := &testHandler{}
	l, ob, _ := newLink(t, f, nil, h)
	run(t, l)
	eventually(t, "welcome", func() bool { return h.welcomes.Load() == 1 })

	l.Stream(message.TypeStatus, message.Status{})
	_ = l.Important(important(message.TypeEvent))
	eventually(t, "отправлено", func() bool { return len(f.received(message.TypeEvent)) == 1 })

	f.noAck.Store(false)
	f.closeWS <- message.CloseRestart
	eventually(t, "переподключение", func() bool { return h.welcomes.Load() == 2 })
	eventually(t, "досылка", func() bool {
		return len(f.received(message.TypeEvent)) == 2 && len(f.received(message.TypeStatus)) == 2 && ob.Len() == 0
	})
	statuses, events := f.received(message.TypeStatus), f.received(message.TypeEvent)
	if statuses[0].Seq != statuses[1].Seq || events[0].ID != events[1].ID {
		t.Fatalf("досылка с тем же seq и id: %+v %+v", statuses, events)
	}
}

// error по важному (§3): retryable: false — удалить из outbox, true —
// повторить позже.
func TestImportantRejected(t *testing.T) {
	defer func(d time.Duration) { retryImportant = d }(retryImportant)
	retryImportant = 150 * time.Millisecond
	f := newFakeServer(t)
	f.reject.Store(&message.Error{Code: "X", Message: "занято", Retryable: true})
	h := &testHandler{}
	l, ob, _ := newLink(t, f, nil, h)
	run(t, l)
	eventually(t, "welcome", func() bool { return h.welcomes.Load() == 1 })
	_ = l.Important(important(message.TypeEvent))
	eventually(t, "повтор после retryable", func() bool { return len(f.received(message.TypeEvent)) >= 2 })
	if ob.Len() != 1 {
		t.Fatalf("retryable — остаётся в outbox: %d", ob.Len())
	}
	f.reject.Store(&message.Error{Code: "Y", Message: "нельзя", Retryable: false})
	eventually(t, "удалено после retryable: false", func() bool { return ob.Len() == 0 })
}

// Нет pong на ping — соединение закрывается и открывается заново.
func TestPingWithoutPong(t *testing.T) {
	defer func(p, w time.Duration) { pingInterval, pongTimeout = p, w }(pingInterval, pongTimeout)
	pingInterval, pongTimeout = 50*time.Millisecond, 50*time.Millisecond
	f := newFakeServer(t)
	f.deaf.Store(true)
	h := &testHandler{}
	l, _, _ := newLink(t, f, nil, h)
	run(t, l)
	eventually(t, "второе соединение", func() bool { return h.welcomes.Load() == 2 && f.conns.Load() == 2 })
}

// Пауза по коду закрытия (§2): 1012 — до 1 с, 4409 — не меньше 30 с,
// 4400 — растущая пауза, 426 — наибольшая.
func TestCloseCodeDelays(t *testing.T) {
	l := New(Options{ServerURLs: []string{"http://a"}, Auth: &testAuth{}, Log: logx.Discard(),
		Backoff: backoff.Policy{Min: 10 * time.Millisecond, Max: time.Second}}, &testHandler{})
	ctx := context.Background()
	if d := l.delay(ctx, &CloseError{Code: message.CloseRestart}, 5); d >= time.Second {
		t.Fatalf("1012: %v", d)
	}
	if d := l.delay(ctx, &CloseError{Code: message.CloseReplaced}, 0); d < replacedPause {
		t.Fatalf("4409: %v", d)
	}
	if d := l.delay(ctx, &CloseError{Code: message.CloseInvalid}, 3); d < 40*time.Millisecond || d > 80*time.Millisecond {
		t.Fatalf("4400: %v", d)
	}
	if d := l.delay(ctx, &DialError{Status: http.StatusUpgradeRequired}, 0); d != time.Second {
		t.Fatalf("426: %v", d)
	}
	if d := l.delay(ctx, &CloseError{Code: message.CloseOverloaded, Reason: "12"}, 0); d != 12*time.Second {
		t.Fatalf("4429: %v", d)
	}
}

// Без канала agent.v2 сервер отвечает 426.
func TestSubprotocolRequired(t *testing.T) {
	f := newFakeServer(t)
	c, resp, err := websocket.Dial(context.Background(), "ws"+f.srv.URL[len("http"):]+message.LinkPath,
		&websocket.DialOptions{HTTPHeader: http.Header{"Authorization": {"Agent a.s"}}})
	if err == nil {
		c.CloseNow()
		t.Fatal("без канала соединение не открывается")
	}
	if resp == nil || resp.StatusCode != http.StatusUpgradeRequired {
		t.Fatalf("ответ: %v", resp)
	}
}

func TestUnauthorizedRenews(t *testing.T) {
	f := newFakeServer(t)
	f.auth.Store("Agent new.s")
	auth := &testAuth{next: "Agent new.s"}
	auth.value.Store("Agent old.s")
	h := &testHandler{}
	l, _, _ := newLink(t, f, auth, h)
	run(t, l)
	eventually(t, "соединение с новым ключом", func() bool { return h.welcomes.Load() == 1 })
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
		ServerURLs: []string{f.srv.URL}, Auth: auth, Outbox: ob, Stream: stream.New(100),
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

// Отклонены оба секрета — регистрация заново.
func TestBothSecretsRejectedRenews(t *testing.T) {
	f := newFakeServer(t)
	f.auth.Store("Agent other.x")
	auth, _ := newKeysAuth(t, identity.Credentials{AgentID: "a", Secret: "s", PendingSecret: "new"})
	runKeysLink(t, f, auth)
	eventually(t, "регистрация заново", func() bool { return auth.renewed.Load() >= 1 })
}

// Несколько адресов: первый недоступен — связь через второй; закрытие кодом
// и отказ 4xx адрес не меняют.
func TestServerURLsRoundRobin(t *testing.T) {
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close() // порт закрыт: соединение отклоняется
	f := newFakeServer(t)
	ob, err := outbox.Open(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	auth := &testAuth{}
	auth.value.Store("Agent a.s")
	h := &testHandler{}
	l := New(Options{
		ServerURLs: []string{deadURL, f.srv.URL, deadURL}, Auth: auth,
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
	for _, c := range []struct {
		err  error
		want bool
	}{
		{errors.New("connection refused"), true},
		{&DialError{Status: 502}, true},
		{&DialError{Status: 401}, false},
		{&CloseError{Code: message.CloseRestart}, false},
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

// Испорченные данные потокового сообщения не отправляются и уходят из буфера:
// досылка не обрывается на них после каждого переподключения.
func TestSendableDropsCorrupted(t *testing.T) {
	buf := stream.New(10)
	l := &Link{opts: Options{Stream: buf, Log: slog.New(slog.DiscardHandler)}}
	good := buf.Add(message.Envelope{Type: message.TypeMetrics, Data: json.RawMessage(`{"a":1}`)})
	bad := buf.Add(message.Envelope{Type: message.TypeMetrics, Data: json.RawMessage("{\x00}")})
	if !l.sendable(good) || l.sendable(bad) {
		t.Fatal("sendable: целое — да, испорченное — нет")
	}
	if left := buf.Unacked(); len(left) != 1 || left[0].Seq != good.Seq {
		t.Fatalf("буфер: %+v", left)
	}
}
