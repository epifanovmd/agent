//go:build unix

package app

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/sysmetrics"
)

// TestMain — этот же тестовый файл служит воркером (TEST_WORKER=1) и
// встроенным воркером sysmetrics (`<файл> sysmetrics`).
func TestMain(m *testing.M) {
	if ok, err := sysmetrics.Dispatch(os.Args[1:]); ok {
		if err != nil {
			os.Exit(1)
		}
		return
	}
	if os.Getenv("TEST_WORKER") != "" {
		echoWorker()
		return
	}
	os.Exit(m.Run())
}

// echoWorker — воркер без SDK: настройки, метрики, health, /echo, /emit
// (событие через сокет агента), /ctx (GET /context агента).
func echoWorker() {
	agent := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", os.Getenv(message.EnvSocket))
	}}}
	toAgent := func(method, path, body string) (*http.Response, error) {
		req, _ := http.NewRequest(method, "http://agent"+path, strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+os.Getenv(message.EnvWorkerToken))
		return agent.Do(req)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /config/{key}", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			Data struct {
				Reject string `json:"reject"`
			} `json:"data"`
		}
		_ = json.NewDecoder(r.Body).Decode(&v)
		if v.Data.Reject != "" {
			w.WriteHeader(http.StatusUnprocessableEntity)
			fmt.Fprintf(w, `{"message":%q}`, v.Data.Reject)
			return
		}
		fmt.Println("настройки применены:", r.PathValue("key"))
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"sent":1}`) })
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"ok":true,"info":{"port":1}}`) })
	mux.HandleFunc("GET /manifest", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprint(w, `{"version":"1.0.0","configs":[{"key":"main"}],"events":[{"type":"example.done"}],`+
			`"routes":[{"method":"POST","path":"/echo"},{"method":"POST","path":"/emit"},{"method":"GET","path":"/ctx"},`+
			`{"method":"POST","path":"/ask"}],"requests":[{"type":"example.ask"}]}`)
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "text/plain")
		_, _ = w.Write(bytes.ToUpper(body))
	})
	mux.HandleFunc("POST /emit", func(w http.ResponseWriter, r *http.Request) {
		typ := cmp.Or(r.URL.Query().Get("type"), "example.done")
		resp, err := toAgent("POST", message.EventsPath, `{"type":"`+typ+`","data":{"n":1}}`)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
	})
	// /ask?type=… — запрос к серверу через сокет агента: статус и тело ответа агента.
	mux.HandleFunc("POST /ask", func(w http.ResponseWriter, r *http.Request) {
		typ := cmp.Or(r.URL.Query().Get("type"), "example.ask")
		resp, err := toAgent("POST", message.RequestsPath, `{"type":"`+typ+`","data":{"q":1}}`)
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	})
	mux.HandleFunc("GET /ctx", func(w http.ResponseWriter, _ *http.Request) {
		resp, err := toAgent("GET", message.ContextPath, "")
		if err != nil {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		_, _ = io.Copy(w, resp.Body)
	})
	ln, err := net.Listen("unix", os.Getenv(message.EnvWorkerSocket))
	if err != nil {
		os.Exit(2)
	}
	_ = http.Serve(ln, mux)
}

// server — сервер для теста агента: регистрация, WebSocket с подтверждением
// важных и потока, запись всего полученного.
type server struct {
	t      *testing.T
	srv    *httptest.Server
	mu     sync.Mutex
	got    []message.Envelope
	auths  []string
	secret string
	conn   *websocket.Conn
	hellos chan message.Hello
}

func newServer(t *testing.T) *server {
	s := &server{t: t, secret: "s1", hellos: make(chan message.Hello, 8)}
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+message.EnrollPath, func(w http.ResponseWriter, r *http.Request) {
		var e message.Enroll
		if json.NewDecoder(r.Body).Decode(&e) != nil || e.Token != "good" || e.Host.OS == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_ = json.NewEncoder(w).Encode(message.EnrollResult{AgentID: "a1", Secret: "s1"})
	})
	mux.HandleFunc(message.LinkPath, s.ws)
	s.srv = httptest.NewServer(mux)
	t.Cleanup(s.srv.Close)
	return s
}

func (s *server) ws(w http.ResponseWriter, r *http.Request) {
	auth := r.Header.Get("Authorization")
	s.mu.Lock()
	s.auths = append(s.auths, auth)
	ok := auth == "Agent a1."+s.secret
	s.mu.Unlock()
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{message.Subprotocol}})
	if err != nil {
		return
	}
	c.SetReadLimit(message.MaxMessageBytes)
	ctx := r.Context()
	for {
		_, raw, err := c.Read(ctx)
		if err != nil {
			return
		}
		var env message.Envelope
		_ = json.Unmarshal(raw, &env)
		s.mu.Lock()
		s.got = append(s.got, env)
		s.mu.Unlock()
		switch {
		case env.Type == message.TypeHello:
			var h message.Hello
			_ = env.Decode(&h)
			s.hellos <- h
			s.mu.Lock()
			s.conn = c
			s.mu.Unlock()
			s.send(message.MustNew(message.TypeWelcome, message.Welcome{ServerTime: 1, MetricsIntervalMs: 300, StatusIntervalMs: 60000}))
		case env.Type == message.TypeRequest:
			// Запрос воркера: ответ — его data и worker.
			var q message.Request
			_ = env.Decode(&q)
			res := message.MustNew(message.TypeRequestResult, message.RequestResult{OK: true,
				Data: json.RawMessage(fmt.Sprintf(`{"worker":%q,"echo":%s}`, q.Worker, q.Data))})
			res.Re = env.ID
			s.send(res)
		case env.Seq > 0:
			s.send(message.MustNew(message.TypeAck, message.Ack{Seq: env.Seq}))
		case env.ID != "":
			s.send(message.MustNew(message.TypeAck, message.Ack{IDs: []string{env.ID}}))
		}
	}
}

func (s *server) send(env message.Envelope) {
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	raw, _ := json.Marshal(env)
	_ = c.Write(context.Background(), websocket.MessageText, raw)
}

func (s *server) closeConn(code int) {
	s.mu.Lock()
	c := s.conn
	s.mu.Unlock()
	_ = c.Close(websocket.StatusCode(code), "")
}

// wait — первое полученное сообщение после from, для которого match — да.
func (s *server) wait(what string, match func(message.Envelope) bool) message.Envelope {
	s.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		s.mu.Lock()
		for _, env := range s.got {
			if match(env) {
				s.mu.Unlock()
				return env
			}
		}
		s.mu.Unlock()
		if time.Now().After(deadline) {
			s.t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func (s *server) action(id, name string, args any) message.ActionResult {
	s.t.Helper()
	data := message.Action{Name: name}
	if args != nil {
		data.Args, _ = json.Marshal(args)
	}
	env := message.MustNew(message.TypeAction, data)
	env.ID = id
	s.send(env)
	got := s.wait("action.result "+id, func(e message.Envelope) bool { return e.Type == message.TypeActionResult && e.Re == id })
	var res message.ActionResult
	_ = got.Decode(&res)
	return res
}

func (s *server) fetch(id string, f message.Fetch) []message.Envelope {
	s.t.Helper()
	env := message.MustNew(message.TypeFetch, f)
	env.ID = id
	s.send(env)
	s.wait("fetch.end "+id, func(e message.Envelope) bool { return e.Type == message.TypeFetchEnd && e.Re == id })
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []message.Envelope
	for _, e := range s.got {
		if e.Re == id {
			out = append(out, e)
		}
	}
	return out
}

func decode[T any](env message.Envelope) T {
	var v T
	_ = env.Decode(&v)
	return v
}

// Агент целиком против сервера на Go: регистрация, hello, welcome → status,
// настройки, запрос к воркеру, события и контекст через сокет агента,
// метрики (host и воркеры), watch, встроенные действия, смена ключа.
func TestAgent(t *testing.T) {
	s := newServer(t)
	dir, _ := os.MkdirTemp("", "a")
	defer os.RemoveAll(dir)
	cfg := config.Defaults()
	cfg.Server.URL = s.srv.URL
	cfg.DataDir = dir
	cfg.Name = "node-01"
	cfg.Labels = map[string]string{"zone": "eu"}
	cfg.Enroll.Token = "good"
	cfg.Log.Level = "error"
	cfg.Telemetry.Metrics = []string{message.MetricsMemory, message.MetricsUptime}
	cfg.Workers = []config.Worker{{Name: "echo", Command: []string{os.Args[0]}, Env: map[string]string{"TEST_WORKER": "1"},
		Lifecycle: config.Lifecycle{StopTimeout: config.Duration(2 * time.Second)}}}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	a, err := New(cfg, "1.0.0-test")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	defer func() {
		cancel()
		<-done
	}()

	h := <-s.hellos
	if h.Agent.Version != "1.0.0-test" || h.Agent.BootID == "" || h.Agent.StartedAt == 0 || h.Host.OS == "" ||
		h.Labels["zone"] != "eu" || len(h.Workers) != 1 || h.Workers[0].Name != "echo" {
		t.Fatalf("hello: %+v", h)
	}
	s.mu.Lock()
	first := s.got[0]
	s.mu.Unlock()
	if first.Type != message.TypeHello {
		t.Fatalf("первое сообщение: %s", first.Type)
	}
	st := s.wait("status с воркерами", func(e message.Envelope) bool {
		if e.Type != message.TypeStatus {
			return false
		}
		ws := decode[message.Status](e).Workers
		return len(ws) == 2 && ws[0].State == message.WorkerRunning && ws[1].State == message.WorkerRunning
	})
	if ws := decode[message.Status](st).Workers; ws[0].Name != "echo" || ws[1].Name != "sysmetrics" || !ws[1].Builtin {
		t.Fatalf("status: %+v", ws)
	}

	// Настройки (§8).
	s.send(message.MustNew(message.TypeConfigPut, message.ConfigPut{Worker: "echo", Key: "main", Version: 1, Data: json.RawMessage(`{"a":1}`)}))
	applied := decode[message.ConfigApplied](s.wait("config.applied ok", func(e message.Envelope) bool { return e.Type == message.TypeConfigApplied }))
	if !applied.OK || applied.Version != 1 || applied.Worker != "echo" {
		t.Fatalf("config.applied: %+v", applied)
	}
	s.send(message.MustNew(message.TypeConfigPut, message.ConfigPut{Worker: "echo", Key: "main", Version: 2, Data: json.RawMessage(`{"reject":"нельзя"}`)}))
	rejected := decode[message.ConfigApplied](s.wait("config.applied ошибка", func(e message.Envelope) bool {
		return e.Type == message.TypeConfigApplied && decode[message.ConfigApplied](e).Version == 2
	}))
	if rejected.OK || rejected.Error.Code != message.CodeConfigRejected || rejected.Error.Message != "нельзя" {
		t.Fatalf("отказ: %+v", rejected)
	}
	s.send(message.MustNew(message.TypeConfigPut, message.ConfigPut{Worker: "echo", Key: "other", Version: 1, Data: json.RawMessage(`{}`)}))
	unknown := decode[message.ConfigApplied](s.wait("config.applied необъявленного ключа", func(e message.Envelope) bool {
		return e.Type == message.TypeConfigApplied && decode[message.ConfigApplied](e).Key == "other"
	}))
	if unknown.OK || unknown.Error.Code != message.CodeConfigKeyUnknown {
		t.Fatalf("необъявленный ключ: %+v", unknown)
	}
	s.wait("status с итогом настроек", func(e message.Envelope) bool {
		if e.Type != message.TypeStatus {
			return false
		}
		c := decode[message.Status](e).Workers[0].Configs["main"]
		return c.Version == 2 && c.OK != nil && !*c.OK
	})

	// Запрос к воркеру (§7).
	r := s.fetch("f1", message.Fetch{Worker: "echo", Method: "POST", Path: "/echo", Body: "привет"})
	if len(r) != 3 || decode[message.FetchHead](r[0]).Status != 200 || decode[message.FetchChunk](r[1]).Data != "ПРИВЕТ" {
		t.Fatalf("fetch: %+v", r)
	}
	if r := s.fetch("f2", message.Fetch{Worker: "sysmetrics", Method: "GET", Path: "/x"}); decode[message.FetchEnd](r[0]).Error.Code != message.CodeWorkerUnknown {
		t.Fatalf("встроенный воркер серверу не виден: %+v", r)
	}

	// Событие и контекст через сокет агента (§12).
	if r := s.fetch("f3", message.Fetch{Worker: "echo", Method: "POST", Path: "/emit"}); decode[message.FetchHead](r[0]).Status != 202 {
		t.Fatalf("событие: %+v", r)
	}
	if r := s.fetch("f3u", message.Fetch{Worker: "echo", Method: "POST", Path: "/emit?type=example.lost"}); decode[message.FetchHead](r[0]).Status != 400 {
		t.Fatalf("необъявленное событие: %+v", r)
	}
	ev := decode[message.Event](s.wait("event", func(e message.Envelope) bool { return e.Type == message.TypeEvent }))
	if ev.Worker != "echo" || ev.Type != "example.done" || ev.At == 0 || string(ev.Data) != `{"n":1}` {
		t.Fatalf("event: %+v", ev)
	}
	// Запрос воркера к серверу (§12) и маршрут не из манифеста (§7).
	r = s.fetch("f5", message.Fetch{Worker: "echo", Method: "POST", Path: "/ask"})
	if decode[message.FetchHead](r[0]).Status != 200 || decode[message.FetchChunk](r[1]).Data != `{"data":{"worker":"echo","echo":{"q":1}}}`+"\n" {
		t.Fatalf("запрос к серверу: %+v", r)
	}
	r = s.fetch("f6", message.Fetch{Worker: "echo", Method: "POST", Path: "/ask?type=example.other"})
	if decode[message.FetchHead](r[0]).Status != 400 || !strings.Contains(decode[message.FetchChunk](r[1]).Data, message.CodeRequestUndeclared) {
		t.Fatalf("необъявленный запрос: %+v", r)
	}
	if r := s.fetch("f7", message.Fetch{Worker: "echo", Method: "GET", Path: "/echo"}); decode[message.FetchEnd](r[0]).Error.Code != message.CodeRouteUndeclared {
		t.Fatalf("необъявленный маршрут: %+v", r)
	}
	r = s.fetch("f4", message.Fetch{Worker: "echo", Method: "GET", Path: "/ctx"})
	ctxBody := decode[message.Context](message.Envelope{Data: json.RawMessage(decode[message.FetchChunk](r[1]).Data)})
	if ctxBody.Agent.ID != "a1" || ctxBody.Agent.Name != "node-01" || !ctxBody.Online {
		t.Fatalf("контекст: %+v", ctxBody)
	}

	// Метрики (§9): host — от sysmetrics, workers — ответы GET /metrics.
	m := decode[message.Metrics](s.wait("metrics", func(e message.Envelope) bool {
		m := decode[message.Metrics](e)
		return e.Type == message.TypeMetrics && m.Host != nil && m.Workers["echo"] != nil
	}))
	if m.Host.MemTotalBytes == nil || m.Host.CPUPercent != nil || string(m.Workers["echo"]) != `{"sent":1}` || m.Workers["sysmetrics"] != nil {
		t.Fatalf("metrics: %+v", m)
	}

	// watch: лог подробнее — записи info уходят серверу.
	s.send(message.MustNew(message.TypeWatch, message.Watch{LogLevel: message.LogInfo, UntilMs: time.Now().Add(time.Minute).UnixMilli()}))
	time.Sleep(100 * time.Millisecond)
	if res := s.action("x1", "agent.nope", nil); res.OK || res.Error.Code != message.CodeActionUnknown {
		t.Fatalf("незнакомое действие: %+v", res)
	}
	s.wait("log", func(e message.Envelope) bool {
		if e.Type != message.TypeLog {
			return false
		}
		for _, entry := range decode[message.Log](e).Entries {
			if entry.Msg == "действие сервера" && entry.Level == message.LogInfo {
				return true
			}
		}
		return false
	})

	// Встроенные действия (§10).
	if res := s.action("x2", "worker.restart", map[string]string{}); res.OK || res.Error.Code != message.CodeMessageInvalid {
		t.Fatalf("без name: %+v", res)
	}
	if res := s.action("x3", "worker.restart", map[string]string{"name": "nope"}); res.Error == nil || res.Error.Code != message.CodeWorkerUnknown {
		t.Fatalf("неизвестный воркер: %+v", res)
	}
	if res := s.action("x4", "worker.restart", map[string]string{"name": "echo"}); !res.OK {
		t.Fatalf("worker.restart: %+v", res)
	}
	res := s.action("x5", "agent.logs", map[string]any{"worker": "echo", "lines": 10})
	logs := decode[message.LogsResult](message.Envelope{Data: res.Result})
	if !res.OK || len(logs.Entries) == 0 || logs.Entries[0].Source != "echo" || !strings.Contains(logs.Entries[0].Msg, "настройки применены") {
		t.Fatalf("agent.logs: %+v", logs)
	}
	if res := s.action("x6", "worker.update", message.WorkerUpdateArgs{Name: "echo", Version: "1", URL: "/x", SHA256: "00"}); res.Error == nil ||
		res.Error.Code != message.CodeWorkerNotReleased {
		t.Fatalf("не из выпуска: %+v", res)
	}
	if res := s.action("x7", "agent.update", message.AgentUpdateArgs{Version: "9", URL: "/x", SHA256: "00"}); res.Error == nil ||
		res.Error.Code != message.CodeUpdateNotVerified {
		t.Fatalf("нет ключа: %+v", res)
	}

	// Смена ключа: хеш нового секрета; сервер закрывает 1012 и принимает
	// только новый секрет.
	res = s.action("x8", "agent.rotateKey", nil)
	rot := decode[message.RotateKeyResult](message.Envelope{Data: res.Result})
	if !res.OK || len(rot.SecretHash) != 64 {
		t.Fatalf("rotateKey: %+v", res)
	}
	creds, _, _ := a.auth.store.Load()
	sum := sha256.Sum256([]byte(creds.PendingSecret))
	if hex.EncodeToString(sum[:]) != rot.SecretHash {
		t.Fatal("хеш не того секрета")
	}
	s.mu.Lock()
	s.secret = creds.PendingSecret
	s.mu.Unlock()
	s.closeConn(message.CloseRestart)
	h2 := <-s.hellos
	if h2.Agent.BootID != h.Agent.BootID || h2.Configs["echo"]["main"] != 2 {
		t.Fatalf("второй hello: %+v", h2)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		creds, _, _ := a.auth.store.Load()
		if creds.Secret == s.secret && creds.PendingSecret == "" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("новый секрет не стал основным: %+v", creds)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// agent.update (§11): итог отправляет следующий запуск после welcome —
// работает новая версия — {version, previous}, прежняя (возврат) —
// UPDATE_FAILED.
func TestFinishAgentUpdate(t *testing.T) {
	for _, running := range []string{"1.1.0", "1.0.0"} {
		cfg := config.Defaults()
		cfg.Server.URL = "http://127.0.0.1:1"
		cfg.DataDir = t.TempDir()
		cfg.Log.Level = "error"
		a, err := New(cfg, running)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(pendingUpdate{ID: "u1", Version: "1.1.0", Previous: "1.0.0"})
		_ = os.WriteFile(cfg.DataDir+"/"+pendingUpdateFile, raw, 0o600)
		a.finishAgentUpdate()
		a.finishAgentUpdate() // второй welcome — итог уже отправлен
		pending, _ := a.outbox.Pending()
		a.Close()
		if len(pending) != 1 || pending[0].Type != message.TypeActionResult || pending[0].Re != "u1" {
			t.Fatalf("%s: %+v", running, pending)
		}
		res := decode[message.ActionResult](pending[0])
		if running == "1.1.0" && (!res.OK || string(res.Result) != `{"version":"1.1.0","previous":"1.0.0"}`) {
			t.Fatalf("новая версия: %+v", res)
		}
		if running == "1.0.0" && (res.OK || res.Error.Code != message.CodeUpdateFailed) {
			t.Fatalf("возврат прежней: %+v", res)
		}
	}
}

// agent cleanup: воркеры запускаются, получают POST /cleanup (404 — убирать
// нечего), останавливаются.
func TestCleanupCommand(t *testing.T) {
	dir, _ := os.MkdirTemp("", "a")
	defer os.RemoveAll(dir)
	cfg := config.Defaults()
	cfg.Server.URL = "http://127.0.0.1:1"
	cfg.DataDir = dir
	cfg.Log.Level = "error"
	cfg.Workers = []config.Worker{{Name: "echo", Command: []string{os.Args[0]}, Env: map[string]string{"TEST_WORKER": "1"},
		Lifecycle: config.Lifecycle{StopTimeout: config.Duration(2 * time.Second)}}}
	_ = cfg.Validate()
	res, err := Cleanup(context.Background(), cfg, "1.0.0")
	if err != nil || len(res) != 1 || res[0].Err != nil {
		t.Fatalf("%+v %v", res, err)
	}
}

// Частота метрик (§9): из watch, но не чаще 1 с и не реже, чем в welcome;
// watch снимается в untilMs и пустым watch.
func TestMetricsIntervalAndWatch(t *testing.T) {
	cfg := config.Defaults()
	cfg.Server.URL = "http://127.0.0.1:1"
	cfg.DataDir = t.TempDir()
	cfg.Log.Level = "error"
	a, err := New(cfg, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	a.obs.setWelcome(message.Welcome{MetricsIntervalMs: 30000, StatusIntervalMs: 60000})
	if d := a.obs.metricsInterval(); d != 30*time.Second {
		t.Fatalf("из welcome: %v", d)
	}
	a.setWatch(message.Watch{MetricsIntervalMs: 100, UntilMs: time.Now().Add(time.Minute).UnixMilli()})
	if d := a.obs.metricsInterval(); d != time.Second {
		t.Fatalf("не чаще 1 с: %v", d)
	}
	a.setWatch(message.Watch{MetricsIntervalMs: 120000, UntilMs: time.Now().Add(time.Minute).UnixMilli()})
	if d := a.obs.metricsInterval(); d != 30*time.Second {
		t.Fatalf("не реже welcome: %v", d)
	}
	a.setWatch(message.Watch{MetricsIntervalMs: 5000, UntilMs: time.Now().Add(150 * time.Millisecond).UnixMilli()})
	if d := a.obs.metricsInterval(); d != 5*time.Second {
		t.Fatalf("watch: %v", d)
	}
	deadline := time.Now().Add(3 * time.Second)
	for a.obs.metricsInterval() != 30*time.Second {
		if time.Now().After(deadline) {
			t.Fatal("watch не снят в untilMs")
		}
		time.Sleep(10 * time.Millisecond)
	}
	a.setWatch(message.Watch{MetricsIntervalMs: 2000, UntilMs: time.Now().Add(time.Minute).UnixMilli()})
	a.setWatch(message.Watch{})
	if d := a.obs.metricsInterval(); d != 30*time.Second {
		t.Fatalf("пустой watch снимает: %v", d)
	}
	a.setWatch(message.Watch{MetricsIntervalMs: 2000, UntilMs: time.Now().Add(-time.Second).UnixMilli()})
	if d := a.obs.metricsInterval(); d != 30*time.Second {
		t.Fatalf("истёкший watch не действует: %v", d)
	}
}
