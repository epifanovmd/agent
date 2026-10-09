package fetch

import (
	"bytes"
	"context"
	"encoding/base64"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/worker"
)

type workers struct {
	client   *http.Client
	running  bool
	open     bool
	manifest *message.WorkerManifest
	// calls — запросы, дошедшие до воркера: метод и путь.
	mu    sync.Mutex
	calls []string
}

// testManifest — маршруты тестового воркера и задачи example.build.
var testManifest = &message.WorkerManifest{Version: "1", Routes: []message.WorkerManifestRoute{
	{Method: "POST", Path: "/echo"}, {Method: "PUT", Path: "/echo"}, {Method: "GET", Path: "/big"},
	{Method: "GET", Path: "/bin"}, {Method: "GET", Path: "/slow"}, {Method: "DELETE", Path: "/items/{id}"},
}, Jobs: []message.WorkerManifestJob{{Type: "example.build"}}}

func (w *workers) Manifest(string) *message.WorkerManifest { return w.manifest }
func (w *workers) OpenRoutes(string) bool                  { return w.open }

func (w *workers) called() []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return slices.Clone(w.calls)
}

func (w *workers) Has(name string) bool { return name == "echo" }
func (w *workers) Client(name string) (*http.Client, error) {
	if name != "echo" {
		return nil, worker.ErrUnknown
	}
	if !w.running {
		return nil, worker.ErrUnavailable
	}
	return w.client, nil
}

// testWorker — воркер на unix-сокете с маршрутами для туннеля.
func testWorker(t *testing.T) *workers {
	dir, _ := os.MkdirTemp("", "f")
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "w.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	ws := &workers{running: true, manifest: testManifest}
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		ws.mu.Lock()
		ws.calls = append(ws.calls, r.Method+" "+r.URL.Path)
		ws.mu.Unlock()
		w.WriteHeader(http.StatusTeapot)
	})
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Add("X-Multi", "a")
		w.Header().Add("X-Multi", "b")
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusCreated)
		fmt.Fprintf(w, "%s %s %s %s", r.Method, r.URL.RawQuery, r.Header.Get("X-Example"), strings.ToUpper(string(body)))
	})
	mux.HandleFunc("/big", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		_, _ = w.Write(bytes.Repeat([]byte("я"), n/2))
	})
	mux.HandleFunc("/bin", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte{0xff, 0xfe, 0x00, 0x01}) })
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("начало"))
		w.(http.Flusher).Flush()
		<-r.Context().Done()
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	ws.client = &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		}}}
	return ws
}

// session — соединение для тестов: собирает ответы.
type session struct {
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex
	sent   []message.Envelope
}

func newSession() *session {
	ctx, cancel := context.WithCancel(context.Background())
	return &session{ctx: ctx, cancel: cancel}
}

func (s *session) Send(env message.Envelope) error {
	if s.ctx.Err() != nil {
		return s.ctx.Err()
	}
	s.mu.Lock()
	s.sent = append(s.sent, env)
	s.mu.Unlock()
	return nil
}
func (s *session) Context() context.Context { return s.ctx }

func (s *session) replies(id string) []message.Envelope {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []message.Envelope
	for _, env := range s.sent {
		if env.Re == id {
			out = append(out, env)
		}
	}
	return out
}

// wait — ответы на id до fetch.end.
func (s *session) wait(t *testing.T, id string) []message.Envelope {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		r := s.replies(id)
		if len(r) > 0 && r[len(r)-1].Type == message.TypeFetchEnd {
			return r
		}
		if time.Now().After(deadline) {
			t.Fatalf("нет fetch.end для %s: %+v", id, r)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func request(id string, f message.Fetch) message.Envelope {
	if f.Worker == "" {
		f.Worker = "echo"
	}
	if f.Method == "" {
		f.Method = http.MethodGet
	}
	env := message.MustNew(message.TypeFetch, f)
	env.ID = id
	return env
}

func endError(t *testing.T, env message.Envelope) *message.ErrorInfo {
	t.Helper()
	var end message.FetchEnd
	if err := env.Decode(&end); err != nil {
		t.Fatal(err)
	}
	return end.Error
}

// body — тело ответа из кусков.
func body(t *testing.T, replies []message.Envelope) []byte {
	var out []byte
	for _, env := range replies {
		if env.Type != message.TypeFetchChunk {
			continue
		}
		var c message.FetchChunk
		_ = env.Decode(&c)
		if c.Encoding == message.EncodingBase64 {
			raw, err := base64.StdEncoding.DecodeString(c.Data)
			if err != nil {
				t.Fatal(err)
			}
			out = append(out, raw...)
		} else {
			out = append(out, c.Data...)
		}
	}
	return out
}

// Запрос и ответ (§7): head (статус, заголовки, несколько значений через
// «, »), куски, end без ошибки; тело запроса — текст и base64.
func TestFetchRoundTrip(t *testing.T) {
	tun := New(testWorker(t), logx.Discard())
	s := newSession()
	tun.Handle(s, request("a", message.Fetch{Method: "POST", Path: "/echo?upper=1", Headers: map[string]string{"x-example": "v"}, Body: "привет"}))
	r := s.wait(t, "a")
	if len(r) != 3 || r[0].Type != message.TypeFetchHead || r[1].Type != message.TypeFetchChunk {
		t.Fatalf("ответ: %+v", r)
	}
	var head message.FetchHead
	_ = r[0].Decode(&head)
	if head.Status != 201 || head.Headers["x-multi"] != "a, b" || head.Headers["content-type"] != "text/plain" {
		t.Fatalf("head: %+v", head)
	}
	var chunk message.FetchChunk
	_ = r[1].Decode(&chunk)
	if chunk.Encoding != message.EncodingUTF8 || chunk.Data != "POST upper=1 v ПРИВЕТ" || endError(t, r[2]) != nil {
		t.Fatalf("кусок: %+v", chunk)
	}
	tun.Handle(s, request("b", message.Fetch{Method: "PUT", Path: "/echo", Body: base64.StdEncoding.EncodeToString([]byte("abc")), Encoding: "base64"}))
	if got := string(body(t, s.wait(t, "b"))); got != "PUT   ABC" {
		t.Fatalf("base64: %q", got)
	}
	tun.Handle(s, request("c", message.Fetch{Path: "/bin"}))
	r = s.wait(t, "c")
	var bin message.FetchChunk
	_ = r[1].Decode(&bin)
	if bin.Encoding != message.EncodingBase64 || !bytes.Equal(body(t, r), []byte{0xff, 0xfe, 0x00, 0x01}) {
		t.Fatalf("двоичное: %+v", bin)
	}
}

// Куски — до 64 КБ, текст UTF-8 не режется посреди символа; ответ больше
// 32 МБ — BODY_TOO_LARGE после уже отправленных кусков.
func TestFetchChunksAndLimit(t *testing.T) {
	tun := New(testWorker(t), logx.Discard())
	s := newSession()
	n := 300_001 // нечётное: символ «я» — 2 байта, граница кусков попадает внутрь символа
	tun.Handle(s, request("a", message.Fetch{Path: "/big?n=" + strconv.Itoa(n)}))
	r := s.wait(t, "a")
	for _, env := range r[1 : len(r)-1] {
		var c message.FetchChunk
		_ = env.Decode(&c)
		if c.Encoding != message.EncodingUTF8 || len(c.Data) > message.MaxChunkBytes || !utf8.ValidString(c.Data) {
			t.Fatalf("кусок: %s %d", c.Encoding, len(c.Data))
		}
	}
	if got := body(t, r); len(got) != n/2*2 || endError(t, r[len(r)-1]) != nil {
		t.Fatalf("тело: %d", len(got))
	}
	tun.Handle(s, request("big", message.Fetch{Path: "/big?n=" + strconv.Itoa(message.MaxFetchBodyBytes+message.MaxChunkBytes*2)}))
	r = s.wait(t, "big")
	if e := endError(t, r[len(r)-1]); e == nil || e.Code != message.CodeBodyTooLarge || len(r) < 3 {
		t.Fatalf("предел: %+v (%d сообщений)", e, len(r))
	}
	if got := len(body(t, r)); got > message.MaxFetchBodyBytes {
		t.Fatalf("отправлено больше предела: %d", got)
	}
}

// Ошибки до ответа воркера — сразу fetch.end без head.
func TestFetchErrors(t *testing.T) {
	w := testWorker(t)
	tun := New(w, logx.Discard())
	s := newSession()
	cases := map[string]struct {
		f    message.Fetch
		code string
	}{
		"unknown":  {message.Fetch{Worker: "nope", Path: "/"}, message.CodeWorkerUnknown},
		"metrics":  {message.Fetch{Path: "/metrics"}, message.CodePathForbidden},
		"health":   {message.Fetch{Path: "/health?x=1"}, message.CodePathForbidden},
		"cleanup":  {message.Fetch{Method: "POST", Path: "/./cleanup"}, message.CodePathForbidden},
		"config":   {message.Fetch{Method: "PUT", Path: "/config/main"}, message.CodePathForbidden},
		"escaped":  {message.Fetch{Path: "/%63onfig/main"}, message.CodePathForbidden},
		"no-slash": {message.Fetch{Path: "echo"}, message.CodeMessageInvalid},
		"base64":   {message.Fetch{Path: "/echo", Body: "%%%", Encoding: "base64"}, message.CodeMessageInvalid},
		"too-big":  {message.Fetch{Path: "/echo", Body: strings.Repeat("x", message.MaxFetchRequestBytes+1)}, message.CodeBodyTooLarge},
	}
	for id, c := range cases {
		tun.Handle(s, request(id, c.f))
		r := s.wait(t, id)
		if len(r) != 1 || endError(t, r[0]) == nil || endError(t, r[0]).Code != c.code {
			t.Errorf("%s: %+v", id, r)
		}
	}
	w.running = false
	tun.Handle(s, request("down", message.Fetch{Path: "/echo"}))
	if r := s.wait(t, "down"); endError(t, r[0]).Code != message.CodeWorkerUnavailable {
		t.Fatalf("не запущен: %+v", r)
	}
	if Forbidden("/configuration") || Forbidden("/metrics-old") || !Forbidden("/config") {
		t.Fatal("похожие пути разрешены, /config — нет")
	}
}

// Срок истёк — TIMEOUT; fetch.cancel — CANCELLED; разрыв соединения —
// запрос прерывается, ничего не досылается; больше 64 сразу — BUSY.
func TestFetchTimeoutCancelDisconnectBusy(t *testing.T) {
	tun := New(testWorker(t), logx.Discard())
	s := newSession()
	tun.Handle(s, request("t", message.Fetch{Path: "/slow", TimeoutMs: 100}))
	r := s.wait(t, "t")
	if e := endError(t, r[len(r)-1]); e == nil || e.Code != message.CodeTimeout || r[0].Type != message.TypeFetchHead {
		t.Fatalf("срок: %+v", r)
	}
	tun.Handle(s, request("c", message.Fetch{Path: "/slow"}))
	time.Sleep(50 * time.Millisecond)
	tun.Cancel(s, "c")
	if e := endError(t, s.wait(t, "c")[len(s.replies("c"))-1]); e == nil || e.Code != message.CodeCancelled {
		t.Fatalf("отмена: %+v", e)
	}

	for i := range message.MaxFetches {
		tun.Handle(s, request(fmt.Sprint("busy", i), message.Fetch{Path: "/slow"}))
	}
	tun.Handle(s, request("extra", message.Fetch{Path: "/slow"}))
	if r := s.wait(t, "extra"); len(r) != 1 || endError(t, r[0]).Code != message.CodeBusy {
		t.Fatalf("BUSY: %+v", r)
	}
	s.cancel()
	deadline := time.Now().Add(5 * time.Second)
	for {
		tun.mu.Lock()
		n := len(tun.active)
		tun.mu.Unlock()
		if n == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("разрыв не прервал запросы: %d", n)
		}
		time.Sleep(5 * time.Millisecond)
	}
	for i := range message.MaxFetches {
		for _, env := range s.replies(fmt.Sprint("busy", i)) {
			if env.Type == message.TypeFetchEnd {
				t.Fatal("после разрыва ничего не досылается")
			}
		}
	}
}

// Маршруты — только из манифеста (§7): необъявленный путь или метод —
// ROUTE_UNDECLARED без обращения к воркеру; {name} — один сегмент, query не
// учитывается; задачи — только у воркера с jobs, тип POST /jobs — из
// манифеста (JOB_UNKNOWN); GET /manifest — всегда; routes: open — любой путь,
// кроме служебных.
func TestFetchRoutes(t *testing.T) {
	w := testWorker(t)
	tun := New(w, logx.Discard())
	s := newSession()
	job := func(typ string) string { return `{"type":"` + typ + `","jobId":"j1"}` }
	cases := []struct {
		id   string
		f    message.Fetch
		code string // "" — дошёл до воркера
	}{
		{"declared", message.Fetch{Method: "DELETE", Path: "/items/7?force=1"}, ""},
		{"method", message.Fetch{Method: "GET", Path: "/items/7"}, message.CodeRouteUndeclared},
		{"segments", message.Fetch{Method: "DELETE", Path: "/items/7/x"}, message.CodeRouteUndeclared},
		{"dotdot", message.Fetch{Method: "DELETE", Path: "/items/.."}, message.CodeRouteUndeclared},
		{"escaped-dotdot", message.Fetch{Method: "DELETE", Path: "/items/%2e%2e"}, message.CodeRouteUndeclared},
		{"unknown", message.Fetch{Method: "GET", Path: "/hidden"}, message.CodeRouteUndeclared},
		{"trailing", message.Fetch{Method: "POST", Path: "/echo/"}, message.CodeRouteUndeclared},
		{"manifest", message.Fetch{Method: "GET", Path: "/manifest"}, ""},
		{"job", message.Fetch{Method: "POST", Path: "/jobs", Body: job("example.build")}, ""},
		{"job-unknown", message.Fetch{Method: "POST", Path: "/jobs", Body: job("example.other")}, message.CodeJobUnknown},
		{"job-base64", message.Fetch{Method: "POST", Path: "/jobs", Encoding: "base64",
			Body: base64.StdEncoding.EncodeToString([]byte(job("example.other")))}, message.CodeJobUnknown},
		{"job-not-json", message.Fetch{Method: "POST", Path: "/jobs", Body: "build"}, message.CodeMessageInvalid},
		{"job-status", message.Fetch{Method: "GET", Path: "/jobs/b81c"}, ""},
		{"job-cancel", message.Fetch{Method: "POST", Path: "/jobs/b81c/cancel"}, ""},
		{"job-other", message.Fetch{Method: "DELETE", Path: "/jobs/b81c"}, message.CodeRouteUndeclared},
	}
	for _, c := range cases {
		tun.Handle(s, request(c.id, c.f))
		r := s.wait(t, c.id)
		got := ""
		if e := endError(t, r[len(r)-1]); e != nil {
			got = e.Code
		}
		if got != c.code || (c.code != "" && len(r) != 1) {
			t.Errorf("%s: %q, ждали %q (%d сообщений)", c.id, got, c.code, len(r))
		}
	}
	want := []string{"DELETE /items/7", "GET /manifest", "POST /jobs", "GET /jobs/b81c", "POST /jobs/b81c/cancel"}
	if got := w.called(); !slices.Equal(got, want) {
		t.Fatalf("до воркера дошли %v, ждали %v", got, want)
	}

	// Без jobs в манифесте задачи — необъявленный маршрут.
	w.manifest = &message.WorkerManifest{Version: "1", Routes: testManifest.Routes}
	tun.Handle(s, request("no-jobs", message.Fetch{Method: "POST", Path: "/jobs", Body: job("example.build")}))
	if r := s.wait(t, "no-jobs"); endError(t, r[0]).Code != message.CodeRouteUndeclared {
		t.Fatalf("задачи без jobs: %+v", r)
	}

	// routes: open — любой путь и тип задачи, служебные — по-прежнему нет.
	w.open = true
	for id, f := range map[string]message.Fetch{
		"open-hidden": {Method: "GET", Path: "/hidden"},
		"open-job":    {Method: "POST", Path: "/jobs", Body: job("example.other")},
	} {
		tun.Handle(s, request(id, f))
		if r := s.wait(t, id); endError(t, r[len(r)-1]) != nil || r[0].Type != message.TypeFetchHead {
			t.Errorf("%s: %+v", id, r)
		}
	}
	tun.Handle(s, request("open-metrics", message.Fetch{Path: "/metrics"}))
	if r := s.wait(t, "open-metrics"); endError(t, r[0]).Code != message.CodePathForbidden {
		t.Fatalf("служебный путь при routes: open: %+v", r)
	}
}
