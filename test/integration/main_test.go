//go:build unix

// Package integration — настоящий агент (internal/app) против сервера на agent-sdk/server
// (test/testserver, процесс node). Воркеры — тестовый бинарь в роли HTTP-сервиса на
// unix-сокете (worker_test.go), без SDK.
package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	stdlog "log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/sysmetrics"
	"github.com/epifanovmd/agent/test/testserver"
)

const (
	token     = "it-token"
	agentName = "it-agent"
)

// TestMain — тестовый бинарь служит и воркером (IT_WORKER), и его дочерним процессом
// (IT_CHILD), и агентом отдельным процессом (IT_AGENT), и встроенным воркером sysmetrics
// (агент запускает его как `<программа> sysmetrics`).
func TestMain(m *testing.M) {
	switch {
	case os.Getenv("IT_WORKER") != "":
		testWorker()
		return
	case os.Getenv("IT_CHILD") != "":
		time.Sleep(time.Hour)
		return
	case os.Getenv("IT_AGENT") != "":
		agentProcess()
		return
	}
	if ok, err := sysmetrics.Dispatch(os.Args[1:]); ok {
		if err != nil {
			os.Exit(1)
		}
		return
	}
	os.Exit(m.Run())
}

// stand — сервер и прокси перед ним: агент подключается к прокси, тест может оборвать связь
// или сделать сервер недоступным.
type stand struct {
	t      *testing.T
	server *testserver.Server
	http   *httptest.Server
	// down — сервер «недоступен»: прокси отвечает 503.
	down atomic.Bool
	// dir — каталог состояния воркеров стенда.
	dir string
}

func newStand(t *testing.T, tune ...func(*testserver.Config)) *stand {
	s := newUnstartedStand(t, tune...)
	s.http.Start()
	return s
}

// newUnstartedStand — стенд, прокси которого запускает тест (Start или StartTLS).
func newUnstartedStand(t *testing.T, tune ...func(*testserver.Config)) *stand {
	cfg := testserver.Config{EnrollToken: token, StatusInterval: 200 * time.Millisecond, MetricsInterval: 500 * time.Millisecond}
	for _, fn := range tune {
		fn(&cfg)
	}
	srv := testserver.Start(t, cfg)
	s := &stand{t: t, server: srv, dir: t.TempDir()}
	proxy := srv.Proxy()
	s.http = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if s.down.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		proxy.ServeHTTP(w, r)
	}))
	s.http.Config.ErrorLog = stdlog.New(io.Discard, "", 0) // отказы TLS в тестах ожидаемы
	t.Cleanup(func() {
		srv.DropConnections()
		s.http.Close()
	})
	return s
}

// setDown — сервер недоступен (true): связь обрывается, новые подключения получают 503.
func (s *stand) setDown(down bool) {
	s.down.Store(down)
	if down {
		s.server.DropConnections()
		s.http.CloseClientConnections()
	}
}

// worker — тестовый воркер name с каталогом состояния <s.dir>/<name>.
func (s *stand) worker(name string) config.Worker {
	exe, err := os.Executable()
	if err != nil {
		s.t.Fatal(err)
	}
	dir := s.stateDir(name)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		s.t.Fatal(err)
	}
	return config.Worker{
		Name: name, Command: []string{exe},
		Env:       map[string]string{"IT_WORKER": "1", "IT_STATE": dir},
		Lifecycle: config.Lifecycle{StopTimeout: config.Duration(5 * time.Second)},
	}
}

// stateDir — каталог состояния воркера name.
func (s *stand) stateDir(name string) string { return filepath.Join(s.dir, name) }

// config — настройки агента стенда: без метрик узла и обновлений, лог агента — только ошибки.
func (s *stand) config(workers ...config.Worker) config.Config {
	cfg := config.Defaults()
	cfg.Server.URL = s.http.URL
	cfg.DataDir = s.t.TempDir()
	cfg.Name = agentName
	cfg.Enroll.Token = token
	cfg.Update.Mode = config.UpdateDisabled
	cfg.Telemetry.Metrics = []string{}
	cfg.Log.Level = message.LogError
	cfg.Workers = workers
	return cfg
}

// node — агент, работающий в процессе теста.
type node struct {
	t      *testing.T
	cfg    config.Config
	cancel context.CancelFunc
	done   chan struct{}
	once   sync.Once
}

// start — запустить агента с настройками cfg; остановка — по завершении теста или stop.
func (s *stand) start(cfg config.Config) *node {
	s.t.Helper()
	if err := cfg.Validate(); err != nil {
		s.t.Fatal(err)
	}
	a, err := app.New(cfg, "it")
	if err != nil {
		s.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	n := &node{t: s.t, cfg: cfg, cancel: cancel, done: make(chan struct{})}
	go func() {
		defer close(n.done)
		_ = a.Run(ctx)
	}()
	s.t.Cleanup(n.stop)
	return n
}

// stop — остановить агента и его воркеры.
func (n *node) stop() {
	n.once.Do(func() {
		n.cancel()
		<-n.done
	})
}

// online — агент на связи (не отозванный, с hello); его снимок.
func (s *stand) online() testserver.Agent {
	s.t.Helper()
	var a testserver.Agent
	eventually(s.t, "агент на связи", func() bool {
		var ok bool
		a, ok = s.server.Agent(agentName)
		return ok && a.Online && !a.Revoked && a.Hello != nil
	})
	return a
}

// running — агент на связи, воркер name запущен; снимок агента.
func (s *stand) running(name string) testserver.Agent {
	s.t.Helper()
	var a testserver.Agent
	eventually(s.t, "воркер "+name+" запущен", func() bool {
		var ok bool
		a, ok = s.server.Agent(agentName)
		if !ok || !a.Online {
			return false
		}
		w, ok := a.Worker(name)
		return ok && w.State == message.WorkerRunning
	})
	return a
}

// fetch — запрос к воркеру; ошибка — провал теста.
func (s *stand) fetch(agentID, worker, path string, init testserver.FetchInit) testserver.FetchResponse {
	s.t.Helper()
	res, err := s.server.Fetch(agentID, worker, path, init)
	if err != nil {
		s.t.Fatalf("%s %s: %v", worker, path, err)
	}
	return res
}

// direct — запрос прямо к сокету воркера (без сервера и агента): статус и тело. Запрос
// без тела повторяется, пока воркер не начнёт отвечать.
func (n *node) direct(worker, method, path string) (int, string) {
	n.t.Helper()
	socket := filepath.Join(app.RunDir(n.cfg.DataDir), worker, "http.sock")
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", socket)
	}}}
	var resp *http.Response
	var err error
	for deadline := time.Now().Add(20 * time.Second); ; time.Sleep(50 * time.Millisecond) {
		req, _ := http.NewRequest(method, "http://worker"+path, nil)
		if resp, err = client.Do(req); err == nil {
			break
		}
		if time.Now().After(deadline) {
			n.t.Fatalf("%s %s: %v", method, path, err)
		}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(raw)
}

// emit — воркер шлёт count событий типа typ в сокет агента (номера с from); все приняты (202).
func (n *node) emit(worker, typ string, from, count int) {
	n.t.Helper()
	_, codes := n.direct(worker, http.MethodPost, fmt.Sprintf("/emit?type=%s&from=%d&n=%d", typ, from, count))
	for _, c := range strings.Split(codes, ",") {
		if c != "202" {
			n.t.Fatalf("POST /events: %s", codes)
		}
	}
}

// pid — pid процесса воркера (GET /pid).
func (s *stand) pid(agentID, worker string) string {
	s.t.Helper()
	return s.fetch(agentID, worker, "/pid", testserver.FetchInit{}).Body
}

// lines — строки файла состояния воркера (нет файла — пусто).
func (s *stand) lines(worker, file string) []string {
	raw, err := os.ReadFile(filepath.Join(s.stateDir(worker), file))
	if err != nil {
		return nil
	}
	return strings.Split(strings.TrimSpace(string(raw)), "\n")
}

// eventually — cond выполняется не позже чем за 20 с.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	within(t, 20*time.Second, what, cond)
}

func within(t *testing.T, d time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(d)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались за %s: %s", d, what)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// errorCode — код ошибки сервера ("" — ошибки нет).
func errorCode(err error) string {
	var e *testserver.Error
	if errors.As(err, &e) {
		return e.Code
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func mustJSON(t *testing.T, raw []byte, v any) {
	t.Helper()
	if err := json.Unmarshal(raw, v); err != nil {
		t.Fatalf("%v: %s", err, raw)
	}
}
