// Package testserver — сервер для тестов агента: server.mjs (Agents из agent-sdk/server в
// памяти) дочерним процессом node и Go-клиент его управляющего API. Агент подключается к
// серверу через прокси теста (Proxy): он видит соединения агента и может их оборвать.
//
// Нужны node (из PATH) и собранный sdk/node (cd sdk/node && npm ci && npm run build). Без них
// тест пропускается; в CI (переменная CI) — падает.
package testserver

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
)

// Config — настройки сервера (опции Agents).
type Config struct {
	// EnrollToken — токен регистрации агентов.
	EnrollToken string
	// StatusInterval, MetricsInterval — интервалы, которые сервер задаёт агентам.
	StatusInterval  time.Duration
	MetricsInterval time.Duration
	// ReleasesDir, PublicKey — раздача выпуска (manifest.json, сборки) и install.sh.
	ReleasesDir string
	PublicKey   string
	// Options — другие опции Agents (offlineGraceMs, actionTimeoutMs…).
	Options map[string]any
}

// Server — запущенный server.mjs.
type Server struct {
	t    testing.TB
	base *url.URL // адрес процесса node
	// log — журнал сервера: вывод опции log Agents (stderr процесса node).
	log func() string

	mu    sync.Mutex
	conns map[net.Conn]struct{} // соединения прокси с сервером
}

// Root — корень репозитория.
func Root() string {
	_, file, _, _ := runtime.Caller(0)
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// Start — сервер дочерним процессом node; остановка — по завершении теста.
func Start(t testing.TB, cfg Config) *Server {
	t.Helper()
	node := need(t)
	opts := map[string]any{"enrollToken": cfg.EnrollToken}
	for k, v := range cfg.Options {
		opts[k] = v
	}
	if cfg.StatusInterval > 0 {
		opts["statusIntervalMs"] = cfg.StatusInterval.Milliseconds()
	}
	if cfg.MetricsInterval > 0 {
		opts["metricsIntervalMs"] = cfg.MetricsInterval.Milliseconds()
	}
	if cfg.ReleasesDir != "" {
		opts["releasesDir"] = cfg.ReleasesDir
	}
	if cfg.PublicKey != "" {
		opts["publicKey"] = cfg.PublicKey
	}
	raw, _ := json.Marshal(opts)

	cmd := exec.Command(node, filepath.Join(Root(), "test", "testserver", "server.mjs"), string(raw))
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	var logs bytes.Buffer
	var logsMu sync.Mutex
	cmd.Stderr = writerFunc(func(p []byte) (int, error) {
		logsMu.Lock()
		defer logsMu.Unlock()
		return logs.Write(p)
	})
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = stdin.Close() // сервер останавливается сам
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = cmd.Process.Kill()
			<-done
		}
		if t.Failed() {
			logsMu.Lock()
			t.Logf("журнал сервера:\n%s", logs.String())
			logsMu.Unlock()
		}
	})

	started := make(chan string, 1)
	go func() {
		line, _ := bufio.NewReader(stdout).ReadString('\n')
		started <- line
		_, _ = io.Copy(io.Discard, stdout)
	}()
	var line string
	select {
	case line = <-started:
	case <-time.After(15 * time.Second):
		t.Fatal("сервер не запустился за 15 с")
	}
	var ready struct {
		URL string `json:"url"`
	}
	if err := json.Unmarshal([]byte(line), &ready); err != nil || ready.URL == "" {
		_ = cmd.Wait()
		t.Fatalf("сервер не запустился: %q\n%s", line, logs.String())
	}
	base, _ := url.Parse(ready.URL)
	log := func() string {
		logsMu.Lock()
		defer logsMu.Unlock()
		return logs.String()
	}
	return &Server{t: t, base: base, log: log, conns: map[net.Conn]struct{}{}}
}

// need — путь к node; без node или без собранного sdk/node тест пропускается (в CI — падает).
func need(t testing.TB) string {
	t.Helper()
	miss := func(why string) {
		t.Helper()
		if os.Getenv("CI") != "" {
			t.Fatal(why)
		}
		t.Skip(why)
	}
	node, err := exec.LookPath("node")
	if err != nil {
		miss("нет node в PATH: тесту нужен сервер на agent-sdk/server (make test — в контейнере с node)")
	}
	if _, err := os.Stat(filepath.Join(Root(), "sdk", "node", "dist", "server", "index.js")); err != nil {
		miss("sdk/node не собран: cd sdk/node && npm ci && npm run build")
	}
	return node
}

// Log — журнал сервера: всё, что Agents передал опции log.
func (s *Server) Log() string { return s.log() }

// Proxy — HTTP-обработчик для агента: всё — серверу (WebSocket тоже), с X-Forwarded-Host
// и X-Forwarded-Proto (install.sh — с адресом прокси).
func (s *Server) Proxy() http.Handler {
	dialer := &net.Dialer{}
	return &httputil.ReverseProxy{
		Rewrite: func(r *httputil.ProxyRequest) {
			r.SetURL(s.base)
			r.SetXForwarded()
		},
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
				c, err := dialer.DialContext(ctx, network, addr)
				if err != nil {
					return nil, err
				}
				s.mu.Lock()
				s.conns[c] = struct{}{}
				s.mu.Unlock()
				return &trackedConn{Conn: c, s: s}, nil
			},
		},
		ErrorLog: log.New(io.Discard, "", 0), // обрывы соединений в тестах ожидаемы
		ErrorHandler: func(w http.ResponseWriter, _ *http.Request, _ error) {
			w.WriteHeader(http.StatusBadGateway)
		},
	}
}

// DropConnections — оборвать все соединения прокси с сервером (и WebSocket агента).
func (s *Server) DropConnections() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for c := range s.conns {
		_ = c.Close()
	}
}

type trackedConn struct {
	net.Conn
	s *Server
}

func (c *trackedConn) Close() error {
	c.s.mu.Lock()
	delete(c.s.conns, c.Conn)
	c.s.mu.Unlock()
	return c.Conn.Close()
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// control — клиент управляющего API: без прокси из окружения.
var control = &http.Client{Transport: &http.Transport{}}

// Error — ошибка управляющего API: код (§15 или код SDK) и текст.
type Error struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Call — метод управляющего API (метод Agents или свой метод server.mjs) с аргументами args;
// итог — в out (nil — не нужен). Ошибка сервера — *Error с кодом.
func (s *Server) Call(method string, out any, args ...any) error {
	if args == nil {
		args = []any{}
	}
	body, _ := json.Marshal(args)
	resp, err := control.Post(s.base.String()+"/test/"+method, "application/json", bytes.NewReader(body))
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	var reply struct {
		Result json.RawMessage `json:"result"`
		Error  *Error          `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return fmt.Errorf("%s: HTTP %d: %w", method, resp.StatusCode, err)
	}
	if reply.Error != nil {
		return reply.Error
	}
	if out != nil && len(reply.Result) > 0 && string(reply.Result) != "null" {
		return json.Unmarshal(reply.Result, out)
	}
	return nil
}

// Must — Call; ошибка — провал теста.
func (s *Server) Must(method string, out any, args ...any) {
	s.t.Helper()
	if err := s.Call(method, out, args...); err != nil {
		s.t.Fatalf("%s: %v", method, err)
	}
}

// Agents — все агенты с их событиями.
func (s *Server) Agents() []Agent {
	s.t.Helper()
	var list []Agent
	s.Must("agents", &list)
	return list
}

// Agent — агент по имени (не отозванный, если такой есть), с его событиями.
func (s *Server) Agent(name string) (Agent, bool) {
	var found Agent
	ok := false
	for _, a := range s.Agents() {
		if a.Name == name && (!ok || found.Revoked) {
			found, ok = a, true
		}
	}
	return found, ok
}

// Logs — принятые записи журнала агента (сообщения log), по порядку.
func (s *Server) Logs(agentID string) []LogEntry {
	s.t.Helper()
	var entries []LogEntry
	s.Must("logs", &entries, agentID)
	return entries
}

// Metrics — все принятые точки метрик агента, по порядку прихода.
func (s *Server) Metrics(agentID string) []MetricsPoint {
	s.t.Helper()
	var points []MetricsPoint
	s.Must("metrics", &points, agentID)
	return points
}

// ConfigEvents — события config (статусы ключей), по порядку.
func (s *Server) ConfigEvents(agentID string) []ConfigStatus {
	s.t.Helper()
	var list []ConfigStatus
	s.Must("configEvents", &list, agentID)
	return list
}

// SetConfig — новое значение ключа настроек воркера; версия растёт с каждым вызовом.
func (s *Server) SetConfig(agentID, worker, key string, data any) ConfigRecord {
	s.t.Helper()
	var rec ConfigRecord
	s.Must("setConfig", &rec, agentID, worker, key, data)
	return rec
}

// DeleteConfig — удалить ключ настроек.
func (s *Server) DeleteConfig(agentID, worker, key string) {
	s.t.Helper()
	s.Must("deleteConfig", nil, agentID, worker, key)
}

// ConfigStatus — статус ключа настроек воркера (ok — ключ есть).
func (s *Server) ConfigStatus(agentID, worker, key string) (ConfigStatus, bool) {
	s.t.Helper()
	var list []ConfigStatus
	s.Must("configStatus", &list, agentID, worker)
	for _, st := range list {
		if st.Key == key {
			return st, true
		}
	}
	return ConfigStatus{}, false
}

// Fetch — HTTP-запрос к воркеру через агента. Ошибка до ответа — *Error с кодом.
func (s *Server) Fetch(agentID, worker, path string, init FetchInit) (FetchResponse, error) {
	var res FetchResponse
	err := s.Call("fetch", &res, agentID, worker, path, init)
	return res, err
}

// Watch — наблюдатель: агент присылает метрики чаще и журнал подробнее.
func (s *Server) Watch(agentID string, opts WatchOptions) WatchRef {
	s.t.Helper()
	var ref WatchRef
	s.Must("watch", &ref, agentID, opts)
	return ref
}

// Actions — итоги действий агента (событие action), по порядку прихода.
func (s *Server) Actions(agentID string) []ActionRecord {
	s.t.Helper()
	var list []ActionRecord
	s.Must("actions", &list, agentID)
	return list
}

// Action — встроенное действие методом Agents (restartWorker, updateWorker, updateAgent,
// rotateKey), agentLogs или runJob с аргументами после agentId; итог — в out.
func (s *Server) Action(method, agentID string, out any, args ...any) error {
	return s.Call(method, out, append([]any{agentID}, args...)...)
}
