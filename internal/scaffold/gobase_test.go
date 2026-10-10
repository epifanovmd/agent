package scaffold

import (
	"bufio"
	"context"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// fakeAgent — сокет агента: принимает POST /events (202) и запоминает их.
type fakeAgent struct {
	mu     sync.Mutex
	events []map[string]any
	srv    *http.Server
}

func newFakeAgent(t *testing.T, sock string) *fakeAgent {
	t.Helper()
	a := &fakeAgent{}
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	a.srv = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer tok" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		if r.URL.Path == "/events" {
			var e map[string]any
			_ = json.NewDecoder(r.Body).Decode(&e)
			a.mu.Lock()
			a.events = append(a.events, e)
			a.mu.Unlock()
		}
		w.WriteHeader(http.StatusAccepted)
		_, _ = w.Write([]byte("{}"))
	})}
	go func() { _ = a.srv.Serve(ln) }()
	t.Cleanup(func() { _ = a.srv.Close() })
	return a
}

// find — событие type (и jobId, если не пусто).
func (a *fakeAgent) find(typ, jobID string) map[string]any {
	a.mu.Lock()
	defer a.mu.Unlock()
	for _, e := range a.events {
		data, _ := e["data"].(map[string]any)
		if e["type"] == typ && (jobID == "" || data["jobId"] == jobID) {
			return e
		}
	}
	return nil
}

func (a *fakeAgent) count(typ, jobID string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	n := 0
	for _, e := range a.events {
		data, _ := e["data"].(map[string]any)
		if e["type"] == typ && (jobID == "" || data["jobId"] == jobID) {
			n++
		}
	}
	return n
}

// workerClient — HTTP к воркеру по его сокету.
func workerClient(sock string) *http.Client {
	return &http.Client{Timeout: 10 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", sock)
		},
	}}
}

func call(t *testing.T, c *http.Client, method, path string, body any) (int, map[string]any, string) {
	t.Helper()
	var rd io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		rd = strings.NewReader(string(raw))
	}
	req, _ := http.NewRequest(method, "http://worker"+path, rd)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatalf("%s %s: %v", method, path, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	out := map[string]any{}
	_ = json.Unmarshal(raw, &out)
	return resp.StatusCode, out, string(raw)
}

func waitFor(t *testing.T, what string, timeout time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// goWorker — воркер на Go из каталога dir: сборка и запуск против поддельного агента.
type goWorker struct {
	t                   *testing.T
	dir, bin, sock, ags string
	cmd                 *exec.Cmd
	agent               *fakeAgent
	client              *http.Client
}

func buildGoWorker(t *testing.T, dir string, checkFmt bool) *goWorker {
	t.Helper()
	if _, err := exec.LookPath("go"); err != nil {
		t.Skip("нет go")
	}
	tmp, err := os.MkdirTemp("", "gw")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(tmp) })
	w := &goWorker{t: t, dir: dir, bin: filepath.Join(tmp, "worker"), sock: filepath.Join(tmp, "w.sock"), ags: filepath.Join(tmp, "a.sock")}
	build := exec.Command("go", "build", "-o", w.bin, ".")
	build.Dir = dir
	build.Env = append(os.Environ(), "CGO_ENABLED=0", "GOWORK=off", "GOFLAGS=-mod=mod")
	if out, err := build.CombinedOutput(); err != nil {
		t.Fatalf("go build: %v\n%s", err, out)
	}
	if out, err := exec.Command("gofmt", "-l", dir).CombinedOutput(); checkFmt && (err != nil || strings.TrimSpace(string(out)) != "") {
		t.Fatalf("gofmt: %s %v", out, err)
	}
	w.agent = newFakeAgent(t, w.ags)
	w.client = workerClient(w.sock)
	t.Cleanup(w.stop)
	return w
}

func (w *goWorker) start(stateDir string) {
	w.t.Helper()
	w.cmd = exec.Command(w.bin)
	w.cmd.Dir = w.dir
	w.cmd.Env = append(os.Environ(), "AGENT_WORKER=demo", "AGENT_WORKER_SOCKET="+w.sock, "AGENT_SOCKET="+w.ags,
		"AGENT_WORKER_TOKEN=tok", "WORKER_STATE_DIR="+stateDir)
	stdout, _ := w.cmd.StdoutPipe()
	w.cmd.Stderr = os.Stderr
	if err := w.cmd.Start(); err != nil {
		w.t.Fatal(err)
	}
	go func() {
		sc := bufio.NewScanner(stdout)
		for sc.Scan() {
			w.t.Log(sc.Text())
		}
	}()
	waitFor(w.t, "запуска воркера", 10*time.Second, func() bool {
		req, _ := http.NewRequest(http.MethodGet, "http://worker/health", nil)
		resp, err := w.client.Do(req)
		if err == nil {
			resp.Body.Close()
		}
		return err == nil && resp.StatusCode == http.StatusOK
	})
}

func (w *goWorker) stop() {
	if w.cmd != nil && w.cmd.Process != nil {
		_ = w.cmd.Process.Kill()
		_, _ = w.cmd.Process.Wait()
		w.cmd = nil
	}
}

// Заготовка на Go (agent worker new --lang go) по контракту §12: health, manifest, настройки,
// маршрут, события, задачи (ход, повтор jobId, busy, отмена, отказ), метрики.
func TestGoSkeleton(t *testing.T) {
	root := t.TempDir()
	if _, _, err := NewWorker(WorkerOptions{Root: root, Name: "demo", Lang: "go"}); err != nil {
		t.Fatal(err)
	}
	w := buildGoWorker(t, filepath.Join(root, "workers", "demo"), true)
	w.start("")
	c, a := w.client, w.agent

	if code, h, _ := call(t, c, "GET", "/health", nil); code != 200 || h["ok"] != true || h["busy"] != nil {
		t.Fatalf("health: %d %v", code, h)
	}
	_, m, raw := call(t, c, "GET", "/manifest", nil)
	for _, want := range []string{`"version":"0.1.0"`, `"key":"settings"`, `"path":"/hello"`, `"type":"demo.greeted"`, `"type":"demo.count"`} {
		if !strings.Contains(raw, want) {
			t.Fatalf("манифест: нет %s\n%s", want, raw)
		}
	}
	_ = m
	if code, _, _ := call(t, c, "PUT", "/config/settings", map[string]any{"version": 3, "data": map[string]any{"greeting": "хай"}}); code != 204 {
		t.Fatalf("config: %d", code)
	}
	if code, body, _ := call(t, c, "POST", "/hello", map[string]any{"name": "Аня"}); code != 200 || body["text"] != "хай, Аня" {
		t.Fatalf("hello: %d %v", code, body)
	}
	waitFor(t, "demo.greeted", 5*time.Second, func() bool { return a.find("demo.greeted", "") != nil })
	if code, _, _ := call(t, c, "DELETE", "/config/settings", nil); code != 204 {
		t.Fatalf("delete config: %d", code)
	}
	if code, _, _ := call(t, c, "POST", "/nope", nil); code != 404 && code != 405 {
		t.Fatalf("нет маршрута: %d", code)
	}

	code, started, _ := call(t, c, "POST", "/jobs", map[string]any{"type": "demo.count", "jobId": "j1", "data": map[string]any{"to": 2}})
	if code != 202 {
		t.Fatalf("задача: %d", code)
	}
	if _, again, _ := call(t, c, "POST", "/jobs", map[string]any{"type": "demo.count", "jobId": "j1", "data": map[string]any{"to": 2}}); again["id"] != started["id"] {
		t.Fatal("повтор jobId — та же задача")
	}
	if _, h, _ := call(t, c, "GET", "/health", nil); h["busy"] != true {
		t.Fatalf("busy: %v", h)
	}
	waitFor(t, "job.done", 10*time.Second, func() bool { return a.find("job.done", "j1") != nil })
	done := a.find("job.done", "j1")["data"].(map[string]any)
	if done["result"].(map[string]any)["counted"] != float64(2) || a.find("job.progress", "j1") == nil {
		t.Fatalf("итог: %v", done)
	}
	if _, view, _ := call(t, c, "GET", "/jobs/"+started["id"].(string), nil); view["state"] != "done" {
		t.Fatalf("состояние: %v", view)
	}

	_, long, _ := call(t, c, "POST", "/jobs", map[string]any{"type": "demo.count", "jobId": "j2", "data": map[string]any{"to": 30}})
	waitFor(t, "хода j2", 5*time.Second, func() bool { return a.find("job.progress", "j2") != nil })
	if code, body, _ := call(t, c, "POST", "/jobs/"+long["id"].(string)+"/cancel", nil); code != 200 || body["state"] != "cancelled" {
		t.Fatalf("отмена: %d %v", code, body)
	}
	waitFor(t, "job.cancelled", 5*time.Second, func() bool { return a.find("job.cancelled", "j2") != nil })

	call(t, c, "POST", "/jobs", map[string]any{"type": "demo.count", "jobId": "j3", "data": map[string]any{"to": 0}})
	waitFor(t, "job.failed", 5*time.Second, func() bool { return a.find("job.failed", "j3") != nil })
	if e := a.find("job.failed", "j3")["data"].(map[string]any)["error"].(map[string]any); e["code"] != "BAD_INPUT" {
		t.Fatalf("отказ: %v", e)
	}
	if code, _, _ := call(t, c, "POST", "/jobs", map[string]any{"type": "other"}); code != 400 {
		t.Fatalf("неизвестный тип: %d", code)
	}
	time.Sleep(300 * time.Millisecond)
	if _, mt, _ := call(t, c, "GET", "/metrics", nil); mt["jobsDone"] != float64(1) || mt["jobsCancelled"] != float64(1) || mt["jobsFailed"] != float64(1) {
		t.Fatalf("метрики: %v", mt)
	}
	if n := a.count("job.cancelled", "j2"); n != 1 {
		t.Fatalf("итог у задачи один, а job.cancelled — %d", n)
	}
}

// extraWorker — воркер с возможностями сверх заготовки: поток, проверка задачи, OnStart,
// продолжение задачи после перезапуска, уборка.
const extraWorker = `package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"time"
)

type Extra struct{ *Worker }

func (e *Extra) OnStart() { _ = e.Emit("x.started", map[string]any{"ok": true}) }

func (e *Extra) PrepareJob(typ string, data json.RawMessage, files JobFiles) error {
	var d struct{ Steps any ` + "`json:\"steps\"`" + ` }
	_ = json.Unmarshal(data, &d)
	if _, ok := d.Steps.(float64); !ok {
		return &HTTPError{Status: http.StatusBadRequest, Message: "steps — число"}
	}
	return nil
}

func (e *Extra) Cleanup() bool { return true }

func main() {
	w := &Extra{Worker: NewWorker()}
	w.Event("x.started", nil, "")
	w.Route("GET", "/stream", RouteOpts{}, func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "text/plain")
		for i := range 3 {
			fmt.Fprintf(rw, "строка %d\n", i)
			rw.(http.Flusher).Flush()
		}
	})
	w.Job("x.steps", JobOpts{Resumable: true}, func(j *Job) (any, error) {
		var d struct{ Steps int ` + "`json:\"steps\"`" + ` }
		_ = j.Decode(&d)
		var s struct{ Step int ` + "`json:\"step\"`" + ` }
		j.Restore(&s)
		for i := s.Step; i < d.Steps; i++ {
			if err := j.Save(map[string]int{"step": i + 1}); err != nil {
				return nil, err
			}
			if err := j.Progress(float64(i+1)/float64(d.Steps), ""); err != nil {
				return nil, err
			}
			if err := j.Sleep(300 * time.Millisecond); err != nil {
				return nil, err
			}
		}
		return map[string]int{"steps": d.Steps}, nil
	})
	Run(w)
}
`

func TestGoBaseExtra(t *testing.T) {
	dir := t.TempDir()
	base, _ := workerFiles.ReadFile("worker/go/agent_worker.go.tmpl")
	_ = os.WriteFile(filepath.Join(dir, "agent_worker.go"), base, 0o644)
	_ = os.WriteFile(filepath.Join(dir, "worker.go"), []byte(extraWorker), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module extra\n\ngo 1.22\n"), 0o644)
	_ = os.WriteFile(filepath.Join(dir, "VERSION"), []byte("1.0.0\n"), 0o644)
	w := buildGoWorker(t, dir, false)
	state := t.TempDir()
	w.start(state)
	c, a := w.client, w.agent

	waitFor(t, "OnStart", 5*time.Second, func() bool { return a.find("x.started", "") != nil })
	req, _ := http.NewRequest(http.MethodGet, "http://worker/stream", nil)
	resp, err := c.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	text, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if len(resp.TransferEncoding) == 0 || resp.TransferEncoding[0] != "chunked" || string(text) != "строка 0\nстрока 1\nстрока 2\n" {
		t.Fatalf("поток: %v %q", resp.TransferEncoding, text)
	}
	if code, _, _ := call(t, c, "POST", "/jobs", map[string]any{"type": "x.steps", "data": map[string]any{"steps": "много"}}); code != 400 {
		t.Fatalf("проверка задачи: %d", code)
	}

	call(t, c, "POST", "/jobs", map[string]any{"type": "x.steps", "jobId": "r1", "data": map[string]any{"steps": 20}})
	waitFor(t, "хода r1", 5*time.Second, func() bool { return a.count("job.progress", "r1") >= 2 })
	w.stop() // воркер упал посреди задачи
	w.start(state)
	waitFor(t, "job.done после перезапуска", 20*time.Second, func() bool { return a.find("job.done", "r1") != nil })
	if n := a.count("job.progress", "r1"); n >= 22 {
		t.Fatalf("задача началась заново: %d шагов", n)
	}

	call(t, c, "POST", "/jobs", map[string]any{"type": "x.steps", "jobId": "r2", "data": map[string]any{"steps": 50}})
	if code, _, _ := call(t, c, "POST", "/cleanup", nil); code != 204 {
		t.Fatalf("уборка: %d", code)
	}
	time.Sleep(500 * time.Millisecond)
	if entries, _ := os.ReadDir(filepath.Join(state, "jobs")); len(entries) != 0 {
		t.Fatalf("после уборки остались задачи: %v", entries)
	}
}
