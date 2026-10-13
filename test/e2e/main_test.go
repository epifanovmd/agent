//go:build unix && e2e

// Package e2e — сквозные тесты: настоящий агент (собирается в TestMain), воркеры-примеры echo
// (Python) и sysinfo (Go) и бэкенд в процессе теста (backend_test.go).
package e2e

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/app"
)

// Собранные программы и корень репозитория.
var (
	agentBin   string
	sysinfoBin string
	// sysinfoNext — sysinfo версии 2.0.0 (worker.update).
	sysinfoNext string
	root        string
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "e2e")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	root, _ = filepath.Abs("../..")
	agentBin, sysinfoBin, sysinfoNext = filepath.Join(dir, "agent"), filepath.Join(dir, "sysinfo"), filepath.Join(dir, "sysinfo-2")
	for _, build := range [][]string{
		{"-o", agentBin, "./cmd/agent"},
		{"-o", sysinfoBin, "./examples/workers/sysinfo"},
		{"-ldflags", "-X main.version=2.0.0", "-o", sysinfoNext, "./examples/workers/sysinfo"},
	} {
		pkg := build[len(build)-1]
		cmd := exec.Command("go", append([]string{"build"}, build...)...)
		cmd.Dir, cmd.Stdout, cmd.Stderr = root, os.Stderr, os.Stderr
		if err := cmd.Run(); err != nil {
			fmt.Fprintln(os.Stderr, "сборка", pkg+":", err)
			os.Exit(1)
		}
	}
	if _, err := exec.LookPath("python3"); err != nil {
		fmt.Fprintln(os.Stderr, "нужен python3 (воркер echo)")
		os.Exit(1)
	}
	code := m.Run()
	os.RemoveAll(dir)
	os.Exit(code)
}

// node — узел: каталог с agent.yaml и запущенный агент.
type node struct {
	t    *testing.T
	dir  string
	cfg  string
	cmd  *exec.Cmd
	mu   sync.Mutex
	out  bytes.Buffer
	done chan struct{}
}

// lockedWriter — вывод агента для журнала теста.
type lockedWriter struct{ n *node }

func (w lockedWriter) Write(p []byte) (int, error) {
	w.n.mu.Lock()
	defer w.n.mu.Unlock()
	return w.n.out.Write(p)
}

// shortDir — короткий каталог (путь сокета ограничен по длине).
func shortDir(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "n")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { os.RemoveAll(dir) })
	return dir
}

// config — agent.yaml узла: бэкенд url ("" — без бэкенда), воркеры echo и sysinfo.
func config(dir, url, extra string) string {
	server := ""
	if url != "" {
		server = fmt.Sprintf("server:\n  url: %s\n  allowInsecure: true\nenroll:\n  token: e2e-token\n", url)
	}
	return server + fmt.Sprintf(`dataDir: %s/data
name: node-e2e
labels: { zone: test }
log:
  level: debug
  forward: info
update:
  mode: disabled
workers:
  - name: echo
    command: [python3, %s/examples/workers/echo/main.py]
    lifecycle: { stopTimeout: 5s }
  - name: sysinfo
    command: [%s]
    lifecycle: { stopTimeout: 5s }
`, dir, root, sysinfoBin) + extra
}

// newNode — узел с настройками config(dir, url) и запущенный агент.
func newNode(t *testing.T, url string) *node {
	t.Helper()
	return newNodeWith(t, func(dir string) string { return config(dir, url, "") }, nil)
}

// newNodeWith — узел с настройками cfg(dir); prepare(dir) — до запуска агента.
func newNodeWith(t *testing.T, cfg func(dir string) string, prepare func(dir string)) *node {
	t.Helper()
	dir := shortDir(t)
	n := &node{t: t, dir: dir, cfg: filepath.Join(dir, "agent.yaml")}
	if prepare != nil {
		prepare(dir)
	}
	if err := os.WriteFile(n.cfg, []byte(cfg(dir)), 0o600); err != nil {
		t.Fatal(err)
	}
	n.start()
	t.Cleanup(func() {
		n.stop()
		// Воркеры, оставшиеся после остановки агента, — тоже.
		_ = exec.Command(agentBin, "stop-workers", "-config", n.cfg).Run()
		if t.Failed() {
			n.mu.Lock()
			t.Logf("вывод агента:\n%s", n.out.String())
			n.mu.Unlock()
		}
	})
	return n
}

// start — запустить агента.
func (n *node) start() {
	n.t.Helper()
	n.cmd = exec.Command(agentBin, "run", "-config", n.cfg)
	n.cmd.Stdout, n.cmd.Stderr = lockedWriter{n}, lockedWriter{n}
	n.cmd.Env = append(os.Environ(), "AGENT_BOOT_GUARD=external")
	if err := n.cmd.Start(); err != nil {
		n.t.Fatal(err)
	}
	n.done = make(chan struct{})
	go func(cmd *exec.Cmd, done chan struct{}) {
		_ = cmd.Wait()
		close(done)
	}(n.cmd, n.done)
}

// signal — сигнал агенту и ожидание выхода.
func (n *node) signal(sig syscall.Signal) {
	if n.cmd == nil {
		return
	}
	_ = n.cmd.Process.Signal(sig)
	select {
	case <-n.done:
	case <-time.After(30 * time.Second):
		_ = n.cmd.Process.Kill()
		<-n.done
	}
	n.cmd = nil
}

// stop — SIGTERM: агент выходит, воркеры работают дальше.
func (n *node) stop() { n.signal(syscall.SIGTERM) }

// admin — клиент локального API агента.
func (n *node) admin() *app.Admin {
	n.t.Helper()
	var c *app.Admin
	deadline := time.Now().Add(30 * time.Second)
	for {
		var err error
		if c, err = app.NewAdmin(filepath.Join(n.dir, "data")); err == nil {
			if err = c.Do(context.Background(), http.MethodGet, "/status", nil, nil); err == nil {
				return c
			}
		}
		if time.Now().After(deadline) {
			n.t.Fatalf("локальный API агента: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// output — вывод агента содержит text.
func (n *node) output(text string) bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return strings.Contains(n.out.String(), text)
}

// eventually — дождаться условия (не дольше 60 с).
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// execCommand — команда агента (для его подкоманд: status, command).
func execCommand(ctx context.Context, name string, args ...string) *exec.Cmd {
	return exec.CommandContext(ctx, name, args...)
}
