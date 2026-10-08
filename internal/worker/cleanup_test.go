//go:build unix

package worker

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// agent cleanup: каждый воркер запускается одной копией, получает
// worker.cleanup; итог — по каждому: убрал, отказал, не ответил, не запустился.
func TestCleanup(t *testing.T) {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	mark := filepath.Join(t.TempDir(), "mark")
	ipcLog := filepath.Join(t.TempDir(), "ipc")
	spec := func(name string, env map[string]string) config.Worker {
		env["TEST_WORKER"] = "1"
		return config.Worker{Name: name, Command: []string{exe}, Env: env, Replicas: 3,
			StopTimeout: config.Duration(500 * time.Millisecond)}
	}
	specs := []config.Worker{
		spec("ok", map[string]string{"CLEANUP_MARK": mark, "IPC_LOG": ipcLog}),
		spec("fail", map[string]string{"WORKER_CLEANUP": "fail"}),
		spec("silent", map[string]string{"WORKER_CLEANUP": "silent"}),
		{Name: "missing", Command: []string{filepath.Join(t.TempDir(), "nope")}, Replicas: 1, StopTimeout: config.Duration(time.Second)},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	wctx := message.WorkerContext{Agent: message.WorkerContextAgent{ID: "a1", Name: "node-01", Version: "test"}, LogLevel: "warn"}
	results := Cleanup(ctx, specs, logx.Discard(), wctx)
	if len(results) != 4 {
		t.Fatalf("итогов: %d", len(results))
	}
	if results[0].Worker != "ok" || results[0].Err != nil {
		t.Fatalf("ok: %+v", results[0])
	}
	if raw, _ := os.ReadFile(mark); string(raw) != "cleaned" {
		t.Fatal("воркер не получил worker.cleanup")
	}
	// Контекст с mode cleanup — после worker.ready и до worker.cleanup.
	lines := ipcLines(t, ipcLog)
	if len(lines) < 3 || !strings.HasPrefix(lines[1], message.TypeWorkerContext+" ") || !strings.HasPrefix(lines[2], message.TypeWorkerCleanup+" ") {
		t.Fatalf("порядок сообщений уборки: %v", lines)
	}
	if c := contexts(t, ipcLog)[0]; c.Mode != message.WorkerModeCleanup || c.Agent.ID != "a1" || c.Online {
		t.Fatalf("контекст уборки: %+v", c)
	}
	if results[1].Err == nil || !strings.Contains(results[1].Err.Error(), "правило не снято") {
		t.Fatalf("fail: %+v", results[1])
	}
	if results[2].Err == nil || !strings.Contains(results[2].Err.Error(), "нет ответа") {
		t.Fatalf("silent: %+v", results[2])
	}
	if results[3].Err == nil {
		t.Fatalf("missing: %+v", results[3])
	}
	if time.Since(start) > 15*time.Second {
		t.Fatalf("уборка шла слишком долго: %s", time.Since(start))
	}
}
