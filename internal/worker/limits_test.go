//go:build unix

package worker

import (
	"context"
	"log/slog"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/cgroup"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/jobs"
	"github.com/epifanovmd/agent/internal/logx"
)

// startLimited — супервизор с одним воркером echo с ограничениями.
func startLimited(t *testing.T, limits config.Limits) (*Supervisor, *jobs.Manager) {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	manager := jobs.New(&recorder{}, logx.Discard(), func() {})
	spec := config.Worker{
		Name: "echo", Command: []string{exe}, Env: map[string]string{"TEST_WORKER": "1"}, Replicas: 1,
		Queues: []string{"echo"}, StopTimeout: config.Duration(5 * time.Second), Limits: limits,
	}
	sup := New([]config.Worker{spec}, manager, logx.Discard(), func() {}, "test")
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = sup.Start(ctx) }()
	t.Cleanup(func() {
		stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		sup.Stop(stopCtx)
		cancel()
	})
	return sup, manager
}

// Воркер с limits — в подгруппе worker-<имя> с memory.max/cpu.max/pids.max.
func TestWorkerLimitsCgroup(t *testing.T) {
	root := t.TempDir()
	service := filepath.Join(root, "agent.service")
	_ = os.MkdirAll(service, 0o755)
	_ = os.WriteFile(filepath.Join(root, "cgroup.controllers"), []byte("cpu memory pids\n"), 0o644)
	_ = os.WriteFile(filepath.Join(service, "cgroup.controllers"), []byte("cpu memory pids\n"), 0o644)
	_ = os.WriteFile(filepath.Join(service, "cgroup.procs"), nil, 0o644)
	defer func(f func(*slog.Logger) (*cgroup.Manager, error)) { systemCgroups = f }(systemCgroups)
	systemCgroups = func(log *slog.Logger) (*cgroup.Manager, error) {
		return cgroup.Detect(root, []byte("0::/agent.service\n"), log)
	}

	sup, manager := startLimited(t, config.Limits{Memory: "512M", CPU: "50%", Pids: 256})
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	sup.mu.Lock()
	pid := sup.workers["echo"].slots[0].current.cmd.Process.Pid
	sup.mu.Unlock()

	dir := filepath.Join(service, "worker-echo")
	for file, want := range map[string]string{
		"memory.max": "536870912", "cpu.max": "50000 100000", "pids.max": "256", "cgroup.procs": strconv.Itoa(pid),
	} {
		raw, _ := os.ReadFile(filepath.Join(dir, file))
		if got := strings.TrimSpace(string(raw)); got != want {
			t.Errorf("%s = %q, want %q", file, got, want)
		}
	}
	raw, _ := os.ReadFile(filepath.Join(service, "agent", "cgroup.procs"))
	if strings.TrimSpace(string(raw)) != strconv.Itoa(os.Getpid()) {
		t.Errorf("агент не перенесён в подгруппу agent: %q", raw)
	}
}

// Нет cgroup v2 — воркер запускается без ограничений.
func TestWorkerLimitsUnavailable(t *testing.T) {
	defer func(f func(*slog.Logger) (*cgroup.Manager, error)) { systemCgroups = f }(systemCgroups)
	calls := 0
	systemCgroups = func(*slog.Logger) (*cgroup.Manager, error) {
		calls++
		return nil, cgroup.ErrUnavailable
	}
	_, manager := startLimited(t, config.Limits{Memory: "64M"})
	waitFor(t, "регистрация без ограничений", func() bool { return slots(manager)["echo"] == 2 })
	if calls != 1 {
		t.Fatalf("проверка cgroup: %d раз", calls)
	}
}

// user — uid/gid пользователя и его HOME; неизвестный — ошибка запуска.
func TestRunAs(t *testing.T) {
	me, err := user.Current()
	if err != nil {
		t.Skip(err)
	}
	cmd := exec.Command("/bin/true")
	if err := runAs(cmd, me.Username); err != nil {
		t.Fatal(err)
	}
	cred := cmd.SysProcAttr.Credential
	if strconv.Itoa(int(cred.Uid)) != me.Uid || strconv.Itoa(int(cred.Gid)) != me.Gid {
		t.Fatalf("credential %+v, пользователь %+v", cred, me)
	}
	if !strings.Contains(strings.Join(cmd.Env, "\n"), "HOME="+me.HomeDir) {
		t.Fatalf("env %v", cmd.Env)
	}
	if err := runAs(exec.Command("/bin/true"), "no-such-user-agent-test"); err == nil {
		t.Fatal("неизвестный пользователь: ожидалась ошибка")
	}
}
