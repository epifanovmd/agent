package cgroup

import (
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

func read(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// fakeRoot — подставной корень cgroupfs: группа службы агента с процессами
// и контроллерами.
func fakeRoot(t *testing.T, controllers string) (root, service string) {
	root = t.TempDir()
	service = filepath.Join(root, "system.slice", "agent.service")
	if err := os.MkdirAll(service, 0o755); err != nil {
		t.Fatal(err)
	}
	_ = os.WriteFile(filepath.Join(root, "cgroup.controllers"), []byte("cpuset cpu io memory pids\n"), 0o644)
	_ = os.WriteFile(filepath.Join(service, "cgroup.controllers"), []byte(controllers+"\n"), 0o644)
	_ = os.WriteFile(filepath.Join(service, "cgroup.procs"), []byte("4242\n"), 0o644)
	return root, service
}

func TestDetectAndWorker(t *testing.T) {
	root, service := fakeRoot(t, "cpuset cpu io memory pids")
	m, err := Detect(root, []byte("0::/system.slice/agent.service\n"), slog.New(slog.DiscardHandler))
	if err != nil {
		t.Fatal(err)
	}
	if m.Dir() != service {
		t.Fatalf("группа агента %s", m.Dir())
	}
	// Агент и прочие процессы группы — в подгруппе agent; контроллеры включены.
	procs := strings.Fields(read(t, filepath.Join(service, "agent", "cgroup.procs")))
	if strings.Join(procs, " ") != "4242 "+strconv.Itoa(os.Getpid()) {
		t.Fatalf("agent/cgroup.procs: %v", procs)
	}
	if got := strings.Fields(read(t, filepath.Join(service, "cgroup.subtree_control"))); strings.Join(got, " ") != "+cpu +memory +pids" {
		t.Fatalf("subtree_control: %v", got)
	}

	g, err := m.Worker("report/1", Limits{MemoryMax: 512 << 20, CPUQuota: 50_000, CPUPeriod: 100_000, PidsMax: 256})
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(service, "worker-report_1")
	if g.Dir() != dir {
		t.Fatalf("группа воркера %s", g.Dir())
	}
	for file, want := range map[string]string{"memory.max": "536870912", "cpu.max": "50000 100000", "pids.max": "256"} {
		if got := strings.TrimSpace(read(t, filepath.Join(dir, file))); got != want {
			t.Errorf("%s = %q, want %q", file, got, want)
		}
	}
	if err := g.Add(777); err != nil {
		t.Fatal(err)
	}
	if err := g.Add(778); err != nil {
		t.Fatal(err)
	}
	if got := strings.Fields(read(t, filepath.Join(dir, "cgroup.procs"))); strings.Join(got, " ") != "777 778" {
		t.Fatalf("cgroup.procs воркера: %v", got)
	}

	// Снятое ограничение — max.
	if _, err := m.Worker("report/1", Limits{MemoryMax: 1 << 30}); err != nil {
		t.Fatal(err)
	}
	for file, want := range map[string]string{"memory.max": "1073741824", "cpu.max": "max 100000", "pids.max": "max"} {
		if got := strings.TrimSpace(read(t, filepath.Join(dir, file))); got != want {
			t.Errorf("после изменения %s = %q, want %q", file, got, want)
		}
	}
}

// Перезапуск внутри подгруппы agent — группа агента та же.
func TestDetectFromAgentSubgroup(t *testing.T) {
	root, service := fakeRoot(t, "memory pids")
	_ = os.MkdirAll(filepath.Join(service, "agent"), 0o755)
	m, err := Detect(root, []byte("0::/system.slice/agent.service/agent\n"), slog.New(slog.DiscardHandler))
	if err != nil || m.Dir() != service {
		t.Fatalf("%v %v", m, err)
	}
	// Контроллера cpu нет — cpu.max не пишется, остальное — да.
	g, err := m.Worker("x", Limits{CPUQuota: 50_000, PidsMax: 10})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(g.Dir(), "cpu.max")); !os.IsNotExist(err) {
		t.Fatalf("cpu.max без контроллера: %v", err)
	}
	if got := strings.TrimSpace(read(t, filepath.Join(g.Dir(), "pids.max"))); got != "10" {
		t.Fatalf("pids.max %q", got)
	}
}

func TestDetectUnavailable(t *testing.T) {
	// cgroup v1 / нет cgroupfs.
	if _, err := Detect(t.TempDir(), []byte("0::/x\n"), slog.New(slog.DiscardHandler)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("без cgroup.controllers: %v", err)
	}
	root, _ := fakeRoot(t, "memory")
	if _, err := Detect(root, []byte("1:name=systemd:/x\n"), slog.New(slog.DiscardHandler)); !errors.Is(err, ErrUnavailable) {
		t.Fatalf("без записи 0::: %v", err)
	}
	// Группа не наша (нет делегирования) — каталог не создать.
	if os.Geteuid() != 0 {
		root, service := fakeRoot(t, "memory")
		_ = os.Chmod(service, 0o555)
		defer os.Chmod(service, 0o755)
		if _, err := Detect(root, []byte("0::/system.slice/agent.service\n"), slog.New(slog.DiscardHandler)); err == nil {
			t.Fatal("без права записи: ожидалась ошибка")
		}
	}
}
