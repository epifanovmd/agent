package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseLimits(t *testing.T) {
	for in, want := range map[string]int64{
		"": 0, "512M": 512 << 20, "512Mi": 512 << 20, "512MB": 512 << 20, "2G": 2 << 30,
		"1.5G": 3 << 29, "100k": 100 << 10, "1048576": 1 << 20,
	} {
		if got, err := ParseMemory(in); err != nil || got != want {
			t.Errorf("ParseMemory(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"abc", "-1M", "0", "12X", "NaN", "Inf", "M"} {
		if _, err := ParseMemory(bad); err == nil {
			t.Errorf("ParseMemory(%q): ожидалась ошибка", bad)
		}
	}
	for in, want := range map[string]int64{"": 0, "50%": 50_000, "200%": 200_000, "1.5": 150_000, " 5% ": 5_000} {
		if got, err := ParseCPU(in); err != nil || got != want {
			t.Errorf("ParseCPU(%q) = %d, %v; want %d", in, got, err, want)
		}
	}
	for _, bad := range []string{"half", "0%", "-50%", "0.5%", "NaN", "%"} {
		if _, err := ParseCPU(bad); err == nil {
			t.Errorf("ParseCPU(%q): ожидалась ошибка", bad)
		}
	}
}

// workers[].limits и user читаются из agent.yaml; ошибки разбора и user без
// root — ошибки конфигурации.
func TestWorkerLimitsAndUser(t *testing.T) {
	defer func(f func() int) { geteuid = f }(geteuid)
	geteuid = func() int { return 0 }
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	_ = os.WriteFile(path, []byte(`
server:
  url: https://api.example.com
workers:
  - name: report
    command: ["/bin/report"]
    user: nobody
    limits: {memory: 512M, cpu: "50%", pids: 256}
`), 0o600)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	w := cfg.Workers[0]
	if w.User != "nobody" || w.Limits != (Limits{Memory: "512M", CPU: "50%", Pids: 256}) || w.Limits.Empty() {
		t.Fatalf("воркер: %+v", w)
	}

	geteuid = func() int { return 1000 }
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "workers[0].user") {
		t.Fatalf("user без root: %v", err)
	}
	cfg.Workers[0].User = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("без user: %v", err)
	}
	for _, l := range []Limits{{Memory: "много"}, {CPU: "половина"}, {Pids: -1}} {
		cfg.Workers[0].Limits = l
		if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "workers[0].limits") {
			t.Errorf("%+v: %v", l, err)
		}
	}
}
