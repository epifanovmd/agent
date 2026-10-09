package sysmetrics

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

// Воркер по контракту (§12): GET /metrics — группы из EnvSettings, вторая
// точка — со скоростями; GET /health — ok; GET /manifest — версия агента
// (обязательный минимум воркера); SIGTERM (отмена) — выход.
func TestRunHTTP(t *testing.T) {
	dir, _ := os.MkdirTemp("", "m")
	defer os.RemoveAll(dir)
	sock := filepath.Join(dir, "http.sock")
	t.Setenv(message.EnvWorkerSocket, sock)
	t.Setenv(EnvSettings, `{"metrics":["memory","uptime","network"],"excludeInterfaces":["lo"]}`)
	t.Setenv(message.EnvVersion, "1.2.3")
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx) }()
	client := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}
	get := func(path string, v any) int {
		t.Helper()
		var resp *http.Response
		var err error
		for range 100 {
			if resp, err = client.Get("http://worker" + path); err == nil {
				break
			}
			time.Sleep(20 * time.Millisecond)
		}
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		_ = json.NewDecoder(resp.Body).Decode(v)
		return resp.StatusCode
	}
	var h message.HostMetrics
	if code := get("/metrics", &h); code != 200 || h.MemTotalBytes == nil || h.UptimeSec == nil || h.CPUPercent != nil || h.Load1 != nil {
		t.Fatalf("метрики: %d %+v", code, h)
	}
	var health message.Health
	if code := get("/health", &health); code != 200 || !health.OK {
		t.Fatalf("health: %d %+v", code, health)
	}
	var manifest json.RawMessage
	if code := get("/manifest", &manifest); code != 200 {
		t.Fatalf("manifest: %d", code)
	}
	if m, err := message.ParseWorkerManifest(manifest); err != nil || m.Version != "1.2.3" {
		t.Fatalf("manifest: %v %s", err, manifest)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("воркер не остановился")
	}
}

// Без сокета или с неверными настройками — ошибка запуска.
func TestRunErrors(t *testing.T) {
	t.Setenv(message.EnvWorkerSocket, "")
	if err := Run(context.Background()); err == nil {
		t.Fatal("без сокета")
	}
	t.Setenv(EnvSettings, "{")
	if err := Run(context.Background()); err == nil {
		t.Fatal("неверные настройки")
	}
}
