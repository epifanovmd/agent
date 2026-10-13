package app

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/releases"
)

// Проверка новой версии: новее своей — в hello и status, итог — в dataDir.
func TestCheckLoop(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/latest/download/manifest.json" {
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(message.Manifest{Version: "1.1.0"})
	}))
	defer srv.Close()
	cfg := config.Defaults()
	cfg.Server.URL = "http://127.0.0.1:1"
	cfg.DataDir = t.TempDir()
	cfg.Log.Level = "error"
	cfg.Update.Releases = srv.URL
	a, err := New(cfg, "1.0.0")
	if err != nil {
		t.Fatal(err)
	}
	defer a.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go a.checkLoop(ctx)
	deadline := time.Now().Add(5 * time.Second)
	for a.latest.Load() == nil && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if u := a.Hello().Agent.Update; u == nil || u.Latest != "1.1.0" || u.CheckedAt == 0 {
		t.Fatalf("hello: %+v", u)
	}
	// Режим обновления и способ установки — в hello.agent.
	InstallMode = "user"
	defer func() { InstallMode = "" }()
	if h := a.Hello().Agent; h.UpdateMode != config.UpdateSelf || h.Install != "user" {
		t.Fatalf("hello.agent: %+v", h)
	}
	if u := a.status().Update; u == nil || u.Latest != "1.1.0" {
		t.Fatalf("status: %+v", u)
	}
	if c := releases.ReadCheck(cfg.DataDir); c.Latest != "1.1.0" {
		t.Fatalf("итог в dataDir: %+v", c)
	}
}
