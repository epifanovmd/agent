package app

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/releases"
	"github.com/epifanovmd/agent/internal/update"
)

// Воркер from: agent без сборки на диске: агент берёт её из релиза своей
// версии под эту машину; подпись сверяется ключами агента; есть — не качает.
func TestFetchAgentWorkers(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	body := []byte("#!/bin/sh\nexit 0\n")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	signature := update.Sign(priv, update.Build{Name: "probe", Version: "1.2.3", OS: runtime.GOOS, Arch: runtime.GOARCH, SHA256: hash})
	hits := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/download/v1.2.3/manifest.json":
			hits++
			_ = json.NewEncoder(w).Encode(message.Manifest{Version: "1.2.3", Workers: []message.WorkerArtifact{
				{Name: "probe", Version: "1.2.3", OS: runtime.GOOS, Arch: runtime.GOARCH, File: "probe-bin", SHA256: hash, Signature: signature},
			}})
		case "/download/v1.2.3/probe-bin":
			_, _ = w.Write(body)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()

	prev := BuiltinUpdateKey
	t.Cleanup(func() { BuiltinUpdateKey = prev })
	for _, tc := range []struct {
		key  []byte
		want bool
	}{{pub, true}, {make([]byte, ed25519.PublicKeySize), false}} {
		BuiltinUpdateKey = base64.StdEncoding.EncodeToString(tc.key)
		cfg := config.Defaults()
		cfg.Server.URL = "http://127.0.0.1:1"
		cfg.DataDir = t.TempDir()
		cfg.Log.Level = "error"
		cfg.Update.Releases = srv.URL
		cfg.Workers = []config.Worker{{Name: "probe", From: config.FromAgent}}
		if err := cfg.Validate(); err != nil {
			t.Fatal(err)
		}
		a, err := New(cfg, "1.2.3")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		a.fetchAgentWorkers(ctx, a.config())
		w := a.config().Workers[0]
		got, err := os.ReadFile(w.Current())
		if tc.want != (err == nil) || (tc.want && string(got) != string(body)) {
			a.Close()
			t.Fatalf("ключ %x: сборка %q %v", tc.key[:2], got, err)
		}
		if tc.want {
			version, _ := os.ReadFile(filepath.Join(w.ReleaseDir, config.ReleaseVersion))
			if strings.TrimSpace(string(version)) != "1.2.3" {
				t.Fatalf("версия: %q", version)
			}
			// Сборка есть — каталог сборок не нужен.
			before := hits
			a.fetchAgentWorkers(ctx, a.config())
			if hits != before {
				t.Fatal("сборка есть, а агент снова пошёл в каталог")
			}
		}
		a.Close()
	}
}

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
	if u := a.status().Update; u == nil || u.Latest != "1.1.0" {
		t.Fatalf("status: %+v", u)
	}
	if c := releases.ReadCheck(cfg.DataDir); c.Latest != "1.1.0" {
		t.Fatalf("итог в dataDir: %+v", c)
	}
}
