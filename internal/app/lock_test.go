//go:build unix

package app

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/identity"
	"github.com/epifanovmd/agent/internal/logx"
)

// Один агент на dataDir: второй New (или agent stop-workers) — ErrLocked; после
// Close — можно.
func TestDataDirLock(t *testing.T) {
	cfg := config.Defaults()
	cfg.Server.URL, cfg.Server.AllowInsecure = "http://127.0.0.1:1", true
	cfg.DataDir = t.TempDir()
	cfg.Log.Level = "error"
	first, err := New(cfg, "test")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(cfg, "test"); !errors.Is(err, ErrLocked) {
		t.Fatalf("второй агент: %v", err)
	}
	if _, err := LockDataDir(cfg.DataDir); !errors.Is(err, ErrLocked) {
		t.Fatalf("уборка при работающем агенте: %v", err)
	}
	first.Close()
	lock, err := LockDataDir(cfg.DataDir)
	if err != nil {
		t.Fatalf("после Close: %v", err)
	}
	lock.Unlock()
}

// agent status: не запущен — ErrNotRunning; блокировка взята, но API не отвечает — ошибка;
// ошибка завершения хранится в каталоге данных.
func TestStatus(t *testing.T) {
	dir := t.TempDir()
	if _, err := Status(dir); !errors.Is(err, ErrNotRunning) {
		t.Fatalf("не запущен: %v", err)
	}
	lock, err := LockDataDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Unlock()
	if _, err := Status(dir); err == nil || errors.Is(err, ErrNotRunning) {
		t.Fatalf("не отвечает — ошибка: %v", err)
	}
	RecordExit(dir, errors.New("токен отклонён"))
	if e := LastExit(dir); e == nil || e.Error != "токен отклонён" || e.At == 0 {
		t.Fatalf("ошибка завершения: %+v", e)
	}
}

// Renew: регистрация не удалась — прежний ключ остаётся на диске.
func TestRenewKeepsCredentialsOnFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer srv.Close()
	dir := t.TempDir()
	store := identity.NewStore(dir)
	old := identity.Credentials{AgentID: "a", Secret: "s"}
	if err := store.Save(old); err != nil {
		t.Fatal(err)
	}
	cfg := config.Defaults()
	cfg.Server.URL = srv.URL
	cfg.Enroll.Token = "t"
	a := &auth{store: store, cfg: cfg, client: srv.Client(), log: logx.Discard()}
	a.keys.Store(identity.NewKeys(store, old))
	if err := a.Renew(context.Background()); err == nil {
		t.Fatal("сервер недоступен — ошибка")
	}
	creds, ok, err := store.Load()
	if err != nil || !ok || creds.AgentID != "a" || creds.Secret != "s" {
		t.Fatalf("учётные данные: %+v %v %v", creds, ok, err)
	}
}
