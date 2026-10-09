//go:build unix

package app

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/identity"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/message"
)

// Один агент на dataDir: второй New (или agent cleanup) — ErrLocked; после
// Close — можно.
func TestDataDirLock(t *testing.T) {
	cfg := config.Defaults()
	cfg.Server.URL = "http://127.0.0.1:1"
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

// agent status: не запущен — ErrNotRunning; запущен и отмечался — ок;
// отметка устарела — ошибка.
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
	if _, err := Status(dir); err == nil {
		t.Fatal("без отметки — ошибка")
	}
	// Ошибка завершения видна, пока агент снова не отметился.
	RecordExit(dir, errors.New("токен отклонён"))
	if e := LastExit(dir); e == nil || e.Error != "токен отклонён" || e.At == 0 {
		t.Fatalf("ошибка завершения: %+v", e)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = heartbeat{path: filepath.Join(dir, StatusFile), snapshot: func() AgentStatus {
			return AgentStatus{Online: true, Version: "test", Workers: []message.WorkerStatus{{Name: "echo", State: message.WorkerRunning}}}
		}}.run(ctx)
		close(done)
	}()
	var st AgentStatus
	deadline := time.Now().Add(3 * time.Second)
	for st, err = Status(dir); err != nil && time.Now().Before(deadline); st, err = Status(dir) {
		time.Sleep(10 * time.Millisecond)
	}
	if err != nil || !st.Online || st.PID != os.Getpid() || st.Version != "test" || len(st.Workers) != 1 {
		t.Fatalf("работает: %+v %v", st, err)
	}
	if LastExit(dir) != nil {
		t.Fatal("агент работает — ошибки завершения нет")
	}
	cancel()
	<-done
	old, _ := json.Marshal(AgentStatus{PID: 1, At: time.Now().Add(-time.Hour).UnixMilli()})
	_ = os.WriteFile(filepath.Join(dir, StatusFile), old, 0o600)
	if _, err := Status(dir); err == nil {
		t.Fatal("старая отметка — завис")
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
