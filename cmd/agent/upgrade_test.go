package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/epifanovmd/agent/internal/update"
)

// agent upgrade в папке агента: программа заменяется новой, прежняя — .prev;
// подпись не сходится — программа не тронута.
func TestReplaceBinary(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	body := []byte("new agent")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { _, _ = w.Write(body) }))
	defer srv.Close()
	exe := filepath.Join(t.TempDir(), "agent")
	if err := os.WriteFile(exe, []byte("old agent"), 0o755); err != nil {
		t.Fatal(err)
	}
	rel := update.Release{Version: "1.1.0", URL: srv.URL + "/agent", SHA256: hash,
		Signature: update.Sign(priv, update.Local(update.AgentName, "1.1.0", hash))}

	other, _, _ := ed25519.GenerateKey(nil)
	if err := replaceBinary(context.Background(), exe, update.Keys{other}, rel); err == nil {
		t.Fatal("чужой ключ — ошибка")
	}
	if got, _ := os.ReadFile(exe); string(got) != "old agent" {
		t.Fatal("программа тронута при неверной подписи")
	}
	if err := replaceBinary(context.Background(), exe, update.Keys{pub}, rel); err != nil {
		t.Fatal(err)
	}
	got, _ := os.ReadFile(exe)
	prev, _ := os.ReadFile(exe + ".prev")
	if string(got) != "new agent" || string(prev) != "old agent" {
		t.Fatalf("%q %q", got, prev)
	}
}
