package identity

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/epifanovmd/agent/sdk/go/message"
)

func TestStoreRoundTripAndPermissions(t *testing.T) {
	dir := t.TempDir()
	s := NewStore(dir)
	if _, ok, err := s.Load(); ok || err != nil {
		t.Fatalf("пусто: ok=%v err=%v", ok, err)
	}
	want := Credentials{AgentID: "a", Secret: "s"}
	if err := s.Save(want); err != nil {
		t.Fatal(err)
	}
	got, ok, err := s.Load()
	if err != nil || !ok || got != want {
		t.Fatalf("%v %v %v", got, ok, err)
	}
	info, _ := os.Stat(filepath.Join(dir, fileName))
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("права: %v", info.Mode().Perm())
	}
	if want.Authorization() != "Agent a.s" {
		t.Fatal(want.Authorization())
	}
	_ = s.Forget()
	if _, ok, _ := s.Load(); ok {
		t.Fatal("после Forget — пусто")
	}
}

func TestEnroll(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req EnrollRequest
		_ = json.NewDecoder(r.Body).Decode(&req)
		if r.URL.Path != message.EnrollPath || req.Token != "good" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"agentId":"id-1","secret":"sec"}`))
	}))
	defer srv.Close()

	c, err := Enroll(context.Background(), srv.Client(), srv.URL, EnrollRequest{Token: "good", Name: "n"})
	if err != nil || c.AgentID != "id-1" {
		t.Fatalf("%v %v", c, err)
	}
	if _, err := Enroll(context.Background(), srv.Client(), srv.URL, EnrollRequest{Token: "bad"}); !errors.Is(err, ErrTokenRejected) {
		t.Fatalf("плохой токен: %v", err)
	}
}

func TestKeysRotateFallbackPromote(t *testing.T) {
	s := NewStore(t.TempDir())
	creds := Credentials{AgentID: "a", Secret: "old"}
	if err := s.Save(creds); err != nil {
		t.Fatal(err)
	}
	k := NewKeys(s, creds)
	if k.Authorization() != "Agent a.old" || k.Fallback() {
		t.Fatal("без ожидающего — основной, отката нет")
	}

	hash, err := k.Rotate()
	if err != nil {
		t.Fatal(err)
	}
	saved, _, _ := s.Load()
	if saved.Secret != "old" || len(saved.PendingSecret) != 48 || SecretHash(saved.PendingSecret) != hash || len(hash) != 64 {
		t.Fatalf("ожидающий в файле: %+v hash=%s", saved, hash)
	}
	pending := "Agent a." + saved.PendingSecret

	// Повторный rotate заменяет ожидающий.
	hash2, _ := k.Rotate()
	saved2, _, _ := s.Load()
	if hash2 == hash || saved2.Secret != "old" || SecretHash(saved2.PendingSecret) != hash2 {
		t.Fatalf("повторный rotate: %+v", saved2)
	}
	pending = "Agent a." + saved2.PendingSecret

	// Сначала ожидающий; 401 — основной; снова 401 — отката нет, серия сначала.
	if k.Authorization() != pending {
		t.Fatal("первым — ожидающий")
	}
	if !k.Fallback() || k.Authorization() != "Agent a.old" {
		t.Fatal("откат на основной")
	}
	if k.Fallback() {
		t.Fatal("оба отклонены — отката нет")
	}
	if k.Authorization() != pending {
		t.Fatal("новая серия — снова с ожидающего")
	}

	// Принят основной — ожидающий остаётся, следующее подключение — снова с него.
	k.Fallback()
	if err := k.Accepted("Agent a.old"); err != nil {
		t.Fatal(err)
	}
	if k.Session() != "Agent a.old" || k.Authorization() != pending {
		t.Fatal("после принятия основного")
	}
	if got, _, _ := s.Load(); got != saved2 {
		t.Fatalf("файл не меняется: %+v", got)
	}

	// Принят ожидающий — становится основным.
	if err := k.Accepted(pending); err != nil {
		t.Fatal(err)
	}
	want := Credentials{AgentID: "a", Secret: saved2.PendingSecret}
	if got, _, _ := s.Load(); got != want || k.Credentials() != want {
		t.Fatalf("повышение: %+v", got)
	}
	if k.Authorization() != pending || k.Session() != pending || k.Fallback() {
		t.Fatal("после повышения — один секрет")
	}
}

// Ключ шифрования создаётся один раз (0600) и дальше читается тот же;
// повреждённый файл — ошибка, а не молча новый ключ.
func TestEncryptionKey(t *testing.T) {
	dir := t.TempDir()
	k1, err := EncryptionKey(dir)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dir, encryptionKeyFile))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("файл ключа: %v %v", info, err)
	}
	k2, err := EncryptionKey(dir)
	if err != nil || !k1.Equal(k2) {
		t.Fatalf("ключ не тот же: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, encryptionKeyFile), []byte("мусор"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := EncryptionKey(dir); err == nil {
		t.Fatal("повреждённый файл принят")
	}
}
