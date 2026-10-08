package update

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
)

func TestInstallVerifiesAndBootCounts(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	newBin := []byte("#!new-binary")
	sum := sha256.Sum256(newBin)
	hash := hex.EncodeToString(sum[:])
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Agent a.s" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		_, _ = w.Write(newBin)
	}))
	defer srv.Close()

	dir := t.TempDir()
	bin := filepath.Join(dir, "agent")
	_ = os.WriteFile(bin, []byte("#!old"), 0o755)
	p := NewPaths(bin)
	rel := Release{Version: "2", URL: srv.URL, SHA256: hash, Signature: Sign(priv, hash)}

	forged := rel
	forged.Signature = Sign(priv, "другой")
	if err := Install(context.Background(), srv.Client(), "Agent a.s", p, pub, forged); err == nil {
		t.Fatal("чужая подпись должна отвергаться")
	}
	bad := rel
	bad.SHA256 = hex.EncodeToString(make([]byte, 32))
	bad.Signature = Sign(priv, bad.SHA256)
	if err := Install(context.Background(), srv.Client(), "Agent a.s", p, pub, bad); err == nil {
		t.Fatal("несовпадение sha256 должно отвергаться")
	}
	if got, _ := os.ReadFile(bin); string(got) != "#!old" {
		t.Fatal("после отказа файл не должен меняться")
	}

	if err := Install(context.Background(), srv.Client(), "Agent a.s", p, pub, rel); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(bin); string(got) != string(newBin) {
		t.Fatal("файл не заменён")
	}
	if got, _ := os.ReadFile(bin + ".prev"); string(got) != "#!old" {
		t.Fatal("нет копии прежней версии")
	}
	for range MaxBootAttempts {
		if rolledBack, err := Guard(p); err != nil || rolledBack {
			t.Fatal("до предела запусков — без отката", err)
		}
	}
	m, _ := readMarker(p.Marker)
	if m == nil || m.Attempts != MaxBootAttempts {
		t.Fatalf("счётчик запусков: %+v", m)
	}
	// Четвёртый запуск без связи — откат на прежнюю версию.
	rolledBack, err := Guard(p)
	if err != nil || !rolledBack {
		t.Fatalf("откат: %v %v", rolledBack, err)
	}
	if got, _ := os.ReadFile(bin); string(got) != "#!old" {
		t.Fatal("после отката — прежняя версия")
	}
	if _, err := os.Stat(p.Marker); !os.IsNotExist(err) {
		t.Fatal("после отката отметка снята")
	}
	_ = writeMarker(p.Marker, marker{Version: "2"})
	Healthy(p)
	if _, err := os.Stat(p.Marker); !os.IsNotExist(err) {
		t.Fatal("отметка снимается после связи")
	}
	if err := Install(context.Background(), srv.Client(), "Agent a.s", p, nil, rel); err == nil {
		t.Fatal("без ключа проверки обновление запрещено")
	}
}

// Fetch (сборка воркера): без ключа — ErrNotVerified; чужая подпись и
// неверный sha256 — ошибка, файла нет; верная — файл скачан.
func TestFetch(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	body := []byte("#!worker")
	sum := sha256.Sum256(body)
	hash := hex.EncodeToString(sum[:])
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
	defer srv.Close()
	dst := filepath.Join(t.TempDir(), "current.new")
	ctx := context.Background()

	if err := Fetch(ctx, srv.Client(), "", nil, srv.URL, hash, Sign(priv, hash), dst); !errors.Is(err, ErrNotVerified) {
		t.Fatalf("без ключа: %v", err)
	}
	if err := Fetch(ctx, srv.Client(), "", pub, srv.URL, hash, Sign(priv, "другой"), dst); err == nil {
		t.Fatal("чужая подпись")
	}
	wrong := hex.EncodeToString(make([]byte, 32))
	if err := Fetch(ctx, srv.Client(), "", pub, srv.URL, wrong, Sign(priv, wrong), dst); err == nil {
		t.Fatal("sha256 не сходится")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatal("после отказа файла быть не должно")
	}
	if err := Fetch(ctx, srv.Client(), "", pub, srv.URL, hash, Sign(priv, hash), dst); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(dst); string(got) != string(body) {
		t.Fatalf("файл: %q", got)
	}
}
