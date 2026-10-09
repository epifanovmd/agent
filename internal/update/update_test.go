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
	"strings"
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
	rel := Release{Version: "2", URL: srv.URL, SHA256: hash, Signature: Sign(priv, Local(AgentName, "2", hash))}

	forged := rel
	forged.Signature = Sign(priv, Local(AgentName, "3", hash)) // подпись другой версии
	if err := Install(context.Background(), srv.Client(), "Agent a.s", p, pub, forged); err == nil {
		t.Fatal("подпись другой версии должна отвергаться")
	}
	bad := rel
	bad.SHA256 = hex.EncodeToString(make([]byte, 32))
	bad.Signature = Sign(priv, Local(AgentName, "2", bad.SHA256))
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

// sha256 прописными в agent.update: подпись сходится, файл скачан и
// заменён; повторная команда с тем же файлом ничего не скачивает.
func TestInstallUpperCaseHash(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	newBin := []byte("#!new-binary")
	sum := sha256.Sum256(newBin)
	hash := strings.ToUpper(hex.EncodeToString(sum[:]))
	downloads := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		downloads++
		_, _ = w.Write(newBin)
	}))
	defer srv.Close()
	bin := filepath.Join(t.TempDir(), "agent")
	_ = os.WriteFile(bin, []byte("#!old"), 0o755)
	p := NewPaths(bin)
	rel := Release{Version: "2", URL: srv.URL, SHA256: hash, Signature: Sign(priv, Local(AgentName, "2", hash))}
	for range 2 {
		if err := Install(context.Background(), srv.Client(), "", p, pub, rel); err != nil {
			t.Fatal(err)
		}
	}
	if got, _ := os.ReadFile(bin); string(got) != string(newBin) {
		t.Fatal("файл не заменён")
	}
	if downloads != 1 {
		t.Fatalf("загрузок: %d, ожидалась одна", downloads)
	}
	if m, _ := readMarker(p.Marker); m == nil || m.SHA256 != strings.ToLower(hash) {
		t.Fatalf("отметка: %+v", m)
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
	b := Local("report", "1.0.0", hash)

	if err := Fetch(ctx, srv.Client(), "", nil, b, srv.URL, Sign(priv, b), dst); !errors.Is(err, ErrNotVerified) {
		t.Fatalf("без ключа: %v", err)
	}
	other := b
	other.Name = "other" // подпись сборки другого воркера
	if err := Fetch(ctx, srv.Client(), "", pub, b, srv.URL, Sign(priv, other), dst); err == nil {
		t.Fatal("подпись другого воркера")
	}
	wrong := Local("report", "1.0.0", hex.EncodeToString(make([]byte, 32)))
	if err := Fetch(ctx, srv.Client(), "", pub, wrong, srv.URL, Sign(priv, wrong), dst); err == nil {
		t.Fatal("sha256 не сходится")
	}
	if _, err := os.Stat(dst); !os.IsNotExist(err) {
		t.Fatal("после отказа файла быть не должно")
	}
	if err := Fetch(ctx, srv.Client(), "", pub, b, srv.URL, Sign(priv, b), dst); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(dst); string(got) != string(body) {
		t.Fatalf("файл: %q", got)
	}
	// sha256 прописными: подпись и проверка файла сходятся одинаково.
	upper := Local("report", "1.0.0", strings.ToUpper(hash))
	if err := Fetch(ctx, srv.Client(), "", pub, upper, srv.URL, Sign(priv, upper), dst); err != nil {
		t.Fatalf("sha256 прописными: %v", err)
	}
}

// Подписываемая строка — по §11: шесть строк через \n, sha256 строчными;
// подпись другой платформы не подходит.
func TestBuildPayload(t *testing.T) {
	b := Build{Name: "agent", Version: "1.0.0", OS: "linux", Arch: "amd64", SHA256: "AB01"}
	if got := b.Payload(); got != "agent-release/1\nagent\n1.0.0\nlinux\namd64\nab01" {
		t.Fatalf("строка: %q", got)
	}
	pub, priv, _ := ed25519.GenerateKey(nil)
	sig := Sign(priv, b)
	if Verify(pub, b, sig) != nil {
		t.Fatal("своя подпись")
	}
	arm := b
	arm.Arch = "arm64"
	if Verify(pub, arm, sig) == nil {
		t.Fatal("подпись другой платформы")
	}
}
