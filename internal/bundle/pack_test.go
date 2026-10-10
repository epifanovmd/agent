package bundle

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"

	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/update"
)

// folder — папка агента: agent.yaml (воркер из папки и воркер из релиза
// агента), agent.prod.yaml поверх, .env.prod с секретом.
func folder(t *testing.T, releasesURL string) string {
	t.Helper()
	dir := t.TempDir()
	files := map[string]string{
		"agent.yaml": "envFiles: [.env]\ndataDir: .data\nworkers:\n  - path: workers/echo\n  - name: probe\n    from: agent\n" +
			"update: { releases: " + releasesURL + " }\n",
		"agent.prod.yaml":                "extends: agent.yaml\nenvFiles: [.env.prod]\nlog: { format: json }\n",
		".env.prod":                      "AGENT_ENROLL_TOKEN=secret\n",
		"workers/echo/run":               "#!/bin/sh\nexec sleep 1\n",
		"workers/echo/VERSION":           "1.4.0\n",
		"workers/echo/__pycache__/x.pyc": "junk",
	}
	for name, body := range files {
		p := filepath.Join(dir, name)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		mode := os.FileMode(0o644)
		if filepath.Base(name) == "run" {
			mode = 0o755
		}
		if err := os.WriteFile(p, []byte(body), mode); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

// Архив: программа, файлы настроек без секретов, воркер из папки и его
// подписанная сборка, сборка воркера из релиза агента; bundle.json.
func TestPack(t *testing.T) {
	probe := []byte("probe build")
	sum := sha256.Sum256(probe)
	probeHash := hex.EncodeToString(sum[:])
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/download/v1.2.0/manifest.json":
			_ = json.NewEncoder(w).Encode(message.Manifest{Version: "1.2.0", Workers: []message.WorkerArtifact{
				{Name: "probe", Version: "1.2.0", OS: runtime.GOOS, Arch: runtime.GOARCH, File: "probe-1.2.0", SHA256: probeHash, Signature: "author"},
			}})
		case "/download/v1.2.0/probe-1.2.0":
			_, _ = w.Write(probe)
		default:
			http.NotFound(w, r)
		}
	}))
	defer srv.Close()
	dir := folder(t, srv.URL)
	self := filepath.Join(t.TempDir(), "agent-self")
	_ = os.WriteFile(self, []byte("agent program"), 0o755)
	pub, priv, _ := ed25519.GenerateKey(nil)
	out := t.TempDir()
	shared := filepath.Join(t.TempDir(), "release")
	archives, err := Pack(context.Background(), Options{
		Config: filepath.Join(dir, "agent.prod.yaml"), Env: "prod", Out: out, ReleaseOut: shared,
		Platforms: []Platform{{runtime.GOOS, runtime.GOARCH}},
		Version:   "1.2.0", Self: self, Signing: priv, Client: http.DefaultClient,
		Warn: func(s string) { t.Log(s) },
	})
	if err != nil {
		t.Fatal(err)
	}
	want := filepath.Join(out, "agent-prod-1.2.0-"+runtime.GOOS+"-"+runtime.GOARCH+".tar.gz")
	if len(archives) != 1 || archives[0] != want {
		t.Fatalf("архивы: %v", archives)
	}
	x := filepath.Join(t.TempDir(), "x")
	if err := update.Extract(archives[0], x); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(x, Dir)
	for _, name := range []string{"agent", "agent.yaml", "agent.prod.yaml", "workers/echo/run", "workers/echo/VERSION", InfoFile, "release/manifest.json", "release/probe-1.2.0"} {
		if _, err := os.Stat(filepath.Join(root, name)); err != nil {
			t.Errorf("нет %s", name)
		}
	}
	for _, name := range []string{".env.prod", "workers/echo/__pycache__"} {
		if _, err := os.Stat(filepath.Join(root, name)); err == nil {
			t.Errorf("лишнее в архиве: %s", name)
		}
	}
	if st, _ := os.Stat(filepath.Join(root, "workers/echo/run")); st == nil || st.Mode().Perm()&0o100 == 0 {
		t.Error("run не исполняемый")
	}
	var info Info
	raw, _ := os.ReadFile(filepath.Join(root, InfoFile))
	_ = json.Unmarshal(raw, &info)
	if info.Version != "1.2.0" || info.Config != "agent.prod.yaml" || info.Env != "prod" || !slices.Equal(info.Workers, []string{"echo", "probe"}) || len(info.PublicKeys) != 1 {
		t.Fatalf("bundle.json: %+v", info)
	}
	var m message.Manifest
	raw, _ = os.ReadFile(filepath.Join(root, "release/manifest.json"))
	_ = json.Unmarshal(raw, &m)
	echo := m.Worker("echo", runtime.GOOS, runtime.GOARCH)
	if echo == nil || echo.Version != "1.4.0" {
		t.Fatalf("manifest: %+v", m)
	}
	hash, _ := update.FileHash(filepath.Join(root, ReleaseDir, echo.File))
	keys := update.Keys{pub}
	if hash != echo.SHA256 || keys.Verify(update.Build{Name: "echo", Version: "1.4.0", OS: runtime.GOOS, Arch: runtime.GOARCH, SHA256: hash}, echo.Signature) != nil {
		t.Fatal("сборка echo: sha256 или подпись")
	}
	if p := m.Worker("probe", runtime.GOOS, runtime.GOARCH); p == nil || p.Signature != "author" {
		t.Fatalf("probe — подпись автора сохраняется: %+v", p)
	}

	// Общий каталог сборок для бэкенда — только воркеры из папки, подписанные.
	var sm message.Manifest
	raw, _ = os.ReadFile(filepath.Join(shared, "manifest.json"))
	_ = json.Unmarshal(raw, &sm)
	if len(sm.Workers) != 1 || sm.Workers[0].Name != "echo" || sm.Workers[0].Signature == "" || sm.PublicKey == "" {
		t.Fatalf("общий каталог: %+v", sm)
	}
	if _, err := os.Stat(filepath.Join(shared, sm.Workers[0].File)); err != nil {
		t.Fatal("нет архива воркера в общем каталоге")
	}

	// --with-env — секреты в архиве; файл вне папки — ошибка.
	if archives, err = Pack(context.Background(), Options{Config: filepath.Join(dir, "agent.prod.yaml"), Env: "prod", Out: out, WithEnv: true,
		Platforms: []Platform{{runtime.GOOS, runtime.GOARCH}}, Version: "1.2.0", Self: self, Client: http.DefaultClient, Warn: func(string) {}}); err != nil {
		t.Fatal(err)
	}
	x = filepath.Join(t.TempDir(), "x")
	_ = update.Extract(archives[0], x)
	if _, err := os.Stat(filepath.Join(x, Dir, ".env.prod")); err != nil {
		t.Fatal("--with-env: нет .env.prod")
	}
	outside := filepath.Join(t.TempDir(), "agent.yaml")
	_ = os.WriteFile(outside, []byte("extends: "+filepath.Join(dir, "agent.yaml")+"\n"), 0o644)
	if _, err := Pack(context.Background(), Options{Config: outside, Out: out, Platforms: []Platform{{runtime.GOOS, runtime.GOARCH}},
		Version: "1.2.0", Self: self, Client: http.DefaultClient, Warn: func(string) {}}); err == nil {
		t.Fatal("файл настроек вне папки — ошибка")
	}
}
