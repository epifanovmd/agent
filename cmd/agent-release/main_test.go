package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/update"
)

// manifest: сборки агента и воркеров (--worker) с подписью ключом подписи сборок.
func TestReleaseManifestWorkers(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	t.Setenv("AGENT_SIGNING_KEY", base64.StdEncoding.EncodeToString(priv.Seed()))
	dir := t.TempDir()
	for _, f := range []string{
		"agent-linux-amd64", "sysinfo-1.3.0-linux-amd64", "sysinfo-1.3.0-darwin-arm64",
		"sysinfo-1.3.0-rc1-linux-amd64", "report-2.0.0-linux-arm64", "report-2.0.0-darwin-arm64.tar.gz",
	} {
		_ = os.WriteFile(filepath.Join(dir, f), []byte(f), 0o755)
	}
	err := manifest([]string{dir, "1.0.0", "--worker", "sysinfo=1.3.0,stopTimeout=45s", "--worker=report=2.0.0,command=bin/report"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "manifest.json"))
	var m message.Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.Version != "1.0.0" || len(m.Artifacts) != 1 || len(m.Workers) != 4 || m.PublicKey != base64.StdEncoding.EncodeToString(pub) {
		t.Fatalf("манифест: %s", raw)
	}
	w := m.Worker("sysinfo", "linux", "amd64")
	if w == nil || w.File != "sysinfo-1.3.0-linux-amd64" || w.Version != "1.3.0" || w.StopTimeout != "45s" {
		t.Fatalf("sysinfo: %+v", w)
	}
	hash, _ := update.FileHash(filepath.Join(dir, w.File))
	if w.SHA256 != hash || update.Verify(pub, update.Build{Name: "sysinfo", Version: "1.3.0", OS: "linux", Arch: "amd64", SHA256: hash}, w.Signature) != nil {
		t.Fatal("sha256 или подпись воркера")
	}
	agentHash, _ := update.FileHash(filepath.Join(dir, "agent-linux-amd64"))
	if a := m.Artifacts[0]; update.Verify(pub, update.Build{Name: "agent", Version: "1.0.0", OS: "linux", Arch: "amd64", SHA256: agentHash}, a.Signature) != nil {
		t.Fatalf("подпись агента: %+v", a)
	}
	if arc := m.Worker("report", "darwin", "arm64"); arc == nil || arc.File != "report-2.0.0-darwin-arm64.tar.gz" || arc.Arch != "arm64" || arc.Command != "bin/report" {
		t.Fatalf("архив воркера: %+v", arc)
	}
	if rep := m.Worker("report", "linux", "arm64"); rep == nil || rep.StopTimeout != "" {
		t.Fatalf("report: %+v", rep)
	}

	for _, bad := range [][]string{
		{dir, "1.0.0", "--worker", "missing=1.0.0"},
		{dir, "1.0.0", "--worker", "sysinfo"},
		{dir, "1.0.0", "--worker", "sysinfo=1.3.0,restart=stop-first"},
		{dir, "1.0.0", "--worker", "../x=1"},
		{dir, "1.0.0", "--worker", "report=2.0.0,command=../evil"},
		{dir},
	} {
		if err := manifest(bad); err == nil {
			t.Errorf("%v: ожидалась ошибка", bad)
		}
	}
}

// Только сборки воркеров (воркеры проекта): сборок агента нет — artifacts
// пустой; без сборок агента и без --worker — ошибка.
func TestReleaseManifestWorkersOnly(t *testing.T) {
	t.Setenv("AGENT_SIGNING_KEY", "")
	dir := t.TempDir()
	_ = os.WriteFile(filepath.Join(dir, "report-1.0.0-linux-amd64"), []byte("r"), 0o755)
	if err := manifest([]string{dir, "2.0.0"}); err == nil {
		t.Fatal("без сборок агента и --worker — ошибка")
	}
	if err := manifest([]string{dir, "2.0.0", "--worker", "report=1.0.0"}); err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "manifest.json"))
	if !strings.Contains(string(raw), `"artifacts": []`) || strings.Contains(string(raw), "publicKey") {
		t.Fatalf("манифест: %s", raw)
	}
}

// Без команды и с неизвестной командой — понятная ошибка, а не паника.
func TestRunUnknownCommand(t *testing.T) {
	for _, args := range [][]string{nil, {"oops"}} {
		if err := run(args); err == nil || !strings.Contains(err.Error(), "keygen | manifest") {
			t.Fatalf("run(%q): %v", args, err)
		}
	}
}
