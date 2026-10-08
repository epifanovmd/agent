package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// release-manifest: сборки агента и воркеров (--worker) с подписью ключом выпуска.
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
	err := releaseManifest([]string{dir, "1.2.0", "--worker", "sysinfo=1.3.0,restart=stop-first,stopTimeout=45s", "--worker=report=2.0.0,command=bin/report"})
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := os.ReadFile(filepath.Join(dir, "manifest.json"))
	var m message.Manifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	if m.Version != "1.2.0" || len(m.Artifacts) != 1 || len(m.Workers) != 4 {
		t.Fatalf("манифест: %s", raw)
	}
	w := m.Worker("sysinfo", "linux", "amd64")
	if w == nil || w.File != "sysinfo-1.3.0-linux-amd64" || w.Version != "1.3.0" || w.Restart != "stop-first" || w.StopTimeout != "45s" {
		t.Fatalf("sysinfo: %+v", w)
	}
	hash, _ := update.FileHash(filepath.Join(dir, w.File))
	if w.SHA256 != hash || update.Verify(pub, update.Build{Name: "sysinfo", Version: "1.3.0", OS: "linux", Arch: "amd64", SHA256: hash}, w.Signature) != nil {
		t.Fatal("sha256 или подпись воркера")
	}
	agentHash, _ := update.FileHash(filepath.Join(dir, "agent-linux-amd64"))
	if a := m.Artifacts[0]; update.Verify(pub, update.Build{Name: "agent", Version: "1.2.0", OS: "linux", Arch: "amd64", SHA256: agentHash}, a.Signature) != nil {
		t.Fatalf("подпись агента: %+v", a)
	}
	if arc := m.Worker("report", "darwin", "arm64"); arc == nil || arc.File != "report-2.0.0-darwin-arm64.tar.gz" || arc.Arch != "arm64" || arc.Command != "bin/report" {
		t.Fatalf("архив воркера: %+v", arc)
	}
	if rep := m.Worker("report", "linux", "arm64"); rep == nil || rep.Restart != "" || rep.StopTimeout != "" {
		t.Fatalf("report: %+v", rep)
	}

	for _, bad := range [][]string{
		{dir, "1.2.0", "--worker", "missing=1.0.0"},
		{dir, "1.2.0", "--worker", "sysinfo"},
		{dir, "1.2.0", "--worker", "sysinfo=1.3.0,restart=never"},
		{dir, "1.2.0", "--worker", "../x=1"},
		{dir, "1.2.0", "--worker", "report=2.0.0,command=../evil"},
		{dir},
	} {
		if err := releaseManifest(bad); err == nil {
			t.Errorf("%v: ожидалась ошибка", bad)
		}
	}
}
