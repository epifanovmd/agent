package releases

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

// releasesServer — каталог сборок в устройстве релизов GitHub: последняя
// версия latest и сборка агента linux/amd64 каждой версии.
func releasesServer(t *testing.T, latest string) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		version := ""
		switch r.URL.Path {
		case "/latest/download/manifest.json":
			version = latest
		case "/download/v1.1.0/manifest.json":
			version = "1.1.0"
		default:
			http.NotFound(w, r)
			return
		}
		_ = json.NewEncoder(w).Encode(message.Manifest{Version: version, Artifacts: []message.Artifact{
			{OS: "linux", Arch: "amd64", File: "agent-linux-amd64", SHA256: "ab", Signature: "sig"},
		}})
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Последняя версия: новее ли своей, свежа ли проверка, что уходит в hello.
func TestLatest(t *testing.T) {
	srv := releasesServer(t, "1.1.0")
	now := time.UnixMilli(1_000_000)
	c, err := Latest(context.Background(), http.DefaultClient, srv.URL, now)
	if err != nil || c.Latest != "1.1.0" || c.CheckedAt != now.UnixMilli() {
		t.Fatalf("%+v %v", c, err)
	}
	if !c.Newer("1.0.9") || c.Newer("1.1.0") || c.Newer("1.2.0") || c.Newer("dev") {
		t.Fatal("Newer")
	}
	if info := c.Info("1.0.0"); info == nil || info.Latest != "1.1.0" || c.Info("1.1.0") != nil {
		t.Fatalf("Info: %+v", info)
	}
	if !c.Fresh(time.Hour, now.Add(time.Minute)) || c.Fresh(time.Hour, now.Add(2*time.Hour)) || (Check{}).Fresh(time.Hour, now) {
		t.Fatal("Fresh")
	}
	dir := t.TempDir() + "/data"
	if err := WriteCheck(dir, c); err != nil || ReadCheck(dir) != c || ReadCheck(t.TempDir()) != (Check{}) {
		t.Fatalf("сохранение: %v", err)
	}

	r, err := AgentBuild(context.Background(), http.DefaultClient, srv.URL, "v1.1.0", "linux", "amd64")
	if err != nil || r.URL != srv.URL+"/download/v1.1.0/agent-linux-amd64" || r.SHA256 != "ab" || r.Signature != "sig" || r.Version != "1.1.0" {
		t.Fatalf("сборка: %+v %v", r, err)
	}
	if _, err := AgentBuild(context.Background(), http.DefaultClient, srv.URL, "1.1.0", "darwin", "arm64"); err == nil {
		t.Fatal("нет сборки под платформу — ошибка")
	}
	if LatestDir("") != DefaultBase+"/latest/download" || VersionDir("https://m.example.com/agent/", "1.2.0") != "https://m.example.com/agent/download/v1.2.0" {
		t.Fatal("адреса каталога")
	}
}
