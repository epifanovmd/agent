package install

import (
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

// install.sh --bundle: архив agent pack по ссылке с {arch}, его программа агента — install,
// run (настройки архива, токен и ключи — окружением) и uninstall.
func TestInstallScriptBundle(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("install.sh — только Linux")
	}
	if _, err := exec.LookPath("curl"); err != nil {
		t.Skip("нет curl")
	}
	// Программа агента в архиве печатает, как её запустили.
	agent := "#!/bin/sh\necho \"args: $*\"\necho \"server=${AGENT_SERVER_URL:-} token=${AGENT_ENROLL_TOKEN:-} keys=${AGENT_UPDATE_PUBLIC_KEYS:-}\"\n"
	archive := tarGz(t, map[string]string{
		"agent/agent":       agent,
		"agent/bundle.json": `{"config": "agent.prod.yaml"}`,
	})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/agent-prod-linux-"+runtime.GOARCH+".tar.gz" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write(archive)
	}))
	defer srv.Close()
	bundle := srv.URL + "/agent-prod-linux-{arch}.tar.gz"
	script := filepath.Join("..", "..", "deploy", "install", "install.sh")

	run := func(args ...string) string {
		t.Helper()
		cmd := exec.Command("sh", append([]string{script}, args...)...)
		cmd.Env = append(os.Environ(), "HOME="+t.TempDir(), "AGENT_DATA_DIR="+t.TempDir())
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("install.sh %v: %v\n%s", args, err, out)
		}
		return string(out)
	}

	if out := run("--bundle", bundle, "--rootless", "--token", "t1", "--update-key", "a2V5"); !strings.Contains(out, "args: install --rootless --token t1 --update-key a2V5") {
		t.Fatalf("install: %s", out)
	}
	if out := run("--bundle", bundle, "--server", "https://api.example.com", "--rootless"); !strings.Contains(out, "args: install --server https://api.example.com --rootless") {
		t.Fatalf("install с --server: %s", out)
	}
	out := run("--bundle", bundle, "--run", "--token", "t2", "--update-key", "a2V5", "--update-key", "b2V5")
	if !strings.Contains(out, "args: run -config ") || !strings.Contains(out, "/agent/agent.prod.yaml") ||
		!strings.Contains(out, "server= token=t2 keys=a2V5,b2V5") {
		t.Fatalf("run: %s", out)
	}
	if out := run("--bundle", bundle, "--uninstall", "--purge"); !strings.Contains(out, "args: uninstall --purge") {
		t.Fatalf("uninstall: %s", out)
	}

	cmd := exec.Command("sh", script, "--bundle", srv.URL+"/нет-{arch}.tar.gz", "--rootless")
	cmd.Env = append(os.Environ(), "HOME="+t.TempDir())
	if out, err := cmd.CombinedOutput(); err == nil || !strings.Contains(string(out), "архив не скачан") {
		t.Fatalf("нет архива: %v %s", err, out)
	}
}
