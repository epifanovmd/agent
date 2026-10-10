package bundle

import (
	"bytes"
	"context"
	"crypto/ed25519"
	b64 "encoding/base64"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/install"
	"github.com/epifanovmd/agent/internal/update"
)

// fakeSystem — узел для установки: каталог вместо корня, команды записываются.
func fakeSystem(t *testing.T) (*install.System, *[]string) {
	t.Helper()
	root := t.TempDir()
	for _, d := range []string{"run/systemd/system", "usr/local/bin", "usr/sbin"} {
		_ = os.MkdirAll(filepath.Join(root, d), 0o755)
	}
	_ = os.WriteFile(filepath.Join(root, "usr/sbin/nologin"), nil, 0o755)
	var cmds []string
	var out bytes.Buffer
	return &install.System{
		Root: root,
		Run: func(_ context.Context, c install.Cmd) error {
			cmds = append(cmds, strings.Join(c.Args, " "))
			return nil
		},
		Output: func(context.Context, ...string) (string, error) { return "", errors.New("exit status 1") },
		LookPath: func(name string) (string, error) {
			if slices.Contains([]string{"systemctl", "sysctl", "useradd"}, name) {
				return "/usr/bin/" + name, nil
			}
			return "", errors.New("not found")
		},
		Euid:   func() int { return 0 },
		Chown:  func(string, string) error { return nil },
		Arch:   runtime.GOARCH,
		Out:    &out,
		ErrOut: &out,
	}, &cmds
}

// Папка агента → архив → установка: настройки одним файлом, воркер из папки —
// сборкой (подпись архива сверена), переменные — в agent.env, instance и
// install — из настроек; повторная установка с другими настройками — .bak.
func TestInstallFromBundle(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("установка службой — только Linux")
	}
	dir := t.TempDir()
	files := map[string]string{
		"agent.yaml":           "envFiles: [.env]\ndataDir: .data\nworkers:\n  - path: workers/echo\n    env: { MODE: ${EXTRA} }\n",
		"agent.prod.yaml":      "extends: agent.yaml\nenvFiles: [.env.prod]\ninstance: example\ninstall: { sysctl: { net.example.x: \"1\" } }\nserver: { url: \"${AGENT_SERVER_URL}\" }\n",
		".env.prod":            "AGENT_SERVER_URL=https://api.example.com\nAGENT_ENROLL_TOKEN=secret\nEXTRA=prod\nAGENT_SIGNING_KEY=must-not-leak\nUNUSED=nope\n",
		"workers/echo/run":     "#!/bin/sh\nexec sleep 1\n",
		"workers/echo/VERSION": "1.4.0\n",
	}
	for name, body := range files {
		p := filepath.Join(dir, name)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		_ = os.WriteFile(p, []byte(body), 0o755)
	}
	self := filepath.Join(t.TempDir(), "agent-self")
	_ = os.WriteFile(self, []byte("agent program"), 0o755)
	pub, priv, _ := ed25519.GenerateKey(nil)
	pack := func() string {
		out := t.TempDir()
		archives, err := Pack(context.Background(), Options{Config: filepath.Join(dir, "agent.prod.yaml"), Env: "prod", Out: out, WithEnv: true,
			Platforms: []Platform{{runtime.GOOS, runtime.GOARCH}}, Version: "1.2.0", Self: self, Signing: priv, Client: http.DefaultClient, Warn: func(string) {}})
		if err != nil {
			t.Fatal(err)
		}
		x := filepath.Join(t.TempDir(), "x")
		if err := update.Extract(archives[0], x); err != nil {
			t.Fatal(err)
		}
		return filepath.Join(x, Dir)
	}
	s, cmds := fakeSystem(t)
	run := func() {
		o := install.Options{Env: "prod"}
		tmp, err := Prepare(pack(), runtime.GOOS, runtime.GOARCH, &o)
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(tmp)
		if err := install.Install(context.Background(), s, o); err != nil {
			t.Fatal(err)
		}
	}
	run()
	read := func(p string) string {
		raw, err := os.ReadFile(filepath.Join(s.Root, p))
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	yaml := read("/etc/agent-example/agent.yaml")
	for _, want := range []string{"dataDir: /var/lib/agent-example", "name: echo", "release: true", "${EXTRA}", "${AGENT_SERVER_URL}"} {
		if !strings.Contains(yaml, want) {
			t.Errorf("agent.yaml: нет %q:\n%s", want, yaml)
		}
	}
	env := read("/etc/agent-example/agent.env")
	for _, want := range []string{"AGENT_ENROLL_TOKEN=secret", "EXTRA=prod", "AGENT_SERVER_URL=https://api.example.com", "AGENT_UPDATE_PUBLIC_KEYS=" + b64.StdEncoding.EncodeToString(pub)} {
		if !strings.Contains(env, want) {
			t.Errorf("agent.env: нет %q:\n%s", want, env)
		}
	}
	if strings.Contains(env, "must-not-leak") || strings.Contains(env, "UNUSED") {
		t.Errorf("на узел попало лишнее (ключ подписи, переменная, которой нет в настройках):\n%s", env)
	}
	if name, err := Instance(pack()); err != nil || name != "example" {
		t.Fatalf("экземпляр архива: %q %v", name, err)
	}
	if read("/var/lib/agent-example/workers/echo/current/run") != files["workers/echo/run"] || read("/opt/agent-example/bin/agent") != "agent program" {
		t.Fatal("сборка воркера или программа")
	}
	if !strings.Contains(read("/etc/sysctl.d/90-agent-example.conf"), "net.example.x") || !slices.Contains(*cmds, "systemctl restart agent-example.service") {
		t.Errorf("sysctl из install, служба: %v", *cmds)
	}

	// Настройки агент прочитает с переменными службы.
	for _, line := range strings.Split(strings.TrimSpace(env), "\n") {
		k, v, _ := strings.Cut(line, "=")
		t.Setenv(k, v)
	}
	cfg, err := config.Load(filepath.Join(s.Root, "/etc/agent-example/agent.yaml"))
	if err != nil || cfg.Server.URL != "https://api.example.com" || cfg.Workers[0].Env["MODE"] != "prod" {
		t.Fatalf("%+v %v", cfg, err)
	}

	// --server — адрес на узле важнее файла переменных.
	o := install.Options{Server: "https://other.example.com/"}
	tmp, err := Prepare(pack(), runtime.GOOS, runtime.GOARCH, &o)
	if err != nil {
		t.Fatal(err)
	}
	os.RemoveAll(tmp)
	if o.Vars["AGENT_SERVER_URL"] != "https://other.example.com" {
		t.Fatalf("--server: %v", o.Vars)
	}

	// Другие настройки — прежние в .bak.
	_ = os.WriteFile(filepath.Join(dir, "agent.prod.yaml"), []byte(files["agent.prod.yaml"]+"log: { level: debug }\n"), 0o644)
	run()
	if !strings.Contains(read("/etc/agent-example/agent.yaml.bak"), "dataDir") || !strings.Contains(read("/etc/agent-example/agent.yaml"), "debug") {
		t.Fatal(".bak")
	}
}
