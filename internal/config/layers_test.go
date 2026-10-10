package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// writeFiles — файлы name → содержимое в новом каталоге; вернёт каталог.
func writeFiles(t *testing.T, files map[string]string) string {
	t.Helper()
	dir := t.TempDir()
	for name, body := range files {
		path := filepath.Join(dir, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	return dir
}

const baseLayer = `server:
  url: https://api.example.com
telemetry:
  metrics: [cpu, memory]
labels: { zone: eu }
workers:
  - name: echo
    command: ["./run"]
    dir: workers/echo
    env: { A: "1" }
  - name: probe
    command: ["/usr/bin/probe"]
`

const devLayer = `extends: agent.yaml
envFiles: [.env.dev, .env.local, none.env]
server:
  url: http://localhost:${PORT}
dataDir: ../data
telemetry:
  metrics: [cpu]
labels: { env: dev }
workers:
  - name: echo
    env: { B: "2" }
  - name: extra
    command: ["./extra"]
`

// unsetEnv — переменные не заданы до конца теста.
func unsetEnv(t *testing.T, names ...string) {
	t.Helper()
	for _, name := range names {
		t.Setenv(name, "")
		os.Unsetenv(name)
	}
}

// Файл поверх базового: словари сливаются, списки заменяются, воркеры — по
// имени; ${ИМЯ} и AGENT_* — из файлов переменных (более поздний важнее),
// окружение процесса важнее файлов; пути — от файла, где записаны.
func TestLayers(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"agent.yaml":     baseLayer,
		"agent.dev.yaml": devLayer,
		".env.dev":       "PORT=1\nAGENT_ENROLL_TOKEN=from-file\n",
		".env.local":     "PORT=8080\n",
	})
	unsetEnv(t, "PORT", "AGENT_ENROLL_TOKEN")
	cfg, err := Load(filepath.Join(dir, "agent.dev.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.URL != "http://localhost:8080" || cfg.Enroll.Token != "from-file" {
		t.Fatalf("переменные из файлов: %q %q", cfg.Server.URL, cfg.Enroll.Token)
	}
	if cfg.DataDir != filepath.Join(filepath.Dir(dir), "data") || !slices.Equal(cfg.Telemetry.Metrics, []string{"cpu"}) {
		t.Fatalf("путь и список: %q %v", cfg.DataDir, cfg.Telemetry.Metrics)
	}
	if cfg.Labels["zone"] != "eu" || cfg.Labels["env"] != "dev" {
		t.Fatalf("словарь: %v", cfg.Labels)
	}
	names := []string{}
	for _, w := range cfg.Workers {
		names = append(names, w.Name)
	}
	if !slices.Equal(names, []string{"echo", "probe", "extra"}) {
		t.Fatalf("воркеры: %v", names)
	}
	echo := cfg.Workers[0]
	if echo.Dir != filepath.Join(dir, "workers/echo") || echo.Env["A"] != "1" || echo.Env["B"] != "2" || echo.Command[0] != "./run" {
		t.Fatalf("воркер по имени: %+v", echo)
	}

	// Окружение процесса важнее файлов переменных.
	t.Setenv("PORT", "9")
	if cfg, err = Load(filepath.Join(dir, "agent.dev.yaml")); err != nil || cfg.Server.URL != "http://localhost:9" {
		t.Fatalf("окружение: %q %v", cfg.Server.URL, err)
	}

	src := ReadSource(filepath.Join(dir, "agent.dev.yaml"))
	if len(src.Files) != 2 || filepath.Base(src.Files[0]) != "agent.yaml" || len(src.EnvFiles) != 2 {
		t.Fatalf("цепочка: %v %v", src.Files, src.EnvFiles)
	}
	if p := src.Origin["server.url"]; filepath.Base(p.File) != "agent.dev.yaml" || p.Line != 4 {
		t.Fatalf("server.url: %+v", p)
	}
	if p := src.Origin["workers[0].dir"]; filepath.Base(p.File) != "agent.yaml" || p.Line != 9 {
		t.Fatalf("workers[0].dir: %+v", p)
	}
	if p := src.Origin["workers[0].env.B"]; filepath.Base(p.File) != "agent.dev.yaml" {
		t.Fatalf("workers[0].env.B: %+v", p)
	}
	if len(src.Warnings) != 1 || !strings.Contains(src.Warnings[0].String(), "none.env") || filepath.Base(src.Warnings[0].File) != "agent.dev.yaml" {
		t.Fatalf("нет файла переменных: %+v", src.Warnings)
	}
}

// Ошибки цепочки: круг, нет базового файла, не словарь.
func TestLayerErrors(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"a.yaml":     "extends: b.yaml\n",
		"b.yaml":     "extends: a.yaml\n",
		"lost.yaml":  "extends: missing.yaml\n",
		"list.yaml":  "extends: agent.yaml\n",
		"agent.yaml": "- one\n",
	})
	for name, want := range map[string]string{"a.yaml": "по кругу", "lost.yaml": "missing.yaml", "list.yaml": "словарь"} {
		if _, err := Load(filepath.Join(dir, name)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

// config check по цепочке: ошибка и замечание — с файлом и строкой; итоговые
// значения помечены, откуда они.
func TestCheckLayers(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"agent.yaml": baseLayer + "    lifecycle: {maxRestarts: -1}\n",
		"agent.prod.yaml": `extends: agent.yaml
envFiles: [.env.prod]
log:
  levl: debug
`,
		".env.prod": "AGENT_ENROLL_TOKEN=secret\n",
	})
	unsetEnv(t, "AGENT_ENROLL_TOKEN")
	t.Setenv("AGENT_NAME", "from-env")
	res := Check(filepath.Join(dir, "agent.prod.yaml"))
	if len(res.Warnings) != 1 || filepath.Base(res.Warnings[0].File) != "agent.prod.yaml" || res.Warnings[0].Line != 4 {
		t.Fatalf("замечания: %+v", res.Warnings)
	}
	if len(res.Errors) != 1 || filepath.Base(res.Errors[0].File) != "agent.yaml" || res.Errors[0].Line != 13 ||
		!strings.Contains(res.Errors[0].String(), "agent.yaml, строка 13: workers[1].lifecycle.maxRestarts") {
		t.Fatalf("ошибки: %+v", res.Errors)
	}
	if !slices.Contains(res.FromEnvFiles, "AGENT_ENROLL_TOKEN") || !slices.Contains(res.FromEnv, "AGENT_NAME") {
		t.Fatalf("переменные: %v %v", res.FromEnv, res.FromEnvFiles)
	}

	doc, err := res.Annotated(res.Config)
	if err != nil {
		t.Fatal(err)
	}
	out, _ := yaml.Marshal(doc)
	lines := strings.Split(string(out), "\n")
	for _, want := range [][2]string{{"url: https://api.example.com # ", "agent.yaml:2"},
		{"token: secret # ", "AGENT_ENROLL_TOKEN из файла переменных"}, {"name: from-env # ", "AGENT_NAME"}, {"metrics: # ", "agent.yaml:4"}} {
		if !slices.ContainsFunc(lines, func(l string) bool {
			l = strings.TrimSpace(l)
			return strings.HasPrefix(l, want[0]) && strings.HasSuffix(l, want[1])
		}) {
			t.Errorf("нет %q…%q:\n%s", want[0], want[1], out)
		}
	}
}

// Воркер из папки (path) и из релиза агента (from: agent).
func TestWorkerPathAndFrom(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"agent.yaml": `server: { url: https://api.example.com }
dataDir: data
workers:
  - path: workers/echo
  - name: probe
    from: agent
`,
		"agent.prod.yaml": `extends: agent.yaml
workers:
  - path: workers/echo
    env: { MODE: prod }
`,
	})
	cfg, err := Load(filepath.Join(dir, "agent.prod.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Workers) != 2 {
		t.Fatalf("воркеры: %+v", cfg.Workers)
	}
	echo, probe := cfg.Workers[0], cfg.Workers[1]
	if echo.Name != "echo" || echo.Dir != filepath.Join(dir, "workers/echo") || echo.Command[0] != "./run" || echo.Env["MODE"] != "prod" || echo.Release {
		t.Fatalf("path: %+v", echo)
	}
	if !probe.Release || probe.ReleaseDir != filepath.Join(dir, "data", "workers", "probe") {
		t.Fatalf("from: %+v", probe)
	}
	for _, w := range []Worker{
		{Path: "/w/a", Release: true},
		{Path: "/w/a", Dir: "/other"},
		{Name: "b", From: "github"},
	} {
		c := Defaults()
		c.Server.URL = "https://api.example.com"
		c.Workers = []Worker{w}
		if err := c.Validate(); err == nil {
			t.Errorf("%+v: нет ошибки", w)
		}
	}
	c := Defaults()
	c.Server.URL = "https://api.example.com"
	c.Update.Releases = "ftp://mirror.example.com/agent"
	if err := c.Validate(); err == nil || !strings.Contains(err.Error(), "update.releases") {
		t.Fatalf("update.releases: %v", err)
	}
}

// Один файл для узла: без extends и envFiles, ${ИМЯ} как есть, dataDir узла,
// воркер из папки — со сборкой; агент читает его.
func TestFlatten(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"agent.yaml": `# общее
envFiles: [.env]
dataDir: .data
workers:
  - path: workers/echo # воркер проекта
    env: { A: "1" }
  - name: probe
    from: agent
`,
		"agent.prod.yaml": `extends: agent.yaml
envFiles: [.env.prod]
instance: example
install: { packages: [python3] }
server:
  url: ${AGENT_SERVER_URL}
log: { format: json }
`,
	})
	raw, err := Flatten(filepath.Join(dir, "agent.prod.yaml"), "/var/lib/agent-example", "собран из agent.yaml + agent.prod.yaml")
	if err != nil {
		t.Fatal(err)
	}
	text := string(raw)
	for _, bad := range []string{"extends", "envFiles", "instance", "install", "path:", ".data"} {
		if strings.Contains(text, bad) {
			t.Errorf("лишнее %q:\n%s", bad, text)
		}
	}
	for _, want := range []string{"# собран из", "${AGENT_SERVER_URL}", "# воркер проекта", "release: true"} {
		if !strings.Contains(text, want) {
			t.Errorf("нет %q:\n%s", want, text)
		}
	}
	path := filepath.Join(t.TempDir(), "agent.yaml")
	_ = os.WriteFile(path, raw, 0o644)
	t.Setenv("AGENT_SERVER_URL", "https://api.example.com")
	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("%v\n%s", err, text)
	}
	if cfg.DataDir != "/var/lib/agent-example" || cfg.Workers[0].Name != "echo" || !cfg.Workers[0].Release || cfg.Workers[0].Env["A"] != "1" ||
		cfg.Workers[0].ReleaseDir != "/var/lib/agent-example/workers/echo" || cfg.Log.Format != "json" {
		t.Fatalf("%+v", cfg)
	}
	if len(Unknown(path)) != 0 {
		t.Fatalf("неизвестные поля: %v", Unknown(path))
	}
}

// ${ИМЯ} в { … } и [ … ]: цепочка и один файл для узла читаются, как раньше.
func TestVarsInFlowStyle(t *testing.T) {
	dir := writeFiles(t, map[string]string{
		"agent.yaml":      "server: { url: ${URL} }\nlabels: { zone: $ZONE }\n",
		"agent.prod.yaml": "extends: agent.yaml\nenvFiles: [.env]\ntelemetry: { disks: [${DISK}] }\n",
		".env":            "URL=https://api.example.com\nZONE=eu\nDISK=/data\n",
	})
	unsetEnv(t, "URL", "ZONE", "DISK")
	cfg, err := Load(filepath.Join(dir, "agent.prod.yaml"))
	if err != nil || cfg.Server.URL != "https://api.example.com" || cfg.Labels["zone"] != "eu" || cfg.Telemetry.Disks[0] != "/data" {
		t.Fatalf("%+v %v", cfg, err)
	}
	raw, err := Flatten(filepath.Join(dir, "agent.prod.yaml"), "/var/lib/agent", "")
	if err != nil || !strings.Contains(string(raw), "${URL}") || !strings.Contains(string(raw), "${ZONE}") || !strings.Contains(string(raw), "${DISK}") {
		t.Fatalf("%v\n%s", err, raw)
	}
}
