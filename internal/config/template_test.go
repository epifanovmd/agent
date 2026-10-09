package config

import (
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func strictDecode(t *testing.T, src []byte) Config {
	t.Helper()
	cfg := Defaults()
	dec := yaml.NewDecoder(strings.NewReader(string(src)))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		t.Fatalf("%v\n%s", err, src)
	}
	return cfg
}

// Созданный файл читается агентом: заданные значения на месте, остальное —
// умолчания; закомментированные примеры — тоже верный YAML с известными
// полями; в примерах есть каждый раздел настроек.
func TestTemplate(t *testing.T) {
	opts := TemplateOptions{
		ServerURL: "https://api.example.com:8443/agents", Token: "tok: #1", Name: "node 01", DataDir: "/srv/agent",
		LogFormat: "json",
		Workers: []TemplateWorker{
			{Name: "example", Release: true, StopTimeout: "1m"},
			{Name: "report", Release: true, Command: []string{"./bin/report"}},
		},
	}
	src := Template(opts)
	cfg, err := Load(writeConfig(t, string(src)))
	if err != nil {
		t.Fatalf("%v\n%s", err, src)
	}
	if cfg.Server.URL != opts.ServerURL || cfg.Enroll.Token != opts.Token || cfg.Name != opts.Name ||
		cfg.DataDir != opts.DataDir || cfg.Log.Format != "json" || cfg.Log.Level != "info" {
		t.Fatalf("значения: %+v", cfg)
	}
	if len(cfg.Workers) != 2 || !cfg.Workers[0].Release ||
		cfg.Workers[0].Lifecycle.StopTimeout.Std().String() != "1m0s" || !slices.Equal(cfg.Workers[1].Command, []string{"./bin/report"}) {
		t.Fatalf("воркеры: %+v", cfg.Workers)
	}
	if !reflect.DeepEqual(cfg.Telemetry, Defaults().Telemetry) || !reflect.DeepEqual(cfg.Update, Defaults().Update) {
		t.Fatalf("умолчания: %+v %+v", cfg.Telemetry, cfg.Update)
	}

	// Минимальный файл: только адрес; без воркеров — пустой список.
	minimal := Template(TemplateOptions{ServerURL: "https://api.example.com"})
	cfg, err = Load(writeConfig(t, string(minimal)))
	if err != nil || cfg.Server.URL != "https://api.example.com" || len(cfg.Workers) != 0 || cfg.DataDir != DefaultDataDir() {
		t.Fatalf("минимальный: %+v %v\n%s", cfg, err, minimal)
	}
	if !strings.Contains(string(minimal), "# enroll:") || !strings.Contains(string(minimal), "#   token:") {
		t.Fatalf("токен — примером:\n%s", minimal)
	}

	// Примеры без «#» — верный YAML с известными полями.
	for _, o := range []TemplateOptions{{uncomment: true}, {ServerURL: "https://api.example.com", Workers: opts.Workers, LogFormat: "json", uncomment: true}} {
		full := strictDecode(t, Template(o))
		if len(full.Workers) < 2 || !slices.Equal(full.Telemetry.Metrics, Defaults().Telemetry.Metrics) {
			t.Fatalf("примеры: %+v", full)
		}
	}
	// Каждый раздел Config есть в примерах.
	example := string(Template(TemplateOptions{uncomment: true}))
	rt := reflect.TypeOf(Config{})
	for i := range rt.NumField() {
		tag := strings.Split(rt.Field(i).Tag.Get("yaml"), ",")[0]
		if tag == "" {
			continue // служебное поле
		}
		if !strings.Contains(example, "\n"+tag+":") {
			t.Errorf("в шаблоне нет раздела %s", tag)
		}
	}
}

// config check: неизвестные поля — предупреждения со строкой; ошибки
// значений — со строкой поля; переменные AGENT_* перечислены.
func TestCheck(t *testing.T) {
	t.Setenv("AGENT_NAME", "from-env")
	path := writeConfig(t, `server:
  url: https://api.example.com
enrol:
  token: x
workers:
  - name: a
    command: ["a"]
  - name: b
    command: ["b"]
    lifecycle: {maxRestarts: -1}
    limits:
      memory: 1G
`)
	res := Check(path)
	if res.Config.Name != "from-env" || !slices.Contains(res.FromEnv, "AGENT_NAME") {
		t.Fatalf("окружение: %+v", res)
	}
	if len(res.Warnings) != 2 || res.Warnings[0].Line != 3 || !strings.Contains(res.Warnings[0].Text, "«enrol»") ||
		res.Warnings[1].Line != 11 || !strings.Contains(res.Warnings[1].Text, "«limits»") {
		t.Fatalf("предупреждения: %+v", res.Warnings)
	}
	if len(res.Errors) != 1 || res.Errors[0].Line != 10 || !strings.Contains(res.Errors[0].String(), "строка 10: workers[1].lifecycle.maxRestarts") {
		t.Fatalf("ошибки: %+v", res.Errors)
	}

	// Разметка и типы — со строкой.
	res = Check(writeConfig(t, "server:\n  url: https://api.example.com\ntelemetry:\n  metrics: many\n"))
	if len(res.Errors) != 1 || res.Errors[0].Line != 4 {
		t.Fatalf("тип: %+v", res.Errors)
	}
	res = Check(writeConfig(t, "server:\n  url: https://api.example.com\nworkers:\n  - name: a\n    command: [a]\n    lifecycle:\n      stopTimeout: soon\n"))
	if len(res.Errors) != 1 || res.Errors[0].Line != 7 || !strings.Contains(res.Errors[0].Text, "длительность") {
		t.Fatalf("длительность: %+v", res.Errors)
	}
	res = Check(writeConfig(t, "server:\n  url: [\n"))
	if len(res.Errors) != 1 || res.Errors[0].Line == 0 || !strings.Contains(res.Errors[0].Text, "YAML") {
		t.Fatalf("разметка: %+v", res.Errors)
	}
	// Без файла — только окружение: нет адреса — ошибка без строки.
	t.Setenv("AGENT_SERVER_URL", "")
	res = Check("")
	if len(res.Errors) != 1 || res.Errors[0].Line != 0 || !strings.Contains(res.Errors[0].Text, "server.url") {
		t.Fatalf("без файла: %+v", res.Errors)
	}
	// Минимальные настройки — адрес; всё остальное по умолчанию.
	t.Setenv("AGENT_SERVER_URL", "https://api.example.com")
	if res = Check(""); len(res.Errors) != 0 {
		t.Fatalf("минимальные: %+v", res.Errors)
	}
}

// agent.env: как EnvironmentFile systemd; заданные переменные не меняются.
func TestApplyEnvFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), EnvFileName)
	_ = os.WriteFile(path, []byte("# токен\nAGENT_TEST_A=\"one\"\n\nAGENT_TEST_B=two=2\nAGENT_TEST_C=keep\n"), 0o600)
	t.Setenv("AGENT_TEST_C", "own")
	os.Unsetenv("AGENT_TEST_A")
	os.Unsetenv("AGENT_TEST_B")
	t.Cleanup(func() { os.Unsetenv("AGENT_TEST_A"); os.Unsetenv("AGENT_TEST_B") })
	applied, err := ApplyEnvFile(path)
	if err != nil || !slices.Equal(applied, []string{"AGENT_TEST_A", "AGENT_TEST_B"}) {
		t.Fatalf("%v %v", applied, err)
	}
	if os.Getenv("AGENT_TEST_A") != "one" || os.Getenv("AGENT_TEST_B") != "two=2" || os.Getenv("AGENT_TEST_C") != "own" {
		t.Fatal("значения")
	}
	if applied, err := ApplyEnvFile(filepath.Join(t.TempDir(), "none")); err != nil || applied != nil {
		t.Fatalf("нет файла: %v %v", applied, err)
	}
	if EnvFile("/etc/agent/agent.yaml") != "/etc/agent/agent.env" || EnvFile("") != "" {
		t.Fatal("EnvFile")
	}
}

// Файл настроек: флаг, AGENT_CONFIG, файл по умолчанию, если он есть.
func TestResolvePath(t *testing.T) {
	t.Setenv("AGENT_CONFIG", "/env/agent.yaml")
	if ResolvePath("/flag.yaml") != "/flag.yaml" || ResolvePath("") != "/env/agent.yaml" {
		t.Fatal("флаг и AGENT_CONFIG")
	}
	t.Setenv("AGENT_CONFIG", "")
	want := ""
	if exists(DefaultPath()) {
		want = DefaultPath()
	}
	if got := ResolvePath(""); got != want {
		t.Fatalf("по умолчанию: %q", got)
	}
}
