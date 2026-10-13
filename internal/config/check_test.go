package config

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent.yaml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// config check: неизвестные поля — предупреждения со строкой; ошибки
// значений — с именем поля; переменные AGENT_* — поверх файла.
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
    path: workers/b
    release: true
    limits:
      memory: 1G
`)
	res := Check(path)
	if res.Config.Name != "from-env" {
		t.Fatalf("окружение: %+v", res)
	}
	if len(res.Warnings) != 2 || res.Warnings[0].Line != 3 || !strings.Contains(res.Warnings[0].Text, "«enrol»") ||
		res.Warnings[1].Line != 11 || !strings.Contains(res.Warnings[1].Text, "«limits»") {
		t.Fatalf("предупреждения: %+v", res.Warnings)
	}
	if len(res.Errors) != 1 || !strings.Contains(res.Errors[0].String(), "workers[1]: path") {
		t.Fatalf("ошибки: %+v", res.Errors)
	}

	// Разметка и типы — со строкой.
	res = Check(writeConfig(t, "server:\n  url: https://api.example.com\noutbox:\n  maxMessages: many\n"))
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
	// Папка агента: первый существующий agent.yaml из списка — раньше файла по умолчанию.
	dir := writeFiles(t, map[string]string{"b/agent.yaml": "", "c/agent.yaml": ""})
	local := []string{filepath.Join(dir, "a/agent.yaml"), filepath.Join(dir, "b/agent.yaml"), filepath.Join(dir, "c/agent.yaml")}
	if got := ResolvePath("", local...); got != local[1] {
		t.Fatalf("папка агента: %q", got)
	}
	if ResolvePath("/flag.yaml", local...) != "/flag.yaml" {
		t.Fatal("флаг важнее папки агента")
	}
}
