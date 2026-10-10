package scaffold

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/internal/config"
)

// agent worker new: файлы заготовки, класс по имени, run исполняемый, строка в agent.yaml
// папки из agent init (агент её читает); повторно — без --force отказ, строка не дублируется.
func TestNewWorker(t *testing.T) {
	dir := t.TempDir()
	if _, err := Init(Options{Dir: dir}); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(dir, "agent.yaml")
	for _, c := range []struct{ name, lang, class string }{
		{"report", "python", "class Report(Worker)"},
		{"report-builder", "go", "type ReportBuilder struct"},
	} {
		name := c.name
		lang, _ := LangByName(c.lang)
		created, added, err := NewWorker(WorkerOptions{Root: dir, Config: cfgPath, Name: name, Lang: c.lang})
		want := map[string]int{"python": 4, "go": 7}[c.lang]
		if err != nil || !added || len(created) != want {
			t.Fatalf("%s: %v %v %v", name, created, added, err)
		}
		main, _ := os.ReadFile(filepath.Join(dir, "workers", name, lang.Main))
		if !strings.Contains(string(main), c.class) || strings.Contains(string(main), "{{") {
			t.Fatalf("%s: заготовка не заполнена:\n%s", name, main)
		}
		if st, _ := os.Stat(filepath.Join(dir, "workers", name, "run")); st == nil || st.Mode().Perm()&0o100 == 0 {
			t.Fatalf("%s: run не исполняемый", name)
		}
		if _, _, err := NewWorker(WorkerOptions{Root: dir, Config: cfgPath, Name: name, Lang: c.lang}); err == nil {
			t.Fatalf("%s: повторно без --force — ошибка", name)
		}
		if _, added, _ := NewWorker(WorkerOptions{Root: dir, Config: cfgPath, Name: name, Lang: c.lang, Force: true}); added {
			t.Fatalf("%s: строка в agent.yaml дважды", name)
		}
	}
	t.Setenv("AGENT_SERVER_URL", "https://api.example.com")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		t.Fatal(err)
	}
	if len(cfg.Workers) != 2 || cfg.Workers[0].Name != "report-builder" || cfg.Workers[1].Name != "report" {
		t.Fatalf("воркеры: %+v", cfg.Workers)
	}
	for _, bad := range []WorkerOptions{{Root: dir, Name: "Bad"}, {Root: dir, Name: "ok", Lang: "ruby"}} {
		if _, _, err := NewWorker(bad); err == nil {
			t.Errorf("%+v: нет ошибки", bad)
		}
	}
	// У папки агента уже свой go.mod — воркер на Go входит в него, своего go.mod нет.
	_ = os.WriteFile(filepath.Join(dir, "go.mod"), []byte("module example\n"), 0o644)
	if created, _, err := NewWorker(WorkerOptions{Root: dir, Name: "inner", Lang: "go"}); err != nil || len(created) != 6 {
		t.Fatalf("воркер в модуле папки: %v %v", created, err)
	}
	if className("report-builder") != "ReportBuilder" || className("x") != "X" {
		t.Fatal("className")
	}
}

// Строка воркера: в пустой список, после «workers:», без списка — в конец.
func TestAddWorker(t *testing.T) {
	for in, want := range map[string]string{
		"server: {}\nworkers: []\n":                  "workers:\n  - path: workers/a\n",
		"workers: # воркеры\n  - name: b\n":          "workers: # воркеры\n  - path: workers/a\n  - name: b\n",
		"log: {level: info}\n":                       "log: {level: info}\n\nworkers:\n  - path: workers/a\n",
		"workers:\n  - path: workers/a # уже есть\n": "workers:\n  - path: workers/a # уже есть\n",
	} {
		p := filepath.Join(t.TempDir(), "agent.yaml")
		_ = os.WriteFile(p, []byte(in), 0o644)
		if _, err := addWorker(p, "workers/a"); err != nil {
			t.Fatal(err)
		}
		if got, _ := os.ReadFile(p); !strings.Contains(string(got), want) {
			t.Errorf("%q → %q", in, got)
		}
	}
}

// agent worker sync: старая база обновляется, новая и чужой файл — нет.
func TestSyncBases(t *testing.T) {
	dir := t.TempDir()
	write := func(worker, file, body string) {
		p := filepath.Join(dir, "workers", worker, file)
		_ = os.MkdirAll(filepath.Dir(p), 0o755)
		_ = os.WriteFile(p, []byte(body), 0o644)
	}
	py, gol := Langs[0], Langs[1]
	write("old", py.Base, "# agent-worker-base: 0\n")
	write("newer", py.Base, "# agent-worker-base: 999\n")
	write("own", py.Base, "# своя библиотека\n")
	write("gold", gol.Base, "// agent-worker-base: 1\npackage main\n")
	got, err := SyncBases(dir)
	if err != nil || len(got) != 1 || got[0].File != filepath.Join("workers", "gold", gol.Base) {
		// версия 0 — не база из заготовки — не трогаем; Go-база старого содержимого — обновляется
		t.Fatalf("%+v %v", got, err)
	}
	write("old", py.Base, "# agent-worker-base: 1\nold body\n")
	got, err = SyncBases(dir)
	if err != nil || len(got) != 1 || got[0].File != filepath.Join("workers", "old", py.Base) || got[0].To != BaseVersion(py) {
		t.Fatalf("%+v %v", got, err)
	}
	if raw, _ := os.ReadFile(filepath.Join(dir, "workers", "newer", py.Base)); string(raw) != "# agent-worker-base: 999\n" {
		t.Fatal("новую базу тронули")
	}
}
