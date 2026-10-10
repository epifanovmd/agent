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
	for _, name := range []string{"report", "report-builder"} {
		created, added, err := NewWorker(WorkerOptions{Root: dir, Config: cfgPath, Name: name})
		if err != nil || !added || len(created) != 4 {
			t.Fatalf("%s: %v %v %v", name, created, added, err)
		}
		main, _ := os.ReadFile(filepath.Join(dir, "workers", name, MainFile))
		if !strings.Contains(string(main), "class "+className(name)+"(Worker)") || strings.Contains(string(main), "{{") {
			t.Fatalf("%s: заготовка не заполнена:\n%s", name, main)
		}
		if st, _ := os.Stat(filepath.Join(dir, "workers", name, "run")); st == nil || st.Mode().Perm()&0o100 == 0 {
			t.Fatalf("%s: run не исполняемый", name)
		}
		if _, _, err := NewWorker(WorkerOptions{Root: dir, Config: cfgPath, Name: name}); err == nil {
			t.Fatalf("%s: повторно без --force — ошибка", name)
		}
		if _, added, _ := NewWorker(WorkerOptions{Root: dir, Config: cfgPath, Name: name, Force: true}); added {
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
	if _, _, err := NewWorker(WorkerOptions{Root: dir, Name: "Bad"}); err == nil {
		t.Error("неверное имя — ошибка")
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
	write("old", BaseFile, "# agent-worker-base: 0\n")
	write("newer", BaseFile, "# agent-worker-base: 999\n")
	write("own", BaseFile, "# своя библиотека\n")
	got, err := SyncBases(dir)
	if err != nil || len(got) != 0 {
		// версия 0 — не база из заготовки (её нет в строке) — не трогаем
		t.Fatalf("%+v %v", got, err)
	}
	write("old", BaseFile, "# agent-worker-base: 1\nold body\n")
	got, err = SyncBases(dir)
	if err != nil || len(got) != 1 || got[0].File != filepath.Join("workers", "old", BaseFile) || got[0].To != BaseVersion() {
		t.Fatalf("%+v %v", got, err)
	}
	if raw, _ := os.ReadFile(filepath.Join(dir, "workers", "newer", BaseFile)); string(raw) != "# agent-worker-base: 999\n" {
		t.Fatal("новую базу тронули")
	}
}
