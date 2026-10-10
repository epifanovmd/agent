package scaffold

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/internal/config"
)

// Папка агента: файлы на месте, agent.yaml и agent.prod.yaml читаются агентом,
// токен — только в .env (0600), программа скопирована; повторно — ничего не
// перезаписано.
func TestInit(t *testing.T) {
	for _, name := range []string{"AGENT_SERVER_URL", "AGENT_ENROLL_TOKEN", "AGENT_DATA_DIR", "AGENT_UPDATE_MODE", "AGENT_LOG_FORMAT"} {
		t.Setenv(name, "")
		os.Unsetenv(name)
	}
	dir := filepath.Join(t.TempDir(), "agent")
	bin := filepath.Join(t.TempDir(), "agent-bin")
	if err := os.WriteFile(bin, []byte("binary"), 0o755); err != nil {
		t.Fatal(err)
	}
	res, err := Init(Options{Dir: dir, Server: "http://localhost:8080", Token: "tok", Binary: bin})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Created) != len(layout)+1 || len(res.Skipped) != 0 {
		t.Fatalf("%+v", res)
	}
	env, _ := os.ReadFile(filepath.Join(dir, ".env"))
	example, _ := os.ReadFile(filepath.Join(dir, ".env.example"))
	if !strings.Contains(string(env), "AGENT_ENROLL_TOKEN=tok") || strings.Contains(string(example), "tok") {
		t.Fatalf(".env:\n%s\n.env.example:\n%s", env, example)
	}
	if st, _ := os.Stat(filepath.Join(dir, ".env")); st.Mode().Perm() != 0o600 {
		t.Fatalf(".env: %v", st.Mode())
	}
	if got, _ := os.ReadFile(filepath.Join(dir, "agent")); string(got) != "binary" {
		t.Fatal("программа не скопирована")
	}

	for _, name := range []string{"agent.yaml", "agent.prod.yaml"} {
		cfg, err := config.Load(filepath.Join(dir, name))
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if cfg.Server.URL != "http://localhost:8080" || cfg.DataDir != filepath.Join(dir, ".data") || cfg.Enroll.Token != "tok" {
			t.Fatalf("%s: %+v", name, cfg)
		}
		if name == "agent.prod.yaml" && (cfg.Update.Mode != "self" || cfg.Log.Format != "json") {
			t.Fatalf("prod: %+v %+v", cfg.Update, cfg.Log)
		}
		warnings := config.Check(filepath.Join(dir, name)).Warnings
		if name == "agent.yaml" && len(warnings) != 0 ||
			name == "agent.prod.yaml" && (len(warnings) != 1 || !strings.Contains(warnings[0].Text, ".env.prod")) {
			t.Fatalf("%s: замечания %+v", name, warnings)
		}
	}

	res, err = Init(Options{Dir: dir, Binary: bin})
	if err != nil || len(res.Created) != 0 || !slices.Contains(res.Skipped, "agent.yaml") || !slices.Contains(res.Skipped, "agent") {
		t.Fatalf("повторно: %+v %v", res, err)
	}
	// Программа уже в папке — копировать нечего.
	if res, _ := Init(Options{Dir: dir, Binary: filepath.Join(dir, "agent"), Force: true}); slices.Contains(res.Created, "agent") {
		t.Fatalf("та же программа: %+v", res)
	}
}
