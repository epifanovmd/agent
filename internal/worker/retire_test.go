//go:build unix

package worker

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"testing"

	"github.com/epifanovmd/agent/internal/config"
)

// Воркер убран из настроек: ./cleanup выполнен, сборка удалена, данные воркера остались.
func TestRetire(t *testing.T) {
	data := t.TempDir()
	rel := filepath.Join(config.ReleasesDir(data), "w")
	current := filepath.Join(rel, config.ReleaseCurrent)
	if err := os.MkdirAll(current, 0o755); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(data, "cleaned")
	script := "#!/bin/sh\necho done > " + marker + "\n"
	if err := os.WriteFile(filepath.Join(current, config.Cleanup), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(current, config.ReleaseRun), []byte("#!/bin/sh\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(rel, config.ReleaseVersion), []byte("1.0.0\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	keep := filepath.Join(rel, "state")
	if err := os.MkdirAll(keep, 0o700); err != nil {
		t.Fatal(err)
	}
	Retire(context.Background(), config.Worker{Name: "w", Release: true}, data, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if _, err := os.Stat(marker); err != nil {
		t.Fatalf("./cleanup не выполнен: %v", err)
	}
	for _, gone := range []string{current, filepath.Join(rel, config.ReleaseVersion)} {
		if _, err := os.Stat(gone); !os.IsNotExist(err) {
			t.Fatalf("%s не удалён: %v", gone, err)
		}
	}
	if _, err := os.Stat(keep); err != nil {
		t.Fatalf("данные воркера удалены: %v", err)
	}
}
