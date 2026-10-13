//go:build unix

package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/fsutil"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/protocol"
)

// setupMark — отметка в рабочем каталоге воркера: ./setup выполнен (sha256 файла setup и
// файлов зависимостей рядом — setupInputs).
const setupMark = ".agent-setup"

// setupInputs — файлы зависимостей в рабочем каталоге: изменился любой — ./setup выполняется
// снова (шаблоны filepath.Match).
var setupInputs = []string{
	"requirements*.txt", "constraints*.txt", "pyproject.toml", "setup.py", "setup.cfg", "Pipfile", "*.lock",
	"package.json", "package-lock.json", "npm-shrinkwrap.json", "go.mod", "go.sum", "Gemfile", "Cargo.toml",
	"composer.json",
}

// setupKill — сколько ждать выхода ./setup после отмены, потом его группа получает SIGKILL.
var setupKill = 5 * time.Second

// runSetup — установка зависимостей воркера (§11): ./setup в рабочем каталоге dir, если он
// есть и ещё не выполнялся в этом виде (отметка — sha256 файла setup и файлов зависимостей). Вывод — в журнал воркера;
// срок — timeout; отмена ctx завершает всю группу процессов ./setup. Отметка пишется только
// после успеха: неудача — повтор при следующем запуске.
func runSetup(ctx context.Context, name, dir string, env []string, timeout time.Duration, log *slog.Logger) error {
	if dir == "" {
		return nil
	}
	path := filepath.Join(dir, config.Setup)
	info, err := os.Stat(path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		return nil
	case err != nil:
		return fmt.Errorf("./%s: %w", config.Setup, err)
	case !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0:
		return fmt.Errorf("./%s: нужен исполняемый файл (chmod +x)", config.Setup)
	}
	sum, err := setupSum(dir)
	if err != nil {
		return fmt.Errorf("./%s: %w", config.Setup, err)
	}
	mark := filepath.Join(dir, setupMark)
	if raw, err := os.ReadFile(mark); err == nil && strings.TrimSpace(string(raw)) == sum {
		return nil
	}
	l := log.With("worker", name)
	l.Info("установка зависимостей воркера: ./"+config.Setup, "dir", dir, "timeout", timeout)
	began := time.Now()
	if err := runScript(ctx, name, dir, config.Setup, env, timeout, log); err != nil {
		return err
	}
	if err := fsutil.WriteFile(mark, []byte(sum+"\n"), 0o644); err != nil {
		l.Warn("отметка установки зависимостей не записана — ./setup выполнится снова при следующем запуске", "err", err)
	}
	l.Info("зависимости воркера установлены", "took", time.Since(began).Round(time.Millisecond))
	return nil
}

// runScript — ./script в каталоге dir с окружением env: вывод — в журнал воркера name, срок —
// timeout (lifecycle.setupTimeout); отмена ctx завершает всю группу процессов.
func runScript(ctx context.Context, name, dir, script string, env []string, timeout time.Duration, log *slog.Logger) error {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	out := log.With(logx.OutputKey, name)
	stdout, stderr := &lineLog{log: out, level: slog.LevelInfo}, &lineLog{log: out, level: slog.LevelWarn}
	cmd := exec.CommandContext(ctx, "./"+script)
	cmd.Dir, cmd.Env, cmd.Stdout, cmd.Stderr = dir, env, stdout, stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Cancel = func() error { return syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
	cmd.WaitDelay = setupKill
	err := cmd.Run()
	stdout.Flush()
	stderr.Flush()
	switch {
	case ctx.Err() == context.DeadlineExceeded:
		return fmt.Errorf("./%s не завершился за %s (lifecycle.setupTimeout)", script, timeout)
	case err != nil:
		return fmt.Errorf("./%s: %w (вывод — в журнале воркера)", script, err)
	}
	return nil
}

// CleanupResult — итог ./cleanup воркера.
type CleanupResult struct {
	Worker string
	Err    error
}

// Cleanup — `agent stop-workers --cleanup` (§11): ./cleanup в рабочем каталоге каждого воркера
// из specs, у которого он есть, — убрать созданное воркером на узле перед удалением агента.
// Воркеры должны быть уже остановлены. Окружение — как у воркера (без сокета агента), вывод —
// в log, срок — lifecycle.setupTimeout.
func Cleanup(ctx context.Context, specs []config.Worker, dataDir string, log *slog.Logger) []CleanupResult {
	var out []CleanupResult
	for _, spec := range specs {
		spec.FillDefaults()
		_, dir, err := argv(spec)
		if err != nil || dir == "" {
			continue
		}
		if info, err := os.Stat(filepath.Join(dir, config.Cleanup)); err != nil || !info.Mode().IsRegular() {
			continue
		}
		stateDir := StateDir(dataDir, spec.Name)
		env := workerEnv(os.Environ(), spec, nil, map[string]string{
			protocol.EnvWorkerName: spec.Name, protocol.EnvStateDir: stateDir, "PYTHONUNBUFFERED": "1"})
		err = runScript(ctx, spec.Name, dir, config.Cleanup, env, spec.Lifecycle.SetupTimeout.Std(), log)
		out = append(out, CleanupResult{Worker: spec.Name, Err: err})
	}
	return out
}

// Retire — воркер убран из настроек (новая установка, правка agent.yaml) и уже остановлен: его
// ./cleanup (убрать созданное на узле) и сборка с сервера (current, previous) удаляются. Данные
// воркера (WORKER_STATE_DIR) остаются — их убирает agent uninstall --purge.
func Retire(ctx context.Context, spec config.Worker, dataDir string, log *slog.Logger) {
	spec.FillDefaults()
	if spec.Release && spec.ReleaseDir == "" {
		spec.ReleaseDir = filepath.Join(config.ReleasesDir(dataDir), spec.Name)
	}
	for _, r := range Cleanup(ctx, []config.Worker{spec}, dataDir, log) {
		if r.Err != nil {
			log.Warn("воркер убрал за собой не всё", "worker", r.Worker, "err", r.Err)
		}
	}
	if !spec.Release || spec.ReleaseDir == "" {
		return
	}
	for _, name := range []string{config.ReleaseCurrent, config.ReleasePrevious, config.ReleaseVersion, config.ReleasePreviousVersion} {
		if err := os.RemoveAll(filepath.Join(spec.ReleaseDir, name)); err != nil {
			log.Warn("сборка воркера не удалена", "worker", spec.Name, "err", err)
		}
	}
	log.Info("воркер убран с узла: уборка выполнена, сборка удалена", "worker", spec.Name)
}

// setupSum — sha256 файла setup и файлов зависимостей рядом (имя и содержимое каждого).
func setupSum(dir string) (string, error) {
	files := []string{config.Setup}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if !e.Type().IsRegular() {
			continue
		}
		for _, pattern := range setupInputs {
			if ok, _ := filepath.Match(pattern, e.Name()); ok {
				files = append(files, e.Name())
				break
			}
		}
	}
	h := sha256.New()
	for _, name := range files {
		f, err := os.Open(filepath.Join(dir, name))
		if err != nil {
			return "", err
		}
		fmt.Fprintf(h, "%s\n", name)
		_, err = io.Copy(h, f)
		f.Close()
		if err != nil {
			return "", err
		}
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
