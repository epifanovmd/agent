//go:build unix

package worker

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/commands"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/jobs"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// releaseSetup — воркер из выпуска (release: true) с каталогом dir; install —
// положить тестовый бинарь как current с версией.
func releaseSetup(t *testing.T, install string, restart string) (*Supervisor, *jobs.Manager, string, context.CancelFunc) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "workers", "echo")
	if install != "" {
		_ = os.MkdirAll(dir, 0o755)
		installBuild(t, filepath.Join(dir, config.ReleaseCurrent), "")
		_ = os.WriteFile(filepath.Join(dir, config.ReleaseVersion), []byte(install+"\n"), 0o644)
	}
	manager := jobs.New(&recorder{}, logx.Discard(), func() {})
	spec := config.Worker{
		Name: "echo", Release: true, ReleaseDir: dir, Env: map[string]string{"TEST_WORKER": "1"}, Replicas: 1,
		Queues: []string{"echo"}, StopTimeout: config.Duration(5 * time.Second), Restart: restart,
	}
	sup := New([]config.Worker{spec}, manager, logx.Discard(), func() {}, "test")
	ctx, cancel := context.WithCancel(context.Background())
	go func() { _ = sup.Start(ctx) }()
	t.Cleanup(func() {
		cancel()
		stopCtx, stop := context.WithTimeout(context.Background(), 5*time.Second)
		defer stop()
		sup.Stop(stopCtx)
	})
	return sup, manager, dir, cancel
}

// installBuild — «сборка» воркера: тестовый бинарь (копия) или скрипт script.
func installBuild(t *testing.T, path, script string) {
	t.Helper()
	if script != "" {
		if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
			t.Fatal(err)
		}
		return
	}
	exe, _ := os.Executable()
	if err := copyFile(exe, path); err != nil {
		t.Fatal(err)
	}
}

func workerStatus(sup *Supervisor) (message.StatusWorker, message.Status) {
	st := message.Status{Slots: map[string]int{}}
	sup.ContributeStatus(&st)
	if len(st.Workers) == 0 {
		return message.StatusWorker{}, st
	}
	return st.Workers[0], st
}

// Путь current, release в статусе, версия из файла version — воркеру
// (AGENT_WORKER_RELEASE_VERSION).
func TestReleaseWorkerRunsCurrent(t *testing.T) {
	sup, manager, _, _ := releaseSetup(t, "1.0.0", "")
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	w, _ := workerStatus(sup)
	if !w.Release || w.Version != "1.0.0" || w.State != "running" {
		t.Fatalf("статус: %+v", w)
	}
	if !sup.Released("echo") || !sup.AnyReleased() {
		t.Fatal("Released")
	}
}

// Сборки нет — backoff с понятной ошибкой в статусе.
func TestReleaseWorkerMissingBuild(t *testing.T) {
	sup, _, _, _ := releaseSetup(t, "", "")
	waitFor(t, "backoff", func() bool { w, _ := workerStatus(sup); return w.State == "backoff" })
	_, st := workerStatus(sup)
	if st.State != message.StateDegraded || !strings.Contains(st.Message, "нет сборки") || !strings.Contains(st.Message, "install.sh --worker echo") {
		t.Fatalf("статус: %+v", st)
	}
}

func fetchBuild(t *testing.T, script string) func(string) error {
	return func(dst string) error {
		installBuild(t, dst, script)
		return nil
	}
}

func cmdCode(err error) string {
	var ce *commands.Error
	if errors.As(err, &ce) {
		return ce.Code
	}
	return ""
}

// Успех: previous отложена, current — новая, воркер зарегистрировался с новой версией.
func TestReleaseUpdate(t *testing.T) {
	for _, restart := range []string{config.RestartRolling, config.RestartStopFirst} {
		t.Run(restart, func(t *testing.T) {
			sup, manager, dir, _ := releaseSetup(t, "1.0.0", restart)
			waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
			var out bytes.Buffer
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			res, err := sup.Update(ctx, "echo", "2.0.0", fetchBuild(t, ""), &out)
			if err != nil {
				t.Fatalf("%v\n%s", err, out.String())
			}
			if res != (message.WorkerUpdateResult{Name: "echo", Version: "2.0.0", Previous: "1.0.0"}) {
				t.Fatalf("итог: %+v", res)
			}
			if readVersion(filepath.Join(dir, "version")) != "2.0.0" || readVersion(filepath.Join(dir, "previous.version")) != "1.0.0" {
				t.Fatal("файлы версий")
			}
			if _, err := os.Stat(filepath.Join(dir, "previous")); err != nil {
				t.Fatal("previous нет")
			}
			if w, _ := workerStatus(sup); w.Version != "2.0.0" || w.Instances != 1 {
				t.Fatalf("статус: %+v", w)
			}
			waitFor(t, "задачи берёт новый", func() bool { return slots(manager)["echo"] == 2 })
		})
	}
}

// Не из выпуска — WORKER_NOT_RELEASED; ошибка fetch (подпись, sha256) —
// как есть или WORKER_UPDATE_FAILED, файлы не тронуты.
func TestReleaseUpdateRejects(t *testing.T) {
	sup, manager, dir, _ := releaseSetup(t, "1.0.0", "")
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	ctx := context.Background()
	if _, err := sup.Update(ctx, "nope", "2", fetchBuild(t, ""), &bytes.Buffer{}); cmdCode(err) != message.ErrWorkerNotReleased {
		t.Fatalf("неизвестный: %v", err)
	}
	plain := New([]config.Worker{{Name: "p", Command: []string{"/bin/true"}}}, manager, logx.Discard(), func() {}, "t")
	if _, err := plain.Update(ctx, "p", "2", fetchBuild(t, ""), &bytes.Buffer{}); cmdCode(err) != message.ErrWorkerNotReleased {
		t.Fatalf("не release: %v", err)
	}
	sig := func(string) error { return errors.New("update: подпись релиза не сходится") }
	if _, err := sup.Update(ctx, "echo", "2", sig, &bytes.Buffer{}); cmdCode(err) != message.ErrWorkerUpdateFailed || !strings.Contains(err.Error(), "подпись") {
		t.Fatalf("подпись: %v", err)
	}
	verified := func(string) error { return commands.Errorf(message.ErrUpdateNotVerified, "ключа нет") }
	if _, err := sup.Update(ctx, "echo", "2", verified, &bytes.Buffer{}); cmdCode(err) != message.ErrUpdateNotVerified {
		t.Fatalf("без ключа: %v", err)
	}
	if readVersion(filepath.Join(dir, "version")) != "1.0.0" {
		t.Fatal("версия не должна меняться")
	}
	if _, err := os.Stat(filepath.Join(dir, "current.new")); !os.IsNotExist(err) {
		t.Fatal("временный файл остался")
	}
}

// Новая сборка не регистрируется (падает) или сообщает другую версию —
// откат на previous, WORKER_UPDATE_FAILED, работает прежняя.
func TestReleaseUpdateRollback(t *testing.T) {
	exe, _ := os.Executable()
	for name, script := range map[string]string{
		"падает":            "#!/bin/sh\necho сломано >&2\nexit 3\n",
		"другая версия":     "#!/bin/sh\nWORKER_VERSION=9.9.9 exec " + exe + "\n",
		"не регистрируется": "#!/bin/sh\nexec sleep 30\n",
	} {
		for _, restart := range []string{config.RestartRolling, config.RestartStopFirst} {
			t.Run(name+"/"+restart, func(t *testing.T) {
				old := updateWait
				updateWait = 2 * time.Second
				defer func() { updateWait = old }()
				sup, manager, dir, _ := releaseSetup(t, "1.0.0", restart)
				waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
				var out bytes.Buffer
				ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
				defer cancel()
				_, err := sup.Update(ctx, "echo", "2.0.0", fetchBuild(t, script), &out)
				if cmdCode(err) != message.ErrWorkerUpdateFailed || !strings.Contains(err.Error(), "возвращена прежняя сборка 1.0.0") {
					t.Fatalf("итог: %v\n%s", err, out.String())
				}
				if readVersion(filepath.Join(dir, "version")) != "1.0.0" {
					t.Fatal("версия не возвращена")
				}
				waitFor(t, "работает прежняя", func() bool {
					w, _ := workerStatus(sup)
					return w.State == "running" && w.Version == "1.0.0" && slots(manager)["echo"] == 2
				})
			})
		}
	}
}

// Повтор во время обновления — WORKER_UPDATE_IN_PROGRESS.
func TestReleaseUpdateInProgress(t *testing.T) {
	sup, manager, _, _ := releaseSetup(t, "1.0.0", "")
	waitFor(t, "регистрация", func() bool { return slots(manager)["echo"] == 2 })
	release := make(chan struct{})
	started := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = sup.Update(context.Background(), "echo", "2.0.0", func(dst string) error {
			close(started)
			<-release
			return errors.New("отменено тестом")
		}, &bytes.Buffer{})
	}()
	<-started
	if _, err := sup.Update(context.Background(), "echo", "3.0.0", fetchBuild(t, ""), &bytes.Buffer{}); cmdCode(err) != message.ErrWorkerUpdateInProgress {
		t.Fatalf("повтор: %v", err)
	}
	close(release)
	wg.Wait()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := sup.Update(ctx, "echo", "3.0.0", fetchBuild(t, ""), &bytes.Buffer{}); err != nil {
		t.Fatalf("после — можно: %v", err)
	}
}
