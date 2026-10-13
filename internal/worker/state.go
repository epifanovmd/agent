//go:build unix

package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/fsutil"
)

// Каталоги воркеров в каталоге данных агента.
const (
	// ProcessesDir — файлы запущенных процессов воркеров (<имя>.json).
	ProcessesDir = "processes"
	// LogsDir — файлы вывода воркеров.
	LogsDir = "logs"
)

// procState — запущенный процесс воркера на диске: по нему следующий
// запуск агента подхватывает воркер (§11).
type procState struct {
	PID int `json:"pid"`
	// Start — время запуска процесса по данным системы (procStart): pid,
	// выданный системой другому процессу, не подходит.
	Start string `json:"start"`
	Token string `json:"token"`
	// Version — версия сборки воркера с сервера.
	Version string `json:"version,omitempty"`
	// Spec — отпечаток настроек, с которыми процесс запущен (specHash).
	Spec      string `json:"spec"`
	StartedAt int64  `json:"startedAt"`
	// Output — с каких мест читать файлы вывода (stdout, stderr).
	Output      [2]int64        `json:"output"`
	StopTimeout config.Duration `json:"stopTimeout"`
}

func statePath(dir, name string) string { return filepath.Join(dir, name+".json") }

func readState(dir, name string) (procState, bool) {
	var st procState
	raw, err := os.ReadFile(statePath(dir, name))
	if err != nil || json.Unmarshal(raw, &st) != nil || st.PID <= 0 {
		return st, false
	}
	return st, true
}

func writeState(dir, name string, st procState) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(st)
	if err != nil {
		return err
	}
	return fsutil.WriteFile(statePath(dir, name), raw, 0o600)
}

func removeState(dir, name string) { _ = os.Remove(statePath(dir, name)) }

// stateNames — имена воркеров, у которых есть файл процесса.
func stateNames(dir string) []string {
	files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
	out := make([]string, 0, len(files))
	for _, f := range files {
		out = append(out, strings.TrimSuffix(filepath.Base(f), ".json"))
	}
	return out
}

// alive — процесс из файла жив и это тот же процесс.
func (st procState) alive() bool {
	start, ok := procStart(st.PID)
	return ok && start == st.Start
}

// specHash — отпечаток настроек, от которых зависит процесс: без lifecycle
// (он меняется на ходу).
func specHash(spec config.Worker) string {
	spec.Lifecycle = config.Lifecycle{}
	raw, _ := json.Marshal(spec)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:12])
}

// stopOrphan — остановить процесс из файла, за которым никто не следит (воркера нет в
// настройках, не подхвачен): SIGTERM, не вышел за stopTimeout — SIGKILL. Чужой процесс с тем
// же pid не трогается. false — процесса нет или нет прав на сигнал.
func stopOrphan(st procState) bool {
	if !st.alive() {
		return false
	}
	// signal — сигнал группе процессов, иначе самому процессу; false — нет прав.
	signal := func(sig syscall.Signal) bool {
		if !st.alive() || syscall.Kill(-st.PID, sig) == nil {
			return true
		}
		return !errors.Is(syscall.Kill(st.PID, sig), syscall.EPERM)
	}
	wait := func(d time.Duration) bool {
		deadline := time.Now().Add(d)
		for st.alive() {
			if time.Now().After(deadline) {
				return false
			}
			time.Sleep(50 * time.Millisecond)
		}
		return true
	}
	if !signal(syscall.SIGTERM) {
		return false
	}
	if !wait(max(st.StopTimeout.Std(), time.Second)) {
		signal(syscall.SIGKILL)
		wait(2 * time.Second)
	}
	return true
}

// StopProcesses — `agent stop-workers`: остановить процессы воркеров,
// оставшиеся работать после остановки агента (файлы dataDir/processes):
// SIGTERM, не вышел за stopTimeout — SIGKILL. Итог — имена остановленных и тех, кого
// остановить не удалось (нет прав: процесс другого пользователя) — их файлы остаются.
func StopProcesses(dataDir string) (stopped, failed []string) {
	dir := filepath.Join(dataDir, ProcessesDir)
	for _, name := range stateNames(dir) {
		st, ok := readState(dir, name)
		if ok && stopOrphan(st) {
			stopped = append(stopped, name)
		}
		if ok && st.alive() {
			failed = append(failed, fmt.Sprintf("%s (pid %d)", name, st.PID))
			continue
		}
		removeState(dir, name)
	}
	return stopped, failed
}
