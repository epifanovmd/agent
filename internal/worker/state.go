//go:build unix

package worker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/epifanovmd/agent/internal/config"
)

// Каталоги воркеров в каталоге данных агента.
const (
	// ProcessesDir — файлы запущенных процессов воркеров (<имя>.json).
	ProcessesDir = "processes"
	// LogsDir — файлы вывода воркеров.
	LogsDir = "logs"
)

// procState — запущенный процесс воркера на диске: по нему следующий
// запуск агента подхватывает воркер (§13).
type procState struct {
	PID int `json:"pid"`
	// Start — время запуска процесса по данным системы (procStart): pid,
	// выданный системой другому процессу, не подходит.
	Start  string `json:"start"`
	Socket string `json:"socket"`
	Token  string `json:"token"`
	// Version — версия сборки воркера с сервера.
	Version string `json:"version,omitempty"`
	// Spec — отпечаток настроек, с которыми процесс запущен (specHash).
	Spec      string `json:"spec"`
	StartedAt int64  `json:"startedAt"`
	// Output — с каких мест читать файлы вывода (stdout, stderr).
	Output       [2]int64        `json:"output"`
	StopTimeout  config.Duration `json:"stopTimeout"`
	KeepChildren bool            `json:"keepChildren,omitempty"`
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
	path := statePath(dir, name)
	if err := os.WriteFile(path+".tmp", raw, 0o600); err != nil {
		return err
	}
	return os.Rename(path+".tmp", path)
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

// specHash — отпечаток настроек, от которых зависит процесс: без lifecycle,
// logs и routes (они меняются на ходу).
func specHash(spec config.Worker) string {
	spec.Lifecycle, spec.Logs, spec.Routes = config.Lifecycle{}, config.Logs{}, ""
	raw, _ := json.Marshal(spec)
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:12])
}

// stopOrphan — остановить процесс из файла, за которым никто не следит
// (воркера нет в настройках, встроенный): SIGTERM, не вышел за stopTimeout
// — SIGKILL. Чужой процесс с тем же pid не трогается.
func stopOrphan(st procState) bool {
	if !st.alive() {
		return false
	}
	signal := func(sig syscall.Signal) {
		if !st.alive() {
			return
		}
		if !st.KeepChildren && syscall.Kill(-st.PID, sig) == nil {
			return
		}
		_ = syscall.Kill(st.PID, sig)
	}
	signal(syscall.SIGTERM)
	deadline := time.Now().Add(max(st.StopTimeout.Std(), time.Second))
	for st.alive() {
		if time.Now().After(deadline) {
			signal(syscall.SIGKILL)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	return true
}
