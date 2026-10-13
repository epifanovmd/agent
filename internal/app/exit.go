//go:build unix

package app

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/worker"
)

// ErrorFile — последняя ошибка, с которой агент завершился (agent status показывает её, пока
// агент не запущен).
const ErrorFile = "agent.error"

// ExitError — последняя ошибка, с которой агент завершился.
type ExitError struct {
	At    int64  `json:"at"`
	Error string `json:"error"`
}

// RecordExit — запомнить ошибку завершения агента в каталоге данных (agent status покажет её).
// Ошибка записи не важна.
func RecordExit(dir string, err error) {
	if dir == "" || err == nil {
		return
	}
	raw, _ := json.Marshal(ExitError{At: time.Now().UnixMilli(), Error: err.Error()})
	_ = os.WriteFile(filepath.Join(dir, ErrorFile), raw, 0o600)
}

// LastExit — последняя ошибка завершения (nil — нет).
func LastExit(dir string) *ExitError {
	raw, err := os.ReadFile(filepath.Join(dir, ErrorFile))
	if err != nil {
		return nil
	}
	var e ExitError
	if json.Unmarshal(raw, &e) != nil || e.Error == "" {
		return nil
	}
	return &e
}

// StopWorkers — `agent stop-workers`: остановить воркеры, оставшиеся работать после остановки
// агента; cleanup — затем выполнить ./cleanup воркеров (перед удалением агента). Агент с этим
// dataDir не должен работать. Итог — имена остановленных и итоги уборки; не остановленные —
// ошибка с их списком (уборка тогда не выполняется).
func StopWorkers(ctx context.Context, cfg config.Config, cleanup bool, log *slog.Logger) ([]string, []worker.CleanupResult, error) {
	lock, err := LockDataDir(cfg.DataDir)
	if err != nil {
		return nil, nil, err
	}
	defer lock.Unlock()
	stopped, failed := worker.StopProcesses(cfg.DataDir)
	if len(failed) > 0 {
		return stopped, nil, fmt.Errorf("не остановлены (нет прав — запустите от root): %s", strings.Join(failed, ", "))
	}
	if !cleanup {
		return stopped, nil, nil
	}
	return stopped, worker.Cleanup(ctx, cfg.Workers, cfg.DataDir, log), nil
}
