//go:build unix

package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

// StatusFile — отметка «агент работает» в каталоге данных: её обновляет
// работающий агент, читает `agent status`.
const StatusFile = "agent.status"

// Отметка обновляется раз в heartbeatEvery; старше heartbeatStale — агент
// считается зависшим (переменные — для тестов).
var (
	heartbeatEvery = 5 * time.Second
	heartbeatStale = time.Minute
)

// ErrorFile — последняя ошибка, с которой агент завершился (agent status
// показывает её, пока агент не запущен); убирается, когда агент снова работает.
const ErrorFile = "agent.error"

// AgentStatus — содержимое отметки: агент жив, связь, воркеры с версиями настроек.
type AgentStatus struct {
	PID int `json:"pid"`
	// At — когда обновлена, мс UTC.
	At int64 `json:"at"`
	// Online — есть ли сейчас связь с сервером.
	Online bool `json:"online"`
	// StartedAt — когда запущен агент, мс UTC.
	StartedAt int64  `json:"startedAt,omitempty"`
	Version   string `json:"version,omitempty"`
	Name      string `json:"name,omitempty"`
	// AgentID — id агента на сервере ("" — ещё не зарегистрирован).
	AgentID string `json:"agentId,omitempty"`
	// Server — адрес сервера, с которым связь (или будет следующей).
	Server string `json:"server,omitempty"`
	// Outbox — сколько важных сообщений ждут ack.
	Outbox  int                    `json:"outbox"`
	Workers []message.WorkerStatus `json:"workers,omitempty"`
	// LastError, LastErrorAt — последняя ошибка связи (мс UTC).
	LastError   string `json:"lastError,omitempty"`
	LastErrorAt int64  `json:"lastErrorAt,omitempty"`
}

// ExitError — последняя ошибка, с которой агент завершился.
type ExitError struct {
	At    int64  `json:"at"`
	Error string `json:"error"`
}

// RecordExit — запомнить ошибку завершения агента в каталоге данных
// (agent status покажет её). Ошибка записи не важна.
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

// heartbeat — отметка «агент работает», пока агент запущен.
type heartbeat struct {
	path     string
	snapshot func() AgentStatus
}

func (h heartbeat) run(ctx context.Context) error {
	defer os.Remove(h.path)
	_ = os.Remove(filepath.Join(filepath.Dir(h.path), ErrorFile))
	for {
		st := h.snapshot()
		st.PID, st.At = os.Getpid(), time.Now().UnixMilli()
		raw, _ := json.Marshal(st)
		tmp := h.path + ".tmp"
		if err := os.WriteFile(tmp, raw, 0o600); err == nil {
			_ = os.Rename(tmp, h.path)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(heartbeatEvery):
		}
	}
}

// ErrNotRunning — агент с этим dataDir не запущен.
var ErrNotRunning = errors.New("агент с этим dataDir не запущен")

// Status — `agent status` (проверка живости, например HEALTHCHECK Docker):
// агент с каталогом данных dir запущен (держит блокировку) и недавно
// обновлял отметку. Ошибка — не запущен или завис.
func Status(dir string) (AgentStatus, error) {
	if lock, err := LockDataDir(dir); err == nil {
		lock.Unlock()
		return AgentStatus{}, ErrNotRunning
	} else if !errors.Is(err, ErrLocked) {
		return AgentStatus{}, err
	}
	var st AgentStatus
	raw, err := os.ReadFile(filepath.Join(dir, StatusFile))
	if err != nil || json.Unmarshal(raw, &st) != nil {
		return st, errors.New("агент запущен, но ещё не отметился (запускается или завис)")
	}
	if age := time.Since(time.UnixMilli(st.At)); age > heartbeatStale {
		return st, fmt.Errorf("агент не отмечался %s — завис", age.Round(time.Second))
	}
	return st, nil
}
