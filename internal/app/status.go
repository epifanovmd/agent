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

	"github.com/epifanovmd/agent/sdk/go/message"
)

// StatusFile — отметка «агент работает» в каталоге данных: её обновляет
// работающий агент, читает `agent status`.
const StatusFile = "agent.status"

// Отметка обновляется раз в heartbeatEvery; старше heartbeatStale — агент
// считается зависшим (переменные — для тестов).
var (
	heartbeatEvery = 15 * time.Second
	heartbeatStale = time.Minute
)

// AgentStatus — содержимое отметки.
type AgentStatus struct {
	PID int `json:"pid"`
	// At — когда обновлена, мс UTC.
	At int64 `json:"at"`
	// Online — есть ли сейчас связь с сервером.
	Online bool `json:"online"`
}

// heartbeat — отметка «агент работает», пока агент запущен.
type heartbeat struct {
	path   string
	online func() bool
}

func (heartbeat) Declare(*message.Capabilities)                  {}
func (heartbeat) Handles() []string                              { return nil }
func (heartbeat) Handle(context.Context, message.Envelope) error { return nil }

func (h heartbeat) Start(ctx context.Context) error {
	defer os.Remove(h.path)
	for {
		raw, _ := json.Marshal(AgentStatus{PID: os.Getpid(), At: time.Now().UnixMilli(), Online: h.online()})
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
