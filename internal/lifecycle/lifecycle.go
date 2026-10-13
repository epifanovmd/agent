// Package lifecycle — состояние агента (docs/PROTOCOL.md §4): pending — ждёт регистрации и
// подтверждения бэкендом, воркеры не запущены; active — воркеры работают (и без связи);
// revoked — отозван бэкендом: воркеры остановлены, связи нет. Хранится в каталоге данных рядом с
// ключом агента и переживает перезапуски; сбрасывает его новая установка с токеном.
package lifecycle

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/epifanovmd/agent/internal/fsutil"
	"github.com/epifanovmd/agent/protocol"
)

// FileName — файл состояния в каталоге данных.
const FileName = "status.json"

// State — состояние агента.
type State struct {
	// Status — protocol.StatusPending | StatusActive | StatusRevoked.
	Status string `json:"status"`
	// Since — с какого момента, мс UTC.
	Since int64 `json:"since"`
	// Reason — почему (у revoked — ответ бэкенда).
	Reason string `json:"reason,omitempty"`
}

// Load — состояние из каталога dir; файла нет — pending.
func Load(dir string) (State, error) {
	raw, err := os.ReadFile(filepath.Join(dir, FileName))
	if errors.Is(err, os.ErrNotExist) {
		return State{Status: protocol.StatusPending}, nil
	}
	if err != nil {
		return State{}, fmt.Errorf("lifecycle: %w", err)
	}
	var st State
	if err := json.Unmarshal(raw, &st); err != nil {
		return State{}, fmt.Errorf("lifecycle: %s: %w", FileName, err)
	}
	switch st.Status {
	case protocol.StatusPending, protocol.StatusActive, protocol.StatusRevoked:
		return st, nil
	}
	return State{}, fmt.Errorf("lifecycle: неизвестное состояние %q", st.Status)
}

// Save — записать состояние status (с причиной reason) в каталог dir.
func Save(dir, status, reason string) (State, error) {
	st := State{Status: status, Since: time.Now().UnixMilli(), Reason: reason}
	raw, err := json.Marshal(st)
	if err != nil {
		return State{}, err
	}
	if err := fsutil.WriteFileSync(filepath.Join(dir, FileName), raw, 0o600); err != nil {
		return State{}, fmt.Errorf("lifecycle: %w", err)
	}
	return st, nil
}

// Reset — забыть состояние (новая регистрация): агент снова pending.
func Reset(dir string) error {
	err := os.Remove(filepath.Join(dir, FileName))
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}
