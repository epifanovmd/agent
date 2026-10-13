//go:build unix

package app

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// LockFile — файл блокировки в каталоге данных.
const LockFile = "agent.lock"

// ErrLocked — каталогом данных уже пользуется другой процесс агента.
var ErrLocked = errors.New("агент с этим dataDir уже запущен")

// Lock — блокировка каталога данных: один агент (agent run или agent
// cleanup) на один dataDir. Снимается Unlock или с завершением процесса.
type Lock struct{ f *os.File }

// LockDataDir — взять блокировку <dir>/agent.lock без ожидания; занята —
// ErrLocked. В файл пишется pid владельца — для человека, агент его не читает.
func LockDataDir(dir string) (*Lock, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("agent: каталог данных: %w", err)
	}
	path := filepath.Join(dir, LockFile)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, fmt.Errorf("agent: блокировка %s: %w", path, err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("agent: %w (%s)", ErrLocked, path)
		}
		return nil, fmt.Errorf("agent: блокировка %s: %w", path, err)
	}
	_ = f.Truncate(0)
	_, _ = fmt.Fprintf(f, "%d\n", os.Getpid())
	return &Lock{f: f}, nil
}

// Unlock — снять блокировку (повторно — без вреда).
func (l *Lock) Unlock() {
	if l == nil || l.f == nil {
		return
	}
	_ = syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	_ = l.f.Close()
	l.f = nil
}
