// Package outbox — журнал надёжных сообщений агента на диске: сообщение
// хранится до подтверждения сервером и переживает обрыв связи и рестарт.
package outbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

const suffix = ".json"

// Outbox — каталог: файл на сообщение, имя — порядок записи и id.
type Outbox struct {
	dir     string
	mu      sync.Mutex
	counter atomic.Uint64
	notify  chan struct{}
}

// Open — журнал в каталоге dir (создаётся).
func Open(dir string) (*Outbox, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("outbox: %w", err)
	}
	// Недописанные временные файлы прошлого запуска — мусор.
	tmp, _ := filepath.Glob(filepath.Join(dir, "*.tmp"))
	for _, f := range tmp {
		_ = os.Remove(f)
	}
	return &Outbox{dir: dir, notify: make(chan struct{}, 1)}, nil
}

// Notify — сигнал «появилось новое сообщение».
func (o *Outbox) Notify() <-chan struct{} { return o.notify }

// Append — сохранить сообщение (атомарно, с fsync). У конверта должен быть id.
func (o *Outbox) Append(env message.Envelope) error {
	if env.ID == "" {
		return errors.New("outbox: у надёжного сообщения нет id")
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	name := fmt.Sprintf("%020d-%06d-%s%s", time.Now().UnixNano(), o.counter.Add(1)%1_000_000, env.ID, suffix)
	path := filepath.Join(o.dir, name)

	o.mu.Lock()
	defer o.mu.Unlock()
	if err := writeAtomic(path, raw); err != nil {
		return err
	}
	select {
	case o.notify <- struct{}{}:
	default:
	}
	return nil
}

// Pending — неподтверждённые сообщения в порядке записи.
func (o *Outbox) Pending() ([]message.Envelope, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	names, err := o.names()
	if err != nil {
		return nil, err
	}
	out := make([]message.Envelope, 0, len(names))
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(o.dir, name))
		if err != nil {
			continue
		}
		var env message.Envelope
		if json.Unmarshal(raw, &env) != nil {
			// Испорченная запись не должна блокировать остальные.
			_ = os.Remove(filepath.Join(o.dir, name))
			continue
		}
		out = append(out, env)
	}
	return out, nil
}

// Len — число неподтверждённых сообщений.
func (o *Outbox) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	names, _ := o.names()
	return len(names)
}

// Remove — сообщения подтверждены (или отклонены без повтора).
func (o *Outbox) Remove(ids ...string) {
	if len(ids) == 0 {
		return
	}
	want := make(map[string]bool, len(ids))
	for _, id := range ids {
		want[id] = true
	}
	o.mu.Lock()
	defer o.mu.Unlock()
	names, _ := o.names()
	for _, name := range names {
		id := strings.TrimSuffix(name[strings.LastIndex(name, "-")+1:], suffix)
		if want[id] {
			_ = os.Remove(filepath.Join(o.dir, name))
		}
	}
}

func (o *Outbox) names() ([]string, error) {
	entries, err := os.ReadDir(o.dir)
	if err != nil {
		return nil, fmt.Errorf("outbox: %w", err)
	}
	names := make([]string, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), suffix) {
			names = append(names, e.Name())
		}
	}
	sort.Strings(names)
	return names, nil
}

// writeAtomic — запись во временный файл, fsync и переименование.
func writeAtomic(path string, data []byte) error {
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o600)
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	if _, err := f.Write(data); err != nil {
		f.Close()
		return fmt.Errorf("outbox: %w", err)
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return fmt.Errorf("outbox: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	return os.Rename(tmp, path)
}

// JobRefs — задачи, чей итог (job.complete, job.fail, job.reject) ещё не
// подтверждён сервером: агент их по-прежнему держит.
func (o *Outbox) JobRefs() []message.JobRef {
	pending, err := o.Pending()
	if err != nil {
		return nil
	}
	var refs []message.JobRef
	for _, env := range pending {
		switch env.Type {
		case message.TypeJobComplete, message.TypeJobFail, message.TypeJobReject:
			var ref message.JobRef
			if env.Decode(&ref) == nil && ref.JobID != "" {
				refs = append(refs, ref)
			}
		}
	}
	return refs
}
