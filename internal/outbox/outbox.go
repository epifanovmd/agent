// Package outbox — очередь важных сообщений агента на диске (event,
// config.applied, action.result): сообщение хранится до подтверждения
// сервером (ack {ids}) и переживает обрыв связи и перезапуск.
// Список сообщений держится в памяти (читается с диска один раз при
// открытии); сами сообщения читаются с диска только при отправке.
package outbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

const suffix = ".json"

// ErrFull — в очереди уже message.MaxOutbox сообщений: событие воркера не
// принимается (итоги настроек и действий записываются всегда).
var ErrFull = errors.New("outbox: очередь важных сообщений переполнена")

// item — запись журнала в памяти: файл, id и то, что нужно без чтения файла.
type item struct {
	name, id, typ string
}

// Outbox — каталог: файл на сообщение, имя — порядок записи и id.
type Outbox struct {
	dir     string
	mu      sync.Mutex
	counter atomic.Uint64
	notify  chan struct{}
	items   []item
	limit   int
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
	o := &Outbox{dir: dir, notify: make(chan struct{}, 1), limit: message.MaxOutbox}
	entries, err := os.ReadDir(dir)
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
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(dir, name))
		var env message.Envelope
		if err != nil || json.Unmarshal(raw, &env) != nil || env.ID == "" {
			// Испорченная запись не должна блокировать остальные.
			_ = os.Remove(filepath.Join(dir, name))
			continue
		}
		o.items = append(o.items, newItem(name, env))
	}
	return o, nil
}

// SetLimit — предел для событий (тесты).
func (o *Outbox) SetLimit(limit int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	o.limit = limit
}

func newItem(name string, env message.Envelope) item {
	return item{name: name, id: env.ID, typ: env.Type}
}

// Notify — сигнал «появилось новое сообщение».
func (o *Outbox) Notify() <-chan struct{} { return o.notify }

// Append — сохранить сообщение (атомарно, с fsync файла и каталога). У
// конверта должен быть id. Событие при полной очереди — ErrFull.
func (o *Outbox) Append(env message.Envelope) error {
	if env.ID == "" {
		return errors.New("outbox: у важного сообщения нет id")
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	name := fmt.Sprintf("%020d-%06d-%s%s", time.Now().UnixNano(), o.counter.Add(1)%1_000_000, env.ID, suffix)

	o.mu.Lock()
	defer o.mu.Unlock()
	if env.Type == message.TypeEvent && len(o.items) >= o.limit {
		return ErrFull
	}
	if err := writeAtomic(filepath.Join(o.dir, name), raw); err != nil {
		return err
	}
	syncDir(o.dir)
	o.items = append(o.items, newItem(name, env))
	select {
	case o.notify <- struct{}{}:
	default:
	}
	return nil
}

// IDs — id неподтверждённых сообщений в порядке записи (без чтения файлов).
func (o *Outbox) IDs() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	ids := make([]string, len(o.items))
	for i, it := range o.items {
		ids[i] = it.id
	}
	return ids
}

// Read — сообщение id с диска; ok false — его уже нет (подтверждено или
// испорчено).
func (o *Outbox) Read(id string) (message.Envelope, bool) {
	o.mu.Lock()
	idx := slices.IndexFunc(o.items, func(it item) bool { return it.id == id })
	var name string
	if idx >= 0 {
		name = o.items[idx].name
	}
	o.mu.Unlock()
	if idx < 0 {
		return message.Envelope{}, false
	}
	raw, err := os.ReadFile(filepath.Join(o.dir, name))
	var env message.Envelope
	if err != nil || json.Unmarshal(raw, &env) != nil {
		o.Remove(id)
		return message.Envelope{}, false
	}
	return env, true
}

// Pending — неподтверждённые сообщения в порядке записи.
func (o *Outbox) Pending() ([]message.Envelope, error) {
	ids := o.IDs()
	out := make([]message.Envelope, 0, len(ids))
	for _, id := range ids {
		if env, ok := o.Read(id); ok {
			out = append(out, env)
		}
	}
	return out, nil
}

// Len — число неподтверждённых сообщений.
func (o *Outbox) Len() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return len(o.items)
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
	o.items = slices.DeleteFunc(o.items, func(it item) bool {
		if !want[it.id] {
			return false
		}
		_ = os.Remove(filepath.Join(o.dir, it.name))
		return true
	})
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

// syncDir — fsync каталога: переименование файла переживает сбой питания.
func syncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}
