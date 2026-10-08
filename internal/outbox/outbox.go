// Package outbox — журнал надёжных сообщений агента на диске: сообщение
// хранится до подтверждения сервером и переживает обрыв связи и рестарт.
// Список сообщений держится в памяти (читается с диска один раз при
// открытии); сами сообщения читаются с диска только при отправке.
package outbox

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

const suffix = ".json"

// DefaultLimit — сообщений в журнале не больше, если предел не задан.
const DefaultLimit = 10000

// ErrFull — журнал полон и отбросить нечего: все сообщения в нём — итоги
// задач и команд.
var ErrFull = errors.New("outbox: очередь важных сообщений переполнена")

// droppable — сообщения, которые при переполнении можно потерять: события
// (сервер их только показывает) и state.applied (сервер пришлёт снимок снова,
// агент применит и сообщит заново). Итоги задач и команд не отбрасываются.
var droppable = map[string]bool{message.TypeEvent: true, message.TypeJobEvent: true, message.TypeStateApplied: true}

// item — запись журнала в памяти: файл, id и то, что нужно без чтения файла.
type item struct {
	name, id, typ string
	ref           *message.JobRef
}

// Outbox — каталог: файл на сообщение, имя — порядок записи и id.
type Outbox struct {
	dir     string
	mu      sync.Mutex
	counter atomic.Uint64
	notify  chan struct{}
	items   []item
	limit   int
	log     *slog.Logger
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
	o := &Outbox{dir: dir, notify: make(chan struct{}, 1), limit: DefaultLimit, log: slog.New(slog.DiscardHandler)}
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

// SetLimit — сообщений не больше limit (0 и меньше — DefaultLimit); log —
// куда писать об отброшенных.
func (o *Outbox) SetLimit(limit int, log *slog.Logger) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if limit <= 0 {
		limit = DefaultLimit
	}
	o.limit = limit
	if log != nil {
		o.log = log
	}
}

func newItem(name string, env message.Envelope) item {
	it := item{name: name, id: env.ID, typ: env.Type}
	switch env.Type {
	case message.TypeJobComplete, message.TypeJobFail, message.TypeJobReject:
		var ref message.JobRef
		if env.Decode(&ref) == nil && ref.JobID != "" {
			it.ref = &ref
		}
	}
	return it
}

// Notify — сигнал «появилось новое сообщение».
func (o *Outbox) Notify() <-chan struct{} { return o.notify }

// Append — сохранить сообщение (атомарно, с fsync файла и каталога). У
// конверта должен быть id. Журнал полон — отбрасываются самые старые из
// сообщений, которые можно потерять (событие, state.applied); таких нет —
// ErrFull.
func (o *Outbox) Append(env message.Envelope) error {
	if env.ID == "" {
		return errors.New("outbox: у надёжного сообщения нет id")
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return fmt.Errorf("outbox: %w", err)
	}
	name := fmt.Sprintf("%020d-%06d-%s%s", time.Now().UnixNano(), o.counter.Add(1)%1_000_000, env.ID, suffix)

	o.mu.Lock()
	defer o.mu.Unlock()
	if len(o.items) >= o.limit {
		idx := slices.IndexFunc(o.items, func(it item) bool { return droppable[it.typ] })
		if idx < 0 {
			o.log.Error("outbox: очередь важных сообщений переполнена — сообщение не записано", "type", env.Type, "limit", o.limit)
			return ErrFull
		}
		old := o.items[idx]
		_ = os.Remove(filepath.Join(o.dir, old.name))
		o.items = slices.Delete(o.items, idx, idx+1)
		o.log.Warn("outbox: очередь переполнена — отброшено самое старое необязательное сообщение", "type", old.typ, "id", old.id, "limit", o.limit)
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

// JobRefs — задачи, чей итог (job.complete, job.fail, job.reject) ещё не
// подтверждён сервером: агент их по-прежнему держит.
func (o *Outbox) JobRefs() []message.JobRef {
	o.mu.Lock()
	defer o.mu.Unlock()
	var refs []message.JobRef
	for _, it := range o.items {
		if it.ref != nil {
			refs = append(refs, *it.ref)
		}
	}
	return refs
}
