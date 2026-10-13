// Package store — хранилище агента в одном файле (bbolt, §18): очередь итогов и событий для
// бэкенда, журнал команд, последнее желаемое состояние и итоги применения настроек воркерами.
// Связанные записи меняются в одной транзакции: событие принято воркером — оно в очереди;
// команда выполнена — её итог в очереди.
package store

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	bolt "go.etcd.io/bbolt"

	"github.com/epifanovmd/agent/protocol"
)

// FileName — файл хранилища в каталоге данных агента.
const FileName = "agent.db"

// Виды записей очереди.
const (
	KindResult = "result"
	KindEvent  = "event"
)

// ErrFull — очередь событий полна (OUTBOX_FULL).
var ErrFull = errors.New("store: очередь событий полна")

var (
	bucketOutbox   = []byte("outbox")
	bucketIndex    = []byte("outbox_ids")
	bucketCommands = []byte("commands")
	bucketOrder    = []byte("commands_order")
	bucketDesired  = []byte("desired")
	bucketApplied  = []byte("applied")
	bucketMeta     = []byte("meta")

	keyDesired  = []byte("doc")
	keyLastSeq  = []byte("lastSeq")
	keyJournalN = []byte("journalN")
)

// Состояние команды в журнале.
const (
	CommandNew     = ""
	CommandRunning = "running"
	CommandDone    = "done"
)

// Store — хранилище; методы безопасны для вызова из разных горутин.
type Store struct {
	// Recovered — куда отложен повреждённый файл при открытии ("" — файл был цел).
	Recovered string

	db      *bolt.DB
	limit   int
	journal int
	notify  chan struct{}

	mu     sync.Mutex
	events int // событий в очереди (для предела)
	total  int // записей в очереди
}

// Item — запись очереди: ID — id события или re итога, Raw — её JSON.
type Item struct {
	ID  string
	Raw json.RawMessage
}

type record struct {
	Kind string          `json:"kind"`
	ID   string          `json:"id"`
	Raw  json.RawMessage `json:"raw"`
}

type commandRecord struct {
	Command protocol.Command `json:"command"`
	State   string           `json:"state"`
	Result  *protocol.Result `json:"result,omitempty"`
	At      int64            `json:"at"`
	Order   uint64           `json:"order"`
}

// Open — хранилище в каталоге dir; limit — предел событий в очереди (0 — без предела).
// Повреждённый файл (сбой диска, выключение питания) откладывается как agent.db.broken-<время>
// и хранилище начинается заново: путь отложенного — Store.Recovered. Файл занят другим
// процессом — ошибка.
func Open(dir string, limit int) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, FileName)
	s, err := openFile(path, limit)
	if err == nil || errors.Is(err, bolt.ErrTimeout) {
		return s, err
	}
	broken := fmt.Sprintf("%s.broken-%d", path, time.Now().Unix())
	if rerr := os.Rename(path, broken); rerr != nil {
		return nil, fmt.Errorf("store: %w (повреждённый файл не отложен: %v)", err, rerr)
	}
	if s, err = openFile(path, limit); err != nil {
		return nil, err
	}
	s.Recovered = broken
	return s, nil
}

// openFile — открыть файл и проверить разделы; паника bbolt на повреждённых страницах — ошибка.
func openFile(path string, limit int) (s *Store, err error) {
	db, err := bolt.Open(path, 0o600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("store: файл повреждён: %v", r)
		}
		if err != nil {
			db.Close()
			s = nil
		}
	}()
	s = &Store{db: db, limit: limit, journal: protocol.CommandJournal, notify: make(chan struct{}, 1)}
	err = db.Update(func(tx *bolt.Tx) error {
		for _, b := range [][]byte{bucketOutbox, bucketIndex, bucketCommands, bucketOrder, bucketDesired, bucketApplied, bucketMeta} {
			if _, err := tx.CreateBucketIfNotExists(b); err != nil {
				return err
			}
		}
		return tx.Bucket(bucketOutbox).ForEach(func(_, v []byte) error {
			var r record
			if json.Unmarshal(v, &r) == nil && r.Kind == KindEvent {
				s.events++
			}
			s.total++
			return nil
		})
	})
	if err == nil {
		err = db.View(func(tx *bolt.Tx) error {
			for _, b := range [][]byte{bucketCommands, bucketDesired, bucketApplied, bucketMeta} {
				if err := tx.Bucket(b).ForEach(func(_, _ []byte) error { return nil }); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err != nil {
		return nil, fmt.Errorf("store: %w", err)
	}
	return s, nil
}

// Close — закрыть файл.
func (s *Store) Close() error { return s.db.Close() }

// SetLimit — предел событий в очереди.
func (s *Store) SetLimit(limit int) {
	s.mu.Lock()
	s.limit = limit
	s.mu.Unlock()
}

// Notify — сигнал «в очереди новое».
func (s *Store) Notify() <-chan struct{} { return s.notify }

func (s *Store) wake() {
	select {
	case s.notify <- struct{}{}:
	default:
	}
}

// Len — записей в очереди.
func (s *Store) Len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.total
}

func u64(n uint64) []byte {
	var b [8]byte
	binary.BigEndian.PutUint64(b[:], n)
	return b[:]
}

func indexKey(kind, id string) []byte { return []byte(kind + "\n" + id) }

// put — запись в очередь в транзакции tx; повтор того же id — без второй записи.
func put(tx *bolt.Tx, kind, id string, v any) (bool, error) {
	idx := tx.Bucket(bucketIndex)
	if idx.Get(indexKey(kind, id)) != nil {
		return false, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return false, err
	}
	b := tx.Bucket(bucketOutbox)
	seq, err := b.NextSequence()
	if err != nil {
		return false, err
	}
	val, _ := json.Marshal(record{Kind: kind, ID: id, Raw: raw})
	if err := b.Put(u64(seq), val); err != nil {
		return false, err
	}
	return true, idx.Put(indexKey(kind, id), u64(seq))
}

// AppendEvents — события воркера в очередь одной транзакцией: все или ни одного; очередь полна
// — ErrFull. Повтор события с тем же id не записывается второй раз.
func (s *Store) AppendEvents(events []protocol.Event) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.limit > 0 && s.events+len(events) > s.limit {
		return ErrFull
	}
	added := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		for _, e := range events {
			ok, err := put(tx, KindEvent, e.ID, e)
			if err != nil {
				return err
			}
			if ok {
				added++
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.events += added
	s.total += added
	s.wake()
	return nil
}

// Pending — первые записи очереди вида kind: не больше max и maxBytes (первая — всегда).
func (s *Store) Pending(kind string, max, maxBytes int) ([]Item, error) {
	var out []Item
	size := 0
	err := s.db.View(func(tx *bolt.Tx) error {
		c := tx.Bucket(bucketOutbox).Cursor()
		for k, v := c.First(); k != nil && len(out) < max; k, v = c.Next() {
			var r record
			if json.Unmarshal(v, &r) != nil || r.Kind != kind {
				continue
			}
			if len(out) > 0 && size+len(r.Raw) > maxBytes {
				break
			}
			size += len(r.Raw)
			out = append(out, Item{ID: r.ID, Raw: r.Raw})
		}
		return nil
	})
	return out, err
}

// Remove — убрать из очереди записи вида kind с этими id (подтверждены бэкендом).
func (s *Store) Remove(kind string, ids []string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	removed := 0
	err := s.db.Update(func(tx *bolt.Tx) error {
		idx, b := tx.Bucket(bucketIndex), tx.Bucket(bucketOutbox)
		for _, id := range ids {
			seq := idx.Get(indexKey(kind, id))
			if seq == nil {
				continue
			}
			if err := b.Delete(seq); err != nil {
				return err
			}
			if err := idx.Delete(indexKey(kind, id)); err != nil {
				return err
			}
			removed++
		}
		return nil
	})
	if err != nil {
		return err
	}
	s.total -= removed
	if kind == KindEvent {
		s.events -= removed
	}
	return nil
}

// StartCommand — команда в журнал: новая — отмечается выполняемой, итог — CommandNew; уже
// известная — её состояние, итог (у выполненной) и сама команда из журнала.
func (s *Store) StartCommand(cmd protocol.Command) (state string, result *protocol.Result, err error) {
	err = s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketCommands)
		if v := b.Get([]byte(cmd.ID)); v != nil {
			var r commandRecord
			if err := json.Unmarshal(v, &r); err != nil {
				return err
			}
			state, result = r.State, r.Result
			return nil
		}
		order, err := tx.Bucket(bucketOrder).NextSequence()
		if err != nil {
			return err
		}
		raw, _ := json.Marshal(commandRecord{Command: cmd, State: CommandRunning, At: time.Now().UnixMilli(), Order: order})
		if err := b.Put([]byte(cmd.ID), raw); err != nil {
			return err
		}
		if err := tx.Bucket(bucketOrder).Put(u64(order), []byte(cmd.ID)); err != nil {
			return err
		}
		state = CommandNew
		return s.prune(tx)
	})
	return state, result, err
}

// prune — журнал не больше s.journal команд: старые выполненные убираются.
func (s *Store) prune(tx *bolt.Tx) error {
	order, cmds, meta := tx.Bucket(bucketOrder), tx.Bucket(bucketCommands), tx.Bucket(bucketMeta)
	n := 1
	if v := meta.Get(keyJournalN); len(v) == 8 {
		n = int(binary.BigEndian.Uint64(v)) + 1
	}
	extra := n - s.journal
	defer func() { _ = meta.Put(keyJournalN, u64(uint64(n))) }()
	if extra <= 0 {
		return nil
	}
	c := order.Cursor()
	var drop [][]byte
	for k, v := c.First(); k != nil && extra > 0; k, v = c.Next() {
		raw := cmds.Get(v)
		var r commandRecord
		if raw != nil && json.Unmarshal(raw, &r) == nil && r.State == CommandRunning {
			continue
		}
		drop = append(drop, append([]byte(nil), k...))
		_ = cmds.Delete(v)
		extra--
		n--
	}
	for _, k := range drop {
		if err := order.Delete(k); err != nil {
			return err
		}
	}
	return nil
}

// FinishCommand — итог команды: отметка в журнале и запись в очередь одной транзакцией.
// Повтор итога той же команды не записывается второй раз.
func (s *Store) FinishCommand(r protocol.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	added := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketCommands)
		if v := b.Get([]byte(r.Re)); v != nil {
			var rec commandRecord
			if err := json.Unmarshal(v, &rec); err != nil {
				return err
			}
			if rec.State == CommandDone {
				return nil
			}
			rec.State, rec.Result = CommandDone, &r
			raw, _ := json.Marshal(rec)
			if err := b.Put([]byte(r.Re), raw); err != nil {
				return err
			}
		}
		ok, err := put(tx, KindResult, r.Re, r)
		added = ok
		return err
	})
	if err != nil {
		return err
	}
	if added {
		s.total++
		s.wake()
	}
	return nil
}

// RequeueResult — итог выполненной команды снова в очередь (бэкенд прислал её повторно, итога
// не получив).
func (s *Store) RequeueResult(r protocol.Result) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	added := false
	err := s.db.Update(func(tx *bolt.Tx) error {
		ok, err := put(tx, KindResult, r.Re, r)
		added = ok
		return err
	})
	if err != nil {
		return err
	}
	if added {
		s.total++
		s.wake()
	}
	return nil
}

// Running — выполняемые команды из журнала (после перезапуска агента).
func (s *Store) Running() ([]protocol.Command, error) {
	var out []protocol.Command
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketCommands).ForEach(func(_, v []byte) error {
			var r commandRecord
			if json.Unmarshal(v, &r) == nil && r.State == CommandRunning {
				out = append(out, r.Command)
			}
			return nil
		})
	})
	return out, err
}

// LastSeq — seq последней полученной команды бэкенда.
func (s *Store) LastSeq() int64 {
	var seq int64
	_ = s.db.View(func(tx *bolt.Tx) error {
		if v := tx.Bucket(bucketMeta).Get(keyLastSeq); len(v) == 8 {
			seq = int64(binary.BigEndian.Uint64(v))
		}
		return nil
	})
	return seq
}

// SetLastSeq — запомнить seq полученной команды (только если больше прежнего).
func (s *Store) SetLastSeq(seq int64) error {
	return s.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(bucketMeta)
		if v := b.Get(keyLastSeq); len(v) == 8 && int64(binary.BigEndian.Uint64(v)) >= seq {
			return nil
		}
		return b.Put(keyLastSeq, u64(uint64(seq)))
	})
}

// SaveDesired — желаемое состояние.
func (s *Store) SaveDesired(d protocol.Desired) error {
	raw, err := json.Marshal(d)
	if err != nil {
		return err
	}
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketDesired).Put(keyDesired, raw) })
}

// LoadDesired — последнее сохранённое желаемое; false — его нет.
func (s *Store) LoadDesired() (protocol.Desired, bool, error) {
	var d protocol.Desired
	found := false
	err := s.db.View(func(tx *bolt.Tx) error {
		v := tx.Bucket(bucketDesired).Get(keyDesired)
		if v == nil {
			return nil
		}
		found = true
		return json.Unmarshal(v, &d)
	})
	return d, found, err
}

// SetApplied — итог применения ключа воркером.
func (s *Store) SetApplied(worker string, a protocol.Applied) error {
	ok := a.OK
	st := protocol.AppliedStatus{Version: a.Version, OK: &ok, Error: a.Error}
	raw, _ := json.Marshal(st)
	return s.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketApplied).Put([]byte(worker+"\n"+a.Key), raw)
	})
}

// DropApplied — ключа больше нет в желаемом: убрать его итог.
func (s *Store) DropApplied(worker, key string) error {
	return s.db.Update(func(tx *bolt.Tx) error { return tx.Bucket(bucketApplied).Delete([]byte(worker + "\n" + key)) })
}

// Applied — итоги применения: воркер → ключ → итог.
func (s *Store) Applied() (map[string]map[string]protocol.AppliedStatus, error) {
	out := map[string]map[string]protocol.AppliedStatus{}
	err := s.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(bucketApplied).ForEach(func(k, v []byte) error {
			worker, key, ok := cutKey(string(k))
			var st protocol.AppliedStatus
			if !ok || json.Unmarshal(v, &st) != nil {
				return nil
			}
			if out[worker] == nil {
				out[worker] = map[string]protocol.AppliedStatus{}
			}
			out[worker][key] = st
			return nil
		})
	})
	return out, err
}

func cutKey(k string) (string, string, bool) {
	for i := 0; i < len(k); i++ {
		if k[i] == '\n' {
			return k[:i], k[i+1:], true
		}
	}
	return "", "", false
}
