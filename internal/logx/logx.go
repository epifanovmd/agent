// Package logx — лог агента (slog): вывод в stderr, журнал последних записей
// по источникам (агент и каждый воркер) для действия agent.logs и записи для
// сервера (сообщение log).
package logx

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

// OutputKey — атрибут записи с выводом воркера: значение — имя воркера, оно
// становится источником записи (LogEntry.Source).
const OutputKey = "output"

// JournalSize — записей в журнале одного источника.
const JournalSize = message.MaxLogLines

// Journal — последние записи лога по источникам: агент и каждый воркер
// отдельно, чтобы шумный источник не вытеснял другие.
type Journal struct {
	mu    sync.Mutex
	size  int
	rings map[string]*ring
}

type ring struct {
	items []message.LogEntry
	next  int
	full  bool
}

// NewJournal — журнал на size записей каждого источника.
func NewJournal(size int) *Journal { return &Journal{size: size, rings: map[string]*ring{}} }

// Add — дописать запись в журнал её источника.
func (j *Journal) Add(e message.LogEntry) {
	j.mu.Lock()
	defer j.mu.Unlock()
	r := j.rings[e.Source]
	if r == nil {
		r = &ring{items: make([]message.LogEntry, j.size)}
		j.rings[e.Source] = r
	}
	r.items[r.next] = e
	r.next = (r.next + 1) % len(r.items)
	if r.next == 0 {
		r.full = true
	}
}

// Tail — последние n записей источника source, старые первыми.
func (j *Journal) Tail(source string, n int) []message.LogEntry {
	j.mu.Lock()
	defer j.mu.Unlock()
	r := j.rings[source]
	if r == nil {
		return []message.LogEntry{}
	}
	size := r.next
	if r.full {
		size = len(r.items)
	}
	if n <= 0 || n > size {
		n = size
	}
	out := make([]message.LogEntry, 0, n)
	for i := size - n; i < size; i++ {
		idx := i
		if r.full {
			idx = (r.next + i) % len(r.items)
		}
		out = append(out, r.items[idx])
	}
	return out
}

// Options — формат и уровень.
type Options struct {
	Level  string // debug | info | warn | error
	Format string // text | json
}

// Control — смена уровня и формата лога на ходу (перечитывание настроек):
// действует и на логгеры, полученные раньше через With.
type Control struct {
	out     io.Writer
	level   slog.LevelVar
	inner   atomic.Pointer[slog.Handler]
	journal *Journal
	forward atomic.Pointer[Forwarder]
}

// SetForwarder — отправлять записи серверу через f (порог — у f); nil — не отправлять.
func (c *Control) SetForwarder(f *Forwarder) { c.forward.Store(f) }

// New — логгер: вывод в out, записи — в journal (может быть nil).
func New(out io.Writer, journal *Journal, opts Options) (*slog.Logger, *Control) {
	c := &Control{out: out, journal: journal}
	c.Set(opts)
	return slog.New(&handler{c: c}), c
}

// Set — новые уровень и формат.
func (c *Control) Set(opts Options) {
	c.level.Set(ParseLevel(opts.Level))
	handlerOpts := &slog.HandlerOptions{Level: &c.level}
	var h slog.Handler
	if opts.Format == "json" {
		h = slog.NewJSONHandler(c.out, handlerOpts)
	} else {
		h = slog.NewTextHandler(c.out, handlerOpts)
	}
	c.inner.Store(&h)
}

// journalLevel — с какого уровня записи попадают в журнал: info или
// подробнее, если так настроен локальный лог.
func (c *Control) journalLevel() slog.Level { return min(c.level.Level(), slog.LevelInfo) }

// handler — обработчик поверх текущего Control.inner; атрибуты и группы из
// With копятся и отдельно — для журнала и записей серверу.
type handler struct {
	c     *Control
	ops   []func(slog.Handler) slog.Handler
	attrs []slog.Attr
	group string
}

func (h *handler) Enabled(_ context.Context, level slog.Level) bool {
	if level >= h.c.level.Level() || (h.c.journal != nil && level >= h.c.journalLevel()) {
		return true
	}
	f := h.c.forward.Load()
	return f != nil && f.Enabled(level)
}

func (h *handler) Handle(ctx context.Context, r slog.Record) error {
	f := h.c.forward.Load()
	toForward := f != nil && f.Enabled(r.Level)
	toJournal := h.c.journal != nil && r.Level >= h.c.journalLevel()
	if toForward || toJournal {
		e := entry(r, h.attrs, h.group)
		if toJournal {
			h.c.journal.Add(e)
		}
		if toForward {
			f.Add(e)
		}
	}
	if r.Level < h.c.level.Level() {
		return nil
	}
	inner := *h.c.inner.Load()
	for _, op := range h.ops {
		inner = op(inner)
	}
	return inner.Handle(ctx, r)
}

func (h *handler) with(op func(slog.Handler) slog.Handler) *handler {
	ops := append(slices.Clone(h.ops), op)
	return &handler{c: h.c, ops: ops, attrs: h.attrs, group: h.group}
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	next := h.with(func(inner slog.Handler) slog.Handler { return inner.WithAttrs(attrs) })
	next.attrs = slices.Clone(h.attrs)
	for _, a := range attrs {
		if h.group != "" {
			a.Key = h.group + a.Key
		}
		next.attrs = append(next.attrs, a)
	}
	return next
}

func (h *handler) WithGroup(name string) slog.Handler {
	next := h.with(func(inner slog.Handler) slog.Handler { return inner.WithGroup(name) })
	if name != "" {
		next.group = h.group + name + "."
	}
	return next
}

// ParseLevel — уровень по имени; неизвестное — info.
func ParseLevel(name string) slog.Level {
	switch strings.ToLower(name) {
	case "debug":
		return slog.LevelDebug
	case "warn", "warning":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

// Discard — логгер в никуда (тесты).
func Discard() *slog.Logger { return slog.New(slog.DiscardHandler) }

// entry — запись лога из slog: источник — атрибут OutputKey (вывод воркера) или agent.
func entry(r slog.Record, attrs []slog.Attr, group string) message.LogEntry {
	at := r.Time
	if at.IsZero() {
		at = time.Now()
	}
	e := message.LogEntry{At: at.UnixMilli(), Level: LevelName(r.Level), Source: message.LogSourceAgent, Msg: r.Message}
	add := func(prefix string, a slog.Attr) {
		flatten(prefix, a, func(key string, v any) {
			if key == OutputKey {
				if s, ok := v.(string); ok && s != "" {
					e.Source = s
					return
				}
			}
			if e.Attrs == nil {
				e.Attrs = map[string]any{}
			}
			e.Attrs[key] = v
		})
	}
	for _, a := range attrs {
		add("", a)
	}
	r.Attrs(func(a slog.Attr) bool {
		add(group, a)
		return true
	})
	return e
}

// flatten — атрибут (с группами) в пары «ключ.через.точку → значение JSON».
func flatten(prefix string, a slog.Attr, put func(string, any)) {
	v := a.Value.Resolve()
	key := prefix + a.Key
	if v.Kind() == slog.KindGroup {
		p := prefix
		if a.Key != "" {
			p = key + "."
		}
		for _, sub := range v.Group() {
			flatten(p, sub, put)
		}
		return
	}
	if a.Key == "" {
		return
	}
	switch v.Kind() {
	case slog.KindString:
		put(key, v.String())
	case slog.KindInt64:
		put(key, v.Int64())
	case slog.KindUint64:
		put(key, v.Uint64())
	case slog.KindFloat64:
		put(key, v.Float64())
	case slog.KindBool:
		put(key, v.Bool())
	case slog.KindDuration:
		put(key, v.Duration().String())
	case slog.KindTime:
		put(key, v.Time().UTC().Format(time.RFC3339Nano))
	default:
		if err, ok := v.Any().(error); ok {
			put(key, err.Error())
			return
		}
		put(key, fmt.Sprint(v.Any()))
	}
}

// LevelName — имя уровня для LogEntry.
func LevelName(l slog.Level) string {
	switch {
	case l >= slog.LevelError:
		return message.LogError
	case l >= slog.LevelWarn:
		return message.LogWarn
	case l >= slog.LevelInfo:
		return message.LogInfo
	}
	return message.LogDebug
}
