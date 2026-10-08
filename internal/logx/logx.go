// Package logx — структурированный лог агента (slog) и кольцевой буфер
// последних строк для команды agent.logs.
package logx

import (
	"context"
	"io"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
)

// Ring — последние строки лога (агента и нагрузок).
type Ring struct {
	mu    sync.Mutex
	lines []string
	next  int
	full  bool
}

// NewRing — буфер на size строк.
func NewRing(size int) *Ring { return &Ring{lines: make([]string, size)} }

// Add — дописать строку.
func (r *Ring) Add(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines[r.next] = line
	r.next = (r.next + 1) % len(r.lines)
	if r.next == 0 {
		r.full = true
	}
}

// Tail — последние n строк (не больше размера буфера), старые первыми.
func (r *Ring) Tail(n int) []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	size := r.next
	if r.full {
		size = len(r.lines)
	}
	if n <= 0 || n > size {
		n = size
	}
	out := make([]string, 0, n)
	for i := size - n; i < size; i++ {
		idx := i
		if r.full {
			idx = (r.next + i) % len(r.lines)
		}
		out = append(out, r.lines[idx])
	}
	return out
}

// Write — io.Writer поверх буфера: каждая строка — запись.
func (r *Ring) Write(p []byte) (int, error) {
	for _, line := range strings.Split(strings.TrimRight(string(p), "\n"), "\n") {
		if line != "" {
			r.Add(line)
		}
	}
	return len(p), nil
}

// Options — формат и уровень.
type Options struct {
	Level  string // debug | info | warn | error
	Format string // text | json
}

// New — логгер, пишущий в out и в кольцевой буфер.
func New(out io.Writer, ring *Ring, opts Options) *slog.Logger {
	log, _ := NewSwitchable(out, ring, opts)
	return log
}

// Control — смена уровня и формата лога на ходу (перечитывание настроек):
// действует и на логгеры, полученные раньше через With.
type Control struct {
	out   io.Writer
	level slog.LevelVar
	inner atomic.Pointer[slog.Handler]
	// forward — записи для сервера (сообщение log), nil — не отправляются.
	forward atomic.Pointer[Forwarder]
}

// SetForwarder — отправлять записи серверу через f (порог — у f, независимо
// от уровня локального лога); nil — не отправлять.
func (c *Control) SetForwarder(f *Forwarder) { c.forward.Store(f) }

// NewSwitchable — логгер, уровень и формат которого меняет Control.Set.
func NewSwitchable(out io.Writer, ring *Ring, opts Options) (*slog.Logger, *Control) {
	c := &Control{out: io.MultiWriter(out, ring)}
	c.Set(opts)
	return slog.New(&switchHandler{c: c}), c
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

// switchHandler — обработчик поверх текущего Control.inner; атрибуты и
// группы из With применяются к нему при каждой записи. Атрибуты копятся и
// отдельно (attrs, group) — для записей, уходящих серверу.
type switchHandler struct {
	c     *Control
	ops   []func(slog.Handler) slog.Handler
	attrs []slog.Attr
	group string
}

func (h *switchHandler) Enabled(_ context.Context, level slog.Level) bool {
	if level >= h.c.level.Level() {
		return true
	}
	f := h.c.forward.Load()
	return f != nil && f.Enabled(level)
}

func (h *switchHandler) Handle(ctx context.Context, r slog.Record) error {
	if f := h.c.forward.Load(); f != nil && f.Enabled(r.Level) {
		f.Add(entry(r, h.attrs, h.group))
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

func (h *switchHandler) with(op func(slog.Handler) slog.Handler) *switchHandler {
	ops := append(append([]func(slog.Handler) slog.Handler{}, h.ops...), op)
	return &switchHandler{c: h.c, ops: ops, attrs: h.attrs, group: h.group}
}

func (h *switchHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
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

func (h *switchHandler) WithGroup(name string) slog.Handler {
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
func Discard() *slog.Logger { return slog.New(discardHandler{}) }

type discardHandler struct{}

func (discardHandler) Enabled(context.Context, slog.Level) bool  { return false }
func (discardHandler) Handle(context.Context, slog.Record) error { return nil }
func (h discardHandler) WithAttrs([]slog.Attr) slog.Handler      { return h }
func (h discardHandler) WithGroup(string) slog.Handler           { return h }
