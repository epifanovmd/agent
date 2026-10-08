package logx

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Пределы отправки лога серверу (сообщение log).
const (
	// ForwardBuffer — записей ждут отправки не больше; лишние отбрасываются
	// с пометкой «пропущено N».
	ForwardBuffer = 1000
	// ForwardBatch — записей в одной пачке не больше.
	ForwardBatch = 500
	// ForwardEvery — пачки уходят не чаще.
	ForwardEvery = time.Second
)

// levelOff — порог «не слать ничего».
const levelOff = slog.Level(1 << 20)

// ParseForward — порог отправки по имени: off | error | warn | info | debug.
func ParseForward(name string) (slog.Level, error) {
	switch strings.ToLower(name) {
	case message.LogOff:
		return levelOff, nil
	case message.LogError:
		return slog.LevelError, nil
	case message.LogWarn:
		return slog.LevelWarn, nil
	case message.LogInfo:
		return slog.LevelInfo, nil
	case message.LogDebug:
		return slog.LevelDebug, nil
	}
	return 0, fmt.Errorf("уровень %q: off | error | warn | info | debug", name)
}

// Forwarder — записи лога агента и вывода воркеров для сервера: порог
// (настройка log.forward или подробнее — из подписки сервера), буфер
// до ForwardBuffer записей и пачки до ForwardBatch. Без связи записи не
// копятся. Безопасен для вызова из любых горутин; сам ничего не логирует.
type Forwarder struct {
	mu       sync.Mutex
	base     slog.Level // log.forward
	override *slog.Level
	buf      []message.LogEntry
	dropped  int
	online   func() bool
}

// NewForwarder — порог level (log.forward; неверный — warn).
func NewForwarder(level string) *Forwarder {
	f := &Forwarder{base: slog.LevelWarn}
	f.SetLevel(level)
	return f
}

// SetLevel — порог из настроек агента (log.forward).
func (f *Forwarder) SetLevel(level string) {
	l, err := ParseForward(level)
	if err != nil {
		l = slog.LevelWarn
	}
	f.mu.Lock()
	f.base = l
	f.mu.Unlock()
}

// Override — порог из сводной подписки сервера: действует, если подробнее
// log.forward; "" — только настройка агента.
func (f *Forwarder) Override(level string) error {
	var next *slog.Level
	if level != "" {
		l, err := ParseForward(level)
		if err != nil {
			return err
		}
		next = &l
	}
	f.mu.Lock()
	f.override = next
	f.mu.Unlock()
	return nil
}

// SetOnline — есть ли связь с сервером (без неё записи не копятся).
func (f *Forwarder) SetOnline(online func() bool) {
	f.mu.Lock()
	f.online = online
	f.mu.Unlock()
}

func (f *Forwarder) threshold() slog.Level {
	if f.override != nil && *f.override < f.base {
		return *f.override
	}
	return f.base
}

// Level — действующий порог отправки: off | error | warn | info | debug.
func (f *Forwarder) Level() string {
	f.mu.Lock()
	l := f.threshold()
	f.mu.Unlock()
	if l >= levelOff {
		return message.LogOff
	}
	return levelName(l)
}

// Enabled — запись уровня level уйдёт серверу.
func (f *Forwarder) Enabled(level slog.Level) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return level >= f.threshold()
}

// Add — запись в буфер (без связи — мимо, буфер полон — счётчик пропущенных).
func (f *Forwarder) Add(e message.LogEntry) {
	f.mu.Lock()
	online := f.online
	f.mu.Unlock()
	// Проверка связи — вне f.mu: связь сама пишет в лог под своими замками.
	if online != nil && !online() {
		return
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.buf) >= ForwardBuffer {
		f.dropped++
		return
	}
	f.buf = append(f.buf, e)
}

// Take — следующая пачка (не больше ForwardBatch, с пометкой о пропущенных
// в конце); nil — отправлять нечего.
func (f *Forwarder) Take() []message.LogEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	limit := ForwardBatch
	if f.dropped > 0 {
		limit--
	}
	n := min(len(f.buf), limit)
	batch := append([]message.LogEntry(nil), f.buf[:n]...)
	f.buf = append(f.buf[:0], f.buf[n:]...)
	if f.dropped > 0 {
		batch = append(batch, message.LogEntry{
			At: time.Now().UnixMilli(), Level: message.LogWarn, Source: message.LogSourceAgent,
			Msg:   fmt.Sprintf("пропущено %d записей лога: буфер отправки переполнен", f.dropped),
			Attrs: map[string]any{"skipped": f.dropped},
		})
		f.dropped = 0
	}
	if len(batch) == 0 {
		return nil
	}
	return batch
}

// Run — раз в every отправлять пачку функцией send до отмены ctx; без связи
// накопленное отбрасывается.
func (f *Forwarder) Run(ctx context.Context, every time.Duration, send func(message.LogBatch)) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		batch := f.Take()
		if batch == nil {
			continue
		}
		f.mu.Lock()
		online := f.online
		f.mu.Unlock()
		if online == nil || online() {
			send(message.LogBatch{Entries: batch})
		}
	}
}

// entry — запись лога из slog: источник — атрибут worker (имя воркера) или agent.
func entry(r slog.Record, attrs []slog.Attr, group string) message.LogEntry {
	at := r.Time
	if at.IsZero() {
		at = time.Now()
	}
	e := message.LogEntry{At: at.UnixMilli(), Level: levelName(r.Level), Source: message.LogSourceAgent, Msg: r.Message}
	add := func(prefix string, a slog.Attr) {
		flatten(prefix, a, func(key string, v any) {
			if key == "worker" {
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

func levelName(l slog.Level) string {
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
