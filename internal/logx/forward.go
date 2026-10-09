package logx

import (
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

// ForwardBuffer — записей ждут отправки не больше; лишние отбрасываются с
// пометкой «пропущено N».
const ForwardBuffer = 2 * message.MaxLogBatch

// levelOff — порог «не слать ничего».
const levelOff = slog.Level(1 << 20)

// ParseForward — порог отправки по имени: off | error | warn | info | debug.
func ParseForward(name string) (slog.Level, error) {
	switch strings.ToLower(name) {
	case "off":
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
// (log.forward или подробнее — из watch), буфер до ForwardBuffer записей и
// пачки до message.MaxLogBatch. Безопасен для вызова из любых горутин; сам
// ничего не логирует.
type Forwarder struct {
	mu       sync.Mutex
	base     slog.Level
	override *slog.Level
	buf      []message.LogEntry
	dropped  int
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

// Override — порог из watch: действует, если подробнее log.forward; "" —
// только настройка агента.
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

func (f *Forwarder) threshold() slog.Level {
	if f.override != nil && *f.override < f.base {
		return *f.override
	}
	return f.base
}

// Enabled — запись уровня level уйдёт серверу.
func (f *Forwarder) Enabled(level slog.Level) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	return level >= f.threshold()
}

// Add — запись в буфер (буфер полон — счётчик пропущенных).
func (f *Forwarder) Add(e message.LogEntry) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.buf) >= ForwardBuffer {
		f.dropped++
		return
	}
	f.buf = append(f.buf, e)
}

// Take — следующая пачка (не больше message.MaxLogBatch, с пометкой о
// пропущенных в конце); nil — отправлять нечего.
func (f *Forwarder) Take() []message.LogEntry {
	f.mu.Lock()
	defer f.mu.Unlock()
	limit := message.MaxLogBatch
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
