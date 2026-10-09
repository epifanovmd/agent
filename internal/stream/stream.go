// Package stream — поток сообщений агента (status, metrics, log): сквозная
// нумерация seq в пределах запуска и последние неподтверждённые сообщения в
// памяти для досылки после переподключения (§3).
package stream

import (
	"sync"

	"github.com/epifanovmd/agent/internal/message"
)

// Buffer — неподтверждённые потоковые сообщения (не больше Limit, старые вытесняются).
type Buffer struct {
	mu      sync.Mutex
	seq     int64
	items   []message.Envelope
	limit   int
	dropped int64
}

// New — буфер на limit сообщений.
func New(limit int) *Buffer { return &Buffer{limit: limit} }

// Add — присвоить следующий seq и запомнить до подтверждения.
func (b *Buffer) Add(env message.Envelope) message.Envelope {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	env.Seq = b.seq
	b.items = append(b.items, env)
	if over := len(b.items) - b.limit; over > 0 {
		b.items = append([]message.Envelope(nil), b.items[over:]...)
		b.dropped += int64(over)
	}
	return env
}

// Ack — сервер принял всё до seq включительно.
func (b *Buffer) Ack(seq int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	i := 0
	for i < len(b.items) && b.items[i].Seq <= seq {
		i++
	}
	if i > 0 {
		b.items = append([]message.Envelope(nil), b.items[i:]...)
	}
}

// Remove — убрать сообщение seq из буфера (его нельзя отправить).
func (b *Buffer) Remove(seq int64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i, env := range b.items {
		if env.Seq == seq {
			b.items = append(b.items[:i:i], b.items[i+1:]...)
			return
		}
	}
}

// Unacked — копия неподтверждённого для досылки по порядку.
func (b *Buffer) Unacked() []message.Envelope {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]message.Envelope(nil), b.items...)
}

// Dropped — сколько сообщений вытеснено переполнением.
func (b *Buffer) Dropped() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.dropped
}
