package stream

import (
	"testing"

	"github.com/epifanovmd/agent/sdk/go/message"
)

func TestSeqAckAndOverflow(t *testing.T) {
	b := New(3)
	for range 5 {
		b.Add(message.Envelope{Type: message.TypeStatus})
	}
	un := b.Unacked()
	if len(un) != 3 || un[0].Seq != 3 || un[2].Seq != 5 || b.Dropped() != 2 {
		t.Fatalf("переполнение: %+v dropped=%d", un, b.Dropped())
	}
	b.Ack(4)
	if un := b.Unacked(); len(un) != 1 || un[0].Seq != 5 {
		t.Fatalf("после ack: %+v", un)
	}
	if env := b.Add(message.Envelope{}); env.Seq != 6 {
		t.Fatalf("нумерация сквозная: %d", env.Seq)
	}
}
