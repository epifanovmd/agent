package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/sdk/go/message"
)

type rec struct {
	mu   sync.Mutex
	msgs []message.Envelope
}

func (r *rec) Stream(typ string, data any) { r.add(typ, data) }
func (r *rec) Reliable(typ string, data any) error {
	r.add(typ, data)
	return nil
}
func (r *rec) add(typ string, data any) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = append(r.msgs, message.MustNew(typ, data))
}
func (r *rec) all(typ string) []message.Envelope {
	r.mu.Lock()
	defer r.mu.Unlock()
	var out []message.Envelope
	for _, m := range r.msgs {
		if m.Type == typ {
			out = append(out, m)
		}
	}
	return out
}

func run(id, name string, args string) message.Envelope {
	return message.MustNew(message.TypeCmdRun, message.CommandRun{CommandID: id, Name: name, Args: json.RawMessage(args), TimeoutSec: 5})
}

func waitDone(t *testing.T, r *rec, n int) []message.Envelope {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for len(r.all(message.TypeCmdDone)) < n {
		if time.Now().After(deadline) {
			t.Fatal("нет cmd.done")
		}
		time.Sleep(5 * time.Millisecond)
	}
	return r.all(message.TypeCmdDone)
}

func TestRunOutputDoneAndDedup(t *testing.T) {
	r := &rec{}
	reg := New(r, logx.Discard())
	reg.Register("echo", func(_ context.Context, args json.RawMessage, out io.Writer) (any, error) {
		fmt.Fprintln(out, "строка")
		return map[string]string{"args": string(args)}, nil
	})
	reg.Register("boom", func(context.Context, json.RawMessage, io.Writer) (any, error) {
		return nil, Errorf("BAD", "плохо")
	})
	var caps message.Capabilities
	reg.Declare(&caps)
	if fmt.Sprint(caps.Commands.Names) != "[boom echo]" {
		t.Fatalf("белый список: %v", caps.Commands.Names)
	}

	_ = reg.Handle(context.Background(), run("c1", "echo", `{"x":1}`))
	_ = reg.Handle(context.Background(), run("c1", "echo", `{"x":1}`)) // повтор доставки
	_ = reg.Handle(context.Background(), run("c2", "boom", `{}`))
	_ = reg.Handle(context.Background(), run("c3", "nope", `{}`))

	dones := waitDone(t, r, 3)
	time.Sleep(50 * time.Millisecond)
	if len(r.all(message.TypeCmdDone)) != 3 || len(r.all(message.TypeCmdAccept)) != 3 {
		t.Fatal("повторная доставка выполнилась")
	}
	byID := map[string]message.CommandDone{}
	for _, env := range dones {
		var d message.CommandDone
		_ = env.Decode(&d)
		byID[d.CommandID] = d
	}
	if !byID["c1"].OK || byID["c2"].Error.Code != "BAD" || byID["c3"].Error.Code != "COMMAND_UNKNOWN" {
		t.Fatalf("итоги: %+v", byID)
	}
	var out message.CommandOutput
	_ = r.all(message.TypeCmdOutput)[0].Decode(&out)
	if out.Chunk != "строка\n" {
		t.Fatalf("вывод: %q", out.Chunk)
	}
}
