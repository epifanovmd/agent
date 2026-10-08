package commands

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

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

// cmd.cancel: выполняющаяся команда прерывается (ctx) — итог CANCELLED;
// неизвестная — без последствий; обработчик, не слушающий ctx, — итог без него.
func TestCancel(t *testing.T) {
	defer func(d time.Duration) { lateGrace = d }(lateGrace)
	lateGrace = 50 * time.Millisecond
	r := &rec{}
	reg := New(r, logx.Discard())
	started := make(chan struct{}, 2)
	reg.Register("wait", func(ctx context.Context, _ json.RawMessage, _ io.Writer) (any, error) {
		started <- struct{}{}
		<-ctx.Done()
		return nil, ctx.Err()
	})
	reg.Register("deaf", func(context.Context, json.RawMessage, io.Writer) (any, error) {
		started <- struct{}{}
		time.Sleep(time.Second)
		return "поздно", nil
	})
	_ = reg.Handle(context.Background(), message.MustNew(message.TypeCmdCancel, message.CommandRef{CommandID: "nope"}))
	for n, name := range []string{"wait", "deaf"} {
		id := "c-" + name
		_ = reg.Handle(context.Background(), run(id, name, `{}`))
		<-started
		_ = reg.Handle(context.Background(), message.MustNew(message.TypeCmdCancel, message.CommandRef{CommandID: id}))
		var done message.CommandDone
		_ = waitDone(t, r, n+1)[n].Decode(&done)
		if done.CommandID != id || done.OK || done.Error == nil || done.Error.Code != message.ErrCancelled {
			t.Fatalf("%s: %+v", name, done)
		}
	}
}

// Не больше MaxConcurrent одновременно: лишняя ждёт места, срок идёт.
func TestMaxConcurrent(t *testing.T) {
	r := &rec{}
	reg := New(r, logx.Discard())
	reg.SetMaxConcurrent(1)
	release := make(chan struct{})
	var mu sync.Mutex
	active, peak := 0, 0
	reg.Register("busy", func(ctx context.Context, _ json.RawMessage, _ io.Writer) (any, error) {
		mu.Lock()
		active++
		peak = max(peak, active)
		mu.Unlock()
		<-release
		mu.Lock()
		active--
		mu.Unlock()
		return nil, nil
	})
	_ = reg.Handle(context.Background(), run("b1", "busy", `{}`))
	_ = reg.Handle(context.Background(), run("b2", "busy", `{}`))
	time.Sleep(50 * time.Millisecond)
	if len(r.all(message.TypeCmdAccept)) != 2 {
		t.Fatal("обе приняты")
	}
	close(release)
	waitDone(t, r, 2)
	if peak != 1 {
		t.Fatalf("одновременно: %d", peak)
	}
	// Не дождалась места за срок — TIMEOUT.
	hold := make(chan struct{})
	defer close(hold)
	holding := make(chan struct{})
	reg.Register("hold", func(context.Context, json.RawMessage, io.Writer) (any, error) {
		close(holding)
		<-hold
		return nil, nil
	})
	_ = reg.Handle(context.Background(), run("h1", "hold", `{}`))
	<-holding
	_ = reg.Handle(context.Background(), message.MustNew(message.TypeCmdRun, message.CommandRun{CommandID: "h2", Name: "hold", TimeoutSec: 1}))
	var done message.CommandDone
	_ = waitDone(t, r, 3)[2].Decode(&done)
	if done.CommandID != "h2" || done.Error == nil || done.Error.Code != "TIMEOUT" {
		t.Fatalf("ожидание места: %+v", done)
	}
}

// Вывод режется по 64 КБ без разрыва символа UTF-8.
func TestOutputChunksUTF8(t *testing.T) {
	r := &rec{}
	o := newOutput(r, "x")
	text := "a" + strings.Repeat("ж", chunkMax) // 1 + 2·64К байт
	_, _ = o.Write([]byte(text))
	o.Close()
	var joined string
	for _, env := range r.all(message.TypeCmdOutput) {
		var out message.CommandOutput
		_ = env.Decode(&out)
		if !utf8.ValidString(out.Chunk) || len(out.Chunk) > chunkMax {
			t.Fatalf("кусок %d байт, UTF-8 %v", len(out.Chunk), utf8.ValidString(out.Chunk))
		}
		joined += out.Chunk
	}
	if joined != text {
		t.Fatal("вывод не совпал")
	}
}
