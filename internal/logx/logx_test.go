package logx

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

func TestSwitchable(t *testing.T) {
	var out bytes.Buffer
	log, ctl := NewSwitchable(&out, NewRing(10), Options{Level: "info", Format: "text"})
	sub := log.With("worker", "echo")
	sub.Debug("скрыто")
	sub.Info("текст")
	ctl.Set(Options{Level: "debug", Format: "json"})
	sub.Debug("видно")
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 || !strings.Contains(lines[0], "msg=текст") || !strings.Contains(lines[0], "worker=echo") ||
		!strings.HasPrefix(lines[1], "{") || !strings.Contains(lines[1], `"worker":"echo"`) {
		t.Fatalf("лог: %q", lines)
	}
}

func TestRingTail(t *testing.T) {
	r := NewRing(3)
	if got := r.Tail(10); len(got) != 0 {
		t.Fatalf("пустой буфер: %v", got)
	}
	r.Add("a")
	r.Add("b")
	if got := r.Tail(10); !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("неполный: %v", got)
	}
	r.Add("c")
	r.Add("d")
	if got := r.Tail(10); !reflect.DeepEqual(got, []string{"b", "c", "d"}) {
		t.Fatalf("переполненный: %v", got)
	}
	if got := r.Tail(2); !reflect.DeepEqual(got, []string{"c", "d"}) {
		t.Fatalf("хвост: %v", got)
	}
	_, _ = r.Write([]byte("e\nf\n"))
	if got := r.Tail(2); !reflect.DeepEqual(got, []string{"e", "f"}) {
		t.Fatalf("writer: %v", got)
	}
}

// Записи для сервера: порог свой (ниже локального тоже), источник — имя
// воркера, атрибуты плоские; сервер меняет порог на ходу.
func TestForward(t *testing.T) {
	var out bytes.Buffer
	log, ctl := NewSwitchable(&out, NewRing(10), Options{Level: "error"})
	f := NewForwarder("warn")
	ctl.SetForwarder(f)
	wlog := log.With("worker", "report", "instance", 2)
	log.Info("не уйдёт")
	log.Warn("связь потеряна", "err", errors.New("reset"), "retryIn", 2*time.Second)
	wlog.WithGroup("g").Error("порт занят", "port", 8080)
	if out.Len() == 0 || strings.Contains(out.String(), "связь потеряна") {
		t.Fatalf("локальный лог — по своему уровню: %q", out.String())
	}
	got := f.Take()
	if len(got) != 2 {
		t.Fatalf("записи: %+v", got)
	}
	if e := got[0]; e.Level != "warn" || e.Source != "agent" || e.Msg != "связь потеряна" ||
		!reflect.DeepEqual(e.Attrs, map[string]any{"err": "reset", "retryIn": "2s"}) || e.At == 0 {
		t.Fatalf("запись агента: %+v", e)
	}
	if e := got[1]; e.Level != "error" || e.Source != "report" ||
		!reflect.DeepEqual(e.Attrs, map[string]any{"instance": int64(2), "g.port": int64(8080)}) {
		t.Fatalf("запись воркера: %+v", e)
	}

	// Подписка просит debug; "" — снова настройка агента; неверное — ошибка.
	if err := f.Override("debug"); err != nil {
		t.Fatal(err)
	}
	log.Debug("подробно")
	if err := f.Override(""); err != nil {
		t.Fatal(err)
	}
	log.Info("снова мимо")
	if err := f.Override("громко"); err == nil {
		t.Fatal("неверный уровень принят")
	}
	if got := f.Take(); len(got) != 1 || got[0].Msg != "подробно" || got[0].Level != "debug" {
		t.Fatalf("после override: %+v", got)
	}
	f.SetLevel("off")
	log.Error("off")
	if got := f.Take(); got != nil {
		t.Fatalf("off: %+v", got)
	}
}

// Буфер: без связи не копится; сверх ForwardBuffer — пометка «пропущено N»;
// пачки не больше ForwardBatch.
func TestForwardBuffer(t *testing.T) {
	f := NewForwarder("info")
	online := false
	f.SetOnline(func() bool { return online })
	f.Add(message.LogEntry{Msg: "без связи"})
	if f.Take() != nil {
		t.Fatal("без связи записи не копятся")
	}
	online = true
	for i := range ForwardBuffer + 7 {
		f.Add(message.LogEntry{Msg: fmt.Sprint(i)})
	}
	first := f.Take()
	if len(first) != ForwardBatch || first[0].Msg != "0" {
		t.Fatalf("первая пачка: %d", len(first))
	}
	last := first[len(first)-1]
	if !strings.Contains(last.Msg, "пропущено 7") || last.Attrs["skipped"] != 7 || last.Source != "agent" {
		t.Fatalf("пометка: %+v", last)
	}
	second := f.Take()
	if len(second) != ForwardBatch || second[0].Msg != fmt.Sprint(ForwardBatch-1) {
		t.Fatalf("вторая пачка: %d %s", len(second), second[0].Msg)
	}
	if third := f.Take(); len(third) != 1 {
		t.Fatalf("остаток: %d", len(third))
	}

	// Run шлёт пачки по таймеру.
	sent := make(chan message.LogBatch, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go f.Run(ctx, 10*time.Millisecond, func(b message.LogBatch) { sent <- b })
	f.Add(message.LogEntry{Msg: "x"})
	select {
	case b := <-sent:
		if len(b.Entries) != 1 || b.Entries[0].Msg != "x" {
			t.Fatalf("пачка: %+v", b)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("пачка не ушла")
	}
}

// Level — действующий порог: более подробный из настройки агента и подписки.
func TestForwardLevel(t *testing.T) {
	f := NewForwarder("")
	if f.Level() != "warn" {
		t.Fatalf("по умолчанию: %s", f.Level())
	}
	_ = f.Override("debug")
	if f.Level() != "debug" {
		t.Fatalf("от сервера: %s", f.Level())
	}
	// Подписка не ослабляет настройку агента: error грубее warn.
	_ = f.Override("error")
	if f.Level() != "warn" {
		t.Fatalf("грубее настройки: %s", f.Level())
	}
	_ = f.Override("")
	f.SetLevel("error")
	if f.Level() != "error" {
		t.Fatalf("настройка: %s", f.Level())
	}
}
