package logx

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

func TestSwitchable(t *testing.T) {
	var out bytes.Buffer
	log, ctl := New(&out, nil, Options{Level: "info", Format: "text"})
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

// Журнал — по источникам: вывод воркера (OutputKey) — в журнал воркера,
// остальное — в журнал агента; переполнение вытесняет старые записи своего
// источника; info попадает в журнал и при уровне warn.
func TestJournal(t *testing.T) {
	j := NewJournal(3)
	log, _ := New(&bytes.Buffer{}, j, Options{Level: "warn"})
	out := log.With(OutputKey, "echo")
	for i := range 5 {
		out.Info(fmt.Sprint("строка ", i))
	}
	log.Info("агент", "worker", "echo")
	log.Debug("мимо")
	got := j.Tail("echo", 10)
	if len(got) != 3 || got[0].Msg != "строка 2" || got[2].Msg != "строка 4" || got[0].Source != "echo" || got[0].Attrs != nil {
		t.Fatalf("журнал воркера: %+v", got)
	}
	if got := j.Tail("echo", 1); len(got) != 1 || got[0].Msg != "строка 4" {
		t.Fatalf("хвост: %+v", got)
	}
	agent := j.Tail(message.LogSourceAgent, 0)
	if len(agent) != 1 || agent[0].Msg != "агент" || agent[0].Attrs["worker"] != "echo" {
		t.Fatalf("журнал агента: %+v", agent)
	}
	if got := j.Tail("нет", 5); len(got) != 0 || got == nil {
		t.Fatalf("нет источника — пустой список: %#v", got)
	}
}

// Записи для сервера: порог свой (ниже локального тоже), источник — вывод
// воркера, атрибуты плоские; watch меняет порог на ходу.
func TestForward(t *testing.T) {
	var out bytes.Buffer
	log, ctl := New(&out, nil, Options{Level: "error"})
	f := NewForwarder("warn")
	ctl.SetForwarder(f)
	wlog := log.With(OutputKey, "report", "instance", 2)
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

	// watch просит debug; "" — снова настройка агента; неверное — ошибка.
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
	// watch не ослабляет настройку агента.
	_ = f.Override("error")
	log.Warn("уйдёт")
	if got := f.Take(); len(got) != 1 {
		t.Fatalf("грубее настройки: %+v", got)
	}
	f.SetLevel("off")
	_ = f.Override("")
	log.Error("off")
	if got := f.Take(); got != nil {
		t.Fatalf("off: %+v", got)
	}
}

// Буфер: сверх ForwardBuffer — пометка «пропущено N»; пачки не больше 500.
func TestForwardBuffer(t *testing.T) {
	f := NewForwarder("info")
	for i := range ForwardBuffer + 7 {
		f.Add(message.LogEntry{Msg: fmt.Sprint(i)})
	}
	first := f.Take()
	if len(first) != message.MaxLogBatch || first[0].Msg != "0" {
		t.Fatalf("первая пачка: %d", len(first))
	}
	last := first[len(first)-1]
	if !strings.Contains(last.Msg, "пропущено 7") || last.Attrs["skipped"] != 7 || last.Source != "agent" {
		t.Fatalf("пометка: %+v", last)
	}
	second := f.Take()
	if len(second) != message.MaxLogBatch || second[0].Msg != fmt.Sprint(message.MaxLogBatch-1) {
		t.Fatalf("вторая пачка: %d %s", len(second), second[0].Msg)
	}
	if third := f.Take(); len(third) != 1 {
		t.Fatalf("остаток: %d", len(third))
	}
	if f.Take() != nil {
		t.Fatal("пусто — nil")
	}
}
