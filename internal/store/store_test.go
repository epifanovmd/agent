package store

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/protocol"
)

func open(t *testing.T, dir string, limit int) *Store {
	t.Helper()
	s, err := Open(dir, limit)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s
}

func ev(id string) protocol.Event {
	return protocol.Event{ID: id, Worker: "wg", Type: "wg.up", Data: json.RawMessage(`{"n":1}`), At: 1}
}

// События: по порядку, повтор id не задваивается, предел — ErrFull без частичной записи,
// подтверждённые убираются, очередь переживает переоткрытие.
func TestEvents(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 3)
	if err := s.AppendEvents([]protocol.Event{ev("e1"), ev("e2")}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvents([]protocol.Event{ev("e2")}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvents([]protocol.Event{ev("e3"), ev("e4")}); !errors.Is(err, ErrFull) {
		t.Fatalf("предел: %v", err)
	}
	items, _ := s.Pending(KindEvent, 10, 1<<20)
	if len(items) != 2 || items[0].ID != "e1" || items[1].ID != "e2" || s.Len() != 2 {
		t.Fatalf("очередь: %+v, len %d", items, s.Len())
	}
	if err := s.Remove(KindEvent, []string{"e1", "nope"}); err != nil {
		t.Fatal(err)
	}
	s.Close()
	s = open(t, dir, 3)
	items, _ = s.Pending(KindEvent, 10, 1<<20)
	if len(items) != 1 || items[0].ID != "e2" || s.Len() != 1 {
		t.Fatalf("после переоткрытия: %+v", items)
	}
	var e protocol.Event
	if json.Unmarshal(items[0].Raw, &e) != nil || e.Worker != "wg" {
		t.Fatalf("запись: %s", items[0].Raw)
	}
}

// Пачка: не больше max записей и maxBytes (первая запись — всегда).
func TestPendingLimits(t *testing.T) {
	s := open(t, t.TempDir(), 0)
	for i := range 5 {
		_ = s.AppendEvents([]protocol.Event{ev(fmt.Sprint("e", i))})
	}
	if items, _ := s.Pending(KindEvent, 2, 1<<20); len(items) != 2 {
		t.Fatalf("max: %d", len(items))
	}
	if items, _ := s.Pending(KindEvent, 10, 1); len(items) != 1 {
		t.Fatalf("maxBytes: %d", len(items))
	}
	if items, _ := s.Pending(KindResult, 10, 1<<20); len(items) != 0 {
		t.Fatalf("другой вид: %d", len(items))
	}
}

// Журнал команд: новая → выполняется, повтор — её состояние; итог — в журнале и в очереди
// один раз; выполняемые видны после переоткрытия.
func TestCommands(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 0)
	cmd := protocol.Command{ID: "c1", Type: protocol.CmdWorkerRestart}
	if st, _, err := s.StartCommand(cmd); err != nil || st != CommandNew {
		t.Fatalf("новая: %q %v", st, err)
	}
	if st, _, _ := s.StartCommand(cmd); st != CommandRunning {
		t.Fatalf("повтор: %q", st)
	}
	s.Close()
	s = open(t, dir, 0)
	if run, _ := s.Running(); len(run) != 1 || run[0].ID != "c1" {
		t.Fatalf("выполняемые: %+v", run)
	}
	res := protocol.Result{Re: "c1", OK: true}
	if err := s.FinishCommand(res); err != nil {
		t.Fatal(err)
	}
	_ = s.FinishCommand(res)
	if st, r, _ := s.StartCommand(cmd); st != CommandDone || r == nil || !r.OK {
		t.Fatalf("выполнена: %q %+v", st, r)
	}
	if items, _ := s.Pending(KindResult, 10, 1<<20); len(items) != 1 || items[0].ID != "c1" {
		t.Fatalf("итог в очереди: %+v", items)
	}
	if run, _ := s.Running(); len(run) != 0 {
		t.Fatalf("выполняемых нет: %+v", run)
	}
}

// Новая регистрация: seq команд забывается, итоги прежней записи убираются, события остаются.
func TestNewIdentity(t *testing.T) {
	s := open(t, t.TempDir(), 0)
	if err := s.SetLastSeq(150); err != nil {
		t.Fatal(err)
	}
	if _, _, err := s.StartCommand(protocol.Command{ID: "c1", Type: protocol.CmdWorkerRestart}); err != nil {
		t.Fatal(err)
	}
	if err := s.FinishCommand(protocol.Result{Re: "c1", OK: true}); err != nil {
		t.Fatal(err)
	}
	if err := s.AppendEvents([]protocol.Event{ev("e1")}); err != nil {
		t.Fatal(err)
	}
	if err := s.NewIdentity(); err != nil {
		t.Fatal(err)
	}
	if seq := s.LastSeq(); seq != 0 {
		t.Fatalf("seq: %d", seq)
	}
	if err := s.SetLastSeq(1); err != nil || s.LastSeq() != 1 {
		t.Fatalf("seq новой записи: %d %v", s.LastSeq(), err)
	}
	if items, _ := s.Pending(KindResult, 10, 1<<20); len(items) != 0 {
		t.Fatalf("итоги прежней записи: %+v", items)
	}
	if items, _ := s.Pending(KindEvent, 10, 1<<20); len(items) != 1 {
		t.Fatalf("события: %+v", items)
	}
}

// Журнал обрезается: старые выполненные уходят, выполняемые остаются.
func TestJournalPrune(t *testing.T) {
	s := open(t, t.TempDir(), 0)
	s.journal = 3
	_, _, _ = s.StartCommand(protocol.Command{ID: "keep"})
	for i := range 5 {
		id := fmt.Sprint("c", i)
		_, _, _ = s.StartCommand(protocol.Command{ID: id})
		_ = s.FinishCommand(protocol.Result{Re: id, OK: true})
	}
	if st, _, _ := s.StartCommand(protocol.Command{ID: "keep"}); st != CommandRunning {
		t.Fatalf("выполняемая убрана: %q", st)
	}
	if st, _, _ := s.StartCommand(protocol.Command{ID: "c0"}); st != CommandNew {
		t.Fatalf("старая выполненная осталась: %q", st)
	}
}

// Желаемое, seq и итоги настроек.
func TestStateRecords(t *testing.T) {
	dir := t.TempDir()
	s := open(t, dir, 0)
	if _, ok, _ := s.LoadDesired(); ok {
		t.Fatal("желаемого ещё нет")
	}
	d := protocol.Desired{Rev: 7, Workers: map[string]protocol.WorkerDesired{
		"wg": {Configs: map[string]protocol.ConfigValue{"main": {Version: 2, Data: json.RawMessage(`{"a":1}`)}}}}}
	_ = s.SaveDesired(d)
	_ = s.SetLastSeq(10)
	_ = s.SetLastSeq(4)
	_ = s.SetApplied("wg", protocol.Applied{Key: "main", Version: 2, OK: true})
	_ = s.SetApplied("wg", protocol.Applied{Key: "old", Version: 1, OK: true})
	_ = s.DropApplied("wg", "old")
	s.Close()
	s = open(t, dir, 0)
	got, ok, _ := s.LoadDesired()
	if !ok || got.Rev != 7 || string(got.Workers["wg"].Configs["main"].Data) != `{"a":1}` {
		t.Fatalf("желаемое: %+v", got)
	}
	if s.LastSeq() != 10 {
		t.Fatalf("seq: %d", s.LastSeq())
	}
	a, _ := s.Applied()
	if st, ok := a["wg"]["main"]; !ok || st.Version != 2 || st.OK == nil || !*st.OK || len(a["wg"]) != 1 {
		t.Fatalf("итоги: %+v", a)
	}
}

// Повреждённый файл откладывается, хранилище начинается заново.
func TestRecoverBroken(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte(strings.Repeat("мусор", 2000)), 0o600); err != nil {
		t.Fatal(err)
	}
	s := open(t, dir, 0)
	if s.Recovered == "" || s.Len() != 0 {
		t.Fatalf("восстановление: %q", s.Recovered)
	}
	if raw, err := os.ReadFile(s.Recovered); err != nil || !strings.HasPrefix(string(raw), "мусор") {
		t.Fatalf("отложенный файл: %v", err)
	}
	if err := s.AppendEvents([]protocol.Event{ev("e1")}); err != nil {
		t.Fatal(err)
	}
}
