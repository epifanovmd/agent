package outbox

import (
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/epifanovmd/agent/internal/message"
)

func TestAppendPendingRemoveSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	o, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a1", "b2", "c3"} {
		env := message.MustNew(message.TypeEvent, message.Event{Worker: "echo", Type: "example.done"})
		env.ID = id
		if err := o.Append(env); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Open(dir); err != nil {
		t.Fatal(err)
	}
	reopened, _ := Open(dir)
	pending, err := reopened.Pending()
	if err != nil || len(pending) != 3 || pending[0].ID != "a1" || pending[2].ID != "c3" {
		t.Fatalf("после переоткрытия порядок нарушен: %v %v", pending, err)
	}
	reopened.Remove("b2")
	if got := reopened.Len(); got != 2 {
		t.Fatalf("после удаления: %d", got)
	}
}

func TestAppendRequiresIDAndSkipsGarbage(t *testing.T) {
	dir := t.TempDir()
	o, _ := Open(dir)
	if err := o.Append(message.Envelope{Type: "x"}); err == nil {
		t.Fatal("без id — ошибка")
	}
	_ = os.WriteFile(filepath.Join(dir, "00000000000000000001-000001-bad.json"), []byte("{oops"), 0o600)
	_ = os.WriteFile(filepath.Join(dir, "half.json.tmp"), []byte("{"), 0o600)
	o, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	pending, err := o.Pending()
	if err != nil || len(pending) != 0 || o.Len() != 0 {
		t.Fatalf("мусор не отброшен: %v %v", pending, err)
	}
	if matches, _ := filepath.Glob(filepath.Join(dir, "*.json")); len(matches) != 0 {
		t.Fatalf("испорченная запись не удалена: %v", matches)
	}
	if matches, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); len(matches) != 0 {
		t.Fatalf("временные файлы не удалены: %v", matches)
	}
}

// Полная очередь не принимает события воркеров (ErrFull), итоги настроек и
// действий записываются всегда.
func TestLimitRejectsEvents(t *testing.T) {
	o, _ := Open(t.TempDir())
	o.SetLimit(2)
	add := func(id, typ string) error {
		env := message.MustNew(typ, struct{}{})
		env.ID = id
		return o.Append(env)
	}
	for i, id := range []string{"e1", "e2"} {
		if err := add(id, message.TypeEvent); err != nil {
			t.Fatal(i, err)
		}
	}
	if err := add("e3", message.TypeEvent); !errors.Is(err, ErrFull) {
		t.Fatalf("событие в полную очередь: %v", err)
	}
	if err := add("r1", message.TypeActionResult); err != nil {
		t.Fatalf("итог действия: %v", err)
	}
	if err := add("c1", message.TypeConfigApplied); err != nil || o.Len() != 4 {
		t.Fatalf("итог настроек: %v, в очереди %d", err, o.Len())
	}
	o.Remove("e1", "e2", "r1", "c1")
	if err := add("e4", message.TypeEvent); err != nil {
		t.Fatalf("после ack событие принимается: %v", err)
	}
}
