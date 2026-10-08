package outbox

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/epifanovmd/agent/sdk/go/message"
)

func TestAppendPendingRemoveSurvivesReopen(t *testing.T) {
	dir := t.TempDir()
	o, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, id := range []string{"a1", "b2", "c3"} {
		env := message.MustNew(message.TypeJobComplete, message.JobComplete{JobRef: message.JobRef{JobID: id}})
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
	pending, err := o.Pending()
	if err != nil || len(pending) != 0 {
		t.Fatalf("мусор не отброшен: %v %v", pending, err)
	}
	if _, err := Open(dir); err != nil {
		t.Fatal(err)
	}
	if matches, _ := filepath.Glob(filepath.Join(dir, "*.tmp")); len(matches) != 0 {
		t.Fatalf("временные файлы не удалены: %v", matches)
	}
}

func TestJobRefsOfUndeliveredResults(t *testing.T) {
	o, _ := Open(t.TempDir())
	for i, typ := range []string{message.TypeJobComplete, message.TypeJobEvent, message.TypeJobFail} {
		env := message.MustNew(typ, message.JobRef{JobID: string(rune('a' + i)), Attempt: i})
		env.ID = typ
		_ = o.Append(env)
	}
	refs := o.JobRefs()
	if len(refs) != 2 || refs[0].JobID != "a" || refs[1].JobID != "c" || refs[1].Attempt != 2 {
		t.Fatalf("итоги в outbox: %+v", refs)
	}
}
