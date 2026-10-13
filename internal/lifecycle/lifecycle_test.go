package lifecycle

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/epifanovmd/agent/protocol"
)

func TestLifecycle(t *testing.T) {
	dir := t.TempDir()
	st, err := Load(dir)
	if err != nil || st.Status != protocol.StatusPending {
		t.Fatalf("без файла — pending: %+v %v", st, err)
	}
	if _, err := Save(dir, protocol.StatusRevoked, "403 AGENT_REVOKED"); err != nil {
		t.Fatal(err)
	}
	st, err = Load(dir)
	if err != nil || st.Status != protocol.StatusRevoked || st.Reason != "403 AGENT_REVOKED" || st.Since == 0 {
		t.Fatalf("после записи: %+v %v", st, err)
	}
	if err := Reset(dir); err != nil {
		t.Fatal(err)
	}
	if st, _ := Load(dir); st.Status != protocol.StatusPending {
		t.Fatalf("после сброса: %+v", st)
	}
	if err := Reset(dir); err != nil {
		t.Fatalf("сброс без файла: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, FileName), []byte(`{"status":"lost"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Load(dir); err == nil {
		t.Fatal("неизвестное состояние — ошибка")
	}
}
