package cgroup

import (
	"os"
	"testing"
)

// Без метки systemd группа своя, только если каталог принадлежит не-root агенту.
func TestDelegated(t *testing.T) {
	if got, want := delegated(t.TempDir()), os.Geteuid() != 0; got != want {
		t.Fatalf("delegated = %v, want %v", got, want)
	}
	if delegated("/nonexistent-cgroup") {
		t.Fatal("нет каталога — не делегирована")
	}
}
