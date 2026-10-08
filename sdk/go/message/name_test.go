package message

import (
	"strings"
	"testing"
)

func TestValidName(t *testing.T) {
	good := []string{"a", "0", "example.convert", "Example_Proxy-1.reload", strings.Repeat("a", 64)}
	bad := []string{"", ".a", "_a", "-a", "a b", "a/b", "пример", "a:b", strings.Repeat("a", 65), "a\n"}
	for _, n := range good {
		if !ValidName(n) {
			t.Errorf("ValidName(%q) = false, ждали true", n)
		}
	}
	for _, n := range bad {
		if ValidName(n) {
			t.Errorf("ValidName(%q) = true, ждали false", n)
		}
	}
}
