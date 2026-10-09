//go:build unix

package integration

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/test/testserver"
)

// Регистрация воркера (§12): без GET /manifest — state invalid с причиной, fetch —
// WORKER_INVALID; манифест появился — повтор проверки регистрирует воркер. Агент сверяется с
// манифестом: необъявленное событие — 400 EVENT_UNDECLARED, необъявленный ключ настроек —
// CONFIG_KEY_UNKNOWN без передачи воркеру.
func TestWorkerRegistration(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	noManifest := filepath.Join(s.stateDir("w"), "no-manifest")
	w := s.worker("w")
	writeFile(t, noManifest, "")
	n := s.start(s.config(w))

	var a testserver.Agent
	eventually(t, "воркер без манифеста — invalid", func() bool {
		var ok bool
		if a, ok = s.server.Agent(agentName); !ok || !a.Online {
			return false
		}
		st, _ := a.Worker("w")
		return st.State == message.WorkerInvalid && strings.Contains(st.Message, "GET /manifest: HTTP 404")
	})
	if _, err := s.server.Fetch(a.ID, "w", "/echo", testserver.FetchInit{Method: http.MethodPost}); errorCode(err) != message.CodeWorkerInvalid {
		t.Fatalf("fetch к незарегистрированному воркеру: %v", err)
	}

	if err := os.Remove(noManifest); err != nil {
		t.Fatal(err)
	}
	a = s.running("w")

	if _, codes := n.direct("w", http.MethodPost, "/emit?type=example.undeclared"); codes != "400" {
		t.Fatalf("необъявленное событие: %s", codes)
	}

	s.server.SetConfig(a.ID, "w", "undeclared", 1)
	eventually(t, "CONFIG_KEY_UNKNOWN", func() bool {
		st, ok := s.server.ConfigStatus(a.ID, "w", "undeclared")
		return ok && st.State == testserver.ConfigFailed && st.Error != nil && st.Error.Code == message.CodeConfigKeyUnknown
	})
	for _, line := range s.lines("w", "configs.log") {
		if strings.Contains(line, "undeclared") {
			t.Fatalf("необъявленный ключ передан воркеру: %s", line)
		}
	}
}
