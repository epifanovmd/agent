//go:build unix

package worker

import (
	"strings"
	"testing"

	"github.com/epifanovmd/agent/internal/config"
)

// Окружение воркера: только обычные переменные агента, без AGENT_*; затем
// переменные агента для воркеров и env воркера (важнее всех).
func TestWorkerEnv(t *testing.T) {
	agent := []string{
		"PATH=/bin", "HOME=/root", "LC_ALL=C", "SECRET=x", "AGENT_ENROLL_TOKEN=t", "AGENT_LOG_LEVEL=debug",
		"EXAMPLE_A=1", "https_proxy=http://proxy.example",
	}
	spec := config.Worker{Env: map[string]string{"PATH": "/opt/bin", "X": "y"}}
	env := workerEnv(agent, spec, []string{"AGENT_SERVER_CA_FILE=/ca.pem"}, map[string]string{"AGENT_WORKER": "report"})
	got := strings.Join(env, " ")
	want := "AGENT_SERVER_CA_FILE=/ca.pem AGENT_WORKER=report HOME=/root LC_ALL=C PATH=/opt/bin X=y https_proxy=http://proxy.example"
	if got != want {
		t.Fatalf("окружение:\n%s\nнужно:\n%s", got, want)
	}
}
