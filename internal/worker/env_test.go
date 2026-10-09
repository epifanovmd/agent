//go:build unix

package worker

import (
	"os/user"
	"strconv"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/internal/config"
)

// user — uid/gid пользователя и его HOME; неизвестный — ошибка запуска.
func TestLookupUser(t *testing.T) {
	me, err := user.Current()
	if err != nil {
		t.Skip(err)
	}
	cred, env, err := lookupUser(me.Username)
	if err != nil {
		t.Fatal(err)
	}
	if strconv.Itoa(int(cred.Uid)) != me.Uid || strconv.Itoa(int(cred.Gid)) != me.Gid {
		t.Fatalf("credential %+v, пользователь %+v", cred, me)
	}
	if env["HOME"] != me.HomeDir || env["USER"] != me.Username {
		t.Fatalf("env %v", env)
	}
	if _, _, err := lookupUser("no-such-user-agent-test"); err == nil {
		t.Fatal("неизвестный пользователь: ожидалась ошибка")
	}
}

// Окружение воркера: только обычные переменные агента и inheritEnv, без
// AGENT_*; затем переменные агента, пользователя и env воркера (важнее всех).
func TestWorkerEnv(t *testing.T) {
	agent := []string{
		"PATH=/bin", "HOME=/root", "LC_ALL=C", "SECRET=x", "AGENT_ENROLL_TOKEN=t", "AGENT_LOG_LEVEL=debug",
		"EXAMPLE_A=1", "EXAMPLE_B=2", "OTHER=3", "https_proxy=http://proxy.example",
	}
	spec := config.Worker{InheritEnv: []string{"EXAMPLE_*", "OTHER"}, Env: map[string]string{"PATH": "/opt/bin", "X": "y"}}
	env := workerEnv(agent, spec, []string{"AGENT_SERVER_CA_FILE=/ca.pem"},
		map[string]string{"AGENT_WORKER": "report"}, map[string]string{"HOME": "/home/report"})
	got := strings.Join(env, " ")
	want := "AGENT_SERVER_CA_FILE=/ca.pem AGENT_WORKER=report EXAMPLE_A=1 EXAMPLE_B=2 HOME=/home/report LC_ALL=C OTHER=3 PATH=/opt/bin X=y https_proxy=http://proxy.example"
	if got != want {
		t.Fatalf("окружение:\n%s\nнужно:\n%s", got, want)
	}
}
