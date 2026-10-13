//go:build unix

package worker

import (
	"maps"
	"os"
	"sort"
	"strings"

	"github.com/epifanovmd/agent/internal/config"
)

// baseEnv — переменные окружения агента, которые воркер получает всегда
// ("X_*" — все с этим началом).
var baseEnv = []string{
	"PATH", "HOME", "USER", "LOGNAME", "SHELL", "LANG", "LANGUAGE", "LC_*", "TZ", "TMPDIR", "TERM",
	"HTTP_PROXY", "HTTPS_PROXY", "NO_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "no_proxy", "all_proxy",
	"SSL_CERT_FILE", "SSL_CERT_DIR",
}

func envMatch(patterns []string, name string) bool {
	for _, p := range patterns {
		if prefix, ok := strings.CutSuffix(p, "*"); ok && strings.HasPrefix(name, prefix) || p == name {
			return true
		}
	}
	return false
}

// workerEnv — окружение воркера по порядку важности (позднее важнее):
// из окружения агента — baseEnv (AGENT_* — никогда); переменные агента для
// всех воркеров (extra, KEY=VALUE); то, что агент задаёт сам (set); env
// воркера из настроек.
func workerEnv(agentEnv []string, spec config.Worker, extra []string, set map[string]string) []string {
	out := map[string]string{}
	for _, kv := range agentEnv {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || strings.HasPrefix(k, "AGENT_") {
			continue
		}
		if envMatch(baseEnv, k) {
			out[k] = v
		}
	}
	for _, kv := range extra {
		if k, v, ok := strings.Cut(kv, "="); ok {
			out[k] = v
		}
	}
	maps.Copy(out, set)
	for k, v := range spec.Env {
		out[k] = os.ExpandEnv(v)
	}
	env := make([]string, 0, len(out))
	for k, v := range out {
		env = append(env, k+"="+v)
	}
	sort.Strings(env)
	return env
}
