//go:build unix

package worker

import (
	"fmt"
	"maps"
	"os"
	"os/user"
	"sort"
	"strconv"
	"strings"
	"syscall"

	"github.com/epifanovmd/agent/internal/config"
)

// lookupUser — запуск воркера от пользователя name: uid, gid, дополнительные
// группы; HOME, USER, LOGNAME — его (настройки env важнее).
func lookupUser(name string) (*syscall.Credential, map[string]string, error) {
	u, err := user.Lookup(name)
	if err != nil {
		return nil, nil, fmt.Errorf("пользователь %q: %w", name, err)
	}
	uid, err1 := strconv.ParseUint(u.Uid, 10, 32)
	gid, err2 := strconv.ParseUint(u.Gid, 10, 32)
	if err1 != nil || err2 != nil {
		return nil, nil, fmt.Errorf("пользователь %q: uid/gid не числа", name)
	}
	cred := &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}
	if ids, err := u.GroupIds(); err == nil {
		for _, id := range ids {
			if g, err := strconv.ParseUint(id, 10, 32); err == nil {
				cred.Groups = append(cred.Groups, uint32(g))
			}
		}
	}
	return cred, map[string]string{"HOME": u.HomeDir, "USER": u.Username, "LOGNAME": u.Username}, nil
}

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
// из окружения агента — baseEnv и inheritEnv воркера (AGENT_* — никогда);
// переменные агента для всех воркеров (extra, KEY=VALUE); то, что агент
// задаёт сам (set); пользователя user; env воркера из настроек.
func workerEnv(agentEnv []string, spec config.Worker, extra []string, set, userEnv map[string]string) []string {
	out := map[string]string{}
	for _, kv := range agentEnv {
		k, v, ok := strings.Cut(kv, "=")
		if !ok || strings.HasPrefix(k, "AGENT_") {
			continue
		}
		if envMatch(baseEnv, k) || envMatch(spec.InheritEnv, k) {
			out[k] = v
		}
	}
	for _, kv := range extra {
		if k, v, ok := strings.Cut(kv, "="); ok {
			out[k] = v
		}
	}
	maps.Copy(out, set)
	maps.Copy(out, userEnv)
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
