//go:build unix

package integration

import (
	"encoding/json"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/internal/identity"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/test/testserver"
)

// Секреты не попадают в журналы: значение настроек, тела fetch, data событий и запросов
// воркера, токен регистрации, секрет агента и заголовок Authorization. Сценарий проходит
// целиком (настройка применена с подробным итогом, отказ воркера, fetch, событие, запрос
// воркера), затем метки-секрета нет ни в журнале агента (agent.logs — тот же поток записей,
// что и stderr), ни в файлах <dataDir>/logs, ни в записях log серверу, ни в журнале сервера
// (опция log SDK). Серверу метка доходит — там, где её передают по назначению.
func TestSecretsNotLogged(t *testing.T) {
	t.Parallel()
	const (
		canary      = "SECRET-CANARY-7f3a"
		enrollToken = "SECRET-CANARY-enroll"
	)
	s := newStand(t, func(c *testserver.Config) { c.EnrollToken = enrollToken })
	cfg := s.config(s.worker("w"))
	cfg.Enroll.Token = enrollToken
	cfg.Log.Level = message.LogDebug
	cfg.Log.Forward = message.LogDebug
	n := s.start(cfg)
	a := s.running("w")
	secret := map[string]string{"token": canary}

	// Настройка применена: подробный итог (тело ответа воркера) — в ConfigStatus.result.
	rec := s.server.SetConfig(a.ID, "w", "main", secret)
	s.applied(a.ID, "w", "main", rec.Version)
	st, _ := s.server.ConfigStatus(a.ID, "w", "main")
	if string(st.Result) != `{"applied":{"token":"`+canary+`"}}` {
		t.Fatalf("result: %s", st.Result)
	}

	// Отказ воркера: его текст (с меткой) — серверу в error, не в журнал.
	writeFile(t, filepath.Join(s.stateDir("w"), "reject"), "")
	rec = s.server.SetConfig(a.ID, "w", "limits", secret)
	eventually(t, "отказ воркера", func() bool {
		st, ok := s.server.ConfigStatus(a.ID, "w", "limits")
		return ok && st.State == testserver.ConfigFailed && st.Error != nil && strings.Contains(st.Error.Message, canary)
	})
	if err := os.Remove(filepath.Join(s.stateDir("w"), "reject")); err != nil {
		t.Fatal(err)
	}

	// fetch с меткой в теле и в Authorization.
	res := s.fetch(a.ID, "w", "/echo", testserver.FetchInit{Method: "POST", Body: `{"token":"` + canary + `"}`,
		Headers: map[string]string{"Content-Type": "application/json", "Authorization": "Bearer " + canary}})
	if res.Status != 201 || !strings.Contains(res.Body, canary) {
		t.Fatalf("fetch: %+v", res)
	}

	// События: объявленное и необъявленное (отклоняется агентом) — с меткой в data.
	data := url.QueryEscape(`{"token":"` + canary + `"}`)
	if _, got := n.direct("w", http.MethodPost, "/emit?type=example.done&data="+data); got != "202" {
		t.Fatalf("событие: %s", got)
	}
	if _, got := n.direct("w", http.MethodPost, "/emit?type=example.secret&data="+data); got != "400" {
		t.Fatalf("необъявленное событие: %s", got)
	}
	eventually(t, "событие на сервере", func() bool {
		b, _ := s.server.Agent(agentName)
		for _, e := range b.Events {
			if e.Type == "example.done" && strings.Contains(string(e.Data), canary) {
				return true
			}
		}
		return false
	})

	// Запросы воркера к серверу: ответ, отказ, необъявленный тип.
	for _, typ := range []string{"example.ask", "example.deny", "example.secret"} {
		res := s.fetch(a.ID, "w", "/ask?type="+typ+"&data="+data, testserver.FetchInit{Method: "POST"})
		if typ == "example.ask" && !strings.Contains(res.Body, canary) {
			t.Fatalf("ответ сервера воркеру: %+v", res)
		}
	}

	creds, ok, err := identity.NewStore(cfg.DataDir).Load()
	if err != nil || !ok {
		t.Fatalf("ключ агента: %v %v", ok, err)
	}
	secrets := []string{canary, enrollToken, creds.Secret, "Bearer "}
	check := func(where, text string) {
		t.Helper()
		for _, x := range secrets {
			if strings.Contains(text, x) {
				t.Fatalf("%s: найдено %q:\n%s", where, x, text)
			}
		}
	}
	// Отказ применения дошёл до журнала: проверка не пустая.
	eventually(t, "отказ в журнале агента", func() bool {
		var entries []testserver.LogEntry
		if err := s.server.Action("agentLogs", a.ID, &entries, map[string]any{"lines": message.MaxLogLines}); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(entries)
		return strings.Contains(string(raw), message.CodeConfigRejected)
	})
	for _, worker := range []string{"", "w"} {
		var entries []testserver.LogEntry
		args := map[string]any{"lines": message.MaxLogLines}
		if worker != "" {
			args["worker"] = worker
		}
		if err := s.server.Action("agentLogs", a.ID, &entries, args); err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(entries)
		check("agent.logs "+worker, string(raw))
	}
	files, _ := filepath.Glob(filepath.Join(cfg.DataDir, "logs", "*"))
	for _, f := range files {
		raw, _ := os.ReadFile(f)
		check(f, string(raw))
	}
	var raw []byte
	eventually(t, "отказ применения в записях log серверу", func() bool {
		raw, _ = json.Marshal(s.server.Logs(a.ID))
		return strings.Contains(string(raw), message.CodeConfigRejected)
	})
	check("log серверу", string(raw))
	check("журнал сервера", s.server.Log())
}
