//go:build unix

package integration

import (
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/test/testserver"
)

// applied — ключ key воркера w применён в версии version.
func (s *stand) applied(agentID, w, key string, version int64) {
	s.t.Helper()
	eventually(s.t, "настройки "+w+"/"+key+" применены", func() bool {
		st, ok := s.server.ConfigStatus(agentID, w, key)
		return ok && st.State == testserver.ConfigApplied && st.Applied == version
	})
}

// puts — сколько раз воркер w получил версию version ключа key.
func (s *stand) puts(w, key string, version int64) int {
	prefix := "put " + key + " " + strconv.FormatInt(version, 10) + " "
	n := 0
	for _, l := range s.lines(w, "configs.log") {
		if strings.HasPrefix(l, prefix) {
			n++
		}
	}
	return n
}

// Настройки воркера: put → файл на диске агента и PUT воркеру → applied; несколько ключей;
// перезапуск воркера — все ключи передаются снова; агент без связи передаёт новому процессу
// воркера последние сохранённые; delete — файл удалён, воркер получил DELETE; неизвестный
// воркер — ошибка WORKER_UNKNOWN.
func TestConfigs(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	cfg := s.config(s.worker("w"))
	n := s.start(cfg)
	a := s.running("w")

	main := s.server.SetConfig(a.ID, "w", "main", map[string]any{"limit": 5})
	limits := s.server.SetConfig(a.ID, "w", "limits", []int{1, 2})
	s.applied(a.ID, "w", "main", main.Version)
	s.applied(a.ID, "w", "limits", limits.Version)
	if !slices.Contains(s.lines("w", "configs.log"), "put main "+strconv.FormatInt(main.Version, 10)+` {"limit":5}`) {
		t.Fatalf("воркер не получил main: %q", s.lines("w", "configs.log"))
	}
	file := filepath.Join(cfg.DataDir, "configs", "w", "main.json")
	if st, err := os.Stat(file); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("файл настроек: %v %v", st, err)
	}
	// Новая версия того же ключа.
	main = s.server.SetConfig(a.ID, "w", "main", map[string]any{"limit": 7})
	s.applied(a.ID, "w", "main", main.Version)
	eventually(t, "статус ключей в status агента", func() bool {
		b, _ := s.server.Agent(agentName)
		w, _ := b.Worker("w")
		c, ok := w.Configs["main"]
		return ok && c.Version == main.Version && c.OK != nil && *c.OK
	})

	// Воркер читает сохранённое значение у агента.
	got := s.fetch(a.ID, "w", "/agent/config/main", testserver.FetchInit{})
	if got.Headers["x-agent-status"] != "200" || !strings.Contains(got.Body, `"limit":7`) {
		t.Fatalf("GET /config/main у агента: %+v", got)
	}
	if got := s.fetch(a.ID, "w", "/agent/config/none", testserver.FetchInit{}); got.Headers["x-agent-status"] != "404" {
		t.Fatalf("GET /config/none у агента: %+v", got)
	}

	// Перезапуск воркера — все ключи заново.
	before := s.puts("w", "limits", limits.Version)
	if err := s.server.Action("restartWorker", a.ID, nil, "w"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "после перезапуска воркер получил ключи снова", func() bool {
		return s.puts("w", "limits", limits.Version) > before && s.puts("w", "main", main.Version) >= 2
	})

	// Без связи: новый запуск агента передаёт воркеру последние настройки с диска.
	n.stop()
	s.setDown(true)
	before = s.puts("w", "main", main.Version)
	n = s.start(cfg)
	eventually(t, "без связи воркер получил сохранённые настройки", func() bool {
		return s.puts("w", "main", main.Version) > before
	})
	s.setDown(false)
	a = s.running("w")

	// Удаление ключа.
	s.server.DeleteConfig(a.ID, "w", "limits")
	eventually(t, "ключ удалён у агента и воркера", func() bool {
		_, err := os.Stat(filepath.Join(cfg.DataDir, "configs", "w", "limits.json"))
		return os.IsNotExist(err) && slices.Contains(s.lines("w", "configs.log"), "delete limits")
	})
	eventually(t, "ключа нет в status", func() bool {
		b, _ := s.server.Agent(agentName)
		w, _ := b.Worker("w")
		_, ok := w.Configs["limits"]
		return !ok
	})
	eventually(t, "событие config: удаление подтверждено агентом", func() bool {
		return slices.ContainsFunc(s.server.ConfigEvents(a.ID), func(c testserver.ConfigStatus) bool {
			return c.Key == "limits" && c.State == testserver.ConfigDeleted && c.Version == nil
		})
	})

	// Воркера нет в настройках агента.
	unknown := s.server.SetConfig(a.ID, "nope", "main", 1)
	eventually(t, "WORKER_UNKNOWN", func() bool {
		st, ok := s.server.ConfigStatus(a.ID, "nope", "main")
		return ok && st.State == testserver.ConfigFailed && st.Error != nil && st.Error.Code == "WORKER_UNKNOWN" &&
			st.Version != nil && *st.Version == unknown.Version
	})
	_ = n
}

// Отказ воркера: CONFIG_REJECTED с его текстом; агент повторяет по таймеру (25 с) и после
// перезапуска воркера, пока воркер не примет.
func TestConfigRetry(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	s.start(s.config(s.worker("w")))
	a := s.running("w")
	reject := filepath.Join(s.stateDir("w"), "reject")
	writeFile(t, reject, "")

	rec := s.server.SetConfig(a.ID, "w", "main", "v1")
	eventually(t, "отказ воркера", func() bool {
		st, ok := s.server.ConfigStatus(a.ID, "w", "main")
		return ok && st.State == testserver.ConfigFailed && st.Error != nil && st.Error.Code == "CONFIG_REJECTED" &&
			strings.Contains(st.Error.Message, "значение не подходит")
	})

	// Повтор после перезапуска воркера.
	rejects := len(s.lines("w", "configs.log"))
	if err := s.server.Action("restartWorker", a.ID, nil, "w"); err != nil {
		t.Fatal(err)
	}
	eventually(t, "повтор после перезапуска воркера", func() bool { return len(s.lines("w", "configs.log")) > rejects })

	// Повтор по таймеру: воркер начал принимать.
	if err := os.Remove(reject); err != nil {
		t.Fatal(err)
	}
	start := time.Now()
	within(t, 40*time.Second, "повтор по таймеру", func() bool {
		st, ok := s.server.ConfigStatus(a.ID, "w", "main")
		return ok && st.State == testserver.ConfigApplied && st.Applied == rec.Version
	})
	t.Logf("применено через %s", time.Since(start).Round(time.Second))
	events := s.server.ConfigEvents(a.ID)
	if len(events) == 0 || events[len(events)-1].State != testserver.ConfigApplied {
		t.Fatalf("события config: %+v", events)
	}
}
