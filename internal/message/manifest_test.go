package message

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestParseManifest(t *testing.T) {
	m, err := ParseWorkerManifest([]byte(`{"version":"1.0.0","extra":1,"configs":[{"key":"main","schema":{"type":"object"}},
		{"key":"limits","schema":null}],"routes":[{"method":"POST","path":"/items/{id}"}],"events":[{"type":"item.done"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if m.Version != "1.0.0" || len(m.Configs) != 2 || m.Configs[1].Schema != nil || m.Routes[0].Path != "/items/{id}" {
		t.Fatalf("манифест: %+v", m)
	}
	raw, _ := json.Marshal(m)
	if strings.Contains(string(raw), "extra") || strings.Contains(string(raw), "null") {
		t.Fatalf("незнакомые поля и null не передаются: %s", raw)
	}
	if m, err := ParseWorkerManifest([]byte(`{"version":"1"}`)); err != nil || m.Version != "1" {
		t.Fatalf("минимальный манифест: %v", err)
	}
	if !m.DeclaresConfig("limits") || m.DeclaresConfig("other") || !m.DeclaresEvent("item.done") || m.DeclaresEvent("other") {
		t.Fatal("DeclaresConfig / DeclaresEvent")
	}
	var none *WorkerManifest
	if none.DeclaresConfig("main") || none.DeclaresEvent("item.done") {
		t.Fatal("без манифеста ничего не объявлено")
	}

	long := strings.Repeat("я", MaxManifestText+1)
	routes := make([]string, MaxManifestItems+1)
	for i := range routes {
		routes[i] = `{"method":"GET","path":"/a"}`
	}
	for name, body := range map[string]string{
		"не JSON":          `{`,
		"массив":           `[]`,
		"без version":      `{}`,
		"пустой version":   `{"version":""}`,
		"version длинный":  `{"version":"` + strings.Repeat("1", MaxManifestVersion+1) + `"}`,
		"description":      `{"version":"1","description":"` + long + `"}`,
		"ключ":             `{"version":"1","configs":[{"key":"Main"}]}`,
		"schema":           `{"version":"1","configs":[{"key":"main","schema":"object"}]}`,
		"метод":            `{"version":"1","routes":[{"method":"get","path":"/a"}]}`,
		"путь без /":       `{"version":"1","routes":[{"method":"GET","path":"a"}]}`,
		"путь с пробелом":  `{"version":"1","routes":[{"method":"GET","path":"/a b"}]}`,
		"маршрутов много":  `{"version":"1","routes":[` + strings.Join(routes, ",") + `]}`,
		"тип события":      `{"version":"1","events":[{"type":"Done"}]}`,
		"описание события": `{"version":"1","events":[{"type":"done","description":"` + long + `"}]}`,
		"событие job.*":    `{"version":"1","events":[{"type":"job.done"}]}`,
		"тип задачи":       `{"version":"1","jobs":[{"type":"Build"}]}`,
		"схема задачи":     `{"version":"1","jobs":[{"type":"build","schema":[]}]}`,
		"схема тела":       `{"version":"1","routes":[{"method":"GET","path":"/a","request":1}]}`,
		"схема ответа":     `{"version":"1","routes":[{"method":"GET","path":"/a","response":"x"}]}`,
		"схема события":    `{"version":"1","events":[{"type":"done","schema":true}]}`,
		"тип запроса":      `{"version":"1","requests":[{"type":"Ask"}]}`,
		"схема запроса":    `{"version":"1","requests":[{"type":"ask","schema":[]}]}`,
		"ответ запроса":    `{"version":"1","requests":[{"type":"ask","response":1}]}`,
		"больше предела":   `{"version":"1","description":"` + strings.Repeat("a", MaxManifestBytes) + `"}`,
	} {
		if _, err := ParseWorkerManifest([]byte(body)); err == nil {
			t.Errorf("%s: ждали ошибку", name)
		}
	}
}

// События задач (job.*) принимаются, только если манифест объявляет jobs, и
// только стандартные; объявлять их в events не нужно.
func TestManifestJobEvents(t *testing.T) {
	m, err := ParseWorkerManifest([]byte(`{"version":"1","events":[{"type":"item.done"}],
		"jobs":[{"type":"item.build","schema":{"type":"object"}},{"type":"item.check","schema":null}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Jobs) != 2 || m.Jobs[1].Schema != nil {
		t.Fatalf("jobs: %+v", m.Jobs)
	}
	for _, typ := range []string{JobEventProgress, JobEventDone, JobEventFailed, JobEventCancelled, "item.done"} {
		if !m.DeclaresEvent(typ) {
			t.Errorf("%s: ждали, что событие принимается", typ)
		}
	}
	if m.DeclaresEvent("job.other") {
		t.Error("job.other зарезервирован")
	}
	plain, _ := ParseWorkerManifest([]byte(`{"version":"1","events":[{"type":"item.done"}]}`))
	if plain.DeclaresEvent(JobEventDone) {
		t.Error("без jobs события задач не принимаются")
	}
}

// Схемы маршрутов, событий и запросов к серверу передаются как есть, null —
// то же, что их нет.
func TestManifestSchemasAndRequests(t *testing.T) {
	m, err := ParseWorkerManifest([]byte(`{"version":"1",
		"routes":[{"method":"POST","path":"/items/{id}","request":{"type":"object"},"response":null}],
		"events":[{"type":"item.done","schema":{"type":"object"}}],
		"requests":[{"type":"item.lookup","schema":{"type":"object"},"response":{"type":"string"}},{"type":"item.other","schema":null}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if string(m.Routes[0].Request) != `{"type":"object"}` || m.Routes[0].Response != nil ||
		string(m.Events[0].Schema) != `{"type":"object"}` || len(m.Requests) != 2 || m.Requests[1].Schema != nil ||
		string(m.Requests[0].Response) != `{"type":"string"}` {
		t.Fatalf("схемы: %+v", m)
	}
	if !m.DeclaresRequest("item.lookup") || m.DeclaresRequest("item.done") || !m.DeclaresRoute("POST", "/items/7") ||
		m.DeclaresRoute("GET", "/items/7") || m.DeclaresJob("x") {
		t.Fatal("DeclaresRequest / DeclaresRoute")
	}
	var none *WorkerManifest
	if none.DeclaresRequest("item.lookup") || none.DeclaresRoute("POST", "/items/7") || none.DeclaresJob("x") {
		t.Fatal("без манифеста ничего не объявлено")
	}
}

// Шаблон маршрута: сегменты совпадают точно, {name} — один непустой сегмент,
// кроме «.» и «..».
func TestMatchRoute(t *testing.T) {
	for _, c := range []struct {
		template, path string
		want           bool
	}{
		{"/echo", "/echo", true},
		{"/echo", "/echo/", false},
		{"/echo", "/Echo", false},
		{"/echo", "/echo/x", false},
		{"/", "/", true},
		{"/reports/{id}/send", "/reports/7/send", true},
		{"/reports/{id}/send", "/reports//send", false},
		{"/reports/{id}/send", "/reports/../send", false},
		{"/reports/{id}/send", "/reports/./send", false},
		{"/reports/{id}/send", "/reports/a/b/send", false},
		{"/reports/{id}", "/reports/{id}", true},
		{"/a/{x}/{y}", "/a/1/2", true},
		{"/a/{}", "/a/{}", true},
		{"/a/{}", "/a/1", false},
	} {
		if got := MatchRoute(c.template, c.path); got != c.want {
			t.Errorf("%s ~ %s: %v, ждали %v", c.template, c.path, got, c.want)
		}
	}
}
