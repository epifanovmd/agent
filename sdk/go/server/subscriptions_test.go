package server

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// rawConfigs — data сообщений config, отправленных сессии.
func rawConfigs(agents *Agents, ss *session) []string {
	var out []string
	for _, env := range ofType(take(agents, ss), message.TypeConfig) {
		out = append(out, string(env.Data))
	}
	return out
}

// sameJSON — JSON a и b равны по содержанию.
func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y any
	if err := json.Unmarshal(a, &x); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(b, &y); err != nil {
		t.Fatal(err)
	}
	return reflect.DeepEqual(x, y)
}

// reopen — новая сессия агента; welcome.config.
func reopen(t *testing.T, agents *Agents, id string) (*session, message.SessionConfig, []byte) {
	t.Helper()
	agents.mu.Lock()
	ss := agents.newSession(id, TransportWS, "")
	agents.open(ss, helloEnv("b", message.Capabilities{}))
	agents.unlock()
	out := take(agents, ss)
	var w struct {
		Config json.RawMessage `json:"config"`
	}
	if len(out) == 0 || out[0].Type != message.TypeWelcome || json.Unmarshal(out[0].Data, &w) != nil {
		t.Fatalf("нет welcome: %+v", out)
	}
	var cfg message.SessionConfig
	_ = json.Unmarshal(w.Config, &cfg)
	return ss, cfg, w.Config
}

// Проверки Subscribe: интервал < 200 мс, группа, уровень, канал не по правилу —
// MESSAGE_INVALID (запись не меняется); агента нет — AGENT_NOT_FOUND.
func TestSubscribeValidation(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	if _, err := agents.Subscribe("nobody", SubscribeRequest{}); protoCode(err) != "AGENT_NOT_FOUND" {
		t.Fatalf("неизвестный агент: %v", err)
	}
	if err := agents.Unsubscribe("nobody", "x"); protoCode(err) != "AGENT_NOT_FOUND" {
		t.Fatalf("Unsubscribe неизвестного агента: %v", err)
	}
	bad := map[string]SubscribeRequest{
		"status 199":    {Status: &IntervalSpec{IntervalMs: 199}},
		"status 0":      {Status: &IntervalSpec{}},
		"metrics 100":   {Metrics: &MetricsSpec{IntervalMs: 100}},
		"metrics -1":    {Metrics: &MetricsSpec{IntervalMs: -1}},
		"группа":        {Metrics: &MetricsSpec{Groups: []string{"sockets", "Bad Group"}}},
		"уровень":       {Logs: &LogsSpec{Level: "trace"}},
		"уровень пуст":  {Logs: &LogsSpec{}},
		"канал":         {Channels: map[string]IntervalSpec{"bad name": {IntervalMs: 1000}}},
		"канал частота": {Channels: map[string]IntervalSpec{"example.app": {IntervalMs: 50}}},
	}
	for name, req := range bad {
		if _, err := agents.Subscribe(id, req); protoCode(err) != "MESSAGE_INVALID" {
			t.Fatalf("%s: %v", name, err)
		}
	}
	if a, _ := agents.Agent(id); a.Subscriptions != nil {
		t.Fatalf("ошибка не меняет запись: %+v", a.Subscriptions)
	}
	// Без частоты метрик — допустимо (только группы).
	sub, err := agents.Subscribe(id, SubscribeRequest{Metrics: &MetricsSpec{Groups: []string{"sockets", "sockets"}}})
	if err != nil || sub.ID == "" || sub.Until < now()+25_000 || !reflect.DeepEqual(sub.Metrics.Groups, []string{"sockets"}) {
		t.Fatalf("подписка: %+v %v", sub, err)
	}
	if err := agents.Unsubscribe(id, "unknown"); err != nil {
		t.Fatalf("неизвестная подписка: %v", err)
	}
}

// Сводная из нескольких подписок: частоты — минимум, группы — объединение,
// уровень — самый подробный, каналы — минимум; config только при изменении;
// welcome со сводной; Unsubscribe — новая сводная, последней — пустая.
func TestSubscriptionSummary(t *testing.T) {
	agents := newTestAgents(t, Options{StatusInterval: 15 * time.Second, MetricsInterval: 15 * time.Second})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))

	a, err := agents.Subscribe(id, SubscribeRequest{
		Metrics:  &MetricsSpec{IntervalMs: 5000, Groups: []string{"diskio"}},
		Logs:     &LogsSpec{Level: message.LogInfo},
		Channels: map[string]IntervalSpec{"example.app": {IntervalMs: 2000}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if c := rawConfigs(agents, ss); len(c) != 1 ||
		c[0] != `{"subscription":{"metricsIntervalMs":5000,"metrics":["diskio"],"logLevel":"info","channels":{"example.app":2000}}}` {
		t.Fatalf("первая подписка: %v", c)
	}
	breq := SubscribeRequest{
		ID:       "b",
		Metrics:  &MetricsSpec{IntervalMs: 1000, Groups: []string{"diskio", "sockets"}},
		Logs:     &LogsSpec{Level: message.LogDebug},
		Channels: map[string]IntervalSpec{"example.app": {IntervalMs: 1000}},
	}
	if b, err := agents.Subscribe(id, breq); err != nil || b.ID != "b" {
		t.Fatalf("вторая подписка: %+v %v", b, err)
	}
	c := rawConfigs(agents, ss)
	if len(c) != 1 || !sameJSON(t, []byte(c[0]), exampleEnv(t, "config.subscription").Data) {
		t.Fatalf("сводная: %v", c)
	}
	if rec, _ := agents.Agent(id); len(rec.Subscriptions) != 2 {
		t.Fatalf("запись: %+v", rec.Subscriptions)
	}
	// Продление тем же содержимым — сводная та же, config нет.
	if _, err := agents.Subscribe(id, breq); err != nil {
		t.Fatal(err)
	}
	if c := rawConfigs(agents, ss); len(c) != 0 {
		t.Fatalf("продление: %v", c)
	}
	if rec, _ := agents.Agent(id); len(rec.Subscriptions) != 2 {
		t.Fatalf("продление не добавляет подписку: %+v", rec.Subscriptions)
	}

	// Подключился при подписках — сводная в welcome, обычные интервалы — свои.
	ss, cfg, _ := reopen(t, agents, id)
	if cfg.MetricsIntervalMs != 15_000 || cfg.Subscription == nil || cfg.Subscription.MetricsIntervalMs != 1000 ||
		cfg.Subscription.LogLevel != message.LogDebug {
		t.Fatalf("welcome: %+v %+v", cfg, cfg.Subscription)
	}

	if err := agents.Unsubscribe(id, "b"); err != nil {
		t.Fatal(err)
	}
	if c := rawConfigs(agents, ss); len(c) != 1 ||
		c[0] != `{"subscription":{"metricsIntervalMs":5000,"metrics":["diskio"],"logLevel":"info","channels":{"example.app":2000}}}` {
		t.Fatalf("после Unsubscribe: %v", c)
	}
	if err := agents.Unsubscribe(id, a.ID); err != nil {
		t.Fatal(err)
	}
	c = rawConfigs(agents, ss)
	if len(c) != 1 || !sameJSON(t, []byte(c[0]), exampleEnv(t, "config.subscription.empty").Data) {
		t.Fatalf("подписок нет: %v", c)
	}
	if rec, _ := agents.Agent(id); rec.Subscriptions != nil {
		t.Fatalf("запись после Unsubscribe: %+v", rec.Subscriptions)
	}
	// Без подписок — в welcome поля нет.
	if _, cfg, _ := reopen(t, agents, id); cfg.Subscription != nil {
		t.Fatalf("welcome без подписок: %+v", cfg.Subscription)
	}
}

// welcome со сводной подпиской — как в образце.
func TestSubscriptionWelcomeExample(t *testing.T) {
	agents := newTestAgents(t, Options{StatusInterval: 15 * time.Second, MetricsInterval: 15 * time.Second})
	id, _ := enroll(t, agents, "a")
	if _, err := agents.Subscribe(id, SubscribeRequest{
		Status:   &IntervalSpec{IntervalMs: 1000},
		Metrics:  &MetricsSpec{IntervalMs: 1000, Groups: []string{"diskio"}},
		Channels: map[string]IntervalSpec{"example.app": {IntervalMs: 1000}},
	}); err != nil {
		t.Fatal(err)
	}
	_, _, raw := reopen(t, agents, id)
	var want struct {
		Config json.RawMessage `json:"config"`
	}
	_ = json.Unmarshal(exampleEnv(t, "welcome.subscription").Data, &want)
	if !sameJSON(t, raw, want.Config) {
		t.Fatalf("welcome.config: %s, ждали %s", raw, want.Config)
	}
}

// Истечение: сверка удаляет истёкшую подписку из записи и шлёт новую
// сводную; продлённая не истекает; последняя истекла — пустая сводная.
func TestSubscriptionExpiry(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))

	if _, err := agents.Subscribe(id, SubscribeRequest{TTL: 300 * time.Millisecond, Status: &IntervalSpec{IntervalMs: 1000}}); err != nil {
		t.Fatal(err)
	}
	long := SubscribeRequest{ID: "long", TTL: 1200 * time.Millisecond, Metrics: &MetricsSpec{IntervalMs: 2000}}
	if _, err := agents.Subscribe(id, long); err != nil {
		t.Fatal(err)
	}
	if c := rawConfigs(agents, ss); len(c) != 2 {
		t.Fatalf("config подписок: %v", c)
	}
	eventually(t, "короткая истекла", func() bool {
		c := rawConfigs(agents, ss)
		if len(c) > 0 && c[0] != `{"subscription":{"metricsIntervalMs":2000}}` {
			t.Fatalf("по истечении короткой: %v", c)
		}
		return len(c) == 1
	})
	if rec, _ := agents.Agent(id); len(rec.Subscriptions) != 1 || rec.Subscriptions[0].ID != "long" {
		t.Fatalf("запись после истечения: %+v", rec.Subscriptions)
	}
	// Продление — срок отодвигается, config нет.
	time.Sleep(700 * time.Millisecond)
	if _, err := agents.Subscribe(id, long); err != nil {
		t.Fatal(err)
	}
	time.Sleep(700 * time.Millisecond) // первый срок прошёл, продлённый — нет
	if c := rawConfigs(agents, ss); len(c) != 0 {
		t.Fatalf("продлённая истекла: %v", c)
	}
	eventually(t, "последняя истекла", func() bool {
		c := rawConfigs(agents, ss)
		if len(c) > 0 && c[0] != `{"subscription":{}}` {
			t.Fatalf("по истечении последней: %v", c)
		}
		return len(c) == 1
	})
	if rec, _ := agents.Agent(id); rec.Subscriptions != nil {
		t.Fatalf("запись: %+v", rec.Subscriptions)
	}
}

// Два Agents на общем Store: подписка из другого процесса — в записи; агент
// этого процесса получает её после Refresh (повторный Refresh — без config),
// подключившийся — в welcome; истечение — сверка процесса с сессией; у агента
// без связи истёкшие удаляет любой процесс.
func TestSubscriptionsAcrossProcesses(t *testing.T) {
	shared := NewMemoryStore()
	local := newTestAgents(t, Options{Store: shared})
	remote := newTestAgents(t, Options{Store: shared})
	id, _ := enroll(t, local, "a")
	offline, _ := enroll(t, local, "b")

	// Агент без связи — только запись; подключился — сводная в welcome.
	sub, err := remote.Subscribe(offline, SubscribeRequest{TTL: time.Minute, Logs: &LogsSpec{Level: message.LogWarn}})
	if err != nil {
		t.Fatal(err)
	}
	if rec, _ := local.Agent(offline); len(rec.Subscriptions) != 1 || rec.Subscriptions[0].ID != sub.ID {
		t.Fatalf("запись: %+v", rec.Subscriptions)
	}
	if _, cfg, _ := reopen(t, local, offline); cfg.Subscription == nil || cfg.Subscription.LogLevel != message.LogWarn {
		t.Fatalf("welcome по записи: %+v", cfg.Subscription)
	}

	ss := open(t, local, id, helloEnv("b", message.Capabilities{}))
	if _, err := remote.Subscribe(id, SubscribeRequest{TTL: 1500 * time.Millisecond, Status: &IntervalSpec{IntervalMs: 500}}); err != nil {
		t.Fatal(err)
	}
	if c := rawConfigs(local, ss); len(c) != 0 {
		t.Fatalf("без Refresh: %v", c)
	}
	local.Refresh(id)
	if c := rawConfigs(local, ss); len(c) != 1 || c[0] != `{"subscription":{"statusIntervalMs":500}}` {
		t.Fatalf("после Refresh: %v", c)
	}
	// Вторая подписка из другого процесса — сводная из обеих.
	if _, err := remote.Subscribe(id, SubscribeRequest{TTL: time.Minute, ID: "ui", Status: &IntervalSpec{IntervalMs: 2000},
		Channels: map[string]IntervalSpec{"example.app": {IntervalMs: 1000}}}); err != nil {
		t.Fatal(err)
	}
	local.Refresh("")
	if c := rawConfigs(local, ss); len(c) != 1 || c[0] != `{"subscription":{"statusIntervalMs":500,"channels":{"example.app":1000}}}` {
		t.Fatalf("сводная из двух процессов: %v", c)
	}
	local.Refresh("")
	if c := rawConfigs(local, ss); len(c) != 0 {
		t.Fatalf("повторный Refresh — без config: %v", c)
	}
	eventually(t, "истечение в процессе с сессией", func() bool {
		c := rawConfigs(local, ss)
		if len(c) > 0 && c[0] != `{"subscription":{"statusIntervalMs":2000,"channels":{"example.app":1000}}}` {
			t.Fatalf("по истечении: %v", c)
		}
		return len(c) == 1
	})
	if rec, _ := remote.Agent(id); len(rec.Subscriptions) != 1 || rec.Subscriptions[0].ID != "ui" {
		t.Fatalf("запись после истечения: %+v", rec.Subscriptions)
	}
	// Unsubscribe из другого процесса — после Refresh.
	if err := remote.Unsubscribe(id, "ui"); err != nil {
		t.Fatal(err)
	}
	local.Refresh(id)
	if c := rawConfigs(local, ss); len(c) != 1 || c[0] != `{"subscription":{}}` {
		t.Fatalf("Unsubscribe другого процесса: %v", c)
	}

	// Агент без связи: истёкшие удаляет сверка (здесь — любого из процессов).
	never, _ := enroll(t, remote, "c")
	if _, err := remote.Subscribe(never, SubscribeRequest{TTL: 300 * time.Millisecond, Status: &IntervalSpec{IntervalMs: 1000}}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "истёкшие подписки агента без связи удалены", func() bool {
		rec, _ := local.Agent(never)
		return rec.Subscriptions == nil
	})
}

// Отзыв удаляет подписки; отозванному — AGENT_REVOKED.
func TestSubscriptionRevoked(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	if _, err := agents.Subscribe(id, SubscribeRequest{TTL: time.Minute, Logs: &LogsSpec{Level: message.LogDebug}}); err != nil {
		t.Fatal(err)
	}
	if err := agents.Revoke(id); err != nil {
		t.Fatal(err)
	}
	if rec, _ := agents.Agent(id); rec.Subscriptions != nil {
		t.Fatalf("подписки после отзыва: %+v", rec.Subscriptions)
	}
	if _, err := agents.Subscribe(id, SubscribeRequest{}); protoCode(err) != "AGENT_REVOKED" {
		t.Fatalf("отозванный: %v", err)
	}
}

// summarize — правила сводной на чистых данных.
func TestSummarize(t *testing.T) {
	ts := int64(1000)
	subs := []Subscription{
		{ID: "old", Until: ts, Status: &IntervalSpec{IntervalMs: 200}},
		{ID: "a", Until: ts + 500, Metrics: &MetricsSpec{Groups: []string{"sockets"}}, Logs: &LogsSpec{Level: message.LogWarn}},
		{ID: "b", Until: ts + 100, Metrics: &MetricsSpec{IntervalMs: 3000, Groups: []string{"diskio", "sockets"}},
			Logs: &LogsSpec{Level: message.LogError}, Channels: map[string]IntervalSpec{"x": {IntervalMs: 900}, "y": {IntervalMs: 400}}},
		{ID: "c", Until: ts + 900, Channels: map[string]IntervalSpec{"x": {IntervalMs: 300}}},
	}
	sum, next := summarize(subs, ts)
	want := message.Subscription{MetricsIntervalMs: 3000, Metrics: []string{"sockets", "diskio"}, LogLevel: message.LogWarn,
		Channels: map[string]int64{"x": 300, "y": 400}}
	if !reflect.DeepEqual(sum, want) || next != ts+100 {
		t.Fatalf("сводная: %+v next %d", sum, next)
	}
	if sum, next := summarize(subs[:1], ts); !reflect.DeepEqual(sum, message.Subscription{}) || next != 0 {
		t.Fatalf("только истёкшие: %+v %d", sum, next)
	}
}
