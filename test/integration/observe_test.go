//go:build unix

package integration

import (
	"encoding/json"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/test/testserver"
)

// События воркера: POST /events → событие у сервера с именем воркера и временем; без связи
// они ждут в outbox и переживают перезапуск агента; каждое доходит один раз. GET /context.
func TestEvents(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	cfg := s.config(s.worker("w"))
	n := s.start(cfg)
	a := s.running("w")

	n.emit("w", "example.done", 0, 3)
	eventually(t, "события на сервере", func() bool { return countEvents(s, "example.done") == 3 })
	b, _ := s.server.Agent(agentName)
	var got []int
	for _, e := range b.Events {
		var d struct{ I int }
		mustJSON(t, e.Data, &d)
		if e.Worker != "w" || e.At == 0 {
			t.Fatalf("событие: %+v", e)
		}
		got = append(got, d.I)
	}
	if !slices.Equal(got, []int{0, 1, 2}) {
		t.Fatalf("порядок событий: %v", got)
	}

	ctx := s.fetch(a.ID, "w", "/agent/context", testserver.FetchInit{})
	var c message.Context
	mustJSON(t, []byte(ctx.Body), &c)
	if c.Agent.ID != a.ID || c.Agent.Name != agentName || !c.Online || c.Agent.Version != "it" {
		t.Fatalf("GET /context: %s", ctx.Body)
	}

	// Без связи и с перезапуском агента.
	s.setDown(true)
	n.emit("w", "example.later", 0, 4)
	_, body := n.direct("w", http.MethodGet, "/agent/context")
	if !strings.Contains(body, `"online":false`) {
		t.Fatalf("GET /context без связи: %s", body)
	}
	n.stop()
	n = s.start(cfg)
	n.emit("w", "example.later", 4, 1)
	s.setDown(false)
	eventually(t, "события после разрыва и перезапуска", func() bool { return countEvents(s, "example.later") == 5 })
	time.Sleep(time.Second)
	if k := countEvents(s, "example.later"); k != 5 {
		t.Fatalf("событий %d, нужно 5 — без повторов", k)
	}

	// Неверное событие агент не принимает.
	if _, codes := n.direct("w", http.MethodPost, "/emit?type=Bad&n=1"); codes != "400" {
		t.Fatalf("POST /events с неверным типом: %s", codes)
	}
}

// Метрики: узла (встроенный sysmetrics → metrics.host, его нет среди воркеров для сервера) и
// воркера (ответ GET /metrics как есть в metrics.workers).
func TestMetrics(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	cfg := s.config(s.worker("w"))
	cfg.Telemetry.Metrics = []string{message.MetricsCPU, message.MetricsMemory, message.MetricsUptime}
	s.start(cfg)
	a := s.running("w")
	eventually(t, "метрики узла и воркера", func() bool {
		for _, p := range s.server.Metrics(a.ID) {
			if p.Host != nil && p.Host.MemTotalBytes != nil && p.Host.UptimeSec != nil && len(p.Workers["w"]) > 0 {
				var w struct{ Calls int }
				return json.Unmarshal(p.Workers["w"], &w) == nil && w.Calls > 0
			}
		}
		return false
	})
	b, _ := s.server.Agent(agentName)
	if w, ok := b.Worker("sysmetrics"); ok && !w.Builtin {
		t.Fatalf("sysmetrics без builtin: %+v", w)
	}
	if _, err := s.server.Fetch(a.ID, "sysmetrics", "/", testserver.FetchInit{}); errorCode(err) != "WORKER_UNKNOWN" {
		t.Fatalf("запрос к встроенному воркеру: %v", err)
	}
}

// watch: метрики чаще (не чаще 1 с), вывод воркера в stdout (info) уходит серверу только
// пока наблюдатель просит info; stderr (warn) уходит всегда (log.forward: warn).
func TestWatch(t *testing.T) {
	t.Parallel()
	s := newStand(t, func(c *testserver.Config) { c.MetricsInterval = 4 * time.Second })
	n := s.start(s.config(s.worker("w")))
	a := s.running("w")

	rate := func(d time.Duration) int {
		before := len(s.server.Metrics(a.ID))
		time.Sleep(d)
		return len(s.server.Metrics(a.ID)) - before
	}
	if k := rate(3 * time.Second); k > 1 {
		t.Fatalf("без watch за 3 с точек %d при частоте 4 с", k)
	}

	logged := func(text string) bool {
		return slices.ContainsFunc(s.server.Logs(a.ID), func(e testserver.LogEntry) bool {
			return e.Source == "w" && e.Msg == text
		})
	}
	n.direct("w", http.MethodPost, "/print?text=stderr-1&stderr=1")
	n.direct("w", http.MethodPost, "/print?text=stdout-1")
	eventually(t, "stderr воркера на сервере", func() bool { return logged("stderr-1") })
	time.Sleep(1500 * time.Millisecond)
	if logged("stdout-1") {
		t.Fatal("stdout воркера (info) ушёл при log.forward: warn")
	}

	s.server.Watch(a.ID, testserver.WatchOptions{MetricsIntervalMs: 200, LogLevel: message.LogInfo, TTLMs: 60_000})
	time.Sleep(500 * time.Millisecond) // watch доходит до агента
	if k := rate(4 * time.Second); k < 3 || k > 6 {
		t.Fatalf("с watch за 4 с точек %d, нужно около 4 (не чаще раза в секунду)", k)
	}
	n.direct("w", http.MethodPost, "/print?text=stdout-2")
	eventually(t, "stdout воркера при watch info", func() bool { return logged("stdout-2") })
	for _, e := range s.server.Logs(a.ID) {
		if e.Msg == "stdout-2" && e.Level != message.LogInfo {
			t.Fatalf("уровень stdout: %+v", e)
		}
		if e.Msg == "stderr-1" && e.Level != message.LogWarn {
			t.Fatalf("уровень stderr: %+v", e)
		}
	}
}
