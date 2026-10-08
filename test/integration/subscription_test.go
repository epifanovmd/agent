//go:build unix

package integration

import (
	"maps"
	"os"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/sdk/go/message"
	"github.com/epifanovmd/agent/sdk/go/server"
	"github.com/epifanovmd/agent/test/testserver"
)

// Подписка сервера (Subscribe): частота метрик (с учётом подписок на каналы),
// группы метрик, отправка лога и контекст воркера (channels — только свои
// каналы); Unsubscribe и истечение срока — снова обычный режим.
func TestSubscription(t *testing.T) {
	s := newStand(t, func(c *testserver.Config) {
		c.StatusInterval, c.MetricsInterval = 2*time.Second, 2*time.Second
	})
	exe, _ := os.Executable()
	s.agent("ws", []config.Worker{{
		Name: "ctl", Command: []string{exe}, Env: map[string]string{"IT_WORKER": "1", "IT_WORKER_KIND": "control"},
		Replicas: 1, StopTimeout: config.Duration(10 * time.Second),
	}}, func(c *config.Config) { c.Telemetry.Metrics = []string{"load"} })

	var id string
	eventually(t, "воркер ctl с контекстом", func() bool {
		a, ok := s.server.Agent("it-agent")
		if !ok || !a.Online || a.Capabilities == nil || a.Capabilities.Commands == nil {
			return false
		}
		id = a.ID
		c := s.ctl().Context
		return c.Online && c.MetricsIntervalMs == 2000
	})
	if c := s.ctl().Context; c.Channels != nil {
		t.Fatalf("без подписки — без channels: %+v", c.Channels)
	}
	// rate — точек метрик за последние window.
	rate := func(window time.Duration) int {
		since := time.Now().Add(-window).UnixMilli()
		points, err := s.server.Agents().Metrics(id, since)
		if err != nil {
			t.Fatal(err)
		}
		n := 0
		for _, p := range points {
			if !p.Backfill && p.At >= since {
				n++
			}
		}
		return n
	}
	host := func() *message.HostMetrics {
		a, _ := s.server.Agent("it-agent")
		if a.Metrics == nil {
			return nil
		}
		return a.Metrics.Host
	}

	sub, err := s.server.Agents().Subscribe(id, server.SubscribeRequest{
		TTL:      time.Minute,
		Metrics:  &server.MetricsSpec{IntervalMs: 1000, Groups: []string{message.MetricsSockets}},
		Logs:     &server.LogsSpec{Level: message.LogInfo},
		Channels: map[string]server.IntervalSpec{"example.ctl": {IntervalMs: 300}, "example.other": {IntervalMs: 250}},
	})
	if err != nil {
		t.Fatal(err)
	}
	// Контекст ctl: действующая частота метрик — минимум с каналами; channels —
	// только канал ctl.
	eventually(t, "контекст с подпиской", func() bool {
		c := s.ctl().Context
		return c.MetricsIntervalMs == 250 && maps.Equal(c.Channels, map[string]int64{"example.ctl": 300}) &&
			c.LogLevel == message.LogInfo
	})
	eventually(t, "группа sockets и частые метрики", func() bool {
		h := host()
		return h != nil && h.TCP != nil && h.Load5 != nil && rate(time.Second) >= 3
	})
	eventually(t, "показатели канала ctl", func() bool {
		a, _ := s.server.Agent("it-agent")
		return a.Metrics != nil && a.Metrics.Channels["example.ctl"] != nil
	})
	// Лог воркера уровня info уходит по подписке.
	s.run("it.print", "по подписке")
	eventually(t, "вывод воркера у сервера", func() bool { return s.logged("go", "по подписке") })

	// Отписка — обычный режим.
	if err := s.server.Agents().Unsubscribe(id, sub.ID); err != nil {
		t.Fatal(err)
	}
	eventually(t, "контекст после отписки", func() bool {
		c := s.ctl().Context
		return c.MetricsIntervalMs == 2000 && c.Channels == nil && c.LogLevel == message.LogWarn
	})
	eventually(t, "метрики после отписки", func() bool {
		h := host()
		return h != nil && h.TCP == nil && rate(2*time.Second) <= 2
	})

	// Подписка истекла — тоже обычный режим.
	if _, err := s.server.Agents().Subscribe(id, server.SubscribeRequest{
		TTL: 1500 * time.Millisecond, Status: &server.IntervalSpec{IntervalMs: 500},
		Channels: map[string]server.IntervalSpec{"example.ctl": {IntervalMs: 500}},
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "контекст со второй подпиской", func() bool {
		c := s.ctl().Context
		return c.MetricsIntervalMs == 500 && c.Channels["example.ctl"] == 500
	})
	eventually(t, "подписка истекла", func() bool {
		c := s.ctl().Context
		return c.MetricsIntervalMs == 2000 && c.Channels == nil
	})
}
