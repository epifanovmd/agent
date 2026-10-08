//go:build unix

package integration

import (
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/sdk/go/message"
	"github.com/epifanovmd/agent/sdk/go/server"
)

// Группы метрик узла: telemetry.metrics агента — только их поля; группы
// подписки (Subscribe → config.subscription.metrics) добавляются, по
// истечении подписки — снимаются.
func TestHostMetricsGroups(t *testing.T) {
	s := newStand(t)
	s.agent("ws", nil, func(c *config.Config) { c.Telemetry.Metrics = []string{"load", "swap"} })
	host := func() (string, *message.HostMetrics) {
		a, ok := s.server.Agent("it-agent")
		if !ok || a.Metrics == nil {
			return "", nil
		}
		return a.ID, a.Metrics.Host
	}
	var id string
	eventually(t, "load и swap без interfaces", func() bool {
		var h *message.HostMetrics
		id, h = host()
		return h != nil && h.Load5 != nil && h.SwapTotalBytes != nil &&
			h.Interfaces == nil && h.NetRxBps == nil && h.CPUPercent == nil && h.TCP == nil
	})
	if _, err := s.server.Agents().Subscribe(id, server.SubscribeRequest{
		TTL: 2 * time.Second, Metrics: &server.MetricsSpec{Groups: []string{message.MetricsSockets}},
	}); err != nil {
		t.Fatal(err)
	}
	eventually(t, "sockets от сервера", func() bool {
		_, h := host()
		return h != nil && h.TCP != nil && h.Load5 != nil && h.Interfaces == nil
	})
	eventually(t, "подписка истекла — снова только настройка агента", func() bool {
		_, h := host()
		return h != nil && h.TCP == nil && h.Load5 != nil
	})
}
