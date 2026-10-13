package sysmetrics

import (
	"context"
	"testing"
)

// Сбор по настройкам: заданные группы есть, остальных нет; второй сбор — со скоростями.
func TestCollect(t *testing.T) {
	c := NewCollector(Settings{Metrics: []string{"memory", "uptime", "network", "nope"}, ExcludeInterfaces: []string{"lo"}})
	h := c.Collect(context.Background())
	if h == nil || h.MemTotalBytes == nil || h.UptimeSec == nil || h.CPUPercent != nil || h.Load1 != nil {
		t.Fatalf("метрики: %+v", h)
	}
	if h = c.Collect(context.Background()); h == nil {
		t.Fatal("второй сбор")
	}
	if ok, err := Dispatch([]string{"run"}); ok || err != nil {
		t.Fatalf("не служебный режим: %v %v", ok, err)
	}
}
