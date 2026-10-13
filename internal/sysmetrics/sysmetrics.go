// Package sysmetrics — метрики узла (§9), которые агент собирает сам: группы DefaultSettings.
// Сбор идёт в отдельной горутине со сроком: зависшее чтение системы не задерживает отправку
// остальных метрик.
package sysmetrics

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/epifanovmd/agent/protocol"
)

// SensorsCommand — `agent sensors`: показания датчиков (JSON) и выход; на macOS датчики
// читаются только в этом отдельном процессе.
const SensorsCommand = "sensors"

// Settings — что собирать: группы (protocol.MetricGroups), точки монтирования для группы disk
// (["all"] — все), исключённые интерфейсы.
type Settings struct {
	Metrics           []string `json:"metrics"`
	Disks             []string `json:"disks,omitempty"`
	ExcludeInterfaces []string `json:"excludeInterfaces,omitempty"`
}

// DefaultSettings — основные группы метрик, корневая файловая система; интерфейсы — без петли,
// контейнеров, мостов и служебных интерфейсов macOS.
func DefaultSettings() Settings {
	return Settings{
		Metrics: []string{
			protocol.MetricsCPU, protocol.MetricsLoad, protocol.MetricsMemory, protocol.MetricsSwap, protocol.MetricsDisk,
			protocol.MetricsNetwork, protocol.MetricsInterfaces, protocol.MetricsConntrack, protocol.MetricsUptime,
		},
		Disks: []string{"/"},
		ExcludeInterfaces: []string{
			"lo", "veth", "docker", "br-", "virbr", "vnet", "cni", "flannel", "cali", "kube", "tunl",
			"gif", "stf", "awdl", "llw", "anpi", "utun", "bridge", "ap",
		},
	}
}

// Dispatch — служебный режим программы агента: args[0] — SensorsCommand. false — это не он.
func Dispatch(args []string) (bool, error) {
	if len(args) == 0 || args[0] != SensorsCommand {
		return false, nil
	}
	return true, PrintSensors(context.Background())
}

// collectTimeout — сбор дольше не ждётся: метрики узла в этот раз не отправляются.
const collectTimeout = 10 * time.Second

// Collector — сбор метрик узла: скорости — по разнице с прошлым сбором, сборы идут по одному.
type Collector struct {
	c      *collector
	set    Settings
	groups []string

	mu   sync.Mutex
	busy bool
}

// NewCollector — сбор по set.
func NewCollector(set Settings) *Collector {
	groups := slices.DeleteFunc(slices.Clone(set.Metrics), func(g string) bool { return !slices.Contains(protocol.MetricGroups, g) })
	return &Collector{c: newCollector(), set: set, groups: groups}
}

// Collect — метрики узла; nil — прошлый сбор ещё идёт или не уложился в срок.
func (c *Collector) Collect(ctx context.Context) *protocol.HostMetrics {
	c.mu.Lock()
	if c.busy {
		c.mu.Unlock()
		return nil
	}
	c.busy = true
	c.mu.Unlock()
	done := make(chan *protocol.HostMetrics, 1)
	go func() {
		defer func() {
			c.mu.Lock()
			c.busy = false
			c.mu.Unlock()
		}()
		done <- c.c.host(context.WithoutCancel(ctx), c.groups, c.set.Disks, c.set.ExcludeInterfaces)
	}()
	timer := time.NewTimer(collectTimeout)
	defer timer.Stop()
	select {
	case h := <-done:
		return h
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return nil
	}
}
