package telemetry

import (
	"context"
	"testing"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/logx"
)

var defaults = Options{Metrics: config.DefaultMetrics, Disks: []string{"/"}, Exclude: config.DefaultExcludeInterfaces}

func TestParseNvidiaSMI(t *testing.T) {
	gpus := ParseNvidiaSMI([]byte("0, NVIDIA GeForce RTX 4090, 97, 20000, 24564, 71\n1, A100, [N/A], 1, 2, 40\nmusor\n"))
	if len(gpus) != 2 || gpus[0].Name != "NVIDIA GeForce RTX 4090" || *gpus[0].UtilPercent != 97 || *gpus[0].MemUsedBytes != 20000<<20 || *gpus[0].TemperatureC != 71 {
		t.Fatalf("%+v", gpus)
	}
	if gpus[1].UtilPercent != nil {
		t.Fatal("[N/A] — пропуск")
	}
}

func TestCollectHost(t *testing.T) {
	c := New(defaults, logx.Discard())
	m := c.Collect(context.Background())
	if m.Host == nil || m.Host.MemTotalBytes == nil || *m.Host.MemTotalBytes == 0 || m.CollectedAt == 0 {
		t.Fatalf("метрики хоста: %+v", m.Host)
	}
	if ch := c.Channels(); len(ch) != 1 || ch[0] != "host" {
		t.Fatalf("каналы: %v", ch)
	}
	// telemetry.metrics: [] — без блока host; каналы воркеров остаются.
	c.Declare("it")
	c.Report("it", []byte(`{"n":1}`))
	c.Configure(Options{Metrics: []string{}})
	m = c.Collect(context.Background())
	if m.Host != nil || string(m.Channels["it"]) != `{"n":1}` {
		t.Fatalf("metrics: []: %+v", m)
	}
	if ch := c.Channels(); len(ch) != 1 || ch[0] != "it" {
		t.Fatalf("каналы без host: %v", ch)
	}
	c.Undeclare("it")
	if m = c.Collect(context.Background()); len(m.Channels) != 0 || c.Has("it") {
		t.Fatalf("канал убран: %+v", m)
	}
	if h := HostInfo(context.Background()); h.OS == "" || h.CPUs == 0 {
		t.Fatalf("хост: %+v", h)
	}
}

func TestRatesAndPhysical(t *testing.T) {
	prev := map[string]netCounter{"eth0": {rx: 1000, tx: 500, errors: 1, drops: 2}, "tun0": {rx: 0, tx: 0}, "eth1": {rx: 900, tx: 900}}
	cur := map[string]netCounter{"eth0": {rx: 3000, tx: 1500, errors: 4, drops: 2}, "tun0": {rx: 200, tx: 400, drops: 1}, "eth1": {rx: 10, tx: 10}, "new0": {rx: 5, tx: 5}}
	ifaces, sum := Rates(prev, cur, 2)
	if len(ifaces) != 2 || ifaces[0].Name != "eth0" || ifaces[0].RxBps != 1000 || ifaces[0].TxBps != 500 || ifaces[0].Errors != 3 ||
		ifaces[0].Drops != 0 || ifaces[1].Name != "tun0" || ifaces[1].TxBps != 200 || ifaces[1].Drops != 1 ||
		sum.RxBps != 1100 || sum.TxBps != 700 || sum.Errors != 3 || sum.Drops != 1 {
		t.Fatalf("скорости: %+v сумма %+v (сброшенный eth1 и новый new0 — пропуск)", ifaces, sum)
	}
	for _, name := range []string{"lo", "lo0", "veth12ab", "docker0", "br-1a2b", "utun3"} {
		if Shown(name, config.DefaultExcludeInterfaces) {
			t.Errorf("%s — виртуальный", name)
		}
	}
	for _, name := range []string{"eth0", "ens3", "tun0", "en0", "wlan0"} {
		if !Shown(name, config.DefaultExcludeInterfaces) {
			t.Errorf("%s — показывается", name)
		}
	}
	// Заданный список заменяет умолчание.
	if !Shown("docker0", []string{"tun"}) || Shown("tun0", []string{"tun"}) || !Shown("lo", nil) {
		t.Error("свой список префиксов")
	}
}

func TestInventory(t *testing.T) {
	c := New(defaults, logx.Discard())
	inv := c.Inventory(context.Background())
	if inv.CollectedAt == 0 || inv.OS == nil || inv.OS.Arch == "" || inv.CPU == nil || inv.CPU.Threads == 0 || inv.MemoryBytes == 0 || inv.Ports == nil {
		t.Fatalf("inventory: %+v", inv)
	}
	again := inv
	again.CollectedAt++
	if !SameInventory(inv, again) {
		t.Fatal("момент сбора не меняет inventory")
	}
	again.MemoryBytes++
	if SameInventory(inv, again) {
		t.Fatal("изменение заметно")
	}
}
