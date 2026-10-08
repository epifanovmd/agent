package telemetry

import (
	"context"
	"errors"
	"os"
	"slices"
	"testing"
	"time"

	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/sensors"

	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/sdk/go/message"
)

var errNone = errors.New("нет")

// fakeHost — подменённые источники: счётчики растут на step за сбор, часы —
// на 10 с за сбор; files — содержимое /proc.
type fakeHost struct {
	at    time.Time
	step  uint64
	files map[string]string
}

func (f *fakeHost) sources() sources {
	return sources{
		now: func() time.Time { return f.at },
		cpu: func(_ context.Context, perCPU bool) ([]float64, error) {
			if perCPU {
				return []float64{10.04, 20.06}, nil
			}
			return []float64{15}, nil
		},
		load: func(context.Context) (*load.AvgStat, error) {
			return &load.AvgStat{Load1: 1, Load5: 0.5, Load15: 0.25}, nil
		},
		memory: func(context.Context) (*mem.VirtualMemoryStat, error) {
			return &mem.VirtualMemoryStat{Total: 100, Used: 40, Available: 55}, nil
		},
		swap: func(context.Context) (*mem.SwapMemoryStat, error) { return &mem.SwapMemoryStat{Total: 8, Used: 2}, nil },
		usage: func(_ context.Context, path string) (*disk.UsageStat, error) {
			switch path {
			case "/":
				return &disk.UsageStat{Total: 1000, Used: 300, InodesTotal: 50, InodesUsed: 5}, nil
			case "/data":
				return &disk.UsageStat{Total: 4000, Used: 1000}, nil
			}
			return nil, errNone
		},
		partitions: func(context.Context) ([]disk.PartitionStat, error) {
			return []disk.PartitionStat{
				{Device: "/dev/sda1", Mountpoint: "/", Fstype: "ext4"},
				{Device: "tmpfs", Mountpoint: "/run", Fstype: "tmpfs"},
				{Device: "/dev/sdb1", Mountpoint: "/data", Fstype: "xfs"},
				{Device: "/dev/sda1", Mountpoint: "/etc/hosts", Fstype: "ext4"}, // bind того же устройства
				{Device: "proc", Mountpoint: "/proc", Fstype: "proc"},
			}, nil
		},
		diskIO: func(context.Context) (map[string]disk.IOCountersStat, error) {
			n := f.step
			return map[string]disk.IOCountersStat{
				"sda":   {ReadBytes: 1000 * n, WriteBytes: 2000 * n, ReadCount: 10 * n, WriteCount: 20 * n},
				"sda1":  {ReadBytes: 1000 * n, WriteBytes: 2000 * n, ReadCount: 10 * n, WriteCount: 20 * n},
				"loop0": {ReadBytes: 9999 * n},
				"dm-0":  {ReadBytes: 9999 * n},
			}, nil
		},
		blockDevices: func() (map[string]bool, error) {
			return map[string]bool{"sda": true, "loop0": true, "dm-0": true}, nil
		},
		netIO: func(context.Context) ([]net.IOCountersStat, error) {
			n := f.step
			return []net.IOCountersStat{
				{Name: "eth0", BytesRecv: 100 * n, BytesSent: 50 * n, Errin: n, Errout: n, Dropin: 3 * n},
				{Name: "lo", BytesRecv: 999 * n, BytesSent: 999 * n},
			}, nil
		},
		uptime: func(context.Context) (uint64, error) { return 3600, nil },
		pids:   func(context.Context) ([]int32, error) { return []int32{1, 2, 3}, nil },
		temperatures: func(context.Context) ([]sensors.TemperatureStat, error) {
			return []sensors.TemperatureStat{
				{SensorKey: "nvme_composite", Temperature: 41.46},
				{SensorKey: "coretemp_package_id_0", Temperature: 64},
				{SensorKey: "acpitz", Temperature: 0},
			}, errors.New("часть датчиков не прочитана")
		},
		connections: nil,
		readFile: func(name string) ([]byte, error) {
			if v, ok := f.files[name]; ok {
				return []byte(v), nil
			}
			return nil, os.ErrNotExist
		},
	}
}

func (f *fakeHost) collector(opts Options) *Collector {
	c := New(opts, logx.Discard())
	c.src = f.sources()
	return c
}

// collect — два сбора с разницей 10 с: скорости считаются по второму.
func (f *fakeHost) collect(c *Collector) *message.HostMetrics {
	f.step++
	c.Collect(context.Background())
	f.step++
	f.at = f.at.Add(10 * time.Second)
	return c.Collect(context.Background()).Host
}

func newFake() *fakeHost {
	return &fakeHost{at: time.Unix(1_000_000, 0), files: map[string]string{
		procConntrackCount: "1234\n",
		procConntrackMax:   "262144\n",
		procFileNr:         "3200\t0\t1048576\n",
		procLoadavg:        "0.10 0.20 0.30 2/640 12345\n",
		procTCP: "  sl  local_address rem_address   st tx_queue rx_queue\n" +
			"   0: 00000000:0016 00000000:0000 0A 00000000:00000000\n" +
			"   1: 0100007F:1F90 0100007F:D1F2 01 00000000:00000000\n" +
			"   2: 0100007F:1F90 0100007F:D1F4 06 00000000:00000000\n",
		procTCP6: "  sl  local_address rem_address   st\n" +
			"   0: 00000000000000000000000000000000:0050 00000000000000000000000000000000:0000 0A\n" +
			"   1: 00000000000000000000000000000000:0050 00000000000000000000000000000001:A000 08\n" +
			"   2: 00000000000000000000000000000000:0050 00000000000000000000000000000001:A002 01\n",
	}}
}

// Все группы: каждое поле из своего источника.
func TestHostAllGroups(t *testing.T) {
	f := newFake()
	c := f.collector(Options{Metrics: message.MetricGroups, Disks: []string{"/", "/data", "/missing"}, Exclude: []string{"lo"}})
	h := f.collect(c)
	switch {
	case h == nil:
		t.Fatal("нет блока host")
	case *h.CPUPercent != 15 || !slices.Equal(h.CPUCores, []float64{10, 20.1}):
		t.Fatalf("cpu: %v %v", *h.CPUPercent, h.CPUCores)
	case *h.Load1 != 1 || *h.Load5 != 0.5 || *h.Load15 != 0.25:
		t.Fatal("load")
	case *h.MemUsedBytes != 40 || *h.MemTotalBytes != 100 || *h.MemAvailableBytes != 55:
		t.Fatal("memory")
	case *h.SwapUsedBytes != 2 || *h.SwapTotalBytes != 8:
		t.Fatal("swap")
	case *h.DiskUsedBytes != 300 || *h.DiskTotalBytes != 1000 || len(h.Disks) != 2 ||
		h.Disks[0] != (message.DiskMetrics{Mount: "/", UsedBytes: 300, TotalBytes: 1000, InodesUsed: 5, InodesTotal: 50}) ||
		h.Disks[1].Mount != "/data" || h.Disks[1].TotalBytes != 4000:
		t.Fatalf("disk: %+v", h.Disks)
	// Только sda: раздел sda1, loop0 и dm-0 не считаются; за 10 с — +1000 байт и +10 операций чтения.
	case *h.DiskReadBps != 100 || *h.DiskWriteBps != 200 || *h.DiskReadIops != 1 || *h.DiskWriteIops != 2:
		t.Fatalf("diskio: %d %d %d %d", *h.DiskReadBps, *h.DiskWriteBps, *h.DiskReadIops, *h.DiskWriteIops)
	case *h.NetRxBps != 10 || *h.NetTxBps != 5 || *h.NetErrors != 2 || *h.NetDrops != 3:
		t.Fatalf("network: %d %d %d %d", *h.NetRxBps, *h.NetTxBps, *h.NetErrors, *h.NetDrops)
	case len(h.Interfaces) != 1 || h.Interfaces[0] != (message.InterfaceMetrics{Name: "eth0", RxBps: 10, TxBps: 5, Errors: 2, Drops: 3}):
		t.Fatalf("interfaces: %+v", h.Interfaces)
	case *h.Conntrack != 1234 || *h.ConntrackMax != 262144:
		t.Fatal("conntrack")
	case *h.TCP != (message.TCPMetrics{Established: 2, TimeWait: 1, CloseWait: 1, Listen: 2}):
		t.Fatalf("sockets: %+v", *h.TCP)
	case *h.Processes != 3 || *h.Threads != 640:
		t.Fatal("processes")
	case *h.FDsOpen != 3200 || *h.FDsMax != 1048576:
		t.Fatal("fds")
	case *h.UptimeSec != 3600:
		t.Fatal("uptime")
	case h.Temperatures == nil || h.Temperatures.MaxC != 64 || len(h.Temperatures.Sensors) != 2 ||
		h.Temperatures.Sensors[0] != (message.TemperatureSensor{Name: "coretemp_package_id_0", C: 64}) ||
		h.Temperatures.Sensors[1].C != 41.5:
		t.Fatalf("temperatures: %+v", h.Temperatures)
	}
}

// Нет группы — нет её полей; [] — нет блока host и канала host.
func TestHostGroupsSelect(t *testing.T) {
	f := newFake()
	c := f.collector(Options{Metrics: []string{"load", "swap"}})
	h := f.collect(c)
	if h == nil || h.Load5 == nil || h.SwapTotalBytes == nil {
		t.Fatalf("load и swap: %+v", h)
	}
	if h.CPUPercent != nil || h.CPUCores != nil || h.MemTotalBytes != nil || h.DiskTotalBytes != nil || h.Disks != nil ||
		h.NetRxBps != nil || h.Interfaces != nil || h.Conntrack != nil || h.TCP != nil || h.Processes != nil ||
		h.FDsOpen != nil || h.UptimeSec != nil || h.Temperatures != nil || h.DiskReadBps != nil {
		t.Fatalf("лишние поля: %+v", h)
	}
	// interfaces без network — только список интерфейсов.
	c.Configure(Options{Metrics: []string{"interfaces"}, Exclude: []string{"lo"}})
	if h = f.collect(c); len(h.Interfaces) != 1 || h.NetRxBps != nil {
		t.Fatalf("interfaces без network: %+v", h)
	}
	c.Configure(Options{Metrics: []string{}})
	if m := c.Collect(context.Background()); m.Host != nil || c.Has("host") {
		t.Fatalf("без групп: %+v", m.Host)
	}
}

// Недоступное на платформе (нет /proc, датчиков) — нет полей, без ошибок.
func TestHostUnavailable(t *testing.T) {
	f := newFake()
	f.files = nil
	c := f.collector(Options{Metrics: message.MetricGroups})
	c.src.temperatures = func(context.Context) ([]sensors.TemperatureStat, error) { return nil, errNone }
	c.src.blockDevices = func() (map[string]bool, error) { return nil, errNone }
	h := f.collect(c)
	if h.Conntrack != nil || h.ConntrackMax != nil || h.FDsOpen != nil || h.FDsMax != nil || h.Threads != nil ||
		h.TCP != nil || h.Temperatures != nil {
		t.Fatalf("недоступное: %+v", h)
	}
	if h.Processes == nil || *h.Processes != 3 {
		t.Fatal("processes — из списка процессов")
	}
	// Без /sys/block считаются все устройства, кроме виртуальных: sda и sda1.
	if *h.DiskReadBps != 200 {
		t.Fatalf("diskio без /sys/block: %d", *h.DiskReadBps)
	}
	// Без /proc/net/tcp — список соединений системы.
	c.src.connections = func(context.Context) ([]net.ConnectionStat, error) {
		return []net.ConnectionStat{{Status: "ESTABLISHED"}, {Status: "LISTEN"}, {Status: "TIME_WAIT"}, {Status: "SYN_SENT"}}, nil
	}
	if h = c.Collect(context.Background()).Host; h.TCP == nil || *h.TCP != (message.TCPMetrics{Established: 1, TimeWait: 1, Listen: 1}) {
		t.Fatalf("sockets из списка соединений: %+v", h.TCP)
	}
}

// telemetry.disks: [all] — реальные файловые системы без повторов устройств.
func TestDisksAll(t *testing.T) {
	f := newFake()
	c := f.collector(Options{Metrics: []string{"disk"}, Disks: []string{"all"}})
	h := c.Collect(context.Background()).Host
	if len(h.Disks) != 2 || h.Disks[0].Mount != "/" || h.Disks[1].Mount != "/data" {
		t.Fatalf("disks all: %+v", h.Disks)
	}
	if !PhysicalDisk("nvme0n1", nil) || PhysicalDisk("sda1", map[string]bool{"sda": true}) || PhysicalDisk("loop3", nil) ||
		PhysicalDisk("zram0", nil) || PhysicalDisk("md0", nil) {
		t.Fatal("физические диски")
	}
}

// Группы подписки сервера (subscription.metrics) добавляются к настройке агента;
// пустой список — снова только настройка агента; неизвестные — пропуск.
func TestSubscriptionGroups(t *testing.T) {
	f := newFake()
	c := f.collector(Options{Metrics: []string{"load"}})
	if c.SetExtra([]string{"sockets", "nope", "sockets"}) {
		t.Fatal("канал host уже был")
	}
	if g := c.Groups(); !slices.Equal(g, []string{"load", "sockets"}) {
		t.Fatalf("группы: %v", g)
	}
	h := c.Collect(context.Background()).Host
	if h.Load1 == nil || h.TCP == nil || h.CPUPercent != nil {
		t.Fatalf("load + sockets: %+v", h)
	}
	// Настройка агента перечитана — группы сервера остаются.
	c.Configure(Options{Metrics: []string{"uptime"}})
	if h = c.Collect(context.Background()).Host; h.UptimeSec == nil || h.TCP == nil || h.Load1 != nil {
		t.Fatalf("uptime + sockets: %+v", h)
	}
	c.SetExtra(nil)
	if h = c.Collect(context.Background()).Host; h.TCP != nil {
		t.Fatalf("без групп сервера: %+v", h)
	}
	// Агент без метрик узла: группы сервера включают блок host и канал.
	c.Configure(Options{Metrics: []string{}})
	if !c.SetExtra([]string{"fds"}) || !c.Has("host") {
		t.Fatal("канал host появился")
	}
	if h = c.Collect(context.Background()).Host; h == nil || h.FDsOpen == nil {
		t.Fatalf("только группы сервера: %+v", h)
	}
	if !c.SetExtra([]string{}) || c.Has("host") {
		t.Fatal("канал host пропал")
	}
}

// Настоящие источники: все группы собираются без паники (недоступное — без полей).
func TestHostSystemAllGroups(t *testing.T) {
	c := New(Options{Metrics: message.MetricGroups, Disks: []string{"all"}}, logx.Discard())
	c.Collect(context.Background())
	h := c.Collect(context.Background()).Host
	if h == nil || h.Load5 == nil || h.MemAvailableBytes == nil || h.Processes == nil {
		t.Fatalf("host: %+v", h)
	}
}
