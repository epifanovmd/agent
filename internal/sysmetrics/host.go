package sysmetrics

import (
	"bytes"
	"context"
	"math"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/load"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"
	"github.com/shirou/gopsutil/v4/process"
	"github.com/shirou/gopsutil/v4/sensors"

	"github.com/epifanovmd/agent/internal/message"
)

// sources — откуда берутся показатели узла (в тестах подменяются).
// Ошибка источника — показателя нет в метриках.
type sources struct {
	now        func() time.Time
	cpu        func(ctx context.Context, perCPU bool) ([]float64, error)
	load       func(ctx context.Context) (*load.AvgStat, error)
	memory     func(ctx context.Context) (*mem.VirtualMemoryStat, error)
	swap       func(ctx context.Context) (*mem.SwapMemoryStat, error)
	usage      func(ctx context.Context, path string) (*disk.UsageStat, error)
	partitions func(ctx context.Context) ([]disk.PartitionStat, error)
	diskIO     func(ctx context.Context) (map[string]disk.IOCountersStat, error)
	// blockDevices — целые диски (/sys/block); ошибка — считать все устройства diskIO.
	blockDevices func() (map[string]bool, error)
	netIO        func(ctx context.Context) ([]net.IOCountersStat, error)
	uptime       func(ctx context.Context) (uint64, error)
	pids         func(ctx context.Context) ([]int32, error)
	temperatures func(ctx context.Context) ([]sensors.TemperatureStat, error)
	// connections — TCP-соединения там, где нет /proc/net/tcp (дорого: только для группы sockets).
	connections func(ctx context.Context) ([]net.ConnectionStat, error)
	// readFile — файлы /proc (Linux).
	readFile func(name string) ([]byte, error)
	// gpus — видеокарты (nil — nvidia-smi нет).
	gpus func(ctx context.Context) []message.GPUMetrics
}

func systemSources() sources {
	return sources{
		now: time.Now,
		cpu: func(ctx context.Context, perCPU bool) ([]float64, error) {
			return cpu.PercentWithContext(ctx, 0, perCPU)
		},
		load:   load.AvgWithContext,
		memory: mem.VirtualMemoryWithContext,
		swap:   mem.SwapMemoryWithContext,
		usage:  disk.UsageWithContext,
		partitions: func(ctx context.Context) ([]disk.PartitionStat, error) {
			return disk.PartitionsWithContext(ctx, false)
		},
		diskIO: func(ctx context.Context) (map[string]disk.IOCountersStat, error) {
			return disk.IOCountersWithContext(ctx)
		},
		blockDevices: func() (map[string]bool, error) {
			entries, err := os.ReadDir("/sys/block")
			if err != nil {
				return nil, err
			}
			out := map[string]bool{}
			for _, e := range entries {
				out[e.Name()] = true
			}
			return out, nil
		},
		netIO: func(ctx context.Context) ([]net.IOCountersStat, error) {
			return net.IOCountersWithContext(ctx, true)
		},
		uptime:       host.UptimeWithContext,
		pids:         process.PidsWithContext,
		temperatures: temperatures,
		connections: func(ctx context.Context) ([]net.ConnectionStat, error) {
			return net.ConnectionsWithoutUidsWithContext(ctx, "tcp")
		},
		readFile: os.ReadFile,
		gpus:     gpuSource(),
	}
}

// Файлы /proc (Linux).
const (
	procConntrackCount = "/proc/sys/net/netfilter/nf_conntrack_count"
	procConntrackMax   = "/proc/sys/net/netfilter/nf_conntrack_max"
	procFileNr         = "/proc/sys/fs/file-nr"
	procLoadavg        = "/proc/loadavg"
	procTCP            = "/proc/net/tcp"
	procTCP6           = "/proc/net/tcp6"
)

// disksAll — settings.disks: все реальные файловые системы.
const disksAll = "all"

// collector — сбор метрик узла: источники и счётчики прошлого сбора (скорости —
// по разнице). Вызывается из одной горутины.
type collector struct {
	src        sources
	prevNet    map[string]netCounter
	prevNetAt  time.Time
	prevDisk   map[string]diskCounter
	prevDiskAt time.Time
}

func newCollector() *collector { return &collector{src: systemSources()} }

// host — блок host по группам; недоступные значения пропускаются.
func (c *collector) host(ctx context.Context, groups, disks, exclude []string) *message.HostMetrics {
	has := func(g string) bool { return slices.Contains(groups, g) }
	src := c.src
	h := &message.HostMetrics{}
	if has(message.MetricsCPU) {
		if pct, err := src.cpu(ctx, false); err == nil && len(pct) > 0 {
			h.CPUPercent = &pct[0]
		}
		if pct, err := src.cpu(ctx, true); err == nil && len(pct) > 0 {
			h.CPUCores = make([]float64, len(pct))
			for i, p := range pct {
				h.CPUCores[i] = round1(p)
			}
		}
	}
	if has(message.MetricsLoad) {
		if avg, err := src.load(ctx); err == nil {
			h.Load1, h.Load5, h.Load15 = &avg.Load1, &avg.Load5, &avg.Load15
		}
	}
	if has(message.MetricsMemory) {
		if vm, err := src.memory(ctx); err == nil {
			h.MemUsedBytes, h.MemTotalBytes, h.MemAvailableBytes = &vm.Used, &vm.Total, &vm.Available
		}
	}
	if has(message.MetricsSwap) {
		if sw, err := src.swap(ctx); err == nil {
			h.SwapUsedBytes, h.SwapTotalBytes = &sw.Used, &sw.Total
		}
	}
	if has(message.MetricsDisk) {
		c.diskUsage(ctx, h, disks)
	}
	if has(message.MetricsDiskIO) {
		c.diskIO(ctx, h)
	} else {
		c.prevDisk, c.prevDiskAt = nil, time.Time{}
	}
	if has(message.MetricsNetwork) || has(message.MetricsInterfaces) {
		c.network(ctx, h, exclude, has(message.MetricsNetwork), has(message.MetricsInterfaces))
	} else {
		c.prevNet, c.prevNetAt = nil, time.Time{}
	}
	if has(message.MetricsConntrack) {
		h.Conntrack = c.procUint(procConntrackCount)
		h.ConntrackMax = c.procUint(procConntrackMax)
	}
	if has(message.MetricsSockets) {
		h.TCP = c.tcp(ctx)
	}
	if has(message.MetricsProcesses) {
		if pids, err := src.pids(ctx); err == nil && len(pids) > 0 {
			n := uint64(len(pids))
			h.Processes = &n
		}
		h.Threads = c.threads()
	}
	if has(message.MetricsFDs) {
		h.FDsOpen, h.FDsMax = c.fds()
	}
	if has(message.MetricsUptime) {
		if up, err := src.uptime(ctx); err == nil {
			h.UptimeSec = &up
		}
	}
	if has(message.MetricsTemperatures) {
		// Часть датчиков может не читаться: ошибка вместе с данными — берём данные.
		stats, _ := src.temperatures(ctx)
		h.Temperatures = Temperatures(stats)
	}
	if has(message.MetricsGPU) && src.gpus != nil {
		h.GPUs = src.gpus(ctx)
	}
	return h
}

// ─── Диски ─────────────────────────────────────────────────────────────

// pseudoFS — файловые системы, которые не диски.
var pseudoFS = map[string]bool{
	"tmpfs": true, "devtmpfs": true, "overlay": true, "squashfs": true, "proc": true, "sysfs": true,
	"cgroup": true, "cgroup2": true, "autofs": true, "devfs": true, "nullfs": true, "ramfs": true,
	"tracefs": true, "debugfs": true, "securityfs": true, "pstore": true, "bpf": true, "fusectl": true,
	"configfs": true, "mqueue": true, "hugetlbfs": true, "nsfs": true, "binfmt_misc": true,
	"devpts": true, "efivarfs": true, "rpc_pipefs": true, "fuse.lxcfs": true, "shm": true,
}

// RealMounts — точки монтирования реальных файловых систем: без pseudoFS и
// без повторов (bind-монтирования того же устройства).
func RealMounts(parts []disk.PartitionStat) []string {
	var out []string
	devices := map[string]bool{}
	for _, p := range parts {
		if pseudoFS[p.Fstype] || p.Mountpoint == "" || slices.Contains(out, p.Mountpoint) {
			continue
		}
		if p.Device != "" && p.Device != "none" {
			if devices[p.Device] {
				continue
			}
			devices[p.Device] = true
		}
		out = append(out, p.Mountpoint)
	}
	return out
}

// diskUsage — корень (diskUsedBytes, diskTotalBytes) и disks по telemetry.disks.
func (c *collector) diskUsage(ctx context.Context, h *message.HostMetrics, mounts []string) {
	if u, err := c.src.usage(ctx, "/"); err == nil {
		h.DiskUsedBytes, h.DiskTotalBytes = &u.Used, &u.Total
	}
	if len(mounts) == 1 && mounts[0] == disksAll {
		parts, err := c.src.partitions(ctx)
		if err != nil {
			return
		}
		mounts = RealMounts(parts)
	}
	for _, mount := range mounts {
		u, err := c.src.usage(ctx, mount)
		if err != nil || u.Total == 0 {
			continue
		}
		h.Disks = append(h.Disks, message.DiskMetrics{
			Mount: mount, UsedBytes: u.Used, TotalBytes: u.Total, InodesUsed: u.InodesUsed, InodesTotal: u.InodesTotal,
		})
	}
}

// diskCounter — счётчики чтения и записи диска.
type diskCounter struct{ readBytes, writeBytes, reads, writes uint64 }

// virtualDisks — префиксы блочных устройств, которые не физические диски
// (их операции уже посчитаны на дисках под ними).
var virtualDisks = []string{"loop", "ram", "zram", "dm-", "md", "nbd", "sr", "fd"}

// PhysicalDisk — устройство считается в diskio: целый диск (есть в block,
// если список известен), не виртуальное.
func PhysicalDisk(name string, block map[string]bool) bool {
	if name == "" || (block != nil && !block[name]) {
		return false
	}
	for _, p := range virtualDisks {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	return true
}

// diskIO — скорость чтения и записи по физическим дискам (по разнице счётчиков).
func (c *collector) diskIO(ctx context.Context, h *message.HostMetrics) {
	stats, err := c.src.diskIO(ctx)
	if err != nil || len(stats) == 0 {
		return
	}
	block, err := c.src.blockDevices()
	if err != nil {
		block = nil
	}
	cur := map[string]diskCounter{}
	for name, s := range stats {
		if PhysicalDisk(name, block) {
			cur[name] = diskCounter{readBytes: s.ReadBytes, writeBytes: s.WriteBytes, reads: s.ReadCount, writes: s.WriteCount}
		}
	}
	now := c.src.now()
	prev, prevAt := c.prevDisk, c.prevDiskAt
	c.prevDisk, c.prevDiskAt = cur, now
	if prevAt.IsZero() {
		return
	}
	if r, ok := diskRates(prev, cur, now.Sub(prevAt).Seconds()); ok {
		h.DiskReadBps, h.DiskWriteBps, h.DiskReadIops, h.DiskWriteIops = &r.readBytes, &r.writeBytes, &r.reads, &r.writes
	}
}

// diskRates — сумма по дискам в секунду; диск без прошлых или со сброшенными
// счётчиками пропускается. false — считать не по чему.
func diskRates(prev, cur map[string]diskCounter, secs float64) (diskCounter, bool) {
	var sum diskCounter
	if secs <= 0 {
		return sum, false
	}
	ok := false
	for name, d := range cur {
		p, found := prev[name]
		if !found || d.readBytes < p.readBytes || d.writeBytes < p.writeBytes || d.reads < p.reads || d.writes < p.writes {
			continue
		}
		ok = true
		sum.readBytes += d.readBytes - p.readBytes
		sum.writeBytes += d.writeBytes - p.writeBytes
		sum.reads += d.reads - p.reads
		sum.writes += d.writes - p.writes
	}
	per := func(v uint64) uint64 { return uint64(math.Round(float64(v) / secs)) }
	return diskCounter{per(sum.readBytes), per(sum.writeBytes), per(sum.reads), per(sum.writes)}, ok
}

// ─── Сеть ──────────────────────────────────────────────────────────────

// netCounter — счётчики интерфейса: байты, ошибки и отброшенные пакеты.
type netCounter struct{ rx, tx, errors, drops uint64 }

// network — сеть по интерфейсам и суммарно (по разнице счётчиков с прошлого
// сбора); интерфейсы из exclude не считаются.
func (c *collector) network(ctx context.Context, h *message.HostMetrics, exclude []string, total, ifaces bool) {
	stats, err := c.src.netIO(ctx)
	if err != nil || len(stats) == 0 {
		return
	}
	cur := map[string]netCounter{}
	for _, s := range stats {
		if Shown(s.Name, exclude) {
			cur[s.Name] = netCounter{rx: s.BytesRecv, tx: s.BytesSent, errors: s.Errin + s.Errout, drops: s.Dropin + s.Dropout}
		}
	}
	now := c.src.now()
	prev, prevAt := c.prevNet, c.prevNetAt
	c.prevNet, c.prevNetAt = cur, now
	if prevAt.IsZero() {
		return
	}
	list, sum := Rates(prev, cur, now.Sub(prevAt).Seconds())
	if len(list) == 0 {
		return
	}
	if total {
		h.NetRxBps, h.NetTxBps, h.NetErrors, h.NetDrops = &sum.RxBps, &sum.TxBps, &sum.Errors, &sum.Drops
	}
	if ifaces {
		h.Interfaces = list
	}
}

// Rates — сеть по интерфейсам (байт/с; ошибки и отброшенные — за интервал,
// по имени) и сумма; интерфейс без прошлых счётчиков или со сброшенными
// (перезапуск) пропускается.
func Rates(prev, cur map[string]netCounter, secs float64) ([]message.InterfaceMetrics, message.InterfaceMetrics) {
	var sum message.InterfaceMetrics
	if secs <= 0 {
		return nil, sum
	}
	var out []message.InterfaceMetrics
	for name, c := range cur {
		p, ok := prev[name]
		if !ok || c.rx < p.rx || c.tx < p.tx || c.errors < p.errors || c.drops < p.drops {
			continue
		}
		m := message.InterfaceMetrics{
			Name: name, RxBps: uint64(float64(c.rx-p.rx) / secs), TxBps: uint64(float64(c.tx-p.tx) / secs),
			Errors: c.errors - p.errors, Drops: c.drops - p.drops,
		}
		out = append(out, m)
		sum.RxBps, sum.TxBps = sum.RxBps+m.RxBps, sum.TxBps+m.TxBps
		sum.Errors, sum.Drops = sum.Errors+m.Errors, sum.Drops+m.Drops
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, sum
}

// Shown — интерфейс показывается в метриках: имя не начинается
// ни с одного из префиксов exclude.
func Shown(name string, exclude []string) bool {
	for _, p := range exclude {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	return name != ""
}

// tcp — TCP-соединения по состояниям: Linux — /proc/net/tcp и tcp6 (дёшево),
// иначе — список соединений системы.
func (c *collector) tcp(ctx context.Context) *message.TCPMetrics {
	t := &message.TCPMetrics{}
	found := false
	for _, name := range []string{procTCP, procTCP6} {
		if raw, err := c.src.readFile(name); err == nil {
			ParseProcTCP(raw, t)
			found = true
		}
	}
	if found {
		return t
	}
	if c.src.connections == nil {
		return nil
	}
	conns, err := c.src.connections(ctx)
	if err != nil {
		return nil
	}
	for _, cs := range conns {
		switch cs.Status {
		case "ESTABLISHED":
			t.Established++
		case "TIME_WAIT":
			t.TimeWait++
		case "CLOSE_WAIT":
			t.CloseWait++
		case "LISTEN":
			t.Listen++
		}
	}
	return t
}

// ParseProcTCP — добавляет к t соединения из /proc/net/tcp(6): состояние —
// четвёртое поле, шестнадцатеричный код.
func ParseProcTCP(raw []byte, t *message.TCPMetrics) {
	lines := bytes.Split(raw, []byte("\n"))
	for i, line := range lines {
		fields := strings.Fields(string(line))
		if i == 0 || len(fields) < 4 {
			continue // заголовок
		}
		switch strings.ToUpper(fields[3]) {
		case "01":
			t.Established++
		case "06":
			t.TimeWait++
		case "08":
			t.CloseWait++
		case "0A":
			t.Listen++
		}
	}
}

// ─── /proc ─────────────────────────────────────────────────────────────

// procUint — число из файла /proc; нет файла — nil.
func (c *collector) procUint(name string) *uint64 {
	raw, err := c.src.readFile(name)
	if err != nil {
		return nil
	}
	n, err := strconv.ParseUint(strings.TrimSpace(string(raw)), 10, 64)
	if err != nil {
		return nil
	}
	return &n
}

// threads — потоков на узле: /proc/loadavg, четвёртое поле «выполняются/всего».
func (c *collector) threads() *uint64 {
	raw, err := c.src.readFile(procLoadavg)
	if err != nil {
		return nil
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 4 {
		return nil
	}
	_, total, ok := strings.Cut(fields[3], "/")
	if !ok {
		return nil
	}
	n, err := strconv.ParseUint(total, 10, 64)
	if err != nil {
		return nil
	}
	return &n
}

// fds — открытые файлы и предел: /proc/sys/fs/file-nr «выделено свободно предел».
func (c *collector) fds() (open, limit *uint64) {
	raw, err := c.src.readFile(procFileNr)
	if err != nil {
		return nil, nil
	}
	fields := strings.Fields(string(raw))
	if len(fields) < 3 {
		return nil, nil
	}
	var v [3]uint64
	for i := range v {
		if v[i], err = strconv.ParseUint(fields[i], 10, 64); err != nil {
			return nil, nil
		}
	}
	o := v[0] - min(v[1], v[0])
	return &o, &v[2]
}

// ─── Температура ───────────────────────────────────────────────────────

// sensorName — имя датчика без управляющих символов: на macOS имена приходят
// с нулевыми байтами, а их не принимают многие хранилища JSON (jsonb в Postgres).
func sensorName(key string) string {
	return strings.TrimSpace(strings.Map(func(r rune) rune {
		if r < 0x20 || r == 0x7f {
			return -1
		}
		return r
	}, key))
}

// Temperatures — датчики (°C, по имени) и самая высокая; нет показаний — nil.
func Temperatures(stats []sensors.TemperatureStat) *message.TemperatureMetrics {
	var out []message.TemperatureSensor
	for _, s := range stats {
		name := sensorName(s.SensorKey)
		// Нули и заведомо неверные значения — датчик не подключён.
		if name == "" || s.Temperature <= 0 || s.Temperature > 200 {
			continue
		}
		out = append(out, message.TemperatureSensor{Name: name, C: round1(s.Temperature)})
	}
	if len(out) == 0 {
		return nil
	}
	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	t := &message.TemperatureMetrics{Sensors: out}
	for _, s := range out {
		t.MaxC = max(t.MaxC, s.C)
	}
	return t
}

func round1(v float64) float64 { return math.Round(v*10) / 10 }
