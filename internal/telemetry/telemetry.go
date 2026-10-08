// Package telemetry — сведения о хосте для hello и метрики для metrics:
// группы метрик узла (message.MetricGroups), GPU (nvidia-smi), каналы воркеров.
package telemetry

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"os/exec"
	goruntime "runtime"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// HostInfo — сведения о хосте для hello.
func HostInfo(ctx context.Context) message.Host {
	h := message.Host{OS: goruntime.GOOS, Arch: goruntime.GOARCH, CPUs: goruntime.NumCPU()}
	if info, err := host.InfoWithContext(ctx); err == nil {
		h.Hostname = info.Hostname
		h.Platform = strings.TrimSpace(info.Platform + " " + info.PlatformVersion)
		h.Kernel = info.KernelVersion
	}
	if vm, err := mem.VirtualMemoryWithContext(ctx); err == nil {
		h.MemoryBytes = vm.Total
	}
	return h
}

// Options — что собирать (меняется на ходу — Configure).
type Options struct {
	// Metrics — группы метрик узла (message.MetricGroups) по настройке агента;
	// пусто — без блока host (если сервер не добавит группы — SetExtra).
	Metrics []string
	// Disks — точки монтирования для host.disks; ["all"] — все реальные
	// файловые системы.
	Disks []string
	// GPU — опрашивать nvidia-smi (если есть в PATH).
	GPU bool
	// Exclude — префиксы имён интерфейсов, которые не показываются.
	Exclude []string
}

// Collector — метрики хоста и GPU.
type Collector struct {
	log *slog.Logger
	src sources

	mu      sync.Mutex
	metrics []string // группы по настройке агента
	extra   []string // группы из подписки сервера (subscription.metrics)
	disks   []string
	gpu     bool
	exclude []string
	// prevNet, prevDisk — счётчики прошлого сбора (скорость — по разнице).
	prevNet    map[string]netCounter
	prevNetAt  time.Time
	prevDisk   map[string]diskCounter
	prevDiskAt time.Time
	// channels — каналы воркеров: объявленные и последние данные.
	declared []string
	latest   map[string]json.RawMessage
}

// New — сборщик метрик.
func New(opts Options, log *slog.Logger) *Collector {
	c := &Collector{log: log, src: systemSources()}
	c.Configure(opts)
	return c
}

// Configure — что собирать; действует со следующего сбора. Группы сервера
// (SetExtra) сохраняются.
func (c *Collector) Configure(opts Options) {
	gpu := opts.GPU
	if gpu {
		if _, err := exec.LookPath("nvidia-smi"); err != nil {
			gpu = false
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	c.metrics, c.disks = slices.Clone(opts.Metrics), slices.Clone(opts.Disks)
	c.gpu, c.exclude = gpu, slices.Clone(opts.Exclude)
}

// SetExtra — группы метрик из подписки сервера (subscription.metrics) в дополнение к
// настройке агента; неизвестные пропускаются. true — изменился набор каналов
// (появился или пропал блок host).
func (c *Collector) SetExtra(groups []string) bool {
	var known []string
	for _, g := range groups {
		if !slices.Contains(message.MetricGroups, g) {
			c.log.Warn("telemetry: неизвестная группа метрик от сервера", "group", g)
			continue
		}
		if !slices.Contains(known, g) {
			known = append(known, g)
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	before := len(c.groupsLocked()) > 0
	c.extra = known
	return before != (len(c.groupsLocked()) > 0)
}

// Groups — действующие группы метрик узла: настройка агента и группы сервера.
func (c *Collector) Groups() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.groupsLocked()
}

func (c *Collector) groupsLocked() []string {
	out := slices.Clone(c.metrics)
	for _, g := range c.extra {
		if !slices.Contains(out, g) {
			out = append(out, g)
		}
	}
	return out
}

// Channels — каналы телеметрии для hello: хост, GPU и каналы воркеров.
func (c *Collector) Channels() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	if len(c.groupsLocked()) > 0 {
		out = append(out, "host")
	}
	if c.gpu {
		out = append(out, "gpu")
	}
	return append(out, c.declared...)
}

// Undeclare — канал воркера убран (воркер удалён из настроек).
func (c *Collector) Undeclare(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.declared = slices.DeleteFunc(c.declared, func(ch string) bool { return ch == channel })
	delete(c.latest, channel)
}

func (c *Collector) options() (gpu bool, exclude []string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gpu, c.exclude
}

// Declare — канал воркера (в hello и capabilities).
func (c *Collector) Declare(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if !slices.Contains(c.declared, channel) {
		c.declared = append(c.declared, channel)
	}
}

// Has — канал объявлен (собственные каналы агента — тоже).
func (c *Collector) Has(channel string) bool {
	return slices.Contains(c.Channels(), channel)
}

// Report — последние данные канала воркера; уйдут в ближайший metrics.
func (c *Collector) Report(channel string, data json.RawMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.latest == nil {
		c.latest = map[string]json.RawMessage{}
	}
	c.latest[channel] = data
}

// Drop — воркер канала завершился: его данные сбрасываются.
func (c *Collector) Drop(channel string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.latest, channel)
}

// Collect — снимок метрик; недоступные значения пропускаются.
func (c *Collector) Collect(ctx context.Context) message.Metrics {
	m := message.Metrics{CollectedAt: c.src.now().UnixMilli()}
	c.mu.Lock()
	groups := c.groupsLocked()
	disks, gpu, exclude := slices.Clone(c.disks), c.gpu, slices.Clone(c.exclude)
	c.mu.Unlock()
	if len(groups) > 0 {
		m.Host = c.hostMetrics(ctx, groups, disks, exclude)
	} else {
		c.resetRates()
	}
	if gpu {
		m.GPUs = c.gpus(ctx)
	}
	c.mu.Lock()
	if len(c.latest) > 0 {
		m.Channels = maps.Clone(c.latest)
	}
	c.mu.Unlock()
	return m
}

// Shown — интерфейс показывается в метриках и inventory: имя не начинается
// ни с одного из префиксов exclude.
func Shown(name string, exclude []string) bool {
	for _, p := range exclude {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	return name != ""
}

func (c *Collector) gpus(ctx context.Context) []message.GPUMetrics {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "nvidia-smi",
		"--query-gpu=index,name,utilization.gpu,memory.used,memory.total,temperature.gpu",
		"--format=csv,noheader,nounits").Output()
	if err != nil {
		c.log.Debug("telemetry: nvidia-smi", "err", err)
		return nil
	}
	return ParseNvidiaSMI(out)
}

// ParseNvidiaSMI — строки CSV nvidia-smi в метрики (память — МиБ → байты).
func ParseNvidiaSMI(out []byte) []message.GPUMetrics {
	var gpus []message.GPUMetrics
	scanner := bufio.NewScanner(bytes.NewReader(out))
	for scanner.Scan() {
		fields := strings.Split(scanner.Text(), ",")
		if len(fields) < 6 {
			continue
		}
		for i := range fields {
			fields[i] = strings.TrimSpace(fields[i])
		}
		index, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		g := message.GPUMetrics{Index: index, Name: fields[1]}
		if v, err := strconv.ParseFloat(fields[2], 64); err == nil {
			g.UtilPercent = &v
		}
		if v, err := strconv.ParseUint(fields[3], 10, 64); err == nil {
			b := v << 20
			g.MemUsedBytes = &b
		}
		if v, err := strconv.ParseUint(fields[4], 10, 64); err == nil {
			b := v << 20
			g.MemTotalBytes = &b
		}
		if v, err := strconv.ParseFloat(fields[5], 64); err == nil {
			g.TemperatureC = &v
		}
		gpus = append(gpus, g)
	}
	return gpus
}
