package telemetry

import (
	"context"
	"encoding/json"
	"os/exec"
	goruntime "runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v4/cpu"
	"github.com/shirou/gopsutil/v4/disk"
	"github.com/shirou/gopsutil/v4/host"
	"github.com/shirou/gopsutil/v4/mem"
	"github.com/shirou/gopsutil/v4/net"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// maxPorts — портов в inventory не больше (узлы с тысячами соединений).
const maxPorts = 256

// Inventory — сведения об узле, меняющиеся редко; недоступное пропускается.
func (c *Collector) Inventory(ctx context.Context) message.Inventory {
	inv := message.Inventory{CollectedAt: time.Now().UnixMilli()}
	if info, err := host.InfoWithContext(ctx); err == nil {
		inv.OS = &message.InventoryOS{
			Hostname: info.Hostname, Platform: strings.TrimSpace(info.Platform + " " + info.PlatformVersion),
			Kernel: info.KernelVersion, Arch: goruntime.GOARCH, Virtualization: info.VirtualizationSystem,
		}
	}
	cpuInfo := &message.InventoryCPU{}
	if infos, err := cpu.InfoWithContext(ctx); err == nil && len(infos) > 0 {
		cpuInfo.Model = strings.TrimSpace(infos[0].ModelName)
	}
	cpuInfo.Cores, _ = cpu.CountsWithContext(ctx, false)
	cpuInfo.Threads, _ = cpu.CountsWithContext(ctx, true)
	inv.CPU = cpuInfo
	if vm, err := mem.VirtualMemoryWithContext(ctx); err == nil {
		inv.MemoryBytes = vm.Total
	}
	if parts, err := disk.PartitionsWithContext(ctx, false); err == nil {
		seen := map[string]bool{}
		for _, p := range parts {
			if pseudoFS[p.Fstype] || seen[p.Mountpoint] {
				continue
			}
			seen[p.Mountpoint] = true
			if u, err := disk.UsageWithContext(ctx, p.Mountpoint); err == nil && u.Total > 0 {
				inv.Disks = append(inv.Disks, message.InventoryDisk{Mount: p.Mountpoint, FS: p.Fstype, TotalBytes: u.Total})
			}
		}
	}
	gpu, exclude := c.options()
	if ifaces, err := net.InterfacesWithContext(ctx); err == nil {
		for _, i := range ifaces {
			if !Shown(i.Name, exclude) {
				continue
			}
			n := message.InventoryNetwork{Name: i.Name, MAC: i.HardwareAddr}
			for _, a := range i.Addrs {
				n.Addresses = append(n.Addresses, a.Addr)
			}
			inv.Interfaces = append(inv.Interfaces, n)
		}
	}
	if gpu {
		inv.GPUs = gpuInventory(ctx)
	}
	inv.Ports = listening(ctx)
	return inv
}

// SameInventory — сведения не изменились (момент сбора не считается).
func SameInventory(a, b message.Inventory) bool {
	a.CollectedAt, b.CollectedAt = 0, 0
	x, _ := json.Marshal(a)
	y, _ := json.Marshal(b)
	return string(x) == string(y)
}

// listening — слушающие порты TCP и занятые UDP (без прав root — свои и что видно).
func listening(ctx context.Context) *message.InventoryPorts {
	ports := &message.InventoryPorts{TCP: []int{}, UDP: []int{}}
	collect := func(kind string, keep func(net.ConnectionStat) bool) []int {
		conns, err := net.ConnectionsWithContext(ctx, kind)
		if err != nil {
			return []int{}
		}
		set := map[int]bool{}
		for _, cs := range conns {
			if keep(cs) && cs.Laddr.Port > 0 {
				set[int(cs.Laddr.Port)] = true
			}
		}
		out := make([]int, 0, len(set))
		for p := range set {
			out = append(out, p)
		}
		slices.Sort(out)
		if len(out) > maxPorts {
			out = out[:maxPorts]
		}
		return out
	}
	ports.TCP = collect("tcp", func(cs net.ConnectionStat) bool { return cs.Status == "LISTEN" })
	ports.UDP = collect("udp", func(cs net.ConnectionStat) bool { return cs.Raddr.Port == 0 })
	return ports
}

// gpuInventory — модели и объём памяти GPU (nvidia-smi).
func gpuInventory(ctx context.Context) []message.InventoryGPU {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	out, err := exec.CommandContext(ctx, "nvidia-smi", "--query-gpu=index,name,memory.total",
		"--format=csv,noheader,nounits").Output()
	if err != nil {
		return nil
	}
	var gpus []message.InventoryGPU
	for _, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		f := strings.Split(line, ",")
		if len(f) < 3 {
			continue
		}
		idx, err := strconv.Atoi(strings.TrimSpace(f[0]))
		if err != nil {
			continue
		}
		g := message.InventoryGPU{Index: idx, Name: strings.TrimSpace(f[1])}
		if mib, err := strconv.ParseUint(strings.TrimSpace(f[2]), 10, 64); err == nil {
			g.MemoryBytes = mib << 20
		}
		gpus = append(gpus, g)
	}
	return gpus
}
