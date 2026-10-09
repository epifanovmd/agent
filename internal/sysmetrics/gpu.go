package sysmetrics

import (
	"bufio"
	"bytes"
	"context"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

// nvidiaSMI — программа опроса видеокарт NVIDIA.
const nvidiaSMI = "nvidia-smi"

// gpuTimeout — сколько ждать nvidia-smi.
const gpuTimeout = 5 * time.Second

// gpuSource — опрос видеокарт, если nvidia-smi есть в PATH; иначе nil.
func gpuSource() func(context.Context) []message.GPUMetrics {
	if _, err := exec.LookPath(nvidiaSMI); err != nil {
		return nil
	}
	return gpus
}

// queryGPU — CSV nvidia-smi по полям fields, без заголовка и единиц.
func queryGPU(ctx context.Context, fields string) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, gpuTimeout)
	defer cancel()
	return exec.CommandContext(ctx, nvidiaSMI, "--query-gpu="+fields, "--format=csv,noheader,nounits").Output()
}

// gpus — загрузка, память и температура видеокарт; ошибка nvidia-smi — nil.
func gpus(ctx context.Context) []message.GPUMetrics {
	out, err := queryGPU(ctx, "index,name,utilization.gpu,memory.used,memory.total,temperature.gpu")
	if err != nil {
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
