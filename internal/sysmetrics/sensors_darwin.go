//go:build darwin

package sysmetrics

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"time"

	"github.com/shirou/gopsutil/v4/sensors"
)

// sensorsTimeout — сколько ждать дочерний процесс с показаниями датчиков.
const sensorsTimeout = 5 * time.Second

// temperatures — показания датчиков из дочернего процесса (`agent sensors`).
// Чтение датчиков через IOKit (gopsutil без cgo) повреждает память процесса,
// поэтому в процессе воркера оно не выполняется.
func temperatures(ctx context.Context) ([]sensors.TemperatureStat, error) {
	exe, err := os.Executable()
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(ctx, sensorsTimeout)
	defer cancel()
	out, err := exec.CommandContext(ctx, exe, SensorsCommand).Output()
	if err != nil {
		return nil, fmt.Errorf("датчики: %w", err)
	}
	var stats []sensors.TemperatureStat
	if err := json.Unmarshal(out, &stats); err != nil {
		return nil, fmt.Errorf("датчики: %w", err)
	}
	return stats, nil
}

// PrintSensors — тело команды `agent sensors`: показания датчиков в stdout.
func PrintSensors(ctx context.Context) error {
	stats, err := sensors.TemperaturesWithContext(ctx)
	if err != nil && len(stats) == 0 {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(stats)
}
