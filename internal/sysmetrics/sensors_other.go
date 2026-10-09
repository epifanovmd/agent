//go:build !darwin

package sysmetrics

import (
	"context"
	"errors"

	"github.com/shirou/gopsutil/v4/sensors"
)

// temperatures — показания датчиков прямо в процессе воркера.
func temperatures(ctx context.Context) ([]sensors.TemperatureStat, error) {
	return sensors.TemperaturesWithContext(ctx)
}

// PrintSensors — на этой системе датчики читаются в процессе воркера.
func PrintSensors(context.Context) error {
	return errors.New("команда нужна только на macOS")
}
