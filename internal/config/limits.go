package config

import (
	"errors"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
)

// geteuid — подменяется в тестах (проверка workers[].user).
var geteuid = os.Geteuid

// Limits — ограничения ресурсов воркера: memory ("512M", "2G", байты),
// cpu ("50%" — половина ядра, "200%" или "2" — два ядра), pids (число
// процессов и потоков). Пусто / 0 — без ограничения.
type Limits struct {
	Memory string `yaml:"memory"`
	CPU    string `yaml:"cpu"`
	Pids   int    `yaml:"pids"`
}

// Empty — ограничений нет.
func (l Limits) Empty() bool { return l.Memory == "" && l.CPU == "" && l.Pids == 0 }

// Validate — значения разбираются.
func (l Limits) Validate() error {
	var errs []error
	if _, err := ParseMemory(l.Memory); err != nil {
		errs = append(errs, err)
	}
	if _, err := ParseCPU(l.CPU); err != nil {
		errs = append(errs, err)
	}
	if l.Pids < 0 {
		errs = append(errs, fmt.Errorf("pids: не меньше 0 (0 — без ограничения), а не %d", l.Pids))
	}
	return errors.Join(errs...)
}

// ParseMemory — объём памяти в байтах: число с необязательной единицей
// K, M, G, T (степени 1024; допустимы KB/KiB и т. п.). "" — 0 (без ограничения).
func ParseMemory(s string) (int64, error) {
	v := strings.TrimSpace(s)
	if v == "" {
		return 0, nil
	}
	num := strings.TrimRight(v, "KMGTkmgtiIbB")
	unit := strings.ToUpper(v[len(num):])
	unit = strings.TrimSuffix(strings.TrimSuffix(unit, "B"), "I")
	mult := map[string]float64{"": 1, "K": 1 << 10, "M": 1 << 20, "G": 1 << 30, "T": 1 << 40}[unit]
	n, err := strconv.ParseFloat(strings.TrimSpace(num), 64)
	if err != nil || mult == 0 || !(n > 0) || math.IsInf(n, 0) || n*mult > math.MaxInt64/2 {
		return 0, fmt.Errorf("memory: объём вида 512M, 2G или в байтах, а не %q", s)
	}
	return int64(n * mult), nil
}

// CPUPeriod — период cpu.max, мкс.
const CPUPeriod = 100_000

// ParseCPU — квота процессорного времени на CPUPeriod, мкс: "50%" — 50000,
// "1.5" (ядра) — 150000. "" — 0 (без ограничения).
func ParseCPU(s string) (int64, error) {
	v := strings.TrimSpace(s)
	if v == "" {
		return 0, nil
	}
	cores, percent := strings.CutSuffix(v, "%")
	n, err := strconv.ParseFloat(strings.TrimSpace(cores), 64)
	if percent {
		n /= 100
	}
	quota := n * CPUPeriod
	if err != nil || !(quota >= 1000 && quota <= 1e12) {
		return 0, fmt.Errorf("cpu: доля процессора вида 50%% или число ядер (от 1%%), а не %q", s)
	}
	return int64(quota), nil
}
