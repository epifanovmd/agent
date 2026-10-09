//go:build linux

package worker

import (
	"os"
	"strconv"
	"strings"
)

// procStart — время запуска процесса pid по данным системы (поле starttime
// /proc/<pid>/stat, такты с загрузки) и жив ли он (зомби — нет). Вместе с
// pid оно отличает процесс воркера от другого, получившего тот же pid.
func procStart(pid int) (string, bool) {
	raw, err := os.ReadFile("/proc/" + strconv.Itoa(pid) + "/stat")
	if err != nil {
		return "", false
	}
	text := string(raw)
	i := strings.LastIndexByte(text, ')')
	if i < 0 {
		return "", false
	}
	fields := strings.Fields(text[i+1:])
	if len(fields) < 20 {
		return "", false
	}
	if fields[0] == "Z" || fields[0] == "X" {
		return "", false
	}
	return fields[19], true
}
