//go:build darwin

package worker

import (
	"fmt"

	"golang.org/x/sys/unix"
)

// sZomb — состояние «зомби» процесса macOS (p_stat).
const sZomb = 5

// procStart — время запуска процесса pid по данным системы (kern.proc.pid) и
// жив ли он (зомби — нет). Вместе с pid оно отличает процесс воркера от
// другого, получившего тот же pid.
func procStart(pid int) (string, bool) {
	kp, err := unix.SysctlKinfoProc("kern.proc.pid", pid)
	if err != nil || int(kp.Proc.P_pid) != pid || kp.Proc.P_stat == sZomb {
		return "", false
	}
	t := kp.Proc.P_starttime
	return fmt.Sprintf("%d.%06d", t.Sec, t.Usec), true
}
