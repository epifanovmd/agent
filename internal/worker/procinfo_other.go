//go:build unix && !linux && !darwin

package worker

import "syscall"

// procStart — жив ли процесс pid; времени запуска на этой системе агент не
// знает (подхват опирается только на pid и ответ сокета).
func procStart(pid int) (string, bool) {
	return "", syscall.Kill(pid, 0) == nil
}
