//go:build linux

package worker

import (
	"os/exec"
	"syscall"
)

// useCgroupFD — запустить процесс сразу в группе dir (clone3 с
// CLONE_INTO_CGROUP): потомки не успевают появиться вне неё. release —
// закрыть дескриптор после запуска; false — группу не открыть.
func useCgroupFD(cmd *exec.Cmd, dir string) (release func(), ok bool) {
	fd, err := syscall.Open(dir, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_CLOEXEC, 0)
	if err != nil {
		return nil, false
	}
	cmd.SysProcAttr.UseCgroupFD = true
	cmd.SysProcAttr.CgroupFD = fd
	return func() { _ = syscall.Close(fd) }, true
}
