//go:build unix && !linux

package worker

import "os/exec"

// useCgroupFD — вне Linux cgroup нет.
func useCgroupFD(*exec.Cmd, string) (func(), bool) { return nil, false }
