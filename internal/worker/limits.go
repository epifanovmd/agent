//go:build unix

package worker

import (
	"fmt"
	"os/exec"
	"os/user"
	"strconv"
	"syscall"

	"github.com/epifanovmd/agent/internal/cgroup"
	"github.com/epifanovmd/agent/internal/config"
)

// systemCgroups — группа агента в cgroup v2 (подменяется в тестах подставным корнем).
var systemCgroups = cgroup.System

// workerGroup — подгруппа cgroup с ограничениями воркера; nil — ограничений
// нет или их не применить (предупреждение — один раз при подготовке и при
// ошибке подгруппы).
func (s *Supervisor) workerGroup(spec config.Worker) *cgroup.Group {
	if spec.Limits.Empty() {
		return nil
	}
	s.cgOnce.Do(func() {
		m, err := systemCgroups(s.log)
		if err != nil {
			s.log.Warn("ограничения воркеров не применены: нужна cgroup v2 и Delegate=yes в службе агента", "err", err)
			return
		}
		s.log.Info("ограничения воркеров: cgroup v2", "dir", m.Dir())
		s.cg = m
	})
	if s.cg == nil {
		return nil
	}
	memory, _ := config.ParseMemory(spec.Limits.Memory)
	quota, _ := config.ParseCPU(spec.Limits.CPU)
	g, err := s.cg.Worker(spec.Name, cgroup.Limits{
		MemoryMax: memory, CPUQuota: quota, CPUPeriod: config.CPUPeriod, PidsMax: int64(spec.Limits.Pids),
	})
	if err != nil {
		s.log.Warn("ограничения воркера не применены", "worker", spec.Name, "err", err)
		return nil
	}
	return g
}

// runAs — запуск воркера от пользователя name: uid, gid, дополнительные
// группы; HOME, USER, LOGNAME — его (настройки env важнее).
func runAs(cmd *exec.Cmd, name string) error {
	u, err := user.Lookup(name)
	if err != nil {
		return fmt.Errorf("пользователь %q: %w", name, err)
	}
	uid, err1 := strconv.ParseUint(u.Uid, 10, 32)
	gid, err2 := strconv.ParseUint(u.Gid, 10, 32)
	if err1 != nil || err2 != nil {
		return fmt.Errorf("пользователь %q: uid/gid не числа", name)
	}
	cred := &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}
	if ids, err := u.GroupIds(); err == nil {
		for _, id := range ids {
			if g, err := strconv.ParseUint(id, 10, 32); err == nil {
				cred.Groups = append(cred.Groups, uint32(g))
			}
		}
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Credential = cred
	cmd.Env = append(cmd.Env, "HOME="+u.HomeDir, "USER="+u.Username, "LOGNAME="+u.Username)
	return nil
}
