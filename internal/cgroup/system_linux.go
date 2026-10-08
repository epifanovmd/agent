package cgroup

import (
	"fmt"
	"log/slog"
	"os"
	"syscall"
)

// System — группа агента в cgroup v2 этой машины (см. Detect). Группа должна
// быть отдана агенту (Delegate=yes в службе), иначе агент перенёс бы чужие
// процессы (например, сеанс пользователя при ручном запуске).
func System(log *slog.Logger) (*Manager, error) {
	raw, err := os.ReadFile("/proc/self/cgroup")
	if err != nil {
		return nil, err
	}
	dir, err := groupDir(Root, raw)
	if err != nil {
		return nil, err
	}
	if !delegated(dir) {
		return nil, fmt.Errorf("группа %s не делегирована агенту (Delegate=yes в службе)", dir)
	}
	return Detect(Root, raw, log)
}

// delegated — systemd отдал группу процессу: метка trusted.delegate или
// user.delegate (systemd ставит её при Delegate=yes) либо каталог группы
// принадлежит этому (не root) пользователю.
func delegated(dir string) bool {
	for _, attr := range []string{"trusted.delegate", "user.delegate"} {
		buf := make([]byte, 8)
		if n, err := syscall.Getxattr(dir, attr, buf); err == nil && string(buf[:n]) == "1" {
			return true
		}
	}
	info, err := os.Stat(dir)
	if err != nil {
		return false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	uid := os.Geteuid()
	return ok && uid != 0 && int(st.Uid) == uid
}
