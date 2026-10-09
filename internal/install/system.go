package install

import (
	"context"
	"fmt"
	"os"
	"strings"
)

// manager — менеджер пакетов узла (имя для --packages-<имя>, программа);
// пусто — не найден.
func (s *System) manager() (name, program string) {
	for _, m := range Managers {
		if s.has(m.Program) {
			return m.Name, m.Program
		}
	}
	return "", ""
}

// pkgInstalled — пакет уже стоит.
func (s *System) pkgInstalled(ctx context.Context, manager, pkg string) bool {
	switch manager {
	case "apt":
		out, err := s.Output(ctx, "dpkg-query", "-W", "-f=${Status}", pkg)
		return err == nil && strings.Contains(out, "install ok installed")
	case "dnf", "yum", "zypper":
		_, err := s.Output(ctx, "rpm", "-q", pkg)
		return err == nil
	case "apk":
		_, err := s.Output(ctx, "apk", "info", "-e", pkg)
		return err == nil
	}
	return false
}

// pkgCommands — команды установки (remove=false) или удаления пакетов.
func pkgCommands(manager string, pkgs []string, remove bool) []Cmd {
	deb := []string{"DEBIAN_FRONTEND=noninteractive"}
	if remove {
		switch manager {
		case "apt":
			return []Cmd{{Args: append([]string{"apt-get", "remove", "-y", "-qq"}, pkgs...), Env: deb}}
		case "dnf", "yum":
			return []Cmd{{Args: append([]string{manager, "remove", "-y", "-q"}, pkgs...)}}
		case "apk":
			return []Cmd{{Args: append([]string{"apk", "del"}, pkgs...)}}
		case "zypper":
			return []Cmd{{Args: append([]string{"zypper", "--non-interactive", "remove"}, pkgs...)}}
		}
		return nil
	}
	switch manager {
	case "apt":
		return []Cmd{
			{Args: []string{"apt-get", "update", "-qq"}, Env: deb},
			{Args: append([]string{"apt-get", "install", "-y", "-qq"}, pkgs...), Env: deb},
		}
	case "dnf", "yum":
		return []Cmd{{Args: append([]string{manager, "install", "-y", "-q"}, pkgs...)}}
	case "apk":
		return []Cmd{{Args: append([]string{"apk", "add", "--no-cache"}, pkgs...)}}
	case "zypper":
		return []Cmd{{Args: append([]string{"zypper", "--non-interactive", "install"}, pkgs...)}}
	}
	return nil
}

// installPackages — системные пакеты для воркеров. Каких не было до
// установки — в журнал: удаление с --purge уберёт только их.
func installPackages(ctx context.Context, s *System, j *journal, o Options) error {
	manager, _ := s.manager()
	pkgs := o.Packages
	if own := o.PackagesBy[manager]; manager != "" && len(own) > 0 {
		pkgs = own
	}
	if len(pkgs) == 0 {
		return nil
	}
	if manager == "" {
		return fmt.Errorf("--packages: не найден менеджер пакетов (apt, dnf, yum, apk, zypper)")
	}
	var fresh []string
	for _, p := range pkgs {
		if !s.pkgInstalled(ctx, manager, p) {
			fresh = append(fresh, p)
		}
	}
	s.say("Ставятся пакеты: %s", strings.Join(pkgs, " "))
	for _, c := range pkgCommands(manager, pkgs, false) {
		c.Quiet = true
		if err := s.Run(ctx, c); err != nil {
			return fmt.Errorf("--packages: не установлены %s: %w", strings.Join(pkgs, " "), err)
		}
	}
	for _, p := range fresh {
		j.add("package", p)
	}
	return j.save()
}

// applySysctls — параметры ядра для воркеров: файл агента в /etc/sysctl.d
// (заданный ключ заменяет прежнее значение), применяются сразу; в журнал —
// что задано и значение до установки (один раз).
func applySysctls(ctx context.Context, s *System, j *journal, kvs []string) error {
	if len(kvs) == 0 {
		return nil
	}
	path := s.p(SysctlFile)
	raw, _ := os.ReadFile(path)
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(raw) == 0 {
		lines = []string{"# Параметры ядра для воркеров агента (agent install --sysctl)"}
	}
	for _, kv := range kvs {
		key, _, _ := strings.Cut(kv, "=")
		if !hasPrefixValue(j.values("sysctl-prev"), key) && s.has("sysctl") {
			if prev, err := s.Output(ctx, "sysctl", "-n", key); err == nil {
				j.lines = append(j.lines, "sysctl-prev "+key+"="+strings.TrimSpace(prev))
			}
		}
		kept := lines[:0]
		for _, l := range lines {
			if !strings.HasPrefix(l, key+"=") {
				kept = append(kept, l)
			}
		}
		lines = append(kept, kv)
		j.drop(func(k, v string) bool { return k == "sysctl" && strings.HasPrefix(v, key+"=") })
		j.lines = append(j.lines, "sysctl "+kv)
	}
	if err := writeFileAtomic(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		return err
	}
	if err := j.save(); err != nil {
		return err
	}
	sysctlApply(ctx, s)
	return nil
}

// sysctlApply — применить файл агента сейчас (нет файла — все файлы заново).
func sysctlApply(ctx context.Context, s *System) {
	if !s.has("sysctl") {
		s.warn("sysctl не найден — параметры ядра вступят в силу при загрузке")
		return
	}
	if isFile(s.p(SysctlFile)) {
		if err := s.Run(ctx, Cmd{Args: []string{"sysctl", "-q", "-p", SysctlFile}, Quiet: true}); err != nil {
			s.warn("параметры ядра не применены сейчас — применятся при загрузке")
		}
		return
	}
	_ = s.Run(ctx, Cmd{Args: []string{"sysctl", "-q", "--system"}, Quiet: true})
}

func hasPrefixValue(values []string, key string) bool {
	for _, v := range values {
		if strings.HasPrefix(v, key+"=") {
			return true
		}
	}
	return false
}
