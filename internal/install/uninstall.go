package install

import (
	"context"
	"os"
	"strings"

	"github.com/epifanovmd/agent/internal/config"
)

// Uninstall — удалить агента: воркеры убирают за собой (agent cleanup),
// затем служба, программа, файл параметров ядра (с возвратом прежних
// значений). purge — ещё настройки, данные, пакеты и пользователь службы,
// если их создала установка (root и пользователей, которые уже были, не
// трогает). self — запущенная программа агента: уборку делает она, если
// установленной уже нет.
func Uninstall(ctx context.Context, s *System, purge bool, self string) error {
	if err := s.preflight(false); err != nil {
		return err
	}
	// Служба — пользователь, от которого работали агент и воркеры.
	runAs := ""
	if raw, err := os.ReadFile(s.p(UnitFile)); err == nil {
		for _, l := range strings.Split(string(raw), "\n") {
			if v, ok := strings.CutPrefix(l, "User="); ok {
				runAs = strings.TrimSpace(v)
				break
			}
		}
	}
	j, err := loadJournal(s.p(JournalFile))
	if err != nil {
		return err
	}
	if runAs == "" {
		runAs = j.last("service-user")
	}
	if s.has("systemctl") {
		_ = s.Run(ctx, Cmd{Args: []string{"systemctl", "disable", "--now", Service}, Quiet: true})
	}
	// Воркеры убирают за собой (без сервера); ошибка — предупреждение, удаление продолжается.
	bin := s.p(Binary)
	if !isFile(bin) {
		bin = self
	}
	if bin != "" && isFile(bin) && isFile(s.p(ConfigFile)) {
		var env []string
		if f, err := os.Open(s.p(EnvFile)); err == nil {
			vars, _ := config.ParseEnv(f)
			f.Close()
			for _, kv := range vars {
				env = append(env, kv[0]+"="+kv[1])
			}
		}
		if runAs == "root" {
			runAs = ""
		}
		if err := s.Run(ctx, Cmd{Args: []string{bin, "cleanup", "-config", ConfigFile}, Env: env, User: runAs}); err != nil {
			s.warn("воркеры убрали за собой не всё (agent cleanup) — удаление продолжается")
		}
	}
	_ = os.Remove(s.p(UnitFile))
	if s.has("systemctl") {
		_ = s.Run(ctx, Cmd{Args: []string{"systemctl", "daemon-reload"}, Quiet: true})
	}
	if target, err := os.Readlink(s.p(Link)); err == nil && target == Binary {
		_ = os.Remove(s.p(Link))
	}
	if err := os.RemoveAll(s.p(OptDir)); err != nil {
		return err
	}
	if isFile(s.p(SysctlFile)) {
		_ = os.Remove(s.p(SysctlFile))
		sysctlApply(ctx, s)
	}
	// Значения параметров ядра — как были до установки.
	for _, kv := range j.values("sysctl-prev") {
		if err := s.Run(ctx, Cmd{Args: []string{"sysctl", "-q", "-w", kv}, Quiet: true}); err != nil {
			s.warn("прежнее значение не возвращено: %s", kv)
		}
	}
	j.drop(func(k, _ string) bool { return k == "sysctl" || k == "sysctl-prev" })
	if isDir(s.p(EtcDir)) {
		if err := j.save(); err != nil {
			return err
		}
	}
	if !purge {
		s.say("Агент удалён. Настройки (%s) и данные (%s) оставлены: повторная установка продолжит с ними; удалить всё — agent uninstall --purge.", EtcDir, DataDir)
		return nil
	}
	if pkgs := j.values("package"); len(pkgs) > 0 {
		manager, _ := s.manager()
		s.say("Удаляются пакеты, поставленные установкой: %s", strings.Join(pkgs, " "))
		cmds := pkgCommands(manager, pkgs, true)
		failed := len(cmds) == 0
		for _, c := range cmds {
			c.Quiet = true
			if s.Run(ctx, c) != nil {
				failed = true
			}
		}
		if failed {
			s.warn("пакеты не удалены: %s", strings.Join(pkgs, " "))
		}
	}
	users := j.values("user")
	if err := os.RemoveAll(s.p(EtcDir)); err != nil {
		return err
	}
	if err := os.RemoveAll(s.p(DataDir)); err != nil {
		return err
	}
	for _, u := range users {
		if u == "root" {
			continue
		}
		if _, err := s.Output(ctx, "id", "-u", u); err != nil {
			continue
		}
		if err := s.Run(ctx, Cmd{Args: []string{"userdel", u}}); err != nil {
			s.warn("пользователь %s не удалён", u)
		}
	}
	s.say("Агент удалён вместе с настройками, данными и тем, что поставила установка (пакеты, пользователь службы).")
	return nil
}
