package install

import (
	"context"
	"os"
	"strings"

	"github.com/epifanovmd/agent/internal/config"
)

// Uninstall — удалить экземпляр агента instance ("" — по умолчанию):
// воркеры убирают за собой (agent cleanup), затем служба, программа, файл
// параметров ядра (с возвратом прежних значений). purge — ещё настройки,
// данные, пакеты и пользователь службы, если их создала установка (root и
// пользователей, которые уже были, не трогает). Другие экземпляры не
// затрагиваются: нужные им пакеты, параметры ядра и пользователь остаются
// (и переходят в журнал того, кому нужны). self — запущенная программа
// агента: уборку делает она, если установленной уже нет.
func Uninstall(ctx context.Context, s *System, instance string, purge bool, self string) error {
	if err := CheckInstance(instance); err != nil {
		return err
	}
	if err := s.preflight(false); err != nil {
		return err
	}
	l := Layout(instance)
	// Служба — пользователь, от которого работали агент и воркеры.
	runAs := ""
	if raw, err := os.ReadFile(s.p(l.UnitFile)); err == nil {
		for _, l := range strings.Split(string(raw), "\n") {
			if v, ok := strings.CutPrefix(l, "User="); ok {
				runAs = strings.TrimSpace(v)
				break
			}
		}
	}
	j, err := loadJournal(s.p(l.JournalFile))
	if err != nil {
		return err
	}
	if runAs == "" {
		runAs = j.last("service-user")
	}
	if s.has("systemctl") {
		_ = s.Run(ctx, Cmd{Args: []string{"systemctl", "disable", "--now", l.Service}, Quiet: true})
	}
	// Воркеры убирают за собой (без сервера); ошибка — предупреждение, удаление продолжается.
	bin := s.p(l.Binary)
	if !isFile(bin) {
		bin = self
	}
	if bin != "" && isFile(bin) && isFile(s.p(l.ConfigFile)) {
		var env []string
		if f, err := os.Open(s.p(l.EnvFile)); err == nil {
			vars, _ := config.ParseEnv(f)
			f.Close()
			for _, kv := range vars {
				env = append(env, kv[0]+"="+kv[1])
			}
		}
		if runAs == "root" {
			runAs = ""
		}
		if err := s.Run(ctx, Cmd{Args: []string{bin, "cleanup", "-config", l.ConfigFile}, Env: env, User: runAs}); err != nil {
			s.warn("воркеры убрали за собой не всё (agent cleanup) — удаление продолжается")
		}
	}
	_ = os.Remove(s.p(l.UnitFile))
	if s.has("systemctl") {
		_ = s.Run(ctx, Cmd{Args: []string{"systemctl", "daemon-reload"}, Quiet: true})
	}
	if target, err := os.Readlink(s.p(l.Link)); err == nil && target == l.Binary {
		_ = os.Remove(s.p(l.Link))
	}
	if err := os.RemoveAll(s.p(l.OptDir)); err != nil {
		return err
	}
	others := s.others(l)
	if isFile(s.p(l.SysctlFile)) {
		_ = os.Remove(s.p(l.SysctlFile))
		sysctlApply(ctx, s, l)
	}
	// Значения параметров ядра — как были до установки; заданные и другим
	// экземпляром остаются его значениями (его журнал помнит прежнее).
	for _, kv := range j.values("sysctl-prev") {
		key, _, _ := strings.Cut(kv, "=")
		if usedBy(others, "sysctl", func(v string) bool { return strings.HasPrefix(v, key+"=") }) != nil {
			continue
		}
		if err := s.Run(ctx, Cmd{Args: []string{"sysctl", "-q", "-w", kv}, Quiet: true}); err != nil {
			s.warn("прежнее значение не возвращено: %s", kv)
		}
	}
	j.drop(func(k, _ string) bool { return k == "sysctl" || k == "sysctl-prev" })
	if isDir(s.p(l.EtcDir)) {
		if err := j.save(); err != nil {
			return err
		}
	}
	if !purge {
		s.say("Агент удалён. Настройки (%s) и данные (%s) оставлены: повторная установка продолжит с ними; удалить всё — agent uninstall%s --purge.", l.EtcDir, l.DataDir, l.Flag())
		return nil
	}
	// Пакеты и пользователь, нужные другому экземпляру, остаются и переходят
	// в его журнал: удалит их последний, кому они нужны.
	var pkgs []string
	for _, p := range j.values("package") {
		match := func(v string) bool { return v == p }
		if o := cmpJournal(usedBy(others, "requires", match), usedBy(others, "package", match)); o != nil {
			o.add("package", p)
			if err := o.save(); err != nil {
				return err
			}
			continue
		}
		pkgs = append(pkgs, p)
	}
	if len(pkgs) > 0 {
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
	var users []string
	for _, u := range j.values("user") {
		if o := usedBy(others, "service-user", func(v string) bool { return v == u }); o != nil {
			o.add("user", u)
			if err := o.save(); err != nil {
				return err
			}
			continue
		}
		users = append(users, u)
	}
	if err := os.RemoveAll(s.p(l.EtcDir)); err != nil {
		return err
	}
	if err := os.RemoveAll(s.p(l.DataDir)); err != nil {
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

// usedBy — первый журнал, где есть запись вида kind, для которой match.
func usedBy(journals []*journal, kind string, match func(string) bool) *journal {
	for _, j := range journals {
		for _, v := range j.values(kind) {
			if match(v) {
				return j
			}
		}
	}
	return nil
}

func cmpJournal(a, b *journal) *journal {
	if a != nil {
		return a
	}
	return b
}
