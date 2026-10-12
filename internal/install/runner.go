package install

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"time"
)

// Чем запускается агент (запись mode в журнале установки).
const (
	// ModeSystemd — служба systemd (root).
	ModeSystemd = "systemd"
	// ModeUser — служба пользователя systemd --user (без root).
	ModeUser = "user"
	// ModeManual — без службы: у пользователя нет systemd --user; установка
	// запускает агента в фоне, автозапуск — @reboot в crontab.
	ModeManual = "manual"
)

// Layout — пути установки по параметрам: агент пользователя (Home) или системный.
func (o *Options) Layout() Paths {
	if o.Home != "" {
		return HomeLayout(o.Home, o.Instance)
	}
	return Layout(o.Instance)
}

// Mode — чем будет запускаться агент: user (служба пользователя; без
// systemd --user установка поставит manual) или systemd.
func (o *Options) Mode() string {
	if o.Home != "" {
		return ModeUser
	}
	return ModeSystemd
}

// Plan — где ставить агента: от root — системный экземпляр (служба systemd),
// от пользователя — его собственный в домашнем каталоге (--rootless: служба
// systemd --user, без неё — без службы). Без --rootless повторная установка от
// пользователя, у которого агент уже стоит, обновляет его.
func Plan(s *System, o *Options) error {
	root := s.Euid() == 0
	if o.Rootless && root {
		return errors.New("--rootless — агент пользователя без root: запустите без sudo от пользователя, для которого он ставится")
	}
	if !root && !o.Rootless && s.Home != "" && s.modeOf(HomeLayout(s.Home, o.Instance)) != "" {
		o.Rootless = true
	}
	if !root && o.Rootless {
		if s.Home == "" {
			return errors.New("не найден домашний каталог пользователя (HOME)")
		}
		o.Home = s.Home
	}
	return nil
}

// checkMode — флаги, которые агенту без root не подходят.
func (o *Options) checkMode() error {
	if o.Home == "" {
		return nil
	}
	var errs []error
	no := func(set bool, flag, why string) {
		if set {
			errs = append(errs, fmt.Errorf("%s: %s", flag, why))
		}
	}
	why := "нужен root, а агент ставится для пользователя без root"
	no(o.User != "", "--user", why)
	no(len(o.Sysctls) > 0, "--sysctl", why)
	no(len(o.Packages) > 0 || len(o.PackagesBy) > 0, "--packages", why)
	no(o.Privileged, "--privileged", why)
	no(len(o.RWPaths) > 0, "--rw-path", "служба пользователя работает без ограничений файловой системы — флаг не нужен")
	return errors.Join(errs...)
}

// modeOf — чем запускается установленный экземпляр l ("" — не установлен).
// Журнал без записи mode — от установки службой systemd.
func (s *System) modeOf(l Paths) string {
	j, err := loadJournal(s.p(l.JournalFile))
	if err != nil {
		return ""
	}
	if m := j.last("mode"); m != "" {
		return m
	}
	switch {
	case isFile(s.p(l.UnitFile)) && l.Home:
		return ModeUser
	case isFile(s.p(l.UnitFile)):
		return ModeSystemd
	case l.Home && isFile(s.p(l.ConfigFile)) && isFile(s.p(l.Binary)):
		return ModeManual
	}
	return ""
}

// ModeOf — чем запускается установленный на этой машине экземпляр l ("" — не установлен).
func ModeOf(l Paths) string { return (&System{}).modeOf(l) }

// userSystemd — работает ли systemd --user у этого пользователя (есть его
// сеанс: вход в систему или включённый linger).
func (s *System) userSystemd(ctx context.Context) bool {
	if !s.has("systemctl") {
		return false
	}
	_, err := s.Output(ctx, "systemctl", "--user", "show-environment")
	return err == nil
}

// linger — служба пользователя работает и без его входа в систему
// (loginctl enable-linger); не вышло — подсказка.
func (s *System) linger(ctx context.Context) {
	if out, err := s.Output(ctx, "loginctl", "show-user", s.User, "--property=Linger", "--value"); err == nil && strings.TrimSpace(out) == "yes" {
		return
	}
	if s.has("loginctl") && s.Run(ctx, Cmd{Args: []string{"loginctl", "enable-linger", s.User}, Quiet: true}) == nil {
		return
	}
	s.warn("служба пользователя остановится, когда %s выйдет из системы, и не запустится после перезагрузки — включите её работу без входа: sudo loginctl enable-linger %s", s.User, s.User)
}

// ReloadHint — как применить изменённые настройки экземпляра l (mode — способ запуска).
func ReloadHint(l Paths, mode string) string {
	switch mode {
	case ModeManual:
		return "agent restart" + l.Flag()
	case ModeUser:
		return "systemctl --user reload " + l.Unit + " (без службы — agent restart" + l.Flag() + ")"
	}
	return "systemctl reload " + l.Unit
}

// ManualLog — файл лога агента без службы (ModeManual): его вывод.
func ManualLog(l Paths) string { return filepath.Join(l.EtcDir, "agent.log") }

// cronLine — строка автозапуска агента без службы в crontab пользователя.
func cronLine(l Paths) string {
	return "@reboot " + l.Binary + " run -config " + l.ConfigFile + " >>" + ManualLog(l) + " 2>&1"
}

// startManual — агент без службы: автозапуск после перезагрузки (@reboot в
// crontab, если он есть) и запуск сейчас — в фоне, в своём сеансе (переживает
// выход из системы и закрытие SSH). Уже работает — перезапускается (SIGUSR1:
// агент без службы перезапускает себя сам, уже новой программой).
func (s *System) startManual(ctx context.Context, l Paths, j *journal) error {
	j.drop(func(k, _ string) bool { return k == "crontab" })
	if s.has("crontab") {
		cur, _ := s.Output(ctx, "crontab", "-l")
		line := cronLine(l)
		if !slices.Contains(strings.Split(cur, "\n"), line) {
			body := strings.TrimRight(cur, "\n")
			if body != "" {
				body += "\n"
			}
			if err := s.Run(ctx, Cmd{Args: []string{"crontab", "-"}, Stdin: body + line + "\n", Quiet: true}); err != nil {
				s.warn("автозапуск не прописан в crontab: %v", err)
			} else {
				j.set("crontab", line)
			}
		} else {
			j.set("crontab", line)
		}
	}
	if err := j.save(); err != nil {
		return err
	}
	if pid := s.Running(s.p(l.DataDir)); pid > 0 {
		if err := s.Kill(pid, syscall.SIGUSR1); err != nil {
			return fmt.Errorf("перезапуск работающего агента (pid %d): %w", pid, err)
		}
		return nil
	}
	return s.Spawn(Cmd{Args: []string{l.Binary, "run", "-config", l.ConfigFile}}, s.p(ManualLog(l)))
}

// manualAutostart — прописан ли автозапуск агента без службы (журнал установки).
func (s *System) manualAutostart(l Paths) bool {
	j, err := loadJournal(s.p(l.JournalFile))
	return err == nil && j.last("crontab") != ""
}

// stopManual — остановить агента без службы (SIGTERM, ждать до timeout) и
// убрать его строку из crontab.
func (s *System) stopManual(ctx context.Context, l Paths, timeout time.Duration) error {
	if s.has("crontab") {
		if cur, err := s.Output(ctx, "crontab", "-l"); err == nil {
			var keep []string
			for _, line := range strings.Split(strings.TrimRight(cur, "\n"), "\n") {
				if line != cronLine(l) {
					keep = append(keep, line)
				}
			}
			body := strings.Join(keep, "\n")
			if body != "" {
				body += "\n"
			}
			if body != cur {
				if err := s.Run(ctx, Cmd{Args: []string{"crontab", "-"}, Stdin: body, Quiet: true}); err != nil {
					s.warn("строка автозапуска не убрана из crontab: %v", err)
				}
			}
		}
	}
	pid := s.Running(s.p(l.DataDir))
	if pid <= 0 {
		return nil
	}
	if err := s.Kill(pid, syscall.SIGTERM); err != nil {
		return fmt.Errorf("остановка агента (pid %d): %w", pid, err)
	}
	for deadline := time.Now().Add(timeout); time.Now().Before(deadline); time.Sleep(200 * time.Millisecond) {
		if s.Running(s.p(l.DataDir)) <= 0 {
			return nil
		}
	}
	return fmt.Errorf("агент (pid %d) не остановился за %s", pid, timeout)
}
