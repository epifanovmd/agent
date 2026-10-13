package install

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/epifanovmd/agent/internal/fsutil"
)

// Чем запускается агент (запись mode в журнале установки).
const (
	// ModeSystemd — служба systemd (root).
	ModeSystemd = "systemd"
	// ModeUser — служба пользователя systemd --user (без root).
	ModeUser = "user"
	// modeManual — в журнале установки агента без службы (запущен в фоне,
	// автозапуск — строкой @reboot в crontab): такой агент останавливают сами.
	modeManual = "manual"
)

// manualHint — как убрать агента без службы перед установкой службой или удалением.
func manualHint(l Paths) string {
	return "агент поставлен без службы и может работать в фоне: остановите его (kill <pid>, pid — в agent status) " +
		"и уберите строку @reboot " + l.Binary + " из crontab (crontab -e)"
}

// Layout — пути установки по параметрам: агент пользователя (Home) или системный.
func (o *Options) Layout() Paths {
	if o.Home != "" {
		return HomeLayout(o.Home, o.Instance)
	}
	return Layout(o.Instance)
}

// Mode — чем будет запускаться агент: user (служба пользователя) или systemd.
func (o *Options) Mode() string {
	if o.Home != "" {
		return ModeUser
	}
	return ModeSystemd
}

// Plan — где ставить агента: от root — системный экземпляр (служба systemd),
// от пользователя — его собственный в домашнем каталоге (--rootless: служба
// systemd --user). Без --rootless повторная установка от
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
	case fsutil.IsFile(s.p(l.UnitFile)) && l.Home:
		return ModeUser
	case fsutil.IsFile(s.p(l.UnitFile)):
		return ModeSystemd
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
	if mode == ModeUser {
		return "systemctl --user reload " + l.Unit
	}
	return "systemctl reload " + l.Unit
}
