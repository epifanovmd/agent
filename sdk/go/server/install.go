package server

import (
	"regexp"
	"slices"
	"strings"
	"unicode"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// InstallOptions — флаги install.sh для команды установки агента на узел.
// Пустые поля в команду не попадают.
type InstallOptions struct {
	// BaseURL — адрес сервера; пусто — PublicURL.
	BaseURL string
	// Token — токен регистрации; ровно одно из Token и TokenFile.
	Token string
	// TokenFile — путь к файлу с токеном на узле (install.sh --token-file):
	// токен не виден в списке процессов.
	TokenFile   string
	Name        string
	Privileged  bool
	Packages    []string
	Sysctl      map[string]string
	RWPaths     []string
	CAFile      string
	StopTimeout string
	User        string
	// Workers — воркеры из выпуска (install.sh --worker NAME); имя — по
	// message.NamePattern.
	Workers []string
	// PackagesByManager — пакеты под менеджер (apt, dnf, yum, apk, zypper):
	// для своего менеджера заменяют Packages (--packages-<менеджер>).
	PackagesByManager map[string][]string
	// Config — путь к agent.yaml на узле (install.sh --config).
	Config string
	// KillMode — KillMode службы: process или mixed (пусто — по умолчанию
	// install.sh, mixed).
	KillMode string
}

// installManagers — менеджеры пакетов install.sh в порядке флагов.
var installManagers = []string{"apt", "dnf", "yum", "apk", "zypper"}

var (
	// Имена пакетов и ключи sysctl — как их принимает install.sh.
	installPackage = regexp.MustCompile(`^[A-Za-z0-9.+_:-]+$`)
	installSysctl  = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_./-]*$`)
)

// InstallCommand — команда установки агента одной строкой:
// curl -fsSL '<адрес>/api/v1/agent-link/install.sh' | sudo sh -s -- --token '…' [флаги].
// Значения — в одинарных кавычках POSIX. Нет токена или адреса, адрес не
// http(s)://хост[:порт][/путь], недопустимое имя пакета, ключ sysctl или имя
// воркера,
// перевод строки в значении — ошибка MESSAGE_INVALID.
func (a *Agents) InstallCommand(opts InstallOptions) (string, error) {
	base := opts.BaseURL
	if base == "" {
		a.mu.Lock()
		base = a.publicURL
		a.mu.Unlock()
	}
	base = strings.TrimRight(base, "/")
	if base == "" {
		return "", protoErr("MESSAGE_INVALID", "Нужен адрес сервера (BaseURL или PublicURL)")
	}
	if !safeServer.MatchString(base) || strings.ContainsFunc(base, func(r rune) bool { return r == '\'' || unicode.IsSpace(r) }) {
		return "", protoErr("MESSAGE_INVALID", "Некорректный адрес сервера")
	}
	if (opts.Token == "") == (opts.TokenFile == "") {
		return "", protoErr("MESSAGE_INVALID", "Нужен ровно один из token и tokenFile")
	}
	if opts.KillMode != "" && opts.KillMode != "process" && opts.KillMode != "mixed" {
		return "", protoErr("MESSAGE_INVALID", "killMode — process или mixed")
	}
	for m := range opts.PackagesByManager {
		if !slices.Contains(installManagers, m) {
			return "", protoErr("MESSAGE_INVALID", "Неизвестный менеджер пакетов "+shellQuote(m))
		}
	}
	var b strings.Builder
	b.WriteString("curl -fsSL " + shellQuote(base+InstallPath) + " | sudo sh -s --")
	flag := func(name, value string) {
		if value != "" {
			b.WriteString(" " + name + " " + shellQuote(value))
		}
	}
	packages := func(name string, list []string) error {
		for _, p := range list {
			if !installPackage.MatchString(p) {
				return protoErr("MESSAGE_INVALID", "Некорректное имя пакета "+shellQuote(p))
			}
		}
		flag(name, strings.Join(list, " "))
		return nil
	}
	flag("--token", opts.Token)
	flag("--token-file", opts.TokenFile)
	flag("--name", opts.Name)
	flag("--user", opts.User)
	flag("--config", opts.Config)
	if opts.Privileged {
		b.WriteString(" --privileged")
	}
	flag("--kill-mode", opts.KillMode)
	if err := packages("--packages", opts.Packages); err != nil {
		return "", err
	}
	for _, m := range installManagers {
		if err := packages("--packages-"+m, opts.PackagesByManager[m]); err != nil {
			return "", err
		}
	}
	keys := make([]string, 0, len(opts.Sysctl))
	for k := range opts.Sysctl {
		if !installSysctl.MatchString(k) {
			return "", protoErr("MESSAGE_INVALID", "Некорректный ключ sysctl "+shellQuote(k))
		}
		keys = append(keys, k)
	}
	slices.Sort(keys)
	for _, k := range keys {
		flag("--sysctl", k+"="+opts.Sysctl[k])
	}
	for _, p := range opts.RWPaths {
		flag("--rw-path", p)
	}
	flag("--ca-file", opts.CAFile)
	for _, w := range opts.Workers {
		if !message.ValidName(w) {
			return "", protoErr("MESSAGE_INVALID", "Некорректное имя воркера "+shellQuote(w))
		}
		flag("--worker", w)
	}
	flag("--stop-timeout", opts.StopTimeout)
	cmd := b.String()
	if strings.ContainsAny(cmd, "\r\n") {
		return "", protoErr("MESSAGE_INVALID", "Перевод строки в значении")
	}
	return cmd, nil
}

// shellQuote — значение в одинарных кавычках POSIX; апостроф внутри закрывает
// кавычки, экранируется и открывает их снова.
func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
