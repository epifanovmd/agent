package install

import (
	"flag"
	"fmt"
	"io"
	"strings"
)

// list — повторяемый флаг; words — значения через пробел («jq curl»).
type list struct {
	to    *[]string
	words bool
}

func (l list) String() string { return "" }
func (l list) Set(v string) error {
	if l.words {
		*l.to = append(*l.to, strings.Fields(v)...)
	} else {
		*l.to = append(*l.to, v)
	}
	return nil
}

// Usage — справка agent install.
const Usage = `agent install — поставить агента службой systemd (Linux, нужен root).

  sudo agent install --server https://api.example.com --token <токен> [флаги]

Повторный запуск обновляет программу и службу; ключ агента, agent.yaml и
agent.env сохраняются (переданные --token и --public-key заменяют прежние).

Флаги:
  --server URL            адрес бэкенда (нужен, если ещё нет /etc/agent/agent.yaml)
  --token ТОКЕН           токен регистрации → /etc/agent/agent.env (0600)
  --token-file ПУТЬ       токен из файла (не виден в списке процессов)
  --name ИМЯ              имя агента (по умолчанию — имя машины)
  --config ФАЙЛ           свой agent.yaml вместо создаваемого
  --ca-file ПУТЬ          свой корневой сертификат сервера → /etc/agent/ca.pem
  --public-key КЛЮЧ       ключ проверки подписи выпусков (в сборки из выпуска вшит)
  --worker ИМЯ            воркер из выпуска (можно несколько раз)
  --releases URL          откуда брать воркеры (по умолчанию <server>/api/v1/agent-link/releases)
  --user ИМЯ              пользователь службы (по умолчанию agent; нет — создаётся)
  --privileged            агент и воркеры — root без ограничений (воркеры настраивают узел)
  --rw-path ПУТЬ          разрешить запись ещё в этот каталог (можно несколько раз)
  --packages "…"          системные пакеты для воркеров (apt, dnf, yum, apk, zypper)
  --packages-apt "…"      свои имена пакетов для менеджера (также -dnf, -yum, -apk, -zypper)
  --sysctl КЛЮЧ=ЗНАЧЕНИЕ  параметр ядра для воркеров (можно несколько раз)
  --kill-mode process|mixed  process (по умолчанию) — systemd останавливает только агента, воркеры
                          работают дальше; mixed — и воркеры (запоминается)
  --stop-timeout СРОК     сколько systemd ждёт остановки (по умолчанию 15min)

Удаление: sudo agent uninstall [--purge]
`

// ParseFlags — флаги agent install.
func ParseFlags(args []string, errOut io.Writer) (Options, error) {
	var o Options
	o.PackagesBy = map[string][]string{}
	fs := flag.NewFlagSet("install", flag.ContinueOnError)
	fs.SetOutput(errOut)
	fs.Usage = func() { fmt.Fprint(errOut, Usage) }
	fs.StringVar(&o.Server, "server", "", "")
	fs.StringVar(&o.Token, "token", "", "")
	fs.StringVar(&o.TokenFile, "token-file", "", "")
	fs.StringVar(&o.PublicKey, "public-key", "", "")
	fs.StringVar(&o.Name, "name", "", "")
	fs.StringVar(&o.Config, "config", "", "")
	fs.StringVar(&o.CAFile, "ca-file", "", "")
	fs.StringVar(&o.User, "user", "", "")
	fs.StringVar(&o.KillMode, "kill-mode", "", "")
	fs.StringVar(&o.StopTimeout, "stop-timeout", "", "")
	fs.StringVar(&o.Releases, "releases", "", "")
	fs.BoolVar(&o.Privileged, "privileged", false, "")
	fs.Var(list{to: &o.RWPaths}, "rw-path", "")
	fs.Var(list{to: &o.Sysctls}, "sysctl", "")
	fs.Var(list{to: &o.Workers}, "worker", "")
	fs.Var(list{to: &o.Packages, words: true}, "packages", "")
	byManager := map[string]*[]string{}
	for _, m := range Managers {
		byManager[m.Name] = new([]string)
		fs.Var(list{to: byManager[m.Name], words: true}, "packages-"+m.Name, "")
	}
	if err := fs.Parse(args); err != nil {
		return o, err
	}
	for name, pkgs := range byManager {
		if len(*pkgs) > 0 {
			o.PackagesBy[name] = *pkgs
		}
	}
	if fs.NArg() > 0 {
		return o, fmt.Errorf("лишние аргументы: %s (флаги — через --)", strings.Join(fs.Args(), " "))
	}
	return o, nil
}
