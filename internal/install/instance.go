package install

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/epifanovmd/agent/internal/fsutil"
	"github.com/epifanovmd/agent/protocol"
)

// Paths — раскладка одного агента на узле. Несколько агентов на одном узле
// (для разных бэкендов) — экземпляры: у экземпляра ИМЯ все пути с окончанием
// «-ИМЯ» (/etc/agent-ИМЯ, /var/lib/agent-ИМЯ, /opt/agent-ИМЯ, служба
// agent-ИМЯ, пользователь agent-ИМЯ) и ничего общего с другими: своя
// программа (обновление и откат — свои), свой ключ, свои воркеры и журнал
// установки. Экземпляр по умолчанию (без имени) — пути без окончания.
type Paths struct {
	// Instance — имя экземпляра ("" — по умолчанию).
	Instance string
	// Home — агент пользователя без root (HomeLayout): всё в ~/.agent[-ИМЯ],
	// служба — systemd --user.
	Home        bool
	EtcDir      string
	ConfigFile  string
	EnvFile     string
	CAFile      string
	JournalFile string
	DataDir     string
	OptDir      string
	BinDir      string
	Binary      string
	UnitFile    string
	SysctlFile  string
	// Link — ссылка на программу в PATH: agent у экземпляра по умолчанию,
	// agent-ИМЯ у остальных (программа по своему пути узнаёт экземпляр).
	Link string
	// Service — имя службы systemd; Unit — оно же без .service (journalctl -u).
	Service string
	Unit    string
	// User — пользователь службы по умолчанию ("" — у агента пользователя: он сам).
	User string
}

// Layout — пути экземпляра name ("" — по умолчанию).
func Layout(name string) Paths {
	if name == "" {
		return Paths{
			EtcDir: EtcDir, ConfigFile: ConfigFile, EnvFile: EnvFile, CAFile: CAFile, JournalFile: JournalFile,
			DataDir: DataDir, OptDir: OptDir, BinDir: BinDir, Binary: Binary, UnitFile: UnitFile, SysctlFile: SysctlFile,
			Link: Link, Service: Service, Unit: "agent", User: "agent",
		}
	}
	base := "agent-" + name
	etc, opt := "/etc/"+base, "/opt/"+base
	return Paths{
		Instance: name, EtcDir: etc, ConfigFile: etc + "/agent.yaml", EnvFile: etc + "/agent.env", CAFile: etc + "/ca.pem",
		JournalFile: etc + "/install-state", DataDir: "/var/lib/" + base, OptDir: opt, BinDir: opt + "/bin",
		Binary: opt + "/bin/agent", UnitFile: "/etc/systemd/system/" + base + ".service",
		SysctlFile: "/etc/sysctl.d/90-" + base + ".conf", Link: "/usr/local/bin/" + base,
		Service: base + ".service", Unit: base, User: base,
	}
}

// HomeLayout — пути экземпляра name агента пользователя без root с домашним
// каталогом home: настройки и журнал установки — в ~/.agent[-ИМЯ], данные — в его data, программа — в bin; служба —
// ~/.config/systemd/user/agent[-ИМЯ].service, ссылка — ~/.local/bin. Те же
// пути, что у агента на macOS по умолчанию (~/.agent/agent.yaml, ~/.agent/data).
func HomeLayout(home, name string) Paths {
	base := "agent"
	if name != "" {
		base += "-" + name
	}
	dir := filepath.Join(home, "."+base)
	bin := filepath.Join(dir, "bin")
	return Paths{
		Instance: name, Home: true, EtcDir: dir, ConfigFile: filepath.Join(dir, "agent.yaml"),
		EnvFile: filepath.Join(dir, "agent.env"), CAFile: filepath.Join(dir, "ca.pem"),
		JournalFile: filepath.Join(dir, "install-state"), DataDir: filepath.Join(dir, "data"),
		OptDir: bin, BinDir: bin, Binary: filepath.Join(bin, "agent"),
		UnitFile: filepath.Join(home, ".config/systemd/user", base+".service"),
		Link:     filepath.Join(home, ".local/bin", base),
		Service:  base + ".service", Unit: base,
	}
}

// Find — экземпляр name на этой машине: от root — системный; от
// пользователя — его собственный (HomeLayout), если он поставлен, иначе системный.
func Find(name, home string, euid int) Paths {
	return (&System{Euid: func() int { return euid }, Home: home}).find(name)
}

func (s *System) find(name string) Paths {
	if s.Euid() != 0 && s.Home != "" {
		if l := HomeLayout(s.Home, name); fsutil.IsFile(s.p(l.JournalFile)) || fsutil.IsFile(s.p(l.ConfigFile)) {
			return l
		}
	}
	return Layout(name)
}

// Self — экземпляр, которому принадлежит программа exe: системный
// (/opt/agent[-ИМЯ]/bin/agent) или пользователя с домашним каталогом home
// (~/.agent[-ИМЯ]/bin/agent); ok=false — программа не установленного агента.
func Self(exe, home string) (Paths, bool) {
	if name, ok := InstanceOf(exe); ok {
		return Layout(name), true
	}
	if exe == Binary {
		return Layout(""), true
	}
	if home == "" || filepath.Base(filepath.Dir(exe)) != "bin" {
		return Paths{}, false
	}
	dir := filepath.Dir(filepath.Dir(exe))
	if filepath.Dir(dir) != filepath.Clean(home) {
		return Paths{}, false
	}
	switch base := filepath.Base(dir); {
	case base == ".agent":
		return HomeLayout(home, ""), true
	case strings.HasPrefix(base, ".agent-") && protocol.ValidName(strings.TrimPrefix(base, ".agent-")):
		return HomeLayout(home, strings.TrimPrefix(base, ".agent-")), true
	}
	return Paths{}, false
}

// CheckInstance — имя экземпляра: как имя воркера (строчная латиница, цифры
// и «-», первая — буква, до 32 символов).
func CheckInstance(name string) error {
	if name != "" && !protocol.ValidName(name) {
		return fmt.Errorf("--instance: имя экземпляра — строчная латиница, цифры и «-», первая — буква, до 32 символов, а не %q", name)
	}
	return nil
}

// InstanceOf — экземпляр, которому принадлежит программа exe
// (/opt/agent-ИМЯ/bin/agent → ИМЯ); ok=false — программа не установленного
// экземпляра (или экземпляра по умолчанию).
func InstanceOf(exe string) (string, bool) {
	dir := filepath.Dir(exe)
	if filepath.Base(dir) != "bin" {
		return "", false
	}
	opt := filepath.Dir(dir)
	name, ok := strings.CutPrefix(filepath.Base(opt), "agent-")
	if !ok || filepath.Dir(opt) != "/opt" || !protocol.ValidName(name) {
		return "", false
	}
	return name, true
}

// Name — имя экземпляра для текстов: «agent» или «agent-ИМЯ».
func (l Paths) Name() string { return l.Unit }

// Systemctl — systemctl для службы экземпляра: «systemctl» или «systemctl --user».
func (l Paths) Systemctl() string {
	if l.Home {
		return "systemctl --user"
	}
	return "systemctl"
}

// Flag — флаг экземпляра для подсказок команд: "" или " --instance ИМЯ".
func (l Paths) Flag() string {
	if l.Instance == "" {
		return ""
	}
	return " --instance " + l.Instance
}

// others — журналы установки остальных экземпляров на узле: общие пакеты,
// параметры ядра и пользователи удаляются, только если они не нужны им.
func (s *System) others(self Paths) []*journal {
	var out []*journal
	add := func(l Paths) {
		if l.JournalFile == self.JournalFile || !fsutil.IsFile(s.p(l.JournalFile)) {
			return
		}
		if j, err := loadJournal(s.p(l.JournalFile)); err == nil {
			out = append(out, j)
		}
	}
	add(Layout(""))
	dirs, _ := filepath.Glob(s.p("/etc/agent-*"))
	for _, d := range dirs {
		if name := strings.TrimPrefix(filepath.Base(d), "agent-"); protocol.ValidName(name) {
			add(Layout(name))
		}
	}
	return out
}
