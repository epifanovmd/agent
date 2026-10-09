package install

import (
	"fmt"
	"path/filepath"
	"strings"

	"github.com/epifanovmd/agent/internal/message"
)

// Paths — раскладка одного агента на узле. Несколько агентов на одном узле
// (для разных бэкендов) — экземпляры: у экземпляра ИМЯ все пути с окончанием
// «-ИМЯ» (/etc/agent-ИМЯ, /var/lib/agent-ИМЯ, /opt/agent-ИМЯ, служба
// agent-ИМЯ, пользователь agent-ИМЯ) и ничего общего с другими: своя
// программа (обновление и откат — свои), свой ключ, свои воркеры и журнал
// установки. Экземпляр по умолчанию (без имени) — пути без окончания.
type Paths struct {
	// Instance — имя экземпляра ("" — по умолчанию).
	Instance    string
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
	// User — пользователь службы по умолчанию.
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

// CheckInstance — имя экземпляра: как имя воркера (строчная латиница, цифры
// и «-», первая — буква, до 32 символов).
func CheckInstance(name string) error {
	if name != "" && !message.ValidName(name) {
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
	if !ok || filepath.Dir(opt) != "/opt" || !message.ValidName(name) {
		return "", false
	}
	return name, true
}

// Name — имя экземпляра для текстов: «agent» или «agent-ИМЯ».
func (l Paths) Name() string { return l.Unit }

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
		if l.JournalFile == self.JournalFile || !isFile(s.p(l.JournalFile)) {
			return
		}
		if j, err := loadJournal(s.p(l.JournalFile)); err == nil {
			out = append(out, j)
		}
	}
	add(Layout(""))
	dirs, _ := filepath.Glob(s.p("/etc/agent-*"))
	for _, d := range dirs {
		if name := strings.TrimPrefix(filepath.Base(d), "agent-"); message.ValidName(name) {
			add(Layout(name))
		}
	}
	return out
}
