// Package install — установка агента службой systemd на Linux (agent install,
// agent uninstall): пользователь службы, каталоги, файл настроек, токен
// регистрации, системные пакеты и параметры ядра для воркеров, воркеры со
// сборкой с сервера, служба. Что поставлено — записывается в журнал установки, чтобы
// удаление вернуло узел как было. Повторная установка обновляет программу и
// службу, не трогая учётные данные агента и готовый файл настроек.
package install

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/update"
)

// Пути установки экземпляра по умолчанию (остальные — Layout).
const (
	EtcDir      = "/etc/agent"
	ConfigFile  = "/etc/agent/agent.yaml"
	EnvFile     = "/etc/agent/agent.env"
	CAFile      = "/etc/agent/ca.pem"
	JournalFile = "/etc/agent/install-state"
	DataDir     = config.LinuxDataDir
	OptDir      = "/opt/agent"
	BinDir      = "/opt/agent/bin"
	Binary      = "/opt/agent/bin/agent"
	UnitFile    = "/etc/systemd/system/agent.service"
	SysctlFile  = "/etc/sysctl.d/90-agent.conf"
	// Link — ссылка на программу в PATH: agent status, agent logs без полного пути.
	Link = "/usr/local/bin/agent"
	// Service — имя службы systemd.
	Service = "agent.service"
	// ReleasesPath — каталог сборок на сервере (от адреса сервера).
	ReleasesPath = "/api/v1/agent-link/releases"
)

// Cmd — запуск программы.
type Cmd struct {
	Args []string
	// Env — переменные сверх окружения установщика.
	Env []string
	// User — от какого пользователя запустить ("" — от текущего).
	User string
	// Quiet — не показывать вывод программы (только ошибки).
	Quiet bool
}

// System — всё, что установка делает с узлом; в тестах — подмена.
type System struct {
	// Root — корень файловой системы ("" — настоящий; в тестах — временный каталог).
	Root string
	// Run — запустить программу, её вывод — пользователю.
	Run func(ctx context.Context, c Cmd) error
	// Output — запустить программу и вернуть её вывод.
	Output func(ctx context.Context, args ...string) (string, error)
	// LookPath — есть ли программа в PATH.
	LookPath func(name string) (string, error)
	// Euid — от кого запущена установка (нужен root).
	Euid func() int
	// Chown — отдать файл или каталог (со всем содержимым) пользователю.
	Chown func(path, user string) error
	// HTTP — клиент для загрузки воркеров (nil — по CA сервера).
	HTTP *http.Client
	// Arch — архитектура узла (GOARCH).
	Arch   string
	Out    io.Writer
	ErrOut io.Writer
}

// p — путь на узле с учётом Root.
func (s *System) p(path string) string {
	if s.Root == "" {
		return path
	}
	return filepath.Join(s.Root, path)
}

func (s *System) say(format string, args ...any) { fmt.Fprintf(s.Out, format+"\n", args...) }
func (s *System) warn(format string, args ...any) {
	fmt.Fprintf(s.ErrOut, "agent install: предупреждение: "+format+"\n", args...)
}

func (s *System) has(name string) bool {
	_, err := s.LookPath(name)
	return err == nil
}

// Options — параметры установки (флаги agent install).
type Options struct {
	// Binary — файл агента, который ставится (обычно запущенный); Version — его версия.
	Binary  string
	Version string
	// BuiltinKey — ключ проверки подписи сборок, вшитый в сборку.
	BuiltinKey string

	Server    string
	Token     string
	TokenFile string
	// PublicKeys — ключи проверки подписи сборок (--update-key, --public-key):
	// вместе со вшитым ключом; в agent.env — AGENT_UPDATE_PUBLIC_KEYS.
	PublicKeys []string
	Name       string
	// Config — свой agent.yaml вместо создаваемого.
	Config string
	CAFile string
	// User — пользователь службы (по умолчанию agent; --privileged — root).
	User        string
	Privileged  bool
	RWPaths     []string
	KillMode    string
	StopTimeout string
	Packages    []string
	// PackagesBy — пакеты под менеджер (apt, dnf, yum, apk, zypper): на узле с
	// ним заменяют Packages.
	PackagesBy map[string][]string
	Sysctls    []string
	Workers    []string
	// Releases — адрес каталога сборок для --worker (по умолчанию
	// <server>/api/v1/agent-link/releases).
	Releases string
	// Instance — экземпляр (несколько агентов на узле, см. Layout); "" — по умолчанию.
	Instance string
	// Env — папка агента (--env): установка из её файла agent.<env>.yaml; само
	// чтение папки — дело команды (bundle), сюда приходит готовый Config.
	Env string
	// Vars — ещё переменные в agent.env (значения ${ИМЯ} из файла настроек).
	Vars map[string]string
}

var (
	reSysctl = regexp.MustCompile(`^[A-Za-z0-9_][A-Za-z0-9_./-]*=.*$`)
	rePkg    = regexp.MustCompile(`^[A-Za-z0-9.+_:-]+$`)
	reUser   = regexp.MustCompile(`^[a-z_][a-z0-9_-]*\$?$`)
)

// Managers — менеджеры пакетов: имя для флага --packages-<имя> → программа.
var Managers = []struct{ Name, Program string }{
	{"apt", "apt-get"}, {"dnf", "dnf"}, {"yum", "yum"}, {"apk", "apk"}, {"zypper", "zypper"},
}

// check — проверка параметров до каких-либо изменений на узле.
func (o *Options) check() error {
	var errs []error
	if err := CheckInstance(o.Instance); err != nil {
		errs = append(errs, err)
	}
	if o.TokenFile != "" && o.Token != "" {
		errs = append(errs, errors.New("--token и --token-file — что-то одно"))
	}
	if o.Server != "" {
		if u, err := url.Parse(o.Server); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Errorf("--server: %q — нужен адрес http(s)://", o.Server))
		}
	}
	if o.KillMode != "" && o.KillMode != "process" && o.KillMode != "mixed" {
		errs = append(errs, fmt.Errorf("--kill-mode: process | mixed, а не %q", o.KillMode))
	}
	if o.User != "" && !reUser.MatchString(o.User) {
		errs = append(errs, fmt.Errorf("--user: %q — не имя пользователя", o.User))
	}
	for _, kv := range o.Sysctls {
		if !reSysctl.MatchString(kv) {
			errs = append(errs, fmt.Errorf("--sysctl КЛЮЧ=ЗНАЧЕНИЕ, а не %q", kv))
		}
	}
	for _, w := range o.Workers {
		if !message.ValidName(w) {
			errs = append(errs, fmt.Errorf("--worker: имя воркера — строчная латиница, цифры и «-», первая — буква, до 32 символов, а не %q", w))
		}
	}
	for m, list := range o.PackagesBy {
		if !slices.ContainsFunc(Managers, func(x struct{ Name, Program string }) bool { return x.Name == m }) {
			errs = append(errs, fmt.Errorf("--packages-%s: неизвестный менеджер пакетов", m))
		}
		for _, p := range list {
			if !rePkg.MatchString(p) {
				errs = append(errs, fmt.Errorf("--packages-%s: %q — не имя пакета", m, p))
			}
		}
	}
	for _, p := range o.Packages {
		if !rePkg.MatchString(p) {
			errs = append(errs, fmt.Errorf("--packages: %q — не имя пакета", p))
		}
	}
	if _, err := update.ParseKeys(o.PublicKeys...); err != nil {
		errs = append(errs, fmt.Errorf("--update-key: %w", err))
	}
	for _, p := range o.RWPaths {
		if !filepath.IsAbs(p) || strings.ContainsAny(p, " \t\n\"'") {
			errs = append(errs, fmt.Errorf("--rw-path: %q — нужен абсолютный путь без пробелов и кавычек", p))
		}
	}
	return errors.Join(errs...)
}

// preflight — root и systemd.
func (s *System) preflight(needSystemd bool) error {
	if s.Euid() != 0 {
		return errors.New("нужны права root: запустите через sudo")
	}
	if needSystemd && (!s.has("systemctl") || !isDir(s.p("/run/systemd/system"))) {
		return errors.New("нужен Linux с работающим systemd (Debian/Ubuntu, RHEL/Fedora, SUSE и т. п.); " +
			"без systemd агент запускают в контейнере или напрямую: agent run")
	}
	return nil
}

// Install — поставить или обновить агента службой systemd.
func Install(ctx context.Context, s *System, o Options) error {
	if err := o.check(); err != nil {
		return err
	}
	if err := s.preflight(true); err != nil {
		return err
	}
	if o.TokenFile != "" {
		raw, err := os.ReadFile(o.TokenFile)
		if err != nil {
			return fmt.Errorf("--token-file: %w", err)
		}
		if o.Token = strings.Join(strings.Fields(string(raw)), ""); o.Token == "" {
			return fmt.Errorf("--token-file: файл %s пуст", o.TokenFile)
		}
	}
	o.Server = strings.TrimRight(o.Server, "/")
	l := Layout(o.Instance)
	configExists := isFile(s.p(l.ConfigFile))
	if o.Server == "" && !configExists && o.Config == "" {
		return errors.New("--server: адрес сервера (например, --server https://api.example.com)")
	}
	// Сборки воркеров — с сервера: без --server — адрес уже установленного агента
	// (agent.env, затем agent.yaml), как и остальные сохранённые значения.
	releasesFrom := o
	if releasesFrom.Server == "" && configExists {
		releasesFrom.Server = savedServer(s, l)
	}
	if releasesFrom.Server == "" && len(o.Workers) > 0 && o.Releases == "" {
		return errors.New("--worker: сборки воркеров берутся с сервера — нужен --server или --releases")
	}
	if o.Config != "" && !isFile(o.Config) {
		return fmt.Errorf("--config: нет файла %s", o.Config)
	}
	user := cmpOr(o.User, l.User)
	if o.Privileged {
		user = "root"
	}

	if err := os.MkdirAll(s.p(l.EtcDir), 0o755); err != nil {
		return err
	}
	if o.CAFile != "" {
		if err := installCA(s, l, o.CAFile); err != nil {
			return err
		}
	}
	j, err := loadJournal(s.p(l.JournalFile))
	if err != nil {
		return err
	}
	// Режим остановки службы: заданный запоминается, без --kill-mode — прежний (или process:
	// systemd останавливает только агента, воркеры переживают его перезапуск).
	killMode := o.KillMode
	if killMode == "" {
		killMode = j.last("kill-mode")
	}
	if killMode != "mixed" {
		killMode = "process"
	}
	j.set("kill-mode", killMode)
	j.set("service-user", user)
	if err := j.save(); err != nil {
		return err
	}

	if err := installPackages(ctx, s, j, o); err != nil {
		return err
	}
	if err := applySysctls(ctx, s, l, j, o.Sysctls); err != nil {
		return err
	}
	if err := ensureUser(ctx, s, l, j, user); err != nil {
		return err
	}
	if err := os.MkdirAll(s.p(l.DataDir), 0o700); err != nil {
		return err
	}
	_ = os.Chmod(s.p(l.DataDir), 0o700)
	if err := s.Chown(s.p(l.DataDir), user); err != nil {
		return err
	}
	workers, err := installWorkers(ctx, s, l, releasesFrom, user)
	if err != nil {
		return err
	}
	if err := installBinary(s, l, o.Binary, user); err != nil {
		return err
	}
	if err := writeConfig(s, l, o, workers, configExists); err != nil {
		return err
	}
	vars := map[string]string{}
	maps.Copy(vars, o.Vars)
	vars["AGENT_ENROLL_TOKEN"] = o.Token
	vars["AGENT_UPDATE_PUBLIC_KEYS"] = strings.Join(keyList(o.PublicKeys), ",")
	if err := writeEnv(s, l, user, vars); err != nil {
		return err
	}
	if !o.Privileged {
		for _, p := range o.RWPaths {
			if err := os.MkdirAll(s.p(p), 0o755); err != nil {
				return err
			}
			if err := s.Chown(s.p(p), user); err != nil {
				return err
			}
		}
	}
	unit := Unit(UnitOptions{Paths: l, User: user, KillMode: killMode, StopTimeout: cmpOr(o.StopTimeout, "15min"), Privileged: o.Privileged, RWPaths: o.RWPaths})
	if err := os.MkdirAll(filepath.Dir(s.p(l.UnitFile)), 0o755); err != nil {
		return err
	}
	if err := os.WriteFile(s.p(l.UnitFile), []byte(unit), 0o644); err != nil {
		return err
	}
	for _, args := range [][]string{{"systemctl", "daemon-reload"}, {"systemctl", "enable", l.Service}, {"systemctl", "restart", l.Service}} {
		if err := s.Run(ctx, Cmd{Args: args, Quiet: true}); err != nil {
			return fmt.Errorf("%s: %w", strings.Join(args, " "), err)
		}
	}
	// Команды экземпляра: agent … --instance ИМЯ (или agent-ИМЯ …, если ссылка есть).
	cmd := "agent"
	if l.Instance != "" {
		cmd = l.Binary
		if target, err := os.Readlink(s.p(l.Link)); err == nil && target == l.Binary {
			cmd = filepath.Base(l.Link)
		}
	}
	s.say("Агент %s установлен и запущен (служба %s, пользователь %s).", o.Version, l.Unit, user)
	s.say("  состояние:  sudo %s status", cmd)
	s.say("  лог:        sudo %s logs -f   (journalctl -u %s -f)", cmd, l.Unit)
	s.say("  настройки:  %s — проверить: sudo %s config check; применить: sudo systemctl reload %s", l.ConfigFile, cmd, l.Unit)
	s.say("  удалить:    sudo %s uninstall [--purge]", cmd)
	return nil
}

func installCA(s *System, l Paths, src string) error {
	raw, err := os.ReadFile(src)
	if err != nil {
		return fmt.Errorf("--ca-file: %w", err)
	}
	if !strings.Contains(string(raw), "BEGIN CERTIFICATE") {
		return fmt.Errorf("--ca-file: в %s нет сертификата PEM", src)
	}
	if abs, _ := filepath.Abs(src); abs == s.p(l.CAFile) {
		return nil
	}
	return writeFileAtomic(s.p(l.CAFile), raw, 0o644)
}

// installBinary — программа в /opt/agent[-ИМЯ]/bin (каталог доступен агенту на
// запись: самообновление кладёт рядом .new, .prev и отметку) и ссылка в PATH.
func installBinary(s *System, l Paths, src, user string) error {
	if err := os.MkdirAll(s.p(l.BinDir), 0o755); err != nil {
		return err
	}
	if err := s.Chown(s.p(l.OptDir), user); err != nil {
		return err
	}
	dst := s.p(l.Binary)
	if !sameFile(src, dst) {
		raw, err := os.ReadFile(src)
		if err != nil {
			return fmt.Errorf("программа агента: %w", err)
		}
		if err := writeFileAtomic(dst, raw, 0o755); err != nil {
			return err
		}
		if err := s.Chown(dst, user); err != nil {
			return err
		}
	}
	link := s.p(l.Link)
	if !isDir(filepath.Dir(link)) {
		return nil
	}
	if target, err := os.Readlink(link); err == nil {
		if target == l.Binary {
			return nil
		}
		s.warn("%s уже есть и ведёт на %s — оставлен как есть; программа агента — %s", l.Link, target, l.Binary)
		return nil
	}
	if _, err := os.Lstat(link); err == nil {
		s.warn("%s уже есть — оставлен как есть; программа агента — %s", l.Link, l.Binary)
		return nil
	}
	return os.Symlink(l.Binary, link)
}

// writeConfig — свой файл (--config), созданный по шаблону (если его нет) или
// прежний без изменений — с подсказками, если в нём чего-то не хватает.
func writeConfig(s *System, l Paths, o Options, workers []config.TemplateWorker, exists bool) error {
	switch {
	case o.Config != "":
		raw, err := os.ReadFile(o.Config)
		if err != nil {
			return err
		}
		if old, err := os.ReadFile(s.p(l.ConfigFile)); err == nil && !bytes.Equal(old, raw) {
			if err := writeFileAtomic(s.p(l.ConfigFile)+".bak", old, 0o644); err != nil {
				return err
			}
			s.warn("%s заменён новым, прежний — %s.bak", l.ConfigFile, l.ConfigFile)
		}
		return writeFileAtomic(s.p(l.ConfigFile), raw, 0o644)
	case !exists:
		t := config.TemplateOptions{ServerURL: o.Server, Name: o.Name, DataDir: l.DataDir, LogFormat: "json", Workers: workers}
		if o.CAFile != "" {
			t.CAFile = l.CAFile
		}
		return writeFileAtomic(s.p(l.ConfigFile), config.Template(t), 0o644)
	}
	cfg := config.Defaults()
	raw, _ := os.ReadFile(s.p(l.ConfigFile))
	if o.CAFile != "" && !strings.Contains(string(raw), "caFile:") {
		s.warn("сертификат скопирован в %s — пропишите server.caFile: %s в %s", l.CAFile, l.CAFile, l.ConfigFile)
	}
	if err := yamlUnmarshal(raw, &cfg); err == nil {
		if o.Server != "" && cfg.Server.URL != "" && strings.TrimRight(cfg.Server.URL, "/") != o.Server {
			s.warn("%s не менялся: в нём server.url = %s, а не %s — поправьте файл, если адрес сменился", l.ConfigFile, cfg.Server.URL, o.Server)
		}
		for _, w := range workers {
			if !slices.ContainsFunc(cfg.Workers, func(c config.Worker) bool { return c.Name == w.Name && c.Release }) {
				s.warn("сборка воркера %s поставлена, но %s не менялся — добавьте в workers:\n  - name: %s\n    release: true\nи выполните systemctl reload %s",
					w.Name, l.ConfigFile, w.Name, l.Unit)
			}
		}
	}
	return nil
}

// savedServer — адрес сервера установленного агента: AGENT_SERVER_URL из
// agent.env (он важнее файла, как при запуске), иначе server.url из agent.yaml.
func savedServer(s *System, l Paths) string {
	if raw, err := os.ReadFile(s.p(l.EnvFile)); err == nil {
		for _, l := range strings.Split(string(raw), "\n") {
			k, v, _ := strings.Cut(l, "=")
			if strings.TrimSpace(k) == "AGENT_SERVER_URL" {
				if v = strings.Trim(strings.TrimSpace(v), `"'`); v != "" {
					return strings.TrimRight(v, "/")
				}
			}
		}
	}
	cfg := config.Defaults()
	raw, err := os.ReadFile(s.p(l.ConfigFile))
	if err != nil || yamlUnmarshal(raw, &cfg) != nil {
		return ""
	}
	return strings.TrimRight(cfg.Server.URL, "/")
}

// keyList — ключи без пустых и повторов.
func keyList(keys []string) []string {
	var out []string
	for _, k := range keys {
		if k = strings.TrimSpace(k); k != "" && !slices.Contains(out, k) {
			out = append(out, k)
		}
	}
	return out
}

// writeEnv — секреты в agent.env (0600): переданные заменяют прежние,
// остальные строки сохраняются. Новые ключи проверки заменяют и прежний
// одиночный AGENT_UPDATE_PUBLIC_KEY.
func writeEnv(s *System, l Paths, user string, vars map[string]string) error {
	path := s.p(l.EnvFile)
	raw, err := os.ReadFile(path)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	var lines []string
	for _, l := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		k, _, _ := strings.Cut(l, "=")
		k = strings.TrimSpace(k)
		if l == "" || vars[k] != "" || (k == "AGENT_UPDATE_PUBLIC_KEY" && vars["AGENT_UPDATE_PUBLIC_KEYS"] != "") {
			continue
		}
		lines = append(lines, l)
	}
	keys := slices.Sorted(maps.Keys(vars))
	for _, k := range keys {
		if v := vars[k]; v != "" {
			lines = append(lines, k+"="+v)
		}
	}
	if len(lines) == 0 && errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err := writeFileAtomic(path, []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		return err
	}
	return s.Chown(path, user)
}

// UnitOptions — служба systemd.
type UnitOptions struct {
	// Paths — экземпляр (Layout).
	Paths       Paths
	User        string
	KillMode    string
	StopTimeout string
	Privileged  bool
	RWPaths     []string
}

// Unit — файл службы. SIGHUP (systemctl reload) — агент перечитывает
// agent.yaml на ходу; SIGTERM — только агенту (KillMode=process): воркеры
// работают дальше по lifecycle.onAgentStop, новый запуск их подхватывает;
// KillMode=mixed — systemd завершает и воркеры (они не переживают
// перезапуск агента). Откат обновления делает прежняя версия до запуска
// новой (ExecStartPre); перезапуск — всегда.
func Unit(o UnitOptions) string {
	l := o.Paths
	if l.Binary == "" {
		l = Layout("")
	}
	hardening := "# --privileged: агент и воркеры настраивают узел — без ограничений файловой системы."
	if !o.Privileged {
		hardening = "NoNewPrivileges=true\nProtectSystem=full\nProtectHome=read-only\nPrivateTmp=true\n" +
			"ReadWritePaths=" + strings.Join(append([]string{l.DataDir, l.OptDir}, o.RWPaths...), " ")
	}
	desc := "Agent"
	if l.Instance != "" {
		desc += " " + l.Instance
	}
	return `[Unit]
Description=` + desc + `
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
User=` + o.User + `
EnvironmentFile=-` + l.EnvFile + `
Environment=AGENT_BOOT_GUARD=external
ExecStartPre=-/bin/sh -c '[ ! -x ` + l.Binary + `.prev ] || ` + l.Binary + `.prev boot-guard ` + l.Binary + `'
ExecStart=` + l.Binary + ` run -config ` + l.ConfigFile + `
ExecReload=/bin/kill -HUP $MAINPID
Restart=always
RestartSec=2
StartLimitIntervalSec=0
KillMode=` + o.KillMode + `
TimeoutStopSec=` + o.StopTimeout + `
` + hardening + `

[Install]
WantedBy=multi-user.target
`
}

// ensureUser — пользователь службы: нет — создаётся (системный, без входа) и
// записывается в журнал: --purge удалит только его.
func ensureUser(ctx context.Context, s *System, l Paths, j *journal, user string) error {
	if user == "root" {
		return nil
	}
	if _, err := s.Output(ctx, "id", "-u", user); err == nil {
		return nil
	}
	if !s.has("useradd") {
		return fmt.Errorf("--user %s: нет useradd — создайте пользователя заранее", user)
	}
	shell := "/bin/false"
	for _, f := range []string{"/usr/sbin/nologin", "/sbin/nologin", "/usr/bin/nologin"} {
		if isFile(s.p(f)) {
			shell = f
			break
		}
	}
	if err := s.Run(ctx, Cmd{Args: []string{"useradd", "--system", "--home-dir", l.DataDir, "--no-create-home", "--shell", shell, user}}); err != nil {
		return fmt.Errorf("useradd %s: %w", user, err)
	}
	j.add("user", user)
	return j.save()
}

func cmpOr(v, def string) string {
	if v != "" {
		return v
	}
	return def
}
