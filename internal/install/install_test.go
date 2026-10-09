package install

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/update"
)

// fake — узел для установки: каталог вместо корня, программы записываются.
type fake struct {
	root      string
	cmds      []string
	users     map[string]bool
	installed map[string]bool
	programs  []string
	sysctl    map[string]string
	owners    map[string]string
	envs      map[string][]string
	out       bytes.Buffer
}

func newFake(t *testing.T, programs ...string) (*fake, *System) {
	t.Helper()
	f := &fake{
		root: t.TempDir(), users: map[string]bool{"root": true}, installed: map[string]bool{},
		programs: programs, sysctl: map[string]string{}, owners: map[string]string{}, envs: map[string][]string{},
	}
	for _, d := range []string{"run/systemd/system", "usr/local/bin", "usr/sbin"} {
		if err := os.MkdirAll(filepath.Join(f.root, d), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	_ = os.WriteFile(filepath.Join(f.root, "usr/sbin/nologin"), nil, 0o755)
	s := &System{
		Root: f.root,
		Run: func(_ context.Context, c Cmd) error {
			line := strings.Join(c.Args, " ")
			if c.User != "" {
				line = "[" + c.User + "] " + line
			}
			f.cmds = append(f.cmds, line)
			f.envs[line] = c.Env
			switch c.Args[0] {
			case "useradd":
				f.users[c.Args[len(c.Args)-1]] = true
			case "userdel":
				delete(f.users, c.Args[1])
			case "sysctl":
				if len(c.Args) == 4 && c.Args[2] == "-w" {
					k, v, _ := strings.Cut(c.Args[3], "=")
					f.sysctl[k] = v
				}
			}
			return nil
		},
		Output: func(_ context.Context, args ...string) (string, error) {
			switch {
			case args[0] == "id" && f.users[args[2]]:
				return "1000\n", nil
			case args[0] == "dpkg-query" && f.installed[args[3]]:
				return "install ok installed", nil
			case args[0] == "sysctl" && f.sysctl[args[2]] != "":
				return f.sysctl[args[2]] + "\n", nil
			}
			return "", errors.New("exit status 1")
		},
		LookPath: func(name string) (string, error) {
			if slices.Contains(f.programs, name) {
				return "/usr/bin/" + name, nil
			}
			return "", errors.New("not found")
		},
		Euid: func() int { return 0 },
		Chown: func(path, user string) error {
			f.owners[strings.TrimPrefix(path, f.root)] = user
			return nil
		},
		Arch:   "amd64",
		Out:    &f.out,
		ErrOut: &f.out,
	}
	return f, s
}

func (f *fake) read(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(f.root, path))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

func (f *fake) exists(path string) bool {
	_, err := os.Lstat(filepath.Join(f.root, path))
	return err == nil
}

func (f *fake) ran(t *testing.T, want ...string) {
	t.Helper()
	for _, w := range want {
		if !slices.Contains(f.cmds, w) {
			t.Errorf("не выполнено %q; выполнено:\n%s", w, strings.Join(f.cmds, "\n"))
		}
	}
}

func binary(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "agent-linux-amd64")
	if err := os.WriteFile(path, []byte(body), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

// Установка на чистый узел, повторная установка и удаление (с --purge и без).
func TestInstallUninstall(t *testing.T) {
	ctx := context.Background()
	f, s := newFake(t, "systemctl", "useradd", "apt-get", "sysctl")
	f.installed["jq"] = true
	f.sysctl["vm.max_map_count"] = "65530"
	o := Options{
		Binary: binary(t, "v1"), Version: "1.0.0", Server: "https://api.example.com/", Token: "tok", Name: "node-01",
		RWPaths: []string{"/etc/example"}, Packages: []string{"jq", "curl"}, Sysctls: []string{"vm.max_map_count=262144"},
	}
	if err := Install(ctx, s, o); err != nil {
		t.Fatal(err)
	}
	if f.read(t, Binary) != "v1" {
		t.Fatal("программа не поставлена")
	}
	if target, err := os.Readlink(filepath.Join(f.root, Link)); err != nil || target != Binary {
		t.Fatalf("ссылка: %q %v", target, err)
	}
	cfg, err := config.Load(filepath.Join(f.root, ConfigFile))
	if err != nil || cfg.Server.URL != "https://api.example.com" || cfg.Name != "node-01" || cfg.DataDir != DataDir ||
		cfg.Log.Format != "json" || cfg.Enroll.Token != "" || len(cfg.Workers) != 0 {
		t.Fatalf("agent.yaml: %+v %v", cfg, err)
	}
	if env := f.read(t, EnvFile); env != "AGENT_ENROLL_TOKEN=tok\n" {
		t.Fatalf("agent.env: %q", env)
	}
	if st, _ := os.Stat(filepath.Join(f.root, EnvFile)); st.Mode().Perm() != 0o600 {
		t.Fatalf("agent.env: права %v", st.Mode())
	}
	if st, _ := os.Stat(filepath.Join(f.root, DataDir)); st.Mode().Perm() != 0o700 {
		t.Fatalf("данные: права %v", st.Mode())
	}
	unit := f.read(t, UnitFile)
	for _, want := range []string{"User=agent\n", "KillMode=process\n", "TimeoutStopSec=15min\n", "ProtectSystem=full\n",
		"ReadWritePaths=/var/lib/agent /opt/agent /etc/example\n", "ExecStart=/opt/agent/bin/agent run -config /etc/agent/agent.yaml\n",
		"ExecStartPre=-/bin/sh -c '[ ! -x /opt/agent/bin/agent.prev ] || /opt/agent/bin/agent.prev boot-guard /opt/agent/bin/agent'\n",
		"EnvironmentFile=-/etc/agent/agent.env\n", "Environment=AGENT_BOOT_GUARD=external\n", "ExecReload=/bin/kill -HUP $MAINPID\n"} {
		if !strings.Contains(unit, want) {
			t.Errorf("в службе нет %q:\n%s", want, unit)
		}
	}
	f.ran(t, "useradd --system --home-dir /var/lib/agent --no-create-home --shell /usr/sbin/nologin agent",
		"apt-get update -qq", "apt-get install -y -qq jq curl", "sysctl -q -p /etc/sysctl.d/90-agent.conf",
		"systemctl daemon-reload", "systemctl enable agent.service", "systemctl restart agent.service")
	if env := f.envs["apt-get install -y -qq jq curl"]; !slices.Contains(env, "DEBIAN_FRONTEND=noninteractive") {
		t.Errorf("apt-get без DEBIAN_FRONTEND: %v", env)
	}
	if sc := f.read(t, SysctlFile); !strings.HasSuffix(sc, "\nvm.max_map_count=262144\n") {
		t.Fatalf("параметры ядра: %q", sc)
	}
	journal := f.read(t, JournalFile)
	for _, want := range []string{"kill-mode process", "package curl", "user agent", "sysctl vm.max_map_count=262144", "sysctl-prev vm.max_map_count=65530"} {
		if !strings.Contains(journal, "\n"+want+"\n") {
			t.Errorf("в журнале нет %q:\n%s", want, journal)
		}
	}
	if strings.Contains(journal, "package jq") {
		t.Error("jq стоял до установки — не в журнал")
	}
	for _, p := range []string{DataDir, OptDir, Binary, EnvFile, "/etc/example"} {
		if f.owners[p] != "agent" {
			t.Errorf("%s: владелец %q", p, f.owners[p])
		}
	}

	// Повторно: без токена — прежний сохраняется; agent.yaml не меняется;
	// режим остановки запоминается; пользователь не создаётся снова.
	_ = os.WriteFile(filepath.Join(f.root, ConfigFile), []byte(f.read(t, ConfigFile)+"# своё\n"), 0o644)
	_ = os.WriteFile(filepath.Join(f.root, EnvFile), []byte(f.read(t, EnvFile)+"EXAMPLE_KEY=1\n"), 0o600)
	f.cmds = nil
	again := Options{Binary: binary(t, "v2"), Version: "1.3.0", KillMode: "mixed", PublicKey: "a2V5"}
	if err := Install(ctx, s, again); err != nil {
		t.Fatal(err)
	}
	if f.read(t, Binary) != "v2" || !strings.HasSuffix(f.read(t, ConfigFile), "# своё\n") {
		t.Fatal("повторная установка: программа или agent.yaml")
	}
	if env := f.read(t, EnvFile); env != "AGENT_ENROLL_TOKEN=tok\nEXAMPLE_KEY=1\nAGENT_UPDATE_PUBLIC_KEY=a2V5\n" {
		t.Fatalf("agent.env: %q", env)
	}
	again.KillMode = ""
	again.Token = "tok2"
	if err := Install(ctx, s, again); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.read(t, UnitFile), "KillMode=mixed\n") || !strings.Contains(f.read(t, EnvFile), "AGENT_ENROLL_TOKEN=tok2\n") {
		t.Fatal("режим остановки запоминается, новый токен заменяет прежний")
	}
	for _, c := range f.cmds {
		if strings.HasPrefix(c, "useradd") || strings.HasPrefix(c, "apt-get") {
			t.Fatalf("повторно: %s", c)
		}
	}

	// Удаление: уборка от пользователя службы с переменными agent.env, параметры
	// ядра — прежние; настройки и данные остаются.
	f.cmds = nil
	if err := Uninstall(ctx, s, "", false, ""); err != nil {
		t.Fatal(err)
	}
	cleanup := "[agent] " + filepath.Join(f.root, Binary) + " cleanup -config /etc/agent/agent.yaml"
	f.ran(t, "systemctl disable --now agent.service", cleanup, "systemctl daemon-reload", "sysctl -q -w vm.max_map_count=65530")
	if env := f.envs[cleanup]; !slices.Contains(env, "AGENT_ENROLL_TOKEN=tok2") {
		t.Errorf("уборка без agent.env: %v", env)
	}
	for _, p := range []string{UnitFile, OptDir, Link, SysctlFile} {
		if f.exists(p) {
			t.Errorf("%s остался", p)
		}
	}
	if !f.exists(ConfigFile) || !f.exists(DataDir) || strings.Contains(f.read(t, JournalFile), "\nsysctl") {
		t.Fatal("без --purge: настройки и данные остаются, параметры ядра — из журнала")
	}

	// --purge: пакеты и пользователь, созданные установкой, настройки, данные;
	// установленной программы уже нет — уборку делает запущенная.
	f.cmds = nil
	self := binary(t, "v3")
	if err := Uninstall(ctx, s, "", true, self); err != nil {
		t.Fatal(err)
	}
	f.ran(t, "[agent] "+self+" cleanup -config /etc/agent/agent.yaml", "apt-get remove -y -qq curl", "userdel agent")
	if f.exists(EtcDir) || f.exists(DataDir) {
		t.Fatal("--purge: настройки и данные")
	}
}

// --privileged: служба от root без ограничений, пользователь не создаётся,
// root не удаляется; свой agent.yaml (--config) копируется как есть.
func TestInstallPrivileged(t *testing.T) {
	ctx := context.Background()
	f, s := newFake(t, "systemctl", "useradd")
	own := filepath.Join(t.TempDir(), "agent.yaml")
	_ = os.WriteFile(own, []byte("server:\n  url: https://api.example.com\n# своё\n"), 0o600)
	if err := Install(ctx, s, Options{Binary: binary(t, "v1"), Config: own, Privileged: true, User: "agent", TokenFile: writeToken(t, " tok \n")}); err != nil {
		t.Fatal(err)
	}
	unit := f.read(t, UnitFile)
	if !strings.Contains(unit, "User=root\n") || strings.Contains(unit, "ProtectSystem") {
		t.Fatalf("служба:\n%s", unit)
	}
	if f.read(t, ConfigFile) != "server:\n  url: https://api.example.com\n# своё\n" || f.read(t, EnvFile) != "AGENT_ENROLL_TOKEN=tok\n" {
		t.Fatal("--config, --token-file")
	}
	for _, c := range f.cmds {
		if strings.HasPrefix(c, "useradd") {
			t.Fatal("root не создаётся")
		}
	}
	f.cmds = nil
	if err := Uninstall(ctx, s, "", true, ""); err != nil {
		t.Fatal(err)
	}
	for _, c := range f.cmds {
		if strings.HasPrefix(c, "userdel") || strings.HasPrefix(c, "[") {
			t.Fatalf("root: %s", c)
		}
	}
}

// Два экземпляра на узле: свои пути, служба, пользователь и журнал; удаление
// одного не трогает другой, общие пакеты и параметры ядра остаются, пока
// нужны, и уходят с последним.
func TestInstances(t *testing.T) {
	ctx := context.Background()
	f, s := newFake(t, "systemctl", "useradd", "apt-get", "sysctl")
	f.sysctl["vm.max_map_count"] = "65530"
	def := Options{Binary: binary(t, "v1"), Server: "https://a.example.com", Token: "ta",
		Packages: []string{"curl"}, Sysctls: []string{"vm.max_map_count=262144"}}
	if err := Install(ctx, s, def); err != nil {
		t.Fatal(err)
	}
	f.sysctl["vm.max_map_count"] = "262144"
	f.installed["curl"] = true
	f.cmds = nil
	b := Options{Instance: "b", Binary: binary(t, "v2"), Server: "https://b.example.com", Token: "tb",
		Packages: []string{"curl", "jq"}, Sysctls: []string{"vm.max_map_count=524288"}}
	if err := Install(ctx, s, b); err != nil {
		t.Fatal(err)
	}
	l := Layout("b")
	if l.ConfigFile != "/etc/agent-b/agent.yaml" || l.DataDir != "/var/lib/agent-b" || l.Binary != "/opt/agent-b/bin/agent" ||
		l.Service != "agent-b.service" || l.User != "agent-b" || l.SysctlFile != "/etc/sysctl.d/90-agent-b.conf" {
		t.Fatalf("пути: %+v", l)
	}
	if f.read(t, l.Binary) != "v2" || f.read(t, Binary) != "v1" {
		t.Fatal("у каждого экземпляра своя программа")
	}
	if target, err := os.Readlink(filepath.Join(f.root, l.Link)); err != nil || target != l.Binary {
		t.Fatalf("ссылка экземпляра: %q %v", target, err)
	}
	cfg, err := config.Load(filepath.Join(f.root, l.ConfigFile))
	if err != nil || cfg.Server.URL != "https://b.example.com" || cfg.DataDir != l.DataDir {
		t.Fatalf("agent.yaml экземпляра: %+v %v", cfg, err)
	}
	if f.read(t, l.EnvFile) != "AGENT_ENROLL_TOKEN=tb\n" || f.read(t, EnvFile) != "AGENT_ENROLL_TOKEN=ta\n" {
		t.Fatal("agent.env: у каждого свой")
	}
	unit := f.read(t, l.UnitFile)
	for _, want := range []string{"Description=Agent b\n", "User=agent-b\n", "EnvironmentFile=-/etc/agent-b/agent.env\n",
		"ExecStart=/opt/agent-b/bin/agent run -config /etc/agent-b/agent.yaml\n",
		"ExecStartPre=-/bin/sh -c '[ ! -x /opt/agent-b/bin/agent.prev ] || /opt/agent-b/bin/agent.prev boot-guard /opt/agent-b/bin/agent'\n",
		"ReadWritePaths=/var/lib/agent-b /opt/agent-b\n"} {
		if !strings.Contains(unit, want) {
			t.Errorf("в службе нет %q:\n%s", want, unit)
		}
	}
	f.ran(t, "useradd --system --home-dir /var/lib/agent-b --no-create-home --shell /usr/sbin/nologin agent-b",
		"systemctl enable agent-b.service", "systemctl restart agent-b.service", "sysctl -q -p /etc/sysctl.d/90-agent-b.conf")
	for _, p := range []string{l.DataDir, l.OptDir, l.EnvFile} {
		if f.owners[p] != "agent-b" {
			t.Errorf("%s: владелец %q", p, f.owners[p])
		}
	}
	journal := f.read(t, l.JournalFile)
	for _, want := range []string{"package jq", "requires curl", "user agent-b", "sysctl-prev vm.max_map_count=65530"} {
		if !strings.Contains(journal, "\n"+want+"\n") {
			t.Errorf("в журнале экземпляра нет %q:\n%s", want, journal)
		}
	}
	if strings.Contains(journal, "package curl") {
		t.Error("curl поставил другой экземпляр")
	}

	// Удаление экземпляра по умолчанию: curl нужен b — остаётся и переходит в его
	// журнал; vm.max_map_count задаёт и b — прежнее значение не возвращается.
	f.cmds = nil
	if err := Uninstall(ctx, s, "", true, ""); err != nil {
		t.Fatal(err)
	}
	for _, c := range f.cmds {
		if strings.HasPrefix(c, "apt-get remove") || strings.HasPrefix(c, "sysctl -q -w") || strings.Contains(c, "agent-b") {
			t.Fatalf("затронут другой экземпляр: %s", c)
		}
	}
	f.ran(t, "userdel agent", "systemctl disable --now agent.service")
	for _, p := range []string{l.ConfigFile, l.EnvFile, l.Binary, l.UnitFile, l.DataDir, l.SysctlFile, l.Link} {
		if !f.exists(p) {
			t.Errorf("%s удалён вместе с другим экземпляром", p)
		}
	}
	if f.exists(EtcDir) || f.exists(OptDir) || f.exists(DataDir) {
		t.Fatal("экземпляр по умолчанию удалён не весь")
	}
	if !strings.Contains(f.read(t, l.JournalFile), "\npackage curl\n") {
		t.Fatal("curl не перешёл в журнал b")
	}

	// Последний экземпляр уносит всё: пакеты, прежнее значение параметра ядра.
	f.cmds = nil
	if err := Uninstall(ctx, s, "b", true, ""); err != nil {
		t.Fatal(err)
	}
	f.ran(t, "systemctl disable --now agent-b.service", "apt-get remove -y -qq jq curl",
		"sysctl -q -w vm.max_map_count=65530", "userdel agent-b",
		"[agent-b] "+filepath.Join(f.root, l.Binary)+" cleanup -config /etc/agent-b/agent.yaml")
	for _, p := range []string{l.EtcDir, l.OptDir, l.DataDir, l.UnitFile, l.Link, l.SysctlFile} {
		if f.exists(p) {
			t.Errorf("%s остался", p)
		}
	}
	if err := Uninstall(ctx, s, "B", false, ""); err == nil {
		t.Fatal("имя экземпляра не по правилу — ошибка")
	}
}

// Программа экземпляра узнаёт его по своему пути.
func TestInstanceOf(t *testing.T) {
	for exe, want := range map[string]string{
		"/opt/agent-b/bin/agent": "b", "/opt/agent-web-2/bin/agent.prev": "web-2",
		"/opt/agent/bin/agent": "", "/tmp/agent": "", "/opt/agent-B/bin/agent": "", "/srv/agent-b/bin/agent": "",
	} {
		if got, ok := InstanceOf(exe); got != want || ok != (want != "") {
			t.Errorf("%s: %q %v", exe, got, ok)
		}
	}
}

func writeToken(t *testing.T, body string) string {
	path := filepath.Join(t.TempDir(), "token")
	_ = os.WriteFile(path, []byte(body), 0o600)
	return path
}

// Проверки до изменений: root, systemd, адрес, флаги — узел не тронут.
func TestInstallRefuses(t *testing.T) {
	ctx := context.Background()
	cases := map[string]func(*fake, *System, *Options){
		"не root":     func(_ *fake, s *System, _ *Options) { s.Euid = func() int { return 1000 } },
		"без systemd": func(f *fake, _ *System, _ *Options) { _ = os.RemoveAll(filepath.Join(f.root, "run/systemd")) },
		"без адреса":  func(_ *fake, _ *System, o *Options) { o.Server = "" },
		"адрес":       func(_ *fake, _ *System, o *Options) { o.Server = "api.example.com" },
		"kill-mode":   func(_ *fake, _ *System, o *Options) { o.KillMode = "all" },
		"sysctl":      func(_ *fake, _ *System, o *Options) { o.Sysctls = []string{"vm.x"} },
		"воркер":      func(_ *fake, _ *System, o *Options) { o.Workers = []string{"a b"} },
		"пакет":       func(_ *fake, _ *System, o *Options) { o.Packages = []string{"a;b"} },
		"менеджер":    func(_ *fake, _ *System, o *Options) { o.PackagesBy = map[string][]string{"pacman": {"x"}} },
		"токен":       func(_ *fake, _ *System, o *Options) { o.TokenFile = "/nonexistent" },
		"два токена":  func(_ *fake, _ *System, o *Options) { o.TokenFile = "/x" },
		"rw-path":     func(_ *fake, _ *System, o *Options) { o.RWPaths = []string{"relative"} },
		"экземпляр":   func(_ *fake, _ *System, o *Options) { o.Instance = "Web" },
		"нет пакетного менеджера": func(_ *fake, _ *System, o *Options) {
			o.Packages = []string{"jq"}
		},
	}
	for name, mutate := range cases {
		f, s := newFake(t, "systemctl", "useradd")
		o := Options{Binary: binary(t, "v1"), Server: "https://api.example.com", Token: "t"}
		mutate(f, s, &o)
		if err := Install(ctx, s, o); err == nil {
			t.Errorf("%s: нет ошибки", name)
		}
		if f.exists(UnitFile) || f.exists(Binary) {
			t.Errorf("%s: узел изменён", name)
		}
	}
}

// --worker: старшая версия под эту машину, подпись и sha256 сверяются,
// архив распаковывается, записи — в создаваемый agent.yaml.
func TestInstallWorkers(t *testing.T) {
	ctx := context.Background()
	pub, priv, _ := ed25519.GenerateKey(rand.Reader)
	files := map[string][]byte{
		"collector-1.0.0-linux-amd64": []byte("old"),
		"collector-1.2.0-linux-amd64": []byte("new"),
		"report-2.0.0-linux-amd64.tar.gz": tarGz(t, map[string]string{
			"bin/report": "#!/bin/sh\n", "lib/x.py": "x",
		}),
	}
	entry := func(name, version, file, command string) message.WorkerArtifact {
		sum := sha256.Sum256(files[file])
		hexSum := hex.EncodeToString(sum[:])
		return message.WorkerArtifact{
			Name: name, Version: version, OS: "linux", Arch: "amd64", File: file, SHA256: hexSum, Command: command,
			Signature:   update.Sign(priv, update.Build{Name: name, Version: version, OS: "linux", Arch: "amd64", SHA256: hexSum}),
			StopTimeout: map[bool]string{true: "45s"}[name == "collector"],
		}
	}
	manifest := message.Manifest{Version: "1.0.0", Workers: []message.WorkerArtifact{
		entry("collector", "1.0.0", "collector-1.0.0-linux-amd64", ""),
		entry("collector", "1.2.0", "collector-1.2.0-linux-amd64", ""),
		entry("report", "2.0.0", "report-2.0.0-linux-amd64.tar.gz", "bin/report"),
	}}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		name := strings.TrimPrefix(r.URL.Path, ReleasesPath+"/")
		if name == "manifest.json" {
			_ = json.NewEncoder(w).Encode(manifest)
			return
		}
		if body, ok := files[name]; ok {
			_, _ = w.Write(body)
			return
		}
		http.NotFound(w, r)
	}))
	defer srv.Close()

	f, s := newFake(t, "systemctl", "useradd")
	key := base64.StdEncoding.EncodeToString(pub)
	o := Options{Binary: binary(t, "v1"), Server: srv.URL, Token: "t", PublicKey: key, Workers: []string{"collector", "report"}}
	if err := Install(ctx, s, o); err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(DataDir, "workers")
	if f.read(t, dir+"/collector/current") != "new" || f.read(t, dir+"/collector/version") != "1.2.0\n" ||
		f.read(t, dir+"/report/current/bin/report") != "#!/bin/sh\n" || f.read(t, dir+"/report/version") != "2.0.0\n" {
		t.Fatal("сборки воркеров")
	}
	if f.owners[dir+"/collector"] != "agent" || f.owners[dir+"/report"] != "agent" {
		t.Fatalf("владелец: %v", f.owners)
	}
	cfg, err := config.Load(filepath.Join(f.root, ConfigFile))
	if err != nil || len(cfg.Workers) != 2 || !cfg.Workers[0].Release || cfg.Workers[0].Lifecycle.StopTimeout.Std() != 45*time.Second ||
		!slices.Equal(cfg.Workers[1].Command, []string{"./bin/report"}) {
		t.Fatalf("agent.yaml: %+v %v", cfg.Workers, err)
	}

	// Готовый agent.yaml не меняется — подсказка; чужая подпись — ошибка.
	f.out.Reset()
	_ = os.WriteFile(filepath.Join(f.root, ConfigFile), []byte("server:\n  url: "+srv.URL+"\n"), 0o644)
	if err := Install(ctx, s, Options{Binary: o.Binary, PublicKey: key, Workers: []string{"collector"}, Server: srv.URL}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(f.out.String(), "server.url") {
		t.Fatalf("адрес тот же — без подсказки:\n%s", f.out.String())
	}
	if !strings.Contains(f.out.String(), "  - name: collector\n    release: true") {
		t.Fatalf("подсказка:\n%s", f.out.String())
	}
	// Повторная установка без --server: адрес — из agent.yaml, затем из agent.env.
	if err := Install(ctx, s, Options{Binary: o.Binary, PublicKey: key, Workers: []string{"collector"}}); err != nil {
		t.Fatalf("адрес из agent.yaml: %v", err)
	}
	_ = os.WriteFile(filepath.Join(f.root, ConfigFile), []byte("server:\n  url: https://wrong.example.com\n"), 0o644)
	_ = os.WriteFile(filepath.Join(f.root, EnvFile), []byte("AGENT_SERVER_URL="+srv.URL+"/\n"), 0o600)
	if err := Install(ctx, s, Options{Binary: o.Binary, PublicKey: key, Workers: []string{"collector"}}); err != nil {
		t.Fatalf("адрес из agent.env: %v", err)
	}
	_ = os.Remove(filepath.Join(f.root, EnvFile))
	_ = os.WriteFile(filepath.Join(f.root, ConfigFile), []byte("server:\n  url: "+srv.URL+"\n"), 0o644)
	other, _, _ := ed25519.GenerateKey(rand.Reader)
	err = Install(ctx, s, Options{Binary: o.Binary, Server: srv.URL, PublicKey: base64.StdEncoding.EncodeToString(other), Workers: []string{"collector"}})
	if err == nil || !strings.Contains(err.Error(), "подпись") {
		t.Fatalf("чужая подпись: %v", err)
	}
	if err := Install(ctx, s, Options{Binary: o.Binary, Server: srv.URL, Workers: []string{"absent"}}); err == nil {
		t.Fatal("нет воркера в выпуске — ошибка")
	}
	files["collector-1.2.0-linux-amd64"] = []byte("tampered")
	if err := Install(ctx, s, Options{Binary: o.Binary, Server: srv.URL, Workers: []string{"collector"}}); err == nil {
		t.Fatal("sha256 не сходится — ошибка")
	}
	if f.read(t, dir+"/collector/current") != "new" {
		t.Fatal("прежняя сборка заменена при ошибке")
	}
}

func tarGz(t *testing.T, entries map[string]string) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for name, body := range entries {
		_ = tw.WriteHeader(&tar.Header{Name: name, Mode: 0o755, Size: int64(len(body)), Typeflag: tar.TypeReg})
		_, _ = tw.Write([]byte(body))
	}
	_ = tw.Close()
	_ = gz.Close()
	return buf.Bytes()
}

// Флаги: повторяемые, пакеты через пробел, пакеты под менеджер.
func TestParseFlags(t *testing.T) {
	o, err := ParseFlags([]string{
		"--server", "https://api.example.com", "--token", "t", "--name", "n", "--privileged", "--kill-mode", "process",
		"--packages", "jq curl", "--packages", "git", "--packages-apk", "bind-tools", "--sysctl", "a=1", "--sysctl", "b=2",
		"--rw-path", "/x", "--worker", "collector", "--worker", "report", "--releases", "https://example.com/r", "--instance", "web",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if o.Server != "https://api.example.com" || o.Token != "t" || o.Name != "n" || !o.Privileged || o.KillMode != "process" ||
		!slices.Equal(o.Packages, []string{"jq", "curl", "git"}) || !slices.Equal(o.PackagesBy["apk"], []string{"bind-tools"}) ||
		len(o.PackagesBy) != 1 || !slices.Equal(o.Sysctls, []string{"a=1", "b=2"}) || !slices.Equal(o.RWPaths, []string{"/x"}) ||
		!slices.Equal(o.Workers, []string{"collector", "report"}) || o.Releases != "https://example.com/r" || o.Instance != "web" {
		t.Fatalf("%+v", o)
	}
	if _, err := ParseFlags([]string{"--unknown"}, &bytes.Buffer{}); err == nil {
		t.Fatal("неизвестный флаг")
	}
	if _, err := ParseFlags([]string{"extra"}, &bytes.Buffer{}); err == nil {
		t.Fatal("лишний аргумент")
	}
}

// Флаги из installCommand серверного SDK (sdk/node/test/install.test.ts) —
// install.sh передаёт их agent install как есть: все известны.
func TestParseFlagsInstallCommand(t *testing.T) {
	o, err := ParseFlags([]string{
		"--instance", "web-2", "--token", "tok'en", "--name", "node 01", "--user", "root", "--config", "/etc/agent/node.yaml", "--privileged",
		"--kill-mode", "process", "--packages", "jq curl", "--packages-apk", "bind-tools",
		"--sysctl", "net.core.somaxconn=1024", "--sysctl", "vm.max_map_count=262144",
		"--rw-path", "/etc/example", "--rw-path", "/var/lib/it's", "--ca-file", "/etc/agent/ca.pem", "--worker", "report",
		"--stop-timeout", "15min", "--token-file", "/root/agent.token", "--public-key", "a2V5", "--server", "https://api.example.com",
	}, &bytes.Buffer{})
	if err != nil {
		t.Fatal(err)
	}
	if o.Token != "tok'en" || o.User != "root" || o.Config != "/etc/agent/node.yaml" || o.CAFile != "/etc/agent/ca.pem" ||
		o.StopTimeout != "15min" || o.TokenFile != "/root/agent.token" || o.PublicKey != "a2V5" || o.Instance != "web-2" {
		t.Fatalf("%+v", o)
	}
}
