package install

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/internal/config"
)

// asUser — установка от пользователя u без root с домашним каталогом /home/u.
func asUser(f *fake, s *System) {
	s.Euid = func() int { return 1000 }
	s.User, s.Home = "u", "/home/u"
	f.users["u"] = true
	_ = os.MkdirAll(filepath.Join(f.root, "home/u"), 0o755)
}

func TestHomeLayout(t *testing.T) {
	l := HomeLayout("/home/u", "")
	if l.ConfigFile != "/home/u/.agent/agent.yaml" || l.DataDir != "/home/u/.agent/data" || l.Binary != "/home/u/.agent/bin/agent" ||
		l.UnitFile != "/home/u/.config/systemd/user/agent.service" || l.Link != "/home/u/.local/bin/agent" ||
		l.OptDir != "/home/u/.agent/bin" || !l.Home || l.User != "" {
		t.Fatalf("%+v", l)
	}
	b := HomeLayout("/home/u", "b")
	if b.ConfigFile != "/home/u/.agent-b/agent.yaml" || b.Service != "agent-b.service" || b.Link != "/home/u/.local/bin/agent-b" {
		t.Fatalf("%+v", b)
	}
	for exe, want := range map[string]string{
		"/home/u/.agent/bin/agent": "", "/home/u/.agent-b/bin/agent": "b", "/opt/agent-b/bin/agent": "b", "/opt/agent/bin/agent": "",
	} {
		if l, ok := Self(exe, "/home/u"); !ok || l.Instance != want || l.Home != strings.HasPrefix(exe, "/home") {
			t.Errorf("%s: %+v %v", exe, l, ok)
		}
	}
	for _, exe := range []string{"/home/u/agent", "/home/v/.agent/bin/agent", "/home/u/.agent-B/bin/agent", "/home/u/.other/bin/agent"} {
		if _, ok := Self(exe, "/home/u"); ok {
			t.Errorf("%s: не установленная программа", exe)
		}
	}
}

// --rootless: всё в ~/.agent, служба пользователя (systemd --user), работает
// без входа в систему (linger); удаление — без root.
func TestInstallRootless(t *testing.T) {
	ctx := context.Background()
	f, s := newFake(t, "systemctl", "loginctl")
	asUser(f, s)
	f.outputs["systemctl --user show-environment"] = "HOME=/home/u\n"
	o := Options{Binary: binary(t, "v1"), Version: "1.0.0", Server: "https://api.example.com", Token: "tok", Rootless: true}
	if err := Install(ctx, s, o); err != nil {
		t.Fatal(err)
	}
	l := HomeLayout("/home/u", "")
	if f.read(t, l.Binary) != "v1" || f.read(t, l.EnvFile) != "AGENT_ENROLL_TOKEN=tok\n" {
		t.Fatal("программа или agent.env")
	}
	if target, err := os.Readlink(filepath.Join(f.root, l.Link)); err != nil || target != l.Binary {
		t.Fatalf("ссылка: %q %v", target, err)
	}
	cfg, err := config.Load(filepath.Join(f.root, l.ConfigFile))
	if err != nil || cfg.DataDir != l.DataDir {
		t.Fatalf("agent.yaml: %+v %v", cfg, err)
	}
	unit := f.read(t, l.UnitFile)
	for _, want := range []string{"ExecStart=/home/u/.agent/bin/agent run -config /home/u/.agent/agent.yaml\n",
		"EnvironmentFile=-/home/u/.agent/agent.env\n", "Environment=AGENT_BOOT_GUARD=external\n", "WantedBy=default.target\n", "NoNewPrivileges=true\n"} {
		if !strings.Contains(unit, want) {
			t.Errorf("в службе нет %q:\n%s", want, unit)
		}
	}
	for _, not := range []string{"User=", "ProtectSystem", "network-online"} {
		if strings.Contains(unit, not) {
			t.Errorf("в службе пользователя %q:\n%s", not, unit)
		}
	}
	f.ran(t, "systemctl --user daemon-reload", "systemctl --user enable agent.service", "systemctl --user restart agent.service", "loginctl enable-linger u")
	if len(f.owners) != 0 {
		t.Fatalf("агент пользователя — без chown: %v", f.owners)
	}
	if !strings.Contains(f.read(t, l.JournalFile), "\nmode user\n") || !strings.Contains(f.out.String(), "journalctl --user -u agent -f") {
		t.Fatalf("журнал или подсказки:\n%s", f.out.String())
	}

	// Повторно без флагов — тем же способом.
	f.cmds = nil
	if err := Install(ctx, s, Options{Binary: binary(t, "v2")}); err != nil {
		t.Fatal(err)
	}
	f.ran(t, "systemctl --user restart agent.service")
	if f.exists(UnitFile) || f.exists(ConfigFile) {
		t.Fatal("агент пользователя не трогает системные пути")
	}
	// ~/.local/bin не в PATH — в подсказках полный путь.
	if !strings.Contains(f.out.String(), "/home/u/.agent/bin/agent status") {
		t.Fatalf("подсказки:\n%s", f.out.String())
	}
	// Служба стоит, а systemd --user сейчас недоступен — ошибка, узел не меняется.
	delete(f.outputs, "systemctl --user show-environment")
	if err := Install(ctx, s, Options{Binary: binary(t, "v3")}); err == nil || f.read(t, l.Binary) != "v2" {
		t.Fatalf("без systemd --user при стоящей службе: %v", err)
	}
	f.outputs["systemctl --user show-environment"] = "HOME=/home/u\n"

	f.cmds = nil
	if err := Uninstall(ctx, s, "", false, ""); err != nil {
		t.Fatal(err)
	}
	f.ran(t, "systemctl --user disable --now agent.service", filepath.Join(f.root, l.Binary)+" cleanup -config /home/u/.agent/agent.yaml",
		"systemctl --user daemon-reload")
	if f.exists(l.UnitFile) || f.exists(l.OptDir) || f.exists(l.Link) || !f.exists(l.ConfigFile) {
		t.Fatal("без --purge: служба и программа удалены, настройки остались")
	}
	if err := Uninstall(ctx, s, "", true, ""); err != nil {
		t.Fatal(err)
	}
	if f.exists(l.EtcDir) {
		t.Fatal("--purge: ~/.agent удалён")
	}
}

// Без systemd --user агент пользователя не ставится: ошибка с подсказкой, узел не изменён.
func TestInstallRootlessNoUserSystemd(t *testing.T) {
	f, s := newFake(t, "systemctl")
	asUser(f, s)
	err := Install(context.Background(), s, Options{Binary: binary(t, "v1"), Server: "https://api.example.com", Token: "tok", Rootless: true})
	if err == nil || !strings.Contains(err.Error(), "agent run") {
		t.Fatalf("ошибка: %v", err)
	}
	if len(f.cmds) != 0 || f.exists("/home/u/.agent") {
		t.Fatalf("узел изменён: %v", f.cmds)
	}
}

// Флаги, которые не подходят способу установки, — ошибка до изменений на узле.
func TestInstallModeRefuses(t *testing.T) {
	ctx := context.Background()
	cases := map[string]func(*fake, *System, *Options){
		"rootless от root":   func(_ *fake, _ *System, o *Options) { o.Rootless = true },
		"без root и флагов":  func(f *fake, s *System, _ *Options) { asUser(f, s) },
		"rootless sysctl":    func(f *fake, s *System, o *Options) { asUser(f, s); o.Rootless, o.Sysctls = true, []string{"vm.x=1"} },
		"rootless пакеты":    func(f *fake, s *System, o *Options) { asUser(f, s); o.Rootless, o.Packages = true, []string{"jq"} },
		"rootless privilege": func(f *fake, s *System, o *Options) { asUser(f, s); o.Rootless, o.Privileged = true, true },
		"rootless без HOME":  func(f *fake, s *System, o *Options) { asUser(f, s); s.Home, o.Rootless = "", true },
	}
	for name, mutate := range cases {
		f, s := newFake(t, "systemctl", "useradd")
		o := Options{Binary: binary(t, "v1"), Server: "https://api.example.com", Token: "t"}
		mutate(f, s, &o)
		if err := Install(ctx, s, o); err == nil {
			t.Errorf("%s: нет ошибки", name)
		}
		if len(f.cmds) != 0 || f.exists(Binary) || f.exists("/home/u/.agent") {
			t.Errorf("%s: узел изменён: %v", name, f.cmds)
		}
	}
}

// Агент, поставленный без службы (в журнале — mode manual): установка службой
// отказывает с подсказкой, как его остановить, и ничего не меняет; удаление —
// предупреждает.
func TestInstallOverManual(t *testing.T) {
	f, s := newFake(t, "systemctl")
	asUser(f, s)
	f.outputs["systemctl --user show-environment"] = "HOME=/home/u\n"
	l := HomeLayout("/home/u", "")
	journal := filepath.Join(f.root, l.JournalFile)
	if err := os.MkdirAll(filepath.Dir(journal), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(journal, []byte("mode manual\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := Install(context.Background(), s, Options{Binary: binary(t, "v1"), Server: "https://api.example.com", Token: "tok", Rootless: true})
	if err == nil || !strings.Contains(err.Error(), "crontab") || len(f.cmds) != 0 || f.exists(l.UnitFile) {
		t.Fatalf("установка поверх агента без службы: %v %v", err, f.cmds)
	}
	if err := Uninstall(context.Background(), s, "", false, ""); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(f.out.String(), "без службы") {
		t.Fatalf("удаление: нет предупреждения:\n%s", f.out.String())
	}
}
