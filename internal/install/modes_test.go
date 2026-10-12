package install

import (
	"context"
	"os"
	"path/filepath"
	"slices"
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
	// Служба стоит, а systemd --user сейчас недоступен — ошибка, а не агент без службы.
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

// Без systemd --user: агент запускается в фоне, автозапуск — @reboot в
// crontab; повторная установка перезапускает работающего, удаление
// останавливает его и убирает строку из crontab.
func TestInstallRootlessManual(t *testing.T) {
	ctx := context.Background()
	f, s := newFake(t, "crontab")
	asUser(f, s)
	f.outputs["crontab -l"] = "0 3 * * * /home/u/backup.sh\n"
	if err := Install(ctx, s, Options{Binary: binary(t, "v1"), Server: "https://api.example.com", Token: "tok", Rootless: true}); err != nil {
		t.Fatal(err)
	}
	l := HomeLayout("/home/u", "")
	line := "@reboot /home/u/.agent/bin/agent run -config /home/u/.agent/agent.yaml >>/home/u/.agent/agent.log 2>&1"
	if f.exists(l.UnitFile) || !f.exists(l.Binary) || !strings.Contains(f.read(t, l.JournalFile), "\nmode manual\n") {
		t.Fatalf("без службы: %v", f.cmds)
	}
	if got := f.stdin["crontab -"]; got != "0 3 * * * /home/u/backup.sh\n"+line+"\n" {
		t.Fatalf("crontab: %q", got)
	}
	if len(f.spawned) != 1 || f.spawned[0] != "/home/u/.agent/bin/agent run -config /home/u/.agent/agent.yaml >>/home/u/.agent/agent.log" {
		t.Fatalf("запуск в фоне: %v", f.spawned)
	}
	if out := f.out.String(); !strings.Contains(out, "запущен в фоне") || strings.Contains(out, "после перезагрузки запустите") {
		t.Fatalf("подсказка:\n%s", out)
	}
	if s.modeOf(l) != ModeManual {
		t.Fatal("способ запуска — из журнала")
	}

	// Повторно: строка в crontab не дублируется, работающий агент перезапускается (SIGUSR1).
	f.cmds, f.spawned = nil, nil
	f.running[l.DataDir] = 42
	if err := Install(ctx, s, Options{Binary: binary(t, "v2")}); err != nil {
		t.Fatal(err)
	}
	if slices.Contains(f.cmds, "crontab -") || len(f.spawned) != 0 || !slices.Contains(f.signals, "42 user defined signal 1") {
		t.Fatalf("повторно: %v %v %v", f.cmds, f.spawned, f.signals)
	}

	// Удаление: агент остановлен, строка убрана, остальное в crontab — как было.
	if err := Uninstall(ctx, s, "", false, ""); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(f.signals, "42 terminated") || f.running[l.DataDir] != 0 {
		t.Fatalf("остановка: %v", f.signals)
	}
	if got := f.outputs["crontab -l"]; got != "0 3 * * * /home/u/backup.sh\n" {
		t.Fatalf("crontab после удаления: %q", got)
	}
}

// Службы пользователя появились — агент без службы останавливается, ставится служба.
func TestInstallManualToUser(t *testing.T) {
	ctx := context.Background()
	f, s := newFake(t, "crontab", "systemctl")
	asUser(f, s)
	o := Options{Binary: binary(t, "v1"), Server: "https://api.example.com", Token: "tok", Rootless: true}
	if err := Install(ctx, s, o); err != nil {
		t.Fatal(err)
	}
	l := HomeLayout("/home/u", "")
	f.running[l.DataDir] = 7
	f.outputs["systemctl --user show-environment"] = "HOME=/home/u\n"
	if err := Install(ctx, s, Options{Binary: binary(t, "v2")}); err != nil {
		t.Fatal(err)
	}
	if !slices.Contains(f.signals, "7 terminated") || f.outputs["crontab -l"] != "" || !f.exists(l.UnitFile) {
		t.Fatalf("переход на службу: %v %q", f.signals, f.outputs["crontab -l"])
	}
	j := f.read(t, l.JournalFile)
	if !strings.Contains(j, "\nmode user\n") || strings.Contains(j, "crontab") {
		t.Fatalf("журнал:\n%s", j)
	}
}

// Без crontab: агент запущен, подсказка — запускать после перезагрузки самому.
func TestInstallRootlessManualNoCron(t *testing.T) {
	f, s := newFake(t)
	asUser(f, s)
	if err := Install(context.Background(), s, Options{Binary: binary(t, "v1"), Server: "https://api.example.com", Token: "tok", Rootless: true}); err != nil {
		t.Fatal(err)
	}
	if len(f.spawned) != 1 || !strings.Contains(f.out.String(), "после перезагрузки запустите снова") {
		t.Fatalf("%v\n%s", f.spawned, f.out.String())
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
		"rootless user":      func(f *fake, s *System, o *Options) { asUser(f, s); o.Rootless, o.User = true, "agent" },
		"rootless privilege": func(f *fake, s *System, o *Options) { asUser(f, s); o.Rootless, o.Privileged = true, true },
		"rootless rw-path":   func(f *fake, s *System, o *Options) { asUser(f, s); o.Rootless, o.RWPaths = true, []string{"/srv/x"} },
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
