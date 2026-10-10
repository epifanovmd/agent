package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/bundle"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/install"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/releases"
	"github.com/epifanovmd/agent/internal/scaffold"
	"github.com/epifanovmd/agent/internal/update"
)

func installCmd(args []string) error {
	o, err := install.ParseFlags(args, os.Stderr)
	if err != nil {
		return err
	}
	if runtime.GOOS != "linux" {
		return errors.New("agent install — для Linux с systemd; здесь запускайте агента напрямую: agent init, затем agent run")
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	if real, err := filepath.EvalSymlinks(exe); err == nil {
		exe = real
	}
	if o.Instance == "" {
		// Программа экземпляра (/opt/agent-ИМЯ/bin/agent) обновляет свой экземпляр.
		o.Instance, _ = install.InstanceOf(exe)
	}
	o.Binary, o.Version, o.BuiltinKey = exe, version, updateKey
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	// Папка агента: распакованный архив agent pack рядом с программой или
	// --env в папке агента (тогда — тот же архив, собранный здесь же).
	dir := filepath.Dir(exe)
	if _, packed, err := bundle.ReadInfo(dir); err != nil {
		return err
	} else if !packed && o.Env != "" {
		path, err := envConfig(o.Env, localDirs())
		if err != nil {
			return err
		}
		if dir, err = packHere(ctx, path, o.Env, exe); err != nil {
			return err
		}
		defer os.RemoveAll(filepath.Dir(dir))
		packed = true
	} else if !packed {
		return install.Install(ctx, install.Host(os.Stdout, os.Stderr), o)
	}
	tmp, err := bundle.Prepare(dir, runtime.GOOS, runtime.GOARCH, &o)
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	if err := install.Install(ctx, install.Host(os.Stdout, os.Stderr), o); err != nil {
		return err
	}
	if o.Token == "" && o.TokenFile == "" {
		fmt.Fprintln(os.Stderr, "agent: предупреждение: токена регистрации нет (--token или AGENT_ENROLL_TOKEN в файле переменных) — без него агент не зарегистрируется, если ещё не зарегистрирован")
	}
	cfg := config.Defaults()
	cfg.DataDir = install.Layout(o.Instance).DataDir
	updateNotice(cfg, true)
	return nil
}

// packHere — архив папки агента (файл настроек path) под эту машину, со
// значениями файлов переменных, распакованный во временный каталог; вернёт
// папку agent в нём.
func packHere(ctx context.Context, path, env, exe string) (string, error) {
	tmp, err := os.MkdirTemp("", "agent-pack-")
	if err != nil {
		return "", err
	}
	cfg := config.Check(path).Config
	keys, err := update.ParseKeys(append([]string{updateKey}, cfg.Update.Keys()...)...)
	if err != nil {
		return "", err
	}
	signing, err := releases.SigningKey()
	if err != nil {
		return "", err
	}
	archives, err := bundle.Pack(ctx, bundle.Options{
		Config: path, Env: env, Platforms: []bundle.Platform{{OS: runtime.GOOS, Arch: runtime.GOARCH}}, Out: tmp, WithEnv: true,
		Version: version, Self: exe, Keys: keys, Signing: signing, Client: releasesClient,
		Warn: func(s string) { fmt.Fprintln(os.Stderr, "agent: предупреждение:", s) },
	})
	if err != nil {
		os.RemoveAll(tmp)
		return "", err
	}
	x := filepath.Join(tmp, "x")
	if err := update.Extract(archives[0], x); err != nil {
		os.RemoveAll(tmp)
		return "", err
	}
	return filepath.Join(x, bundle.Dir), nil
}

func uninstallCmd(args []string) error {
	set := flag.NewFlagSet("uninstall", flag.ContinueOnError)
	purge := set.Bool("purge", false, "удалить ещё настройки, данные и то, что поставила установка (пакеты, пользователь службы)")
	instance := set.String("instance", "", "экземпляр агента (agent install --instance ИМЯ); без флага — по умолчанию или тот, чья это программа")
	if err := set.Parse(args); err != nil {
		return err
	}
	self, _ := os.Executable()
	if *instance == "" {
		*instance = selfInstance()
	}
	if *instance == "" && self != "" {
		// Из распакованного архива agent pack — экземпляр из его настроек.
		name, err := bundle.Instance(filepath.Dir(self))
		if err != nil {
			return err
		}
		*instance = name
	}
	if runtime.GOOS != "linux" {
		return errors.New("agent uninstall — для Linux с systemd")
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return install.Uninstall(ctx, install.Host(os.Stdout, os.Stderr), *instance, *purge, self)
}

// initCmd — agent init [ПАПКА]: папка агента (программа, настройки, образцы
// переменных, воркеры); agent init -config ФАЙЛ — один файл настроек с пояснениями.
func initCmd(args []string) error {
	set := flag.NewFlagSet("init", flag.ContinueOnError)
	path := set.String("config", "", "вместо папки агента — один файл настроек с пояснениями (например, "+config.DefaultPath()+")")
	var o config.TemplateOptions
	set.StringVar(&o.ServerURL, "server", "", "адрес бэкенда, например https://api.example.com")
	set.StringVar(&o.Token, "token", "", "токен регистрации (файл будет доступен только владельцу)")
	set.StringVar(&o.Name, "name", "", "имя агента (по умолчанию — имя машины; только с -config)")
	set.StringVar(&o.DataDir, "data-dir", "", "каталог данных (только с -config; по умолчанию "+config.DefaultDataDir()+")")
	force := set.Bool("force", false, "перезаписать готовые файлы")
	// Папка — где угодно среди флагов: agent init agent --server URL.
	var dirs []string
	for {
		if err := set.Parse(args); err != nil {
			return err
		}
		if set.NArg() == 0 {
			break
		}
		dirs = append(dirs, set.Arg(0))
		args = set.Args()[1:]
	}
	if len(dirs) > 1 {
		return fmt.Errorf("папка агента — одна, а не %s", strings.Join(dirs, ", "))
	}
	if *path != "" {
		if len(dirs) > 0 {
			return errors.New("-config — один файл настроек, без папки агента")
		}
		return initFile(*path, o, *force)
	}
	if o.Name != "" || o.DataDir != "" {
		return errors.New("--name и --data-dir — только с -config; в папке агента имя и данные — в agent.yaml")
	}
	dir := "."
	if len(dirs) == 1 {
		dir = dirs[0]
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	res, err := scaffold.Init(scaffold.Options{Dir: dir, Server: o.ServerURL, Token: o.Token, Binary: exe, Force: *force})
	if err != nil {
		return err
	}
	abs, _ := filepath.Abs(dir)
	fmt.Printf("Папка агента: %s\n", abs)
	if len(res.Created) > 0 {
		fmt.Println("  создано:   ", strings.Join(res.Created, ", "))
	}
	if len(res.Skipped) > 0 {
		fmt.Println("  уже были:  ", strings.Join(res.Skipped, ", "), "(не тронуты; перезаписать — --force)")
	}
	run := "./agent"
	if dir != "." {
		run = filepath.Join(dir, "agent")
	}
	fmt.Println("\nДальше:")
	if o.Token == "" {
		fmt.Printf("  впишите адрес бэкенда и токен регистрации в %s\n", filepath.Join(dir, ".env"))
	}
	fmt.Printf("  проверить:  %s config check\n  запустить:  %s run\n", run, run)
	fmt.Printf("  что здесь что — %s\n", filepath.Join(dir, "README.md"))
	return nil
}

// initFile — agent init -config ФАЙЛ: один файл настроек с пояснениями.
func initFile(path string, o config.TemplateOptions, force bool) error {
	if _, err := os.Stat(path); err == nil && !force {
		return fmt.Errorf("%s уже есть — проверьте его: agent config check; перезаписать: agent init --force", path)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	perm := os.FileMode(0o644)
	if o.Token != "" {
		perm = 0o600
	}
	tmp := path + ".new"
	if err := os.WriteFile(tmp, config.Template(o), perm); err != nil {
		return err
	}
	if err := os.Rename(tmp, path); err != nil {
		return err
	}
	_ = os.Chmod(path, perm)
	flagHint := ""
	if path != config.DefaultPath() {
		flagHint = " -config " + path
	}
	fmt.Printf("Создан %s.\n", path)
	if o.ServerURL == "" {
		fmt.Println("Впишите адрес сервера (server.url) — или задайте AGENT_SERVER_URL.")
	}
	if o.Token == "" {
		fmt.Println("Токен регистрации — enroll.token в файле или переменная AGENT_ENROLL_TOKEN.")
	}
	fmt.Printf("Проверить: agent config check%s\nЗапустить: agent run%s\n", flagHint, flagHint)
	return nil
}

func configCmd(args []string) error {
	if len(args) == 0 {
		return errors.New("agent config check | agent config init")
	}
	switch args[0] {
	case "check":
		return configCheck(args[1:])
	case "init":
		return initCmd(args[1:])
	}
	return fmt.Errorf("agent config: неизвестная команда %q (check | init)", args[0])
}

// configCheck — agent config check: файл, переменные, ошибки со строками,
// итоговые значения.
func configCheck(args []string) error {
	set := flag.NewFlagSet("config check", flag.ContinueOnError)
	flagPath := configFlag(set)
	if err := set.Parse(args); err != nil {
		return err
	}
	path, err := flagPath.path()
	if err != nil {
		return err
	}
	envFile := config.EnvFile(path)
	applied, envErr := config.ApplyEnvFile(envFile)
	res := config.Check(path)

	switch {
	case res.Source != nil && len(res.Source.Files) > 1:
		short := make([]string, len(res.Source.Files))
		for i, f := range res.Source.Files {
			short[i] = config.ShortPath(f)
		}
		fmt.Println("Файлы настроек (следующий поверх предыдущего):", strings.Join(short, " → "))
	case path != "":
		fmt.Println("Файл настроек:", path)
	default:
		fmt.Printf("Файл настроек: нет (%s не найден) — только переменные окружения. Создать: agent init\n", config.DefaultPath())
	}
	switch {
	case errors.Is(envErr, fs.ErrPermission):
		fmt.Printf("%s не прочитан (нужны права root) — токен и ключ из него не учтены\n", envFile)
	case envErr != nil:
		fmt.Printf("%s: %v\n", envFile, envErr)
	case len(applied) > 0:
		fmt.Printf("Переменные из %s: %s\n", envFile, strings.Join(applied, ", "))
	}
	if res.Source != nil && len(res.Source.EnvFiles) > 0 {
		short := make([]string, len(res.Source.EnvFiles))
		for i, f := range res.Source.EnvFiles {
			short[i] = config.ShortPath(f)
		}
		fmt.Println("Файлы переменных (envFiles):", strings.Join(short, ", "))
	}
	var fromShell []string
	for _, name := range res.FromEnv {
		if !contains(applied, name) {
			fromShell = append(fromShell, name)
		}
	}
	if len(fromShell) > 0 {
		fmt.Println("Из окружения (важнее файла):", strings.Join(fromShell, ", "))
	}
	warnings := res.Warnings
	if len(res.Errors) == 0 && res.Config.Enroll.Token == "" {
		_, err := os.Stat(filepath.Join(res.Config.DataDir, "credentials.json"))
		if errors.Is(err, fs.ErrNotExist) {
			warnings = append(warnings, config.Problem{Text: "агент ещё не зарегистрирован, а токена регистрации нет: задайте enroll.token или AGENT_ENROLL_TOKEN"})
		}
	}
	if len(warnings) > 0 {
		fmt.Println("\nЗамечания:")
		for _, p := range warnings {
			fmt.Println("  " + p.String())
		}
	}
	defer updateNotice(res.Config, true)
	if len(res.Errors) > 0 {
		fmt.Println("\nОшибки — с ними агент не запустится:")
		for _, p := range res.Errors {
			fmt.Println("  " + p.String())
		}
		return errQuiet
	}
	cfg := res.Config
	if cfg.Enroll.Token != "" {
		cfg.Enroll.Token = "<скрыт>"
	}
	doc, err := res.Annotated(cfg)
	if err != nil {
		return err
	}
	fmt.Printf("\nНастройки в порядке. Итоговые значения; в комментарии — откуда значение (без комментария — по умолчанию):\n\n")
	enc := yaml.NewEncoder(os.Stdout)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		return err
	}
	return enc.Close()
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if v == s {
			return true
		}
	}
	return false
}

// statusReport — agent status --json.
type statusReport struct {
	Running bool   `json:"running"`
	DataDir string `json:"dataDir"`
	// Error — почему агент не считается работающим.
	Error  string           `json:"error,omitempty"`
	Status *app.AgentStatus `json:"status,omitempty"`
	// Denied — нет прав на каталог данных: работает ли агент, неизвестно.
	Denied bool `json:"denied,omitempty"`
	// LastExit — ошибка, с которой агент завершился в последний раз.
	LastExit *app.ExitError `json:"lastExit,omitempty"`
	// unit — экземпляр: его служба для подсказок.
	unit install.Paths
}

// status — работает ли агент (HEALTHCHECK в Docker: код выхода 0 — да),
// связь, воркеры, последняя ошибка.
func status(args []string) error {
	set := flag.NewFlagSet("status", flag.ContinueOnError)
	flagPath := configFlag(set)
	asJSON := set.Bool("json", false, "вывести JSON")
	if err := set.Parse(args); err != nil {
		return err
	}
	path, err := flagPath.path()
	if err != nil {
		return err
	}
	l, _ := flagPath.layout()
	_, _ = config.ApplyEnvFile(config.EnvFile(path))
	// Нужен только каталог данных: ошибки других настроек статусу не мешают.
	dataDir := config.Check(path).Config.DataDir
	st, err := app.Status(dataDir)
	rep := statusReport{Running: err == nil, DataDir: dataDir, unit: l}
	if st.PID != 0 {
		rep.Status = &st
	}
	if err != nil {
		rep.Error = err.Error()
		if errors.Is(err, fs.ErrPermission) {
			rep.Error = fmt.Sprintf("нет доступа к %s — запустите через sudo", dataDir)
			rep.Denied = true
		}
		rep.LastExit = app.LastExit(dataDir)
	}
	if *asJSON {
		enc := json.NewEncoder(os.Stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(rep)
	} else {
		printStatus(os.Stdout, rep, time.Now())
		cfg := config.Check(path).Config
		updateNotice(cfg, false)
	}
	if !rep.Running {
		return errQuiet
	}
	return nil
}

func printStatus(w io.Writer, rep statusReport, now time.Time) {
	when := func(ms int64) string {
		t := time.UnixMilli(ms)
		return t.Format("2006-01-02 15:04:05") + " (" + ago(now.Sub(t)) + ")"
	}
	if !rep.Running && rep.Denied {
		fmt.Fprintf(w, "Не удалось проверить агента: %s.\n", rep.Error)
		return
	}
	if !rep.Running {
		fmt.Fprintf(w, "Агент не работает: %s.\n", rep.Error)
		fmt.Fprintln(w, "Каталог данных:", rep.DataDir)
		if e := rep.LastExit; e != nil {
			fmt.Fprintf(w, "Последний раз завершился с ошибкой %s:\n  %s\n", when(e.At), short(e.Error))
		}
		if u := rep.unit; u.UnitFile != "" && isFile(u.UnitFile) {
			fmt.Fprintf(w, "Служба: systemctl status %s; лог: agent logs%s (journalctl -u %s -n 100)\n", u.Unit, u.Flag(), u.Unit)
		}
		return
	}
	st := rep.Status
	fmt.Fprintf(w, "Агент работает: pid %d, версия %s, запущен %s\n", st.PID, st.Version, when(st.StartedAt))
	name := st.Name
	if st.AgentID != "" {
		name += " (id " + st.AgentID + ")"
	} else {
		name += " (ещё не зарегистрирован)"
	}
	fmt.Fprintln(w, "Имя:       ", name)
	if st.Online {
		fmt.Fprintln(w, "Связь:      есть —", st.Server)
	} else {
		fmt.Fprintln(w, "Связь:      нет — подключается к", st.Server)
	}
	fmt.Fprintf(w, "Outbox:     важных сообщений ждут подтверждения сервера: %d\n", st.Outbox)
	if len(st.Workers) > 0 {
		fmt.Fprintln(w, "Воркеры:")
		for _, wk := range st.Workers {
			fmt.Fprintf(w, "  %-14s %-9s %s\n", wk.Name, wk.State, workerNote(wk))
		}
	}
	if st.LastError != "" {
		fmt.Fprintf(w, "Последняя ошибка связи или регистрации %s:\n  %s\n", when(st.LastErrorAt), short(st.LastError))
	}
	fmt.Fprintln(w, "Каталог данных:", rep.DataDir)
}

func workerNote(w message.WorkerStatus) string {
	var parts []string
	if w.Version != "" {
		parts = append(parts, "версия "+w.Version)
	}
	if w.Builtin {
		parts = append(parts, "встроенный")
	}
	if w.Release {
		parts = append(parts, "сборка с сервера")
	}
	if w.Restarts > 0 {
		parts = append(parts, "перезапусков: "+strconv.Itoa(w.Restarts))
	}
	if h := w.Health; h != nil && !h.OK {
		parts = append(parts, strings.TrimSpace("нездоров "+h.Message))
	}
	keys := make([]string, 0, len(w.Configs))
	for k := range w.Configs {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		c := w.Configs[k]
		mark := "применяется"
		switch {
		case c.OK != nil && *c.OK:
			mark = "применено"
		case c.OK != nil && c.Error != nil:
			mark = "ошибка " + c.Error.Code
		}
		parts = append(parts, fmt.Sprintf("%s v%d %s", k, c.Version, mark))
	}
	return strings.Join(parts, ", ")
}

// short — первая строка ошибки, не длиннее 300 символов (полностью — agent status --json).
func short(s string) string {
	s, _, _ = strings.Cut(strings.TrimSpace(s), "\n")
	if r := []rune(s); len(r) > 300 {
		s = string(r[:300]) + "…"
	}
	return s
}

// ago — «5 мин назад».
func ago(d time.Duration) string {
	switch {
	case d < time.Minute:
		return "только что"
	case d < time.Hour:
		return fmt.Sprintf("%d мин назад", int(d.Minutes()))
	case d < 48*time.Hour:
		return fmt.Sprintf("%d ч %d мин назад", int(d.Hours()), int(d.Minutes())%60)
	}
	return fmt.Sprintf("%d дн назад", int(d.Hours()/24))
}

func isFile(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// logs — лог службы: journalctl -u agent[-ИМЯ]. Без службы systemd — где искать лог.
func logs(args []string) error {
	set := flag.NewFlagSet("logs", flag.ContinueOnError)
	follow := set.Bool("f", false, "следить за новыми записями")
	lines := set.Int("n", 100, "сколько последних записей показать")
	flagPath := configFlag(set)
	if err := set.Parse(args); err != nil {
		return err
	}
	l, err := flagPath.layout()
	if err != nil {
		return err
	}
	journalctl, err := exec.LookPath("journalctl")
	if err != nil || !isFile(l.UnitFile) {
		fmt.Println("Агент пишет лог в stderr, своих файлов лога нет:")
		fmt.Printf("  служба systemd — journalctl -u %s -f (служба не найдена на этой машине);\n", l.Unit)
		fmt.Println("  контейнер — docker logs -f <контейнер>;")
		fmt.Println("  agent run в терминале — лог в этом терминале.")
		return errQuiet
	}
	argv := []string{"journalctl", "-u", l.Unit, "-n", strconv.Itoa(*lines), "--no-pager"}
	if *follow {
		argv = append(argv, "-f")
	}
	return syscall.Exec(journalctl, argv, os.Environ())
}
