package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"slices"
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
	"github.com/epifanovmd/agent/internal/releases"
	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/protocol"
)

func installCmd(args []string) error {
	o, err := install.ParseFlags(args, os.Stderr)
	if err != nil {
		return err
	}
	if runtime.GOOS != "linux" {
		return errors.New("agent install — для Linux (служба systemd или агент пользователя без root); здесь запускайте агента напрямую: agent run")
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	if real, err := filepath.EvalSymlinks(exe); err == nil {
		exe = real
	}
	sys := install.Host(os.Stdout, os.Stderr)
	if o.Instance == "" {
		// Программа экземпляра (/opt/agent-ИМЯ/bin/agent, ~/.agent-ИМЯ/bin/agent) обновляет свой экземпляр.
		if l, ok := install.Self(exe, sys.Home); ok {
			o.Instance = l.Instance
		}
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
		return install.Install(ctx, sys, o)
	}
	// Экземпляр из настроек архива — до выбора путей установки.
	if o.Instance == "" {
		if o.Instance, err = bundle.Instance(dir); err != nil {
			return err
		}
	}
	if err := install.Plan(sys, &o); err != nil {
		return err
	}
	tmp, err := bundle.Prepare(dir, runtime.GOOS, runtime.GOARCH, &o)
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	if err := install.Install(ctx, sys, o); err != nil {
		return err
	}
	if o.Token == "" && o.TokenFile == "" {
		fmt.Fprintln(os.Stderr, "agent: предупреждение: токена регистрации нет (--token или AGENT_ENROLL_TOKEN в файле переменных) — без него агент не зарегистрируется, если ещё не зарегистрирован")
	}
	cfg := config.Defaults()
	cfg.DataDir = o.Layout().DataDir
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
		if l, ok := selfLayout(); ok {
			*instance = l.Instance
		}
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
		return errors.New("agent uninstall — для Linux")
	}
	if err := install.CheckInstance(*instance); err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	return install.Uninstall(ctx, install.Host(os.Stdout, os.Stderr), *instance, *purge, self)
}

func configCmd(args []string) error {
	if len(args) == 0 || args[0] != "check" {
		return errors.New("agent config check [-config ФАЙЛ | --env ИМЯ]")
	}
	return configCheck(args[1:])
}

// configCheck — agent config check: проверить настройки и показать итоговые
// значения.
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
	_, envErr := config.ApplyEnvFile(envFile)
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
		fmt.Printf("Файл настроек: нет (%s не найден) — только переменные окружения\n", config.DefaultPath())
	}
	switch {
	case errors.Is(envErr, fs.ErrPermission):
		fmt.Printf("%s не прочитан (нужны права root) — токен и ключ из него не учтены\n", envFile)
	case envErr != nil:
		fmt.Printf("%s: %v\n", envFile, envErr)
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
	fmt.Printf("\nНастройки в порядке. Итоговые значения:\n\n")
	enc := yaml.NewEncoder(os.Stdout)
	enc.SetIndent(2)
	if err := enc.Encode(cfg); err != nil {
		return err
	}
	return enc.Close()
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
	if err := parseOnly(set, args); err != nil {
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
		if u := rep.unit; install.ModeOf(u) != "" {
			fmt.Fprintf(w, "Служба: %s status %s; лог: agent logs%s\n", u.Systemctl(), u.Unit, u.Flag())
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
	switch st.Status {
	case protocol.StatusPending:
		fmt.Fprintln(w, "Состояние:  ждёт регистрации и подтверждения бэкендом — воркеры не запущены")
	case protocol.StatusActive:
		fmt.Fprintln(w, "Состояние:  работает (active)")
	case protocol.StatusRevoked:
		fmt.Fprintf(w, "Состояние:  отозван бэкендом %s — воркеры остановлены; вернуть — новая установка с токеном\n", when(st.StatusSince))
	}
	switch {
	case st.Server == "":
		fmt.Fprintln(w, "Связь:      без бэкенда — желаемое из файлов desired/")
	case st.Status == protocol.StatusRevoked:
		fmt.Fprintln(w, "Связь:      прекращена —", st.Server)
	case st.Online:
		fmt.Fprintln(w, "Связь:      есть —", st.Server)
	default:
		fmt.Fprintln(w, "Связь:      нет — подключается к", st.Server)
	}
	if st.Server != "" {
		fmt.Fprintf(w, "Очередь:    итогов и событий ждут подтверждения бэкенда: %d; желаемое rev %d\n", st.Outbox, st.DesiredRev)
	}
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

func workerNote(w protocol.WorkerState) string {
	var parts []string
	if w.Version != "" {
		parts = append(parts, "версия "+w.Version)
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

// logs — лог службы: journalctl -u agent[-ИМЯ] (у агента пользователя —
// journalctl --user).
func logs(args []string) error {
	set := flag.NewFlagSet("logs", flag.ContinueOnError)
	follow := set.Bool("f", false, "следить за новыми записями")
	lines := set.Int("n", 100, "сколько последних записей показать")
	name := set.String("worker", "", "последние записи журнала воркера (через локальный API агента)")
	flagPath := configFlag(set)
	if err := set.Parse(args); err != nil {
		return err
	}
	if *name != "" {
		return workerLogs(flagPath, *name, *lines)
	}
	l, err := flagPath.layout()
	if err != nil {
		return err
	}
	var argv []string
	if install.ModeOf(l) != "" {
		argv = []string{"journalctl", "-u", l.Unit, "-n", strconv.Itoa(*lines), "--no-pager"}
		if l.Home {
			argv = slices.Insert(argv, 1, "--user")
		}
		if *follow {
			argv = append(argv, "-f")
		}
	}
	prog := ""
	if argv != nil {
		prog, err = exec.LookPath(argv[0])
	}
	if argv == nil || err != nil {
		fmt.Println("Агент пишет лог в stderr, своих файлов лога нет:")
		fmt.Printf("  служба systemd — journalctl -u %s -f, служба пользователя — journalctl --user -u %s -f (служба не найдена на этой машине);\n", l.Unit, l.Unit)
		fmt.Println("  контейнер — docker logs -f <контейнер>;")
		fmt.Println("  agent run в терминале — лог в этом терминале.")
		return errQuiet
	}
	return syscall.Exec(prog, argv, os.Environ())
}

// workerLogs — последние записи журнала воркера name из памяти агента.
func workerLogs(t target, name string, lines int) error {
	cfg, _, err := loadForTool(t)
	if err != nil {
		return err
	}
	c, err := app.NewAdmin(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("агент не работает: %w", err)
	}
	var res protocol.LogsResult
	q := url.Values{"worker": {name}, "lines": {strconv.Itoa(lines)}}
	if err := c.Do(context.Background(), http.MethodGet, "/logs?"+q.Encode(), nil, &res); err != nil {
		return err
	}
	for _, e := range res.Entries {
		fmt.Printf("%s %-5s %s\n", time.UnixMilli(e.At).Format("2006-01-02 15:04:05"), e.Level, e.Msg)
	}
	return nil
}
