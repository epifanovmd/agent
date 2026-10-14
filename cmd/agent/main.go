// Агент: запускает воркеры и следит за ними, держит связь с бэкендом, передаёт воркерам
// желаемое и команды, собирает метрики узла, обновляется сам; без бэкенда работает по файлам.
//
//	agent [run] [-config ФАЙЛ | --env ИМЯ]   работа (по умолчанию); SIGHUP — перечитать настройки,
//	                                   SIGUSR1 — перезапуск, SIGTERM/SIGINT — остановка
//	agent restart [--worker ИМЯ]       перезапустить агента (воркеры работают дальше) или воркер
//	agent command ВОРКЕР ТИП [JSON]    команда воркеру через локальный API, вывод — итог
//	agent stop-workers [--cleanup]     остановить воркеры, оставшиеся работать после остановки агента;
//	                                   --cleanup — затем ./cleanup воркеров (его вызывает uninstall)
//	agent install --server URL --token ТОКЕН [флаги]   поставить службой systemd (Linux, root);
//	                                   --rootless — агент пользователя без root
//	agent uninstall [--purge]          удалить службу (с --purge — настройки и данные)
//	                                   --instance ИМЯ у install, uninstall, status, logs, restart,
//	                                   stop-workers, command, config check — экземпляр агента на узле;
//	                                   --env ИМЯ у run, status, restart, stop-workers, command,
//	                                   config check — файл agent.ИМЯ.yaml папки агента
//	agent config check                 проверить настройки и показать итоговые значения
//	agent status [--json]              работает ли агент, связь, воркеры (код выхода 0 — работает)
//	agent logs [-f] [-n N] [--worker ИМЯ]   лог службы (journalctl) или журнал воркера
//	agent upgrade [--check] [--version X]   поставить новую версию агента из каталога сборок
//	agent pack [--env ИМЯ] [--platform linux/amd64,…]   архив папки агента для узла
//	agent manifest DIR VERSION [--worker NAME=VERSION[,…]]…   manifest.json сборок в DIR
//	agent version                      версия
//	agent boot-guard BINARY            откат версии, не дошедшей до связи (ExecStartPre)
//
// Файл настроек — -config, иначе --env ИМЯ (agent.ИМЯ.yaml папки агента), иначе
// --instance ИМЯ (/etc/agent-ИМЯ/agent.yaml; у пользователя без root — его
// ~/.agent-ИМЯ/agent.yaml, если он есть), иначе AGENT_CONFIG, иначе экземпляр,
// чья это программа (/opt/agent-ИМЯ/bin/agent, ~/.agent/bin/agent), иначе
// agent.yaml папки агента (рядом с программой, затем в текущем каталоге),
// иначе ~/.agent/agent.yaml агента пользователя, иначе /etc/agent/agent.yaml
// (на macOS ~/.agent/agent.yaml), если он есть. Файл может опираться на другой
// (extends) и брать значения из файлов переменных (envFiles).
//
// Ключ проверки обновлений вшивается при сборке: -ldflags "-X main.updateKey=<base64>";
// ключи из настроек (update.publicKey, update.publicKeys) действуют вместе с ним.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"slices"
	"strings"
	"syscall"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/bundle"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/fsutil"
	"github.com/epifanovmd/agent/internal/install"
	"github.com/epifanovmd/agent/internal/sysmetrics"
	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/protocol"
)

// version — задаётся при сборке: -ldflags "-X main.version=1.2.3".
var version = "dev"

// updateKey — открытый ключ проверки сборок (base64 Ed25519), вшитый при
// сборке: -ldflags "-X main.updateKey=…". Ключи из настроек действуют вместе с ним.
var updateKey = ""

// errQuiet — команда уже всё сказала сама: только код выхода 1.
var errQuiet = errors.New("")

const usage = `agent — агент для узлов: воркеры, связь с бэкендом, желаемое и команды, метрики, обновление.

Команды:
  agent run [-config ФАЙЛ | --env ИМЯ]
                                 работать (по умолчанию)
  agent install --server URL --token ТОКЕН [флаги]
                                 поставить службой systemd (Linux, sudo); --rootless — для пользователя
                                 без root (~/.agent); флаги — agent install -h
  agent uninstall [--purge]      удалить службу; --purge — ещё настройки и данные
  agent config check [-config ФАЙЛ | --env ИМЯ]
                                 проверить настройки и показать итоговые значения
  agent status [--json]          работает ли агент, связь, воркеры, последняя ошибка
  agent logs [-f] [-n N] [--worker ИМЯ]
                                 лог службы; --worker — последние записи журнала воркера
  agent restart [--worker ИМЯ] [--force]
                                 перезапустить агента (воркеры работают дальше) или воркер
  agent command ВОРКЕР ТИП [JSON] [--timeout 30s]
                                 команда воркеру; вывод — итог
  agent stop-workers [--cleanup] остановить воркеры, оставшиеся после остановки агента (агент не работает);
                                 --cleanup — затем ./cleanup воркеров (убрать созданное ими на узле)
  agent pack [--env ИМЯ | -config ФАЙЛ] [--platform linux/amd64,linux/arm64] [--out КАТАЛОГ] [--with-env]
                                 архив папки агента для узла: программа под его платформу, настройки,
                                 воркеры и их сборки; на узле — tar xzf …, sudo ./agent install
  agent manifest КАТАЛОГ ВЕРСИЯ [--worker ИМЯ=ВЕРСИЯ[,stopTimeout=…][,command=…]]…
                                 manifest.json сборок агента и воркеров в каталоге (подпись — AGENT_SIGNING_KEY)
  agent upgrade [--check] [--version X] [--force]
                                 поставить новую версию агента (подпись проверяется); --check — только узнать
  agent version                  версия

Несколько агентов на узле: agent install --instance ИМЯ — экземпляр со своими путями
(/etc/agent-ИМЯ, /var/lib/agent-ИМЯ, /opt/agent-ИМЯ, служба agent-ИМЯ); остальные команды
с --instance ИМЯ (или через ссылку agent-ИМЯ) работают с ним.

Файл настроек: -config, иначе --env ИМЯ (agent.ИМЯ.yaml папки агента), иначе --instance,
иначе AGENT_CONFIG, иначе agent.yaml рядом с программой или в текущем каталоге, иначе
~/.agent/agent.yaml (агент пользователя), иначе %s (если есть). Файл может опираться
на другой (extends: agent.yaml) и брать значения ${ИМЯ} из файлов переменных (envFiles: [.env]).
`

func main() {
	args := os.Args[1:]
	cmd := "run"
	if len(args) > 0 && !strings.HasPrefix(args[0], "-") {
		cmd, args = args[0], args[1:]
	}
	var err error
	switch cmd {
	case "run":
		err = run(args)
	case "install":
		err = installCmd(args)
	case "uninstall":
		err = uninstallCmd(args)
	case "config":
		err = configCmd(args)
	case "status":
		err = status(args)
	case "logs":
		err = logs(args)
	case "command":
		err = commandCmd(args)
	case "restart":
		err = restartCmd(args)
	case "stop-workers":
		err = stopWorkersCmd(args)
	case "upgrade":
		err = upgradeCmd(args)
	case "pack":
		err = packCmd(args)
	case "manifest":
		err = manifestCmd(args)
	case "version":
		fmt.Println(version)
	case "boot-guard":
		err = bootGuard(args)
	case "help":
		fmt.Printf(usage, config.DefaultPath())
	case sysmetrics.SensorsCommand:
		// Служебная: показания датчиков (macOS) — агент запускает её сам дочерним процессом.
		_, err = sysmetrics.Dispatch(os.Args[1:])
	default:
		fmt.Fprintf(os.Stderr, usage, config.DefaultPath())
		err = fmt.Errorf("неизвестная команда %q", cmd)
	}
	if errors.Is(err, flag.ErrHelp) {
		return
	}
	if err != nil {
		if err != errQuiet {
			fmt.Fprintln(os.Stderr, "agent:", err)
		}
		os.Exit(1)
	}
}

// parseOnly — флаги команды без позиционных аргументов: лишнее — ошибка (agent -config ФАЙЛ status
// без неё запустил бы агента, а «status» молча отбросил бы: флаги идут после команды).
func parseOnly(fs *flag.FlagSet, args []string) error {
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() > 0 {
		return fmt.Errorf("%s: лишние аргументы %q — команда идёт первой, флаги после неё (agent status -config ФАЙЛ)", fs.Name(), fs.Args())
	}
	return nil
}

// target — чей агент: флаги -config, --env и --instance у команды.
type target struct{ config, env, instance *string }

// configFlag — флаги -config (он же --config), --env и --instance у команды.
func configFlag(fs *flag.FlagSet) target {
	return target{
		config:   fs.String("config", "", "файл настроек (AGENT_CONFIG; по умолчанию agent.yaml рядом с программой или в текущем каталоге, иначе "+config.DefaultPath()+", если есть)"),
		env:      fs.String("env", "", "окружение папки агента: --env prod — файл agent.prod.yaml рядом с программой или в текущем каталоге"),
		instance: fs.String("instance", "", "экземпляр агента на узле (agent install --instance ИМЯ): его файл настроек и служба"),
	}
}

// layout — экземпляр: тот, чья это программа (/opt/agent-ИМЯ/bin/agent,
// ~/.agent/bin/agent, в том числе по ссылке), если флагов нет; иначе
// --instance (или по умолчанию) — от пользователя его собственный агент,
// если он поставлен, иначе системный.
func (t target) layout() (install.Paths, error) {
	name := *t.instance
	if name == "" && *t.config == "" && *t.env == "" && os.Getenv("AGENT_CONFIG") == "" {
		if l, ok := selfLayout(); ok {
			return l, nil
		}
	}
	if err := install.CheckInstance(name); err != nil {
		return install.Paths{}, err
	}
	return install.Find(name, userHome(), os.Geteuid()), nil
}

// path — файл настроек: -config, --env (файл окружения в папке агента),
// --instance (или экземпляр программы), AGENT_CONFIG, agent.yaml в папке
// агента, агент пользователя (~/.agent/agent.yaml), файл по умолчанию.
// -config, --env и --instance — что-то одно.
func (t target) path() (string, error) {
	set := 0
	for _, v := range []string{*t.config, *t.env, *t.instance} {
		if v != "" {
			set++
		}
	}
	if set > 1 {
		return "", errors.New("-config, --env и --instance — что-то одно")
	}
	if *t.env != "" {
		return envConfig(*t.env, localDirs())
	}
	l, err := t.layout()
	if err != nil {
		return "", err
	}
	if l.Instance != "" {
		return l.ConfigFile, nil
	}
	var local []string
	for _, dir := range localDirs() {
		local = append(local, filepath.Join(dir, config.FileName))
	}
	if l.Home {
		local = append(local, l.ConfigFile)
	}
	return config.ResolvePath(*t.config, local...), nil
}

// envConfig — файл настроек окружения env в одном из каталогов dirs.
func envConfig(env string, dirs []string) (string, error) {
	if !protocol.ValidName(env) {
		return "", fmt.Errorf("--env: имя окружения — строчная латиница, цифры и «-», первая — буква, а не %q", env)
	}
	name := config.EnvConfigName(env)
	for _, dir := range dirs {
		if p := filepath.Join(dir, name); fsutil.IsFile(p) {
			return p, nil
		}
	}
	if len(dirs) == 0 {
		l, ok := selfLayout()
		if !ok {
			l = install.Layout("")
		}
		return "", fmt.Errorf("--env: у установленного агента нет папки агента — его настройки в %s", l.ConfigFile)
	}
	return "", fmt.Errorf("--env %s: нет %s (искал в %s)", env, name, strings.Join(dirs, ", "))
}

// localDirs — папка агента: каталог программы (у программы из bin/ архива — каталог выше),
// затем текущий. У
// установленной программы (/opt/agent…/bin, ~/.agent…/bin) папки нет — её
// настройки в /etc/agent… или ~/.agent….
func localDirs() []string {
	var dirs []string
	if exe, err := os.Executable(); err == nil {
		if real, err := filepath.EvalSymlinks(exe); err == nil {
			exe = real
		}
		if _, ok := install.Self(exe, userHome()); ok {
			return nil
		}
		dirs = append(dirs, bundle.HomeOf(exe))
	}
	if wd, err := os.Getwd(); err == nil && !slices.Contains(dirs, wd) {
		dirs = append(dirs, wd)
	}
	return dirs
}

// selfLayout — установленный экземпляр, которому принадлежит запущенная
// программа (ok=false — программа не установленного агента).
func selfLayout() (install.Paths, bool) {
	exe, err := os.Executable()
	if err != nil {
		return install.Paths{}, false
	}
	if real, err := filepath.EvalSymlinks(exe); err == nil {
		exe = real
	}
	return install.Self(exe, userHome())
}

// userHome — домашний каталог пользователя ("" — нет).
func userHome() string {
	h, _ := os.UserHomeDir()
	return h
}

func run(args []string) error {
	// Первым делом: новая версия, не связавшаяся с сервером за несколько
	// запусков, откатывается — даже если падает на конфигурации.
	exe, exeErr := os.Executable()
	if exeErr == nil {
		if real, err := filepath.EvalSymlinks(exe); err == nil {
			exe = real
		}
		if err := update.Boot(update.NewPaths(exe)); err != nil {
			fmt.Fprintln(os.Stderr, "agent: проверка обновления:", err)
		}
	}
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	flagPath := configFlag(fs)
	if err := parseOnly(fs, args); err != nil {
		return err
	}
	path, err := flagPath.path()
	if err != nil {
		return err
	}
	if path != "" {
		// agent.env рядом с настройками (токен, ключи) — как у службы; окружение важнее.
		if _, err := config.ApplyEnvFile(config.EnvFile(path)); err != nil {
			return err
		}
	}
	cfg, err := config.Load(path)
	if err != nil {
		if path == "" {
			return fmt.Errorf("%w\nфайла настроек нет (ни agent.yaml рядом с программой или в текущем каталоге, ни %s): создайте его (server.url, enroll.token, workers) или задайте AGENT_SERVER_URL и AGENT_ENROLL_TOKEN", err, config.DefaultPath())
		}
		return err
	}
	app.BuiltinUpdateKey = updateKey
	if l, ok := selfLayout(); ok {
		app.InstallMode = install.ModeOf(l)
	}
	agent, err := app.New(cfg, version)
	if err != nil {
		app.RecordExit(cfg.DataDir, err)
		return err
	}
	// SIGTERM, SIGINT — остановка; SIGUSR1 — перезапуск; SIGHUP (systemctl reload) — перечитать настройки.
	err = agent.Serve(path)
	if errors.Is(err, app.ErrRestart) || errors.Is(err, context.Canceled) {
		// Перезапуск — дело менеджера процесса (systemd Restart=always, Docker restart).
		return nil
	}
	app.RecordExit(cfg.DataDir, err)
	return err
}

// loadForTool — настройки для служебных команд (status, restart, command): файл,
// agent.env рядом с ним (как у службы), окружение.
func loadForTool(t target) (config.Config, string, error) {
	path, err := t.path()
	if err != nil {
		return config.Config{}, "", err
	}
	_, _ = config.ApplyEnvFile(config.EnvFile(path))
	cfg, err := config.Load(path)
	return cfg, path, err
}

// bootGuard — вызывает прежняя версия перед запуском новой (systemd
// ExecStartPre): откат работает, даже если новая падает до main.
func bootGuard(args []string) error {
	if len(args) != 1 {
		return errors.New("boot-guard BINARY")
	}
	rolledBack, err := update.Guard(update.NewPaths(args[0]))
	if rolledBack {
		fmt.Fprintln(os.Stderr, "agent: новая версия не вышла на связь — возвращена прежняя")
	}
	return err
}

// restartCmd — `agent restart`: SIGUSR1 работающему агенту (pid — из локального API): он
// выходит, менеджер службы запускает его снова, воркеры работают дальше. --worker — перезапуск
// воркера через локальный API (итог — после его регистрации).
func restartCmd(args []string) error {
	fs := flag.NewFlagSet("restart", flag.ContinueOnError)
	flagPath := configFlag(fs)
	name := fs.String("worker", "", "перезапустить воркер, а не агента")
	force := fs.Bool("force", false, "не ждать, пока воркер занят")
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, _, err := loadForTool(flagPath)
	if err != nil {
		return err
	}
	if *name != "" {
		c, err := app.NewAdmin(cfg.DataDir)
		if err != nil {
			return fmt.Errorf("агент не работает: %w", err)
		}
		path := "/workers/" + url.PathEscape(*name) + "/restart"
		if *force {
			path += "?force=true"
		}
		if err := c.Do(context.Background(), http.MethodPost, path, nil, nil); err != nil {
			return err
		}
		fmt.Printf("воркер %s перезапущен\n", *name)
		return nil
	}
	st, err := app.Status(cfg.DataDir)
	if err != nil || st.PID <= 0 {
		return fmt.Errorf("агент не работает: %v", err)
	}
	if err := syscall.Kill(st.PID, syscall.SIGUSR1); err != nil {
		return fmt.Errorf("сигнал агенту (pid %d): %w", st.PID, err)
	}
	fmt.Printf("агент (pid %d) перезапускается\n", st.PID)
	return nil
}

// commandCmd — `agent command ВОРКЕР ТИП [JSON]`: команда воркеру через локальный API;
// вывод — итог (JSON), ошибка — код выхода 1.
func commandCmd(args []string) error {
	fs := flag.NewFlagSet("command", flag.ContinueOnError)
	flagPath := configFlag(fs)
	timeout := fs.Duration("timeout", protocol.DefaultRequestTimeout, "сколько ждать итога")
	var pos []string
	for len(args) > 0 {
		if err := fs.Parse(args); err != nil {
			return err
		}
		if fs.NArg() == 0 {
			break
		}
		pos, args = append(pos, fs.Arg(0)), fs.Args()[1:]
	}
	if len(pos) < 2 || len(pos) > 3 {
		return errors.New("agent command ВОРКЕР ТИП [JSON]")
	}
	req := app.CommandRequest{Type: pos[1], TimeoutMs: timeout.Milliseconds()}
	if len(pos) == 3 {
		if !json.Valid([]byte(pos[2])) {
			return fmt.Errorf("args: %q — не JSON", pos[2])
		}
		req.Args = json.RawMessage(pos[2])
	}
	cfg, _, err := loadForTool(flagPath)
	if err != nil {
		return err
	}
	c, err := app.NewAdmin(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("агент не работает: %w", err)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	var res protocol.Result
	if err := c.Do(ctx, http.MethodPost, "/workers/"+url.PathEscape(pos[0])+"/command", req, &res); err != nil {
		return err
	}
	if !res.OK {
		return res.Error
	}
	if len(res.Result) > 0 {
		// Итог — с отступами и без \uXXXX: как прислал воркер, но читаемо.
		var v any
		if json.Unmarshal(res.Result, &v) == nil {
			if pretty, err := json.MarshalIndent(v, "", "  "); err == nil {
				res.Result = pretty
			}
		}
		fmt.Println(string(res.Result))
	}
	return nil
}

// stopWorkersCmd — `agent stop-workers`: остановить воркеры, оставшиеся
// работать после остановки агента.
func stopWorkersCmd(args []string) error {
	fs := flag.NewFlagSet("stop-workers", flag.ContinueOnError)
	flagPath := configFlag(fs)
	cleanup := fs.Bool("cleanup", false, "затем ./cleanup воркеров — убрать созданное ими на узле (перед удалением агента)")
	if err := parseOnly(fs, args); err != nil {
		return err
	}
	cfg, _, err := loadForTool(flagPath)
	if err != nil {
		return err
	}
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))
	names, cleaned, err := app.StopWorkers(context.Background(), cfg, *cleanup, log)
	if errors.Is(err, app.ErrLocked) {
		l, _ := flagPath.layout()
		return fmt.Errorf("агент работает — сначала остановите его (%s stop %s)", l.Systemctl(), l.Unit)
	}
	if len(names) > 0 && err != nil {
		fmt.Println("остановлены:", strings.Join(names, ", "))
	}
	if err != nil {
		return err
	}
	if len(names) == 0 {
		fmt.Println("работающих воркеров нет")
	} else {
		fmt.Println("остановлены:", strings.Join(names, ", "))
	}
	failed := 0
	for _, c := range cleaned {
		if c.Err != nil {
			failed++
			fmt.Printf("уборка %s: %v\n", c.Worker, c.Err)
		} else {
			fmt.Printf("уборка %s: готово\n", c.Worker)
		}
	}
	if failed > 0 {
		return fmt.Errorf("уборка не завершена у воркеров: %d", failed)
	}
	return nil
}
