// Агент: держит связь с сервером, запускает воркеры и следит за ними,
// передаёт им настройки и запросы, собирает метрики узла, обновляется сам.
//
//	agent [run] [-config agent.yaml]   работа (по умолчанию); SIGHUP — перечитать настройки,
//	                                   SIGUSR1 — перезапуск, SIGTERM/SIGINT — остановка
//	agent restart                      перезапустить работающего агента (воркеры — по lifecycle.onAgentRestart)
//	agent stop-workers                 остановить воркеры, оставшиеся работать после остановки агента
//	agent install --server URL --token ТОКЕН [флаги]   поставить службой systemd (Linux, root)
//	agent uninstall [--purge]          удалить службу (с --purge — настройки и данные)
//	                                   --instance ИМЯ у install, uninstall, status, logs, restart,
//	                                   stop-workers, cleanup, config check — экземпляр агента на узле
//	agent init [--server URL] [--token ТОКЕН]   создать файл настроек с пояснениями
//	agent config check                 проверить настройки и показать итоговые значения
//	agent status [--json]              работает ли агент, связь, воркеры (код выхода 0 — работает)
//	agent logs [-f] [-n N]             лог службы (journalctl)
//	agent cleanup                      уборка воркеров перед удалением агента (без сервера; её вызывает uninstall)
//	agent version                      версия
//	agent boot-guard BINARY            откат версии, не дошедшей до связи (ExecStartPre)
//	agent sysmetrics                   встроенный воркер метрик узла (агент запускает его сам)
//
// Файл настроек — -config, иначе --instance ИМЯ (/etc/agent-ИМЯ/agent.yaml),
// иначе AGENT_CONFIG, иначе экземпляр, чья это программа (/opt/agent-ИМЯ/bin/agent),
// иначе /etc/agent/agent.yaml (на macOS ~/.agent/agent.yaml), если он есть.
//
// Ключи подписи и манифест выпуска — отдельная программа cmd/agent-release.
//
// Ключ проверки обновлений вшивается при сборке: -ldflags "-X main.updateKey=<base64>";
// настройка update.publicKey важнее.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/install"
	"github.com/epifanovmd/agent/internal/sysmetrics"
	"github.com/epifanovmd/agent/internal/update"
)

// version — задаётся при сборке: -ldflags "-X main.version=1.2.3".
var version = "dev"

// updateKey — открытый ключ проверки сборок (base64 Ed25519), вшитый при
// сборке: -ldflags "-X main.updateKey=…". Настройка update.publicKey важнее.
var updateKey = ""

// errQuiet — команда уже всё сказала сама: только код выхода 1.
var errQuiet = errors.New("")

const usage = `agent — агент для узлов: связь с сервером, воркеры, их настройки, метрики, обновление.

Команды:
  agent run [-config ФАЙЛ]       работать (по умолчанию)
  agent install --server URL --token ТОКЕН [флаги]
                                 поставить службой systemd (Linux, sudo); флаги — agent install -h
  agent uninstall [--purge]      удалить службу; --purge — ещё настройки и данные
  agent init [--server URL] [--token ТОКЕН] [--name ИМЯ] [-config ФАЙЛ] [--force]
                                 создать файл настроек с пояснениями
  agent config check [-config ФАЙЛ]
                                 проверить настройки и показать итоговые значения
  agent status [--json]          работает ли агент, связь, воркеры, последняя ошибка
  agent logs [-f] [-n N]         лог службы
  agent restart                  перезапустить агента; воркеры работают дальше (lifecycle.onAgentRestart)
  agent stop-workers             остановить воркеры, оставшиеся после остановки агента (агент не работает)
  agent cleanup                  воркеры убирают за собой (перед удалением агента)
  agent version                  версия

Несколько агентов на узле: agent install --instance ИМЯ — экземпляр со своими путями
(/etc/agent-ИМЯ, /var/lib/agent-ИМЯ, /opt/agent-ИМЯ, служба agent-ИМЯ); остальные команды
с --instance ИМЯ (или через ссылку agent-ИМЯ) работают с ним.

Файл настроек: -config, иначе --instance, иначе AGENT_CONFIG, иначе %s (если есть).
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
	case "init":
		err = initCmd(args)
	case "config":
		err = configCmd(args)
	case "status":
		err = status(args)
	case "logs":
		err = logs(args)
	case "cleanup":
		err = cleanup(args)
	case "restart":
		err = restartCmd(args)
	case "stop-workers":
		err = stopWorkersCmd(args)
	case "version":
		fmt.Println(version)
	case "boot-guard":
		err = bootGuard(args)
	case "help":
		fmt.Printf(usage, config.DefaultPath())
	case sysmetrics.Command, sysmetrics.SensorsCommand:
		// Служебные: встроенный воркер метрик узла и показания датчиков (macOS) —
		// агент запускает их сам дочерними процессами.
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

// target — чей агент: флаги -config и --instance у команды.
type target struct{ config, instance *string }

// configFlag — флаги -config (он же --config) и --instance у команды.
func configFlag(fs *flag.FlagSet) target {
	return target{
		config:   fs.String("config", "", "файл настроек (AGENT_CONFIG; по умолчанию "+config.DefaultPath()+", если есть)"),
		instance: fs.String("instance", "", "экземпляр агента на узле (agent install --instance ИМЯ): его файл настроек и служба"),
	}
}

// layout — экземпляр: --instance, иначе тот, чья это программа
// (/opt/agent-ИМЯ/bin/agent, в том числе по ссылке agent-ИМЯ), иначе по умолчанию.
func (t target) layout() (install.Paths, error) {
	name := *t.instance
	if name == "" && *t.config == "" && os.Getenv("AGENT_CONFIG") == "" {
		name = selfInstance()
	}
	if err := install.CheckInstance(name); err != nil {
		return install.Paths{}, err
	}
	return install.Layout(name), nil
}

// path — файл настроек: -config, --instance (или экземпляр программы),
// AGENT_CONFIG, файл по умолчанию. -config и --instance — что-то одно.
func (t target) path() (string, error) {
	if *t.config != "" && *t.instance != "" {
		return "", errors.New("-config и --instance — что-то одно")
	}
	l, err := t.layout()
	if err != nil {
		return "", err
	}
	if l.Instance != "" {
		return l.ConfigFile, nil
	}
	return config.ResolvePath(*t.config), nil
}

// selfInstance — экземпляр, которому принадлежит запущенная программа ("" — нет).
func selfInstance() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	name, _ := install.InstanceOf(exe)
	return name
}

func run(args []string) error {
	// Первым делом: новая версия, не связавшаяся с сервером за несколько
	// запусков, откатывается — даже если падает на конфигурации.
	if exe, err := os.Executable(); err == nil {
		if err := update.Boot(update.NewPaths(exe)); err != nil {
			fmt.Fprintln(os.Stderr, "agent: проверка обновления:", err)
		}
	}
	fs := flag.NewFlagSet("run", flag.ContinueOnError)
	flagPath := configFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	path, err := flagPath.path()
	if err != nil {
		return err
	}
	if path != "" {
		for _, p := range config.Unknown(path) {
			fmt.Fprintf(os.Stderr, "agent: предупреждение: %s: %s\n", path, p)
		}
	}
	cfg, err := config.Load(path)
	if err != nil {
		if path == "" {
			return fmt.Errorf("%w\nфайла настроек нет (%s): создайте его — agent init --server URL --token ТОКЕН — или задайте AGENT_SERVER_URL", err, config.DefaultPath())
		}
		return err
	}
	app.BuiltinUpdateKey = updateKey
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

// loadForTool — настройки для служебных команд (cleanup, status): файл,
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

// cleanup — перед удалением агента с узла: каждый воркер из настроек
// убирает за собой (POST /cleanup). Связи с сервером нет. Хоть один не
// убрал — ошибка (код выхода 1).
func cleanup(args []string) error {
	fs := flag.NewFlagSet("cleanup", flag.ContinueOnError)
	flagPath := configFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, _, err := loadForTool(flagPath)
	if err != nil {
		return err
	}
	if len(cfg.Workers) == 0 {
		fmt.Println("воркеров нет — убирать нечего")
		return nil
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	results, err := app.Cleanup(ctx, cfg, version)
	if err != nil {
		return err
	}
	failed := 0
	for _, r := range results {
		if r.Err != nil {
			failed++
			fmt.Printf("%s: не убрано — %v\n", r.Worker, r.Err)
		} else {
			fmt.Printf("%s: убрано\n", r.Worker)
		}
	}
	if failed > 0 {
		return fmt.Errorf("уборка не завершена: воркеров с ошибкой — %d из %d", failed, len(cfg.Workers))
	}
	return nil
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

// restartCmd — `agent restart`: SIGUSR1 работающему агенту (pid — из
// отметки agent.status): он выходит, менеджер службы запускает его снова,
// воркеры работают дальше (lifecycle.onAgentRestart: keep).
func restartCmd(args []string) error {
	fs := flag.NewFlagSet("restart", flag.ContinueOnError)
	flagPath := configFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, _, err := loadForTool(flagPath)
	if err != nil {
		return err
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

// stopWorkersCmd — `agent stop-workers`: остановить воркеры, оставшиеся
// работать после остановки агента.
func stopWorkersCmd(args []string) error {
	fs := flag.NewFlagSet("stop-workers", flag.ContinueOnError)
	flagPath := configFlag(fs)
	if err := fs.Parse(args); err != nil {
		return err
	}
	cfg, _, err := loadForTool(flagPath)
	if err != nil {
		return err
	}
	names, err := app.StopWorkers(cfg)
	if errors.Is(err, app.ErrLocked) {
		l, _ := flagPath.layout()
		return fmt.Errorf("агент работает — сначала остановите его (systemctl stop %s)", l.Unit)
	}
	if err != nil {
		return err
	}
	if len(names) == 0 {
		fmt.Println("работающих воркеров нет")
		return nil
	}
	fmt.Println("остановлены:", strings.Join(names, ", "))
	return nil
}
