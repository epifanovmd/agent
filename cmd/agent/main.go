// Агент: держит связь с бэкендом, выполняет задачи воркерами,
// команды и желаемое состояние, шлёт статус и телеметрию.
//
//	agent [run] [-config agent.yaml]   работа (по умолчанию); SIGHUP — перечитать настройки
//	agent cleanup [-config agent.yaml] уборка воркеров перед удалением агента (без сервера)
//	agent status [-config agent.yaml]  работает ли агент с этим dataDir (код выхода 0 — да)
//	agent version                      версия
//	agent keygen                       ключи подписи релизов (Ed25519)
//	agent boot-guard BINARY            откат версии, не дошедшей до связи (ExecStartPre)
//	agent release-manifest DIR VERSION [--worker NAME=VERSION[,restart=…][,stopTimeout=…][,command=…]]…
//	                                   manifest.json сборок агента и воркеров в DIR (подпись — AGENT_SIGNING_KEY)
//
// Ключ проверки обновлений вшивается при сборке: -ldflags "-X main.updateKey=<base64>";
// настройка update.publicKey важнее.
package main

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/internal/worker"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// version — задаётся при сборке: -ldflags "-X main.version=1.2.3".
var version = "dev"

// updateKey — открытый ключ проверки сборок (base64 Ed25519), вшитый при
// сборке: -ldflags "-X main.updateKey=…". Настройка update.publicKey важнее.
var updateKey = ""

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
	case "cleanup":
		err = cleanup(args)
	case "status":
		err = status(args)
	case "version":
		fmt.Println(version)
	case "keygen":
		err = keygen()
	case "boot-guard":
		err = bootGuard(args)
	case "release-manifest":
		err = releaseManifest(args)
	default:
		err = fmt.Errorf("неизвестная команда %q (run | cleanup | status | version | keygen | boot-guard | release-manifest)", cmd)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "agent:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	// Первым делом: новая версия, не связавшаяся с сервером за несколько
	// запусков, откатывается — даже если падает на конфигурации.
	if exe, err := os.Executable(); err == nil {
		if err := update.Boot(update.NewPaths(exe)); err != nil {
			fmt.Fprintln(os.Stderr, "agent: проверка обновления:", err)
		}
	}
	fs := flag.NewFlagSet("run", flag.ExitOnError)
	path := fs.String("config", os.Getenv("AGENT_CONFIG"), "файл конфигурации YAML (AGENT_CONFIG)")
	_ = fs.Parse(args)

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	app.BuiltinUpdateKey = updateKey
	agent, err := app.New(cfg, version)
	if err != nil {
		return err
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	// SIGHUP (systemctl reload) — перечитать настройки на ходу.
	agent.ReloadOnSignal(ctx, *path)
	err = agent.Run(ctx)
	if errors.Is(err, app.ErrRestart) || errors.Is(err, context.Canceled) {
		// Перезапуск — дело менеджера процесса (systemd Restart=always, Docker restart).
		return nil
	}
	return err
}

// cleanup — перед удалением агента с узла: каждый воркер из конфигурации
// убирает за собой (worker.cleanup). Связи с сервером нет. Хоть один не
// убрал — ошибка (код выхода 1).
func cleanup(args []string) error {
	fs := flag.NewFlagSet("cleanup", flag.ExitOnError)
	path := fs.String("config", os.Getenv("AGENT_CONFIG"), "файл конфигурации YAML (AGENT_CONFIG)")
	_ = fs.Parse(args)

	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	if len(cfg.Workers) == 0 {
		fmt.Println("воркеров нет — убирать нечего")
		return nil
	}
	// Уборка — только когда агент с этим dataDir остановлен.
	lock, err := app.LockDataDir(cfg.DataDir)
	if err != nil {
		return err
	}
	defer lock.Unlock()
	log := logx.New(os.Stderr, logx.NewRing(1), logx.Options{Level: cfg.Log.Level, Format: cfg.Log.Format})
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	failed := 0
	for _, r := range worker.Cleanup(ctx, cfg.Workers, log, app.CleanupContext(cfg, version)) {
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

// status — проверка живости (HEALTHCHECK в Docker): агент с dataDir из
// настроек запущен и недавно отмечался. Не так — ошибка (код выхода 1).
func status(args []string) error {
	fs := flag.NewFlagSet("status", flag.ExitOnError)
	path := fs.String("config", os.Getenv("AGENT_CONFIG"), "файл конфигурации YAML (AGENT_CONFIG)")
	_ = fs.Parse(args)
	cfg, err := config.Load(*path)
	if err != nil {
		return err
	}
	st, err := app.Status(cfg.DataDir)
	if err != nil {
		return err
	}
	online := "нет связи с сервером"
	if st.Online {
		online = "на связи"
	}
	fmt.Printf("работает (pid %d), %s\n", st.PID, online)
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

// signingKey — ключ подписи из AGENT_SIGNING_KEY (nil — не задан).
func signingKey() (ed25519.PrivateKey, error) {
	seed := os.Getenv("AGENT_SIGNING_KEY")
	if seed == "" {
		return nil, nil
	}
	raw, err := base64.StdEncoding.DecodeString(seed)
	if err != nil || len(raw) != ed25519.SeedSize {
		return nil, errors.New("AGENT_SIGNING_KEY — base64 seed Ed25519 (agent keygen)")
	}
	return ed25519.NewKeyFromSeed(raw), nil
}

func keygen() error {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return err
	}
	fmt.Printf("AGENT_SIGNING_KEY=%s\n", base64.StdEncoding.EncodeToString(priv.Seed()))
	fmt.Printf("AGENT_UPDATE_PUBLIC_KEY=%s\n", base64.StdEncoding.EncodeToString(pub))
	return nil
}

// releaseManifest — manifest.json для сборок agent-<os>-<arch> в каталоге и
// воркеров из выпуска: `--worker NAME=VERSION[,restart=…][,stopTimeout=…]`
// (повторяемый) берёт файлы DIR/<name>-<version>-<os>-<arch> или архивы
// DIR/<name>-<version>-<os>-<arch>.tar.gz. Подпись — AGENT_SIGNING_KEY над
// строкой сборки (§7: имя, версия, os, arch, sha256).
func releaseManifest(args []string) error {
	usage := errors.New("release-manifest DIR VERSION [--worker NAME=VERSION[,restart=rolling|stop-first][,stopTimeout=30s][,command=bin/report]]…")
	var positional []string
	var workers []message.WorkerArtifact
	for i := 0; i < len(args); i++ {
		arg := args[i]
		value, isWorker := strings.CutPrefix(arg, "--worker=")
		if arg == "--worker" || arg == "-worker" {
			if i+1 >= len(args) {
				return usage
			}
			i++
			value, isWorker = args[i], true
		}
		if !isWorker {
			if strings.HasPrefix(arg, "-") {
				return usage
			}
			positional = append(positional, arg)
			continue
		}
		w, err := parseWorkerFlag(value)
		if err != nil {
			return err
		}
		workers = append(workers, w)
	}
	if len(positional) != 2 {
		return usage
	}
	dir, ver := positional[0], positional[1]
	priv, err := signingKey()
	if err != nil {
		return err
	}
	sign := func(b update.Build) string {
		if priv == nil {
			return ""
		}
		return update.Sign(priv, b)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "agent-*-*"))
	m := message.Manifest{Version: ver}
	for _, file := range files {
		parts := strings.Split(filepath.Base(file), "-")
		if len(parts) != 3 {
			continue
		}
		hash, err := update.FileHash(file)
		if err != nil {
			return err
		}
		b := update.Build{Name: update.AgentName, Version: ver, OS: parts[1], Arch: parts[2], SHA256: hash}
		m.Artifacts = append(m.Artifacts, message.Artifact{OS: parts[1], Arch: parts[2], File: filepath.Base(file), SHA256: hash, Signature: sign(b)})
	}
	if len(m.Artifacts) == 0 {
		return fmt.Errorf("в %s нет сборок agent-<os>-<arch>", dir)
	}
	for _, w := range workers {
		prefix := w.Name + "-" + w.Version + "-"
		found, _ := filepath.Glob(filepath.Join(dir, prefix+"*-*"))
		n := 0
		platforms := map[string]string{}
		for _, file := range found {
			rest := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(file), prefix), ".tar.gz")
			goos, arch, ok := strings.Cut(rest, "-")
			if !ok || goos == "" || arch == "" || strings.ContainsAny(arch, "-.") {
				continue
			}
			if prev, dup := platforms[goos+"/"+arch]; dup {
				return fmt.Errorf("сборка воркера %s %s под %s/%s дважды: %s и %s", w.Name, w.Version, goos, arch, prev, filepath.Base(file))
			}
			platforms[goos+"/"+arch] = filepath.Base(file)
			hash, err := update.FileHash(file)
			if err != nil {
				return err
			}
			a := w
			a.OS, a.Arch, a.File, a.SHA256 = goos, arch, filepath.Base(file), hash
			a.Signature = sign(update.Build{Name: w.Name, Version: w.Version, OS: goos, Arch: arch, SHA256: hash})
			m.Workers = append(m.Workers, a)
			n++
		}
		if n == 0 {
			return fmt.Errorf("в %s нет сборок воркера %s-<os>-<arch>[.tar.gz]", dir, strings.TrimSuffix(prefix, "-"))
		}
	}
	raw, _ := json.MarshalIndent(m, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(raw, '\n'), 0o644); err != nil {
		return err
	}
	if priv == nil {
		fmt.Fprintln(os.Stderr, "agent: AGENT_SIGNING_KEY не задан — релиз без подписи, самообновление на него не встанет")
	}
	fmt.Println(filepath.Join(dir, "manifest.json"))
	return nil
}

// parseWorkerFlag — NAME=VERSION[,restart=…][,stopTimeout=…][,command=…].
func parseWorkerFlag(value string) (message.WorkerArtifact, error) {
	parts := strings.Split(value, ",")
	name, ver, ok := strings.Cut(parts[0], "=")
	var w message.WorkerArtifact
	if !ok || !message.ValidName(name) || ver == "" || strings.ContainsAny(ver, "/ ") {
		return w, fmt.Errorf("--worker %q: нужно NAME=VERSION (имя — латиница, цифры, «.», «_», «-»)", value)
	}
	w.Name, w.Version = name, ver
	for _, opt := range parts[1:] {
		k, v, _ := strings.Cut(opt, "=")
		switch k {
		case "restart":
			if v != config.RestartRolling && v != config.RestartStopFirst {
				return w, fmt.Errorf("--worker %s: restart — rolling | stop-first, а не %q", name, v)
			}
			w.Restart = v
		case "stopTimeout":
			if d, err := time.ParseDuration(v); err != nil || d <= 0 {
				return w, fmt.Errorf("--worker %s: stopTimeout — длительность (30s), а не %q", name, v)
			}
			w.StopTimeout = v
		case "command":
			if clean := path.Clean(v); v == "" || path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
				return w, fmt.Errorf("--worker %s: command — путь внутри архива (bin/report), а не %q", name, v)
			}
			w.Command = path.Clean(v)
		default:
			return w, fmt.Errorf("--worker %s: неизвестный параметр %q (restart, stopTimeout, command)", name, k)
		}
	}
	return w, nil
}
