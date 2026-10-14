package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/epifanovmd/agent/internal/bundle"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/releases"
	"github.com/epifanovmd/agent/internal/update"
)

// packCmd — agent pack: архив папки агента для узла — программа под его
// платформу, файлы настроек, воркеры из папки и их сборки (release/).
func packCmd(args []string) error {
	set := flag.NewFlagSet("pack", flag.ContinueOnError)
	t := configFlag(set)
	platforms := set.String("platform", "linux/amd64,linux/arm64", "платформы узлов через запятую")
	out := set.String("out", "", "куда положить архивы (по умолчанию — в папку агента)")
	withEnv := set.Bool("with-env", false, "положить в архив и файлы переменных (envFiles) — в них секреты")
	releaseOut := set.String("release-out", "", "ещё и общий каталог сборок воркеров из папки под все платформы — для бэкенда (releasesDir)")
	if err := set.Parse(args); err != nil {
		return err
	}
	if set.NArg() > 0 {
		return fmt.Errorf("лишние аргументы: %s", strings.Join(set.Args(), " "))
	}
	if *t.instance != "" {
		return fmt.Errorf("--instance — для агента на узле; упаковывается папка агента (-config или --env)")
	}
	path, err := t.path()
	if err != nil {
		return err
	}
	if path == "" {
		return fmt.Errorf("нет файла настроек папки агента (agent.yaml)")
	}
	list, err := bundle.ParsePlatforms(*platforms)
	if err != nil {
		return err
	}
	cfg := looseConfig(t)
	keys, err := update.ParseKeys(append([]string{updateKey}, cfg.Update.Keys()...)...)
	if err != nil {
		return err
	}
	signing, err := releases.SigningKey()
	if err != nil {
		return err
	}
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	if real, err := filepath.EvalSymlinks(exe); err == nil {
		exe = real
	}
	dir := *out
	if dir == "" {
		dir = filepath.Dir(path)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	archives, err := bundle.Pack(ctx, bundle.Options{
		Config: path, Env: *t.env, Platforms: list, Out: dir, WithEnv: *withEnv, ReleaseOut: *releaseOut,
		Version: version, Self: exe, Keys: keys, Signing: signing, Client: releasesClient,
		Warn: func(s string) { fmt.Fprintln(os.Stderr, "agent: предупреждение:", s) },
	})
	if err != nil {
		return err
	}
	fmt.Println("Архив для узлов:")
	for _, a := range archives {
		fmt.Println("  " + config.ShortPath(a))
	}
	if signing == nil {
		fmt.Fprintln(os.Stderr, "agent: предупреждение: AGENT_SIGNING_KEY не задан — сборки воркеров без подписи: поставить их можно, обновить с сервера — нет (ключи — docs/ARCHITECTURE.md, «Ключи подписи»)")
	}
	if *withEnv {
		fmt.Fprintln(os.Stderr, "agent: предупреждение: в архивах файлы переменных с секретами — передавайте их только надёжным способом")
	}
	env := ""
	if *t.env != "" {
		env = " --env " + *t.env
	}
	fmt.Printf("\nНа узле:\n  tar xzf %s && cd agent && sudo ./agent install%s", filepath.Base(archives[0]), env)
	if !*withEnv {
		fmt.Print(" --token <токен регистрации>")
	}
	fmt.Println()
	return nil
}
