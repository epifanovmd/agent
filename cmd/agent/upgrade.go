package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/install"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/releases"
	"github.com/epifanovmd/agent/internal/update"
)

var releasesClient = &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{Proxy: http.ProxyFromEnvironment}}

// looseConfig — настройки для подсказок и обновления: ошибки в них (например,
// нет адреса сервера) этим командам не мешают.
func looseConfig(t target) config.Config {
	path, err := t.path()
	if err != nil {
		return config.Defaults()
	}
	_, _ = config.ApplyEnvFile(config.EnvFile(path))
	return config.Check(path).Config
}

// updateNotice — подсказка в stderr, если есть версия агента новее этой.
// online — можно сходить в каталог сборок, если сохранённая проверка устарела
// (status — нельзя: он работает как HEALTHCHECK и без сети).
func updateNotice(cfg config.Config, online bool) {
	every := cfg.Update.CheckEvery()
	if os.Getenv("AGENT_NO_UPDATE_CHECK") != "" || version == "dev" || every == 0 {
		return
	}
	c := releases.ReadCheck(cfg.DataDir)
	if online && !c.Fresh(every, time.Now()) {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if fresh, err := releases.Latest(ctx, releasesClient, cfg.Update.Releases, time.Now()); err == nil {
			c = fresh
			_ = releases.WriteCheck(cfg.DataDir, c)
		}
	}
	if c.Newer(version) {
		fmt.Fprintf(os.Stderr, "\nДоступна версия агента %s (у вас %s): agent upgrade\n", c.Latest, version)
	}
}

// major — первая часть версии.
func major(v string) string {
	return strings.SplitN(strings.TrimPrefix(v, "v"), ".", 2)[0]
}

// upgradeCmd — agent upgrade: поставить новую версию агента из каталога
// сборок (релизы на GitHub или update.releases) с проверкой подписи. Программа
// папки агента заменяется на месте (прежняя — .prev); программа службы —
// как по agent.update (откат, если новая не выйдет на связь), служба
// перезапускается.
func upgradeCmd(args []string) error {
	set := flag.NewFlagSet("upgrade", flag.ContinueOnError)
	t := configFlag(set)
	check := set.Bool("check", false, "только показать, есть ли новая версия")
	want := set.String("version", "", "поставить эту версию (по умолчанию — последнюю)")
	force := set.Bool("force", false, "поставить, даже если версия та же")
	if err := set.Parse(args); err != nil {
		return err
	}
	if set.NArg() > 0 {
		return fmt.Errorf("лишние аргументы: %s", strings.Join(set.Args(), " "))
	}
	cfg := looseConfig(t)
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()

	target := strings.TrimPrefix(*want, "v")
	if target == "" {
		c, err := releases.Latest(ctx, releasesClient, cfg.Update.Releases, time.Now())
		if err != nil {
			return fmt.Errorf("последняя версия агента: %w", err)
		}
		_ = releases.WriteCheck(cfg.DataDir, c)
		target = c.Latest
	}
	newer := version == "dev" || message.CompareVersions(target, version) > 0
	if *check {
		if newer {
			fmt.Printf("Доступна версия %s (у вас %s): agent upgrade\n", target, version)
		} else {
			fmt.Printf("У вас последняя версия: %s\n", version)
		}
		return nil
	}
	if target == version && !*force {
		fmt.Printf("У вас уже версия %s\n", version)
		return nil
	}
	if *want == "" && version != "dev" && major(target) != major(version) {
		return fmt.Errorf("версия %s — новая основная версия (у вас %s): поставьте её явно — agent upgrade --version %s", target, version, target)
	}
	keys, err := update.ParseKeys(append([]string{updateKey}, cfg.Update.Keys()...)...)
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		return update.ErrNotVerified
	}
	rel, err := releases.AgentBuild(ctx, releasesClient, cfg.Update.Releases, target, runtime.GOOS, runtime.GOARCH)
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
	if instance, ok := install.InstanceOf(exe); ok || exe == install.Binary {
		return upgradeService(ctx, exe, instance, keys, rel)
	}
	if err := replaceBinary(ctx, exe, keys, rel); err != nil {
		return err
	}
	fmt.Printf("Агент обновлён: %s → %s (прежняя версия — %s.prev)\n", version, target, filepath.Base(exe))
	fmt.Println("Если агент работает — перезапустите его, чтобы заработала новая версия (воркеры продолжат работу).")
	return nil
}

// replaceBinary — программа папки агента exe заменяется сборкой rel (подпись
// и sha256 сверяются), прежняя остаётся рядом как .prev.
func replaceBinary(ctx context.Context, exe string, keys update.Keys, rel update.Release) error {
	if err := keys.Verify(update.Local(update.AgentName, rel.Version, rel.SHA256), rel.Signature); err != nil {
		return err
	}
	next := exe + ".new"
	if err := update.Download(ctx, releasesClient, "", rel.URL, next, rel.SHA256); err != nil {
		return err
	}
	if err := copyBinary(exe, exe+".prev"); err != nil {
		os.Remove(next)
		return fmt.Errorf("копия текущей версии: %w", err)
	}
	return os.Rename(next, exe)
}

// upgradeService — новая версия программы службы: как agent.update (отметка
// для отката, если новая не выйдет на связь), затем перезапуск службы.
func upgradeService(ctx context.Context, exe, instance string, keys update.Keys, rel update.Release) error {
	if runtime.GOOS != "linux" || os.Geteuid() != 0 {
		return errors.New("программа службы обновляется от root: sudo agent upgrade")
	}
	if err := update.Install(ctx, releasesClient, "", update.NewPaths(exe), keys, rel); err != nil {
		return err
	}
	l := install.Layout(instance)
	if out, err := exec.CommandContext(ctx, "systemctl", "restart", l.Service).CombinedOutput(); err != nil {
		return fmt.Errorf("systemctl restart %s: %v: %s", l.Service, err, strings.TrimSpace(string(out)))
	}
	fmt.Printf("Агент обновлён: %s → %s, служба %s перезапущена; не выйдет на связь — вернётся прежняя версия.\n", version, rel.Version, l.Unit)
	return nil
}

func copyBinary(src, dst string) error {
	raw, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	tmp := dst + ".new"
	if err := os.WriteFile(tmp, raw, 0o755); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}
