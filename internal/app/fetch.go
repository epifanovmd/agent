package app

import (
	"context"
	"net/http"
	"os"
	"runtime"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/releases"
)

// releasesClient — запросы к каталогу сборок агента (не к серверу: свой TLS не нужен).
var releasesClient = &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{Proxy: http.ProxyFromEnvironment}}

// fetchAgentWorkers — сборки воркеров from: agent, которых ещё нет на диске:
// из релиза агента той же версии под эту машину, подпись — ключами агента.
// Поставленный воркер перезапускается; ошибка — в лог, воркер ждёт сборку.
func (a *App) fetchAgentWorkers(ctx context.Context, cfg config.Config) {
	var manifest *message.Manifest
	dir := releases.VersionDir(cfg.Update.Releases, a.version)
	for _, w := range cfg.Workers {
		if w.From != config.FromAgent {
			continue
		}
		if _, err := os.Stat(w.Current()); err == nil {
			continue
		}
		if a.version == "dev" {
			a.log.Error("воркер: сборки нет, а у агента без версии (dev) нет релиза — положите сборку сами", "worker", w.Name, "path", w.Current())
			continue
		}
		if manifest == nil {
			m, err := releases.Manifest(ctx, releasesClient, dir)
			if err != nil {
				a.log.Error("воркер: сборки нет, каталог сборок агента недоступен", "worker", w.Name, "err", err)
				a.recordError(err)
				return
			}
			manifest = m
		}
		placed, err := releases.PlaceWorker(ctx, releasesClient, dir, manifest, w.Name, runtime.GOOS, runtime.GOARCH, w.ReleaseDir, a.keys)
		if err != nil {
			a.log.Error("воркер: сборка из релиза агента не поставлена", "worker", w.Name, "err", err)
			a.recordError(err)
			continue
		}
		switch {
		case len(a.keys) == 0:
			a.log.Warn("воркер: ключей проверки подписи нет — у сборки сверена только контрольная сумма", "worker", w.Name)
		case placed.Unsigned:
			a.log.Warn("воркер: у сборки нет подписи — сверена только контрольная сумма", "worker", w.Name)
		}
		a.log.Info("воркер: сборка из релиза агента поставлена", "worker", w.Name, "version", placed.Version, "path", w.Current())
		go func(name string) {
			ctx, cancel := context.WithTimeout(ctx, time.Minute)
			defer cancel()
			if err := a.workers.Restart(ctx, name, true); err != nil && ctx.Err() == nil {
				a.log.Warn("воркер: перезапуск после установки сборки", "worker", name, "err", err)
			}
		}(w.Name)
	}
}
