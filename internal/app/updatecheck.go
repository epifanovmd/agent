package app

import (
	"context"
	"time"

	"github.com/epifanovmd/agent/internal/releases"
)

// checkLoop — раз в update.checkInterval проверить последнюю версию в каталоге
// сборок агента (§11): новее своей — в hello.agent.update, status.update и в
// журнал. Сам агент её не ставит. Итог хранится в dataDir: перезапуск не
// повторяет свежую проверку, а команды на узле видят её без сети.
func (a *App) checkLoop(ctx context.Context) {
	cfg := a.config()
	every := cfg.Update.CheckEvery()
	if every == 0 || a.version == "dev" {
		return
	}
	last := releases.ReadCheck(cfg.DataDir)
	a.setCheck(last)
	wait := time.Duration(0)
	if last.Fresh(every, time.Now()) {
		wait = every - time.Since(time.UnixMilli(last.CheckedAt))
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		cfg := a.config()
		c, err := releases.Latest(ctx, releasesClient, cfg.Update.Releases, time.Now())
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			a.log.Warn("проверка новой версии агента не удалась — повтор позже", "err", err)
			timer.Reset(min(every, time.Hour))
			continue
		}
		if err := releases.WriteCheck(cfg.DataDir, c); err != nil {
			a.log.Warn("итог проверки новой версии не сохранён", "err", err)
		}
		if c.Newer(a.version) && c.Latest != last.Latest {
			a.log.Info("доступна новая версия агента", "version", c.Latest, "current", a.version)
		}
		last = c
		a.setCheck(c)
		timer.Reset(every)
	}
}

// setCheck — запомнить итог проверки; новая версия появилась или пропала —
// внеочередной status.
func (a *App) setCheck(c releases.Check) {
	next := c.Info(a.version)
	prev := a.latest.Swap(next)
	if (prev == nil) != (next == nil) || (prev != nil && prev.Latest != next.Latest) {
		a.statusChanged()
	}
}
