//go:build unix

package app

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/worker"
)

// ReloadOnSignal — по SIGHUP перечитать настройки из path (и окружения) до
// отмены ctx. Подписка на сигнал действует уже при возврате.
func (a *App) ReloadOnSignal(ctx context.Context, path string) {
	hup := make(chan os.Signal, 1)
	signal.Notify(hup, syscall.SIGHUP)
	go func() {
		defer signal.Stop(hup)
		for {
			select {
			case <-ctx.Done():
				return
			case <-hup:
				a.log.Info("SIGHUP: перечитываю настройки", "config", path)
				_ = a.ReloadFile(path)
			}
		}
	}()
}

// ReloadFile — перечитать настройки из path и окружения и применить.
// Ошибка в файле — в лог, агент работает со старыми настройками.
func (a *App) ReloadFile(path string) error {
	cfg, err := config.Load(path)
	if err != nil {
		a.log.Error("настройки не перечитаны — агент работает со старыми", "err", err)
		return err
	}
	a.warnUnknown(path)
	a.Reload(cfg)
	return nil
}

// manualInstall — способ запуска в журнале установки у агента без службы: такой
// агент запущен в фоне, перезапускать его некому.
const manualInstall = "manual"

// warnUnknown — неизвестные поля файлов настроек (опечатка, поле другой версии) —
// предупреждением в журнал агента (уходит и серверу): агент их не читает.
func (a *App) warnUnknown(path string) {
	for _, p := range config.Unknown(path) {
		file := p.File
		if file == "" {
			file = config.ShortPath(path)
		}
		a.log.Warn("настройки: "+p.Text, "file", file, "line", p.Line)
	}
}

// Reload — применить новые настройки без перезапуска агента: лог — сразу,
// воркеры — заменой изменённых, имя и метки — новым
// hello. Ключи, которые на ходу не меняются, остаются прежними до перезапуска.
func (a *App) Reload(next config.Config) {
	a.reloadMu.Lock()
	defer a.reloadMu.Unlock()
	old := a.config()
	diff := config.Compare(old, next)
	if !diff.Changed() {
		a.log.Info("настройки не изменились")
		return
	}
	if len(diff.Restart) > 0 {
		a.log.Warn("нужен перезапуск: "+strings.Join(diff.Restart, ", ")+" — применятся после перезапуска агента, остальное применено",
			"keys", diff.Restart)
	}
	next = config.KeepRestartOnly(old, next)
	a.cfgMu.Lock()
	a.cfg = next
	a.cfgMu.Unlock()
	if diff.Log {
		a.logCtl.Set(logx.Options{Level: next.Log.Level, Format: next.Log.Format})
		a.forward.SetLevel(next.Log.Forward)
	}
	if diff.Workers {
		a.workers.Apply(workerSpecs(next, a.sysmetricsCmd))
	}
	a.log.Info("настройки перечитаны и применены", "log", diff.Log,
		"workers", diff.Workers, "hello", diff.Hello || diff.Workers)
	if diff.Hello || diff.Workers {
		// hello несёт имя, метки и список воркеров: серверу нужен новый.
		a.link.Reconnect()
	}
}

// Serve — работать как служба до сигнала (§13): SIGTERM и SIGINT —
// остановка (воркеры работают дальше), SIGUSR1 (`agent restart`) — выход
// для перезапуска менеджером службы (ErrRestart; воркеры работают дальше),
// SIGHUP — перечитать настройки из path.
func (a *App) Serve(path string) error {
	a.configPath = path
	if path != "" {
		a.warnUnknown(path)
	}
	if InstallMode == manualInstall {
		a.log.Warn("агент поставлен без службы (запущен в фоне): перезапустить его после обновления или agent restart некому — " +
			"поставьте его службой (agent install --rootless) или запускайте agent run своим менеджером процессов")
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	sig := make(chan os.Signal, 4)
	signal.Notify(sig, syscall.SIGTERM, syscall.SIGINT, syscall.SIGUSR1)
	defer signal.Stop(sig)
	a.ReloadOnSignal(ctx, path)
	done := make(chan error, 1)
	go func() { done <- a.Run(ctx) }()
	for {
		select {
		case err := <-done:
			return err
		case s := <-sig:
			if s == syscall.SIGUSR1 {
				a.log.Info("SIGUSR1: перезапуск агента")
				a.restart.Store(true)
				a.setExit(worker.ExitRestart)
			} else {
				a.log.Info("остановка агента", "signal", s.String())
				a.setExit(worker.ExitStop)
			}
			cancel()
		}
	}
}
