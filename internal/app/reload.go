//go:build unix

package app

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/telemetry"
)

// httpClient — клиент для всех запросов агента к серверу (WebSocket, HTTP
// sync, регистрация, загрузка обновлений): прокси из окружения, системные
// корни + server.caFile, клиентский сертификат server.certFile/keyFile.
func httpClient(s config.Server) (*http.Client, error) {
	tlsCfg, err := s.TLSConfig()
	if err != nil {
		return nil, err
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	// Прокси — HTTPS_PROXY / HTTP_PROXY / NO_PROXY на момент запуска (proxyFromEnv).
	transport.Proxy = proxyFromEnv()
	if tlsCfg != nil {
		transport.TLSClientConfig = tlsCfg
	}
	return &http.Client{Transport: transport}, nil
}

func telemetryOptions(cfg config.Config) telemetry.Options {
	return telemetry.Options{
		Metrics: cfg.Telemetry.Metrics,
		Disks:   cfg.Telemetry.Disks,
		GPU:     cfg.Telemetry.GPU != "off",
		Exclude: cfg.Telemetry.ExcludeInterfaces,
	}
}

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
	a.Reload(cfg)
	return nil
}

// Reload — применить новые настройки без перезапуска агента и без разрыва
// очереди важных сообщений: лог, показатели, state.resyncInterval — сразу;
// воркеры — Supervisor.Apply; имя, метки, встроенные команды и другие
// изменения возможностей — новым hello (переподключение). Ключи, которые на
// ходу не меняются (config.Compare: Restart), остаются прежними до перезапуска.
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
	channels, cmds := a.telemetry.Channels(), a.commands.Names()
	if diff.Telemetry {
		a.telemetry.Configure(telemetryOptions(next))
		a.rt.SetTelemetry(next.Telemetry.Backlog, next.Telemetry.InventoryInterval.Std())
	}
	if diff.Resync {
		a.state.SetResync(next.State.ResyncInterval.Std())
	}
	if diff.Tuning {
		a.state.SetApplyTimeout(next.State.ApplyTimeout.Std())
		a.commands.SetMaxConcurrent(next.Commands.MaxConcurrent)
		a.jobs.SetCancelTimeout(next.Jobs.CancelTimeout.Std())
		a.outbox.SetLimit(next.Outbox.MaxMessages, nil)
	}
	a.applyBuiltins(next)
	var removed []string
	if diff.Workers {
		removed = a.workers.Apply(next.Workers)
	}
	a.rt.SetIdentity(next.Name, next.Labels)
	a.pushWorkerContext() // имя, метки, log.forward
	renew := diff.Hello || len(removed) > 0 ||
		!slices.Equal(channels, a.telemetry.Channels()) || !slices.Equal(cmds, a.commands.Names())
	a.log.Info("настройки перечитаны и применены", "log", diff.Log, "telemetry", diff.Telemetry,
		"resync", diff.Resync, "tuning", diff.Tuning, "workers", diff.Workers, "hello", renew)
	if renew {
		// Возможности сузились или изменилось приветствие: capabilities
		// только добавляет — серверу нужен новый hello. Очередь сохраняется.
		a.link.Reconnect()
	}
}
