package config

import (
	"maps"
	"path/filepath"
	"reflect"
	"slices"
)

// Diff — что изменилось между настройками, применёнными и прочитанными заново
// (перечитывание agent.yaml по SIGHUP).
type Diff struct {
	// Restart — ключи, которые на ходу не меняются (нужен перезапуск агента).
	Restart []string
	// Log — уровень, формат лога или порог отправки серверу (log.forward).
	Log bool
	// Telemetry — показатели: metrics, disks, gpu, inventoryInterval, excludeInterfaces, backlog.
	Telemetry bool
	// Resync — state.resyncInterval.
	Resync bool
	// Hello — меняется приветствие сессии (name, labels, commands.disabled):
	// после применения агент переподключается.
	Hello bool
	// Workers — набор воркеров или их настройки.
	Workers bool
	// Tuning — пределы и сроки, которые применяются сразу:
	// state.applyTimeout, commands.maxConcurrent, jobs.cancelTimeout,
	// outbox.maxMessages.
	Tuning bool
}

// Changed — есть что применять или о чём предупредить.
func (d Diff) Changed() bool {
	return len(d.Restart) > 0 || d.Log || d.Telemetry || d.Resync || d.Hello || d.Workers || d.Tuning
}

// Compare — разница настроек old → next (обе после Validate).
func Compare(old, next Config) Diff {
	var d Diff
	restart := func(key string, changed bool) {
		if changed {
			d.Restart = append(d.Restart, key)
		}
	}
	restart("server.url", old.Server.URL != next.Server.URL)
	restart("server.urls", !slices.Equal(old.Server.URLs, next.Server.URLs))
	restart("server.transport", old.Server.Transport != next.Server.Transport)
	restart("server.caFile", old.Server.CAFile != next.Server.CAFile)
	restart("server.certFile", old.Server.CertFile != next.Server.CertFile)
	restart("server.keyFile", old.Server.KeyFile != next.Server.KeyFile)
	restart("dataDir", old.DataDir != next.DataDir)
	restart("update.mode", old.Update.Mode != next.Update.Mode)
	restart("update.publicKey", old.Update.PublicKey != next.Update.PublicKey)
	restart("enroll.token", old.Enroll.Token != next.Enroll.Token)

	d.Log = old.Log != next.Log
	d.Telemetry = !reflect.DeepEqual(old.Telemetry, next.Telemetry)
	d.Resync = old.State.ResyncInterval != next.State.ResyncInterval
	d.Hello = old.Name != next.Name || !maps.Equal(old.Labels, next.Labels) ||
		!sameSet(old.Commands.Disabled, next.Commands.Disabled)
	d.Workers = !reflect.DeepEqual(old.Workers, next.Workers)
	d.Tuning = old.State.ApplyTimeout != next.State.ApplyTimeout || old.Commands.MaxConcurrent != next.Commands.MaxConcurrent ||
		old.Jobs != next.Jobs || old.Outbox != next.Outbox
	return d
}

// KeepRestartOnly — next, в котором ключи, требующие перезапуска, остаются
// как в old: они вступят в силу при следующем запуске.
func KeepRestartOnly(old, next Config) Config {
	next.Server = old.Server
	next.DataDir = old.DataDir
	next.Update = old.Update
	next.Enroll = old.Enroll
	// Каталоги воркеров из выпуска — в прежнем dataDir.
	next.Workers = slices.Clone(next.Workers)
	for i := range next.Workers {
		if w := &next.Workers[i]; w.Release {
			w.ReleaseDir = filepath.Join(ReleasesDir(old.DataDir), w.Name)
		}
	}
	return next
}

func sameSet(a, b []string) bool {
	a, b = slices.Clone(a), slices.Clone(b)
	slices.Sort(a)
	slices.Sort(b)
	return slices.Equal(slices.Compact(a), slices.Compact(b))
}
