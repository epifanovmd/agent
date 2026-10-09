package config

import (
	"maps"
	"path/filepath"
	"reflect"
	"slices"
)

// Diff — что изменилось между применёнными настройками и прочитанными заново
// (перечитывание agent.yaml по SIGHUP).
type Diff struct {
	// Restart — ключи, которые на ходу не меняются (нужен перезапуск агента).
	Restart []string
	// Log — уровень, формат лога или порог отправки серверу (log.forward).
	Log bool
	// Telemetry — группы метрик, диски, исключённые интерфейсы.
	Telemetry bool
	// Hello — имя или метки: агент переподключается с новым hello.
	Hello bool
	// Workers — набор воркеров или их настройки (в том числе lifecycle и
	// logs — они применяются без перезапуска воркера).
	Workers bool
}

// Changed — есть что применять или о чём предупредить.
func (d Diff) Changed() bool {
	return len(d.Restart) > 0 || d.Log || d.Telemetry || d.Hello || d.Workers
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
	restart("server.caFile", old.Server.CAFile != next.Server.CAFile)
	restart("server.certFile", old.Server.CertFile != next.Server.CertFile)
	restart("server.keyFile", old.Server.KeyFile != next.Server.KeyFile)
	restart("dataDir", old.DataDir != next.DataDir)
	restart("update.mode", old.Update.Mode != next.Update.Mode)
	restart("update.publicKey", old.Update.PublicKey != next.Update.PublicKey)
	restart("update.publicKeys", !slices.Equal(old.Update.PublicKeys, next.Update.PublicKeys))
	restart("enroll.token", old.Enroll.Token != next.Enroll.Token)
	restart("server.reconnect", old.Server.Reconnect != next.Server.Reconnect)
	restart("server.streamBuffer", old.Server.StreamBuffer != next.Server.StreamBuffer)
	restart("outbox.maxMessages", old.Outbox != next.Outbox)
	restart("log.buffer", old.Log.Buffer != next.Log.Buffer)

	oldLog, nextLog := old.Log, next.Log
	oldLog.Buffer, nextLog.Buffer = 0, 0
	d.Log = oldLog != nextLog
	d.Telemetry = !reflect.DeepEqual(old.Telemetry, next.Telemetry)
	d.Hello = old.Name != next.Name || !maps.Equal(old.Labels, next.Labels)
	d.Workers = !reflect.DeepEqual(old.Workers, next.Workers)
	return d
}

// KeepRestartOnly — next, в котором ключи, требующие перезапуска, остаются
// как в old: они вступят в силу при следующем запуске.
func KeepRestartOnly(old, next Config) Config {
	next.Server = old.Server
	next.DataDir = old.DataDir
	next.Update = old.Update
	next.Enroll = old.Enroll
	next.Outbox = old.Outbox
	next.Log.Buffer = old.Log.Buffer
	// Каталоги сборок воркеров — в прежнем dataDir.
	next.Workers = slices.Clone(next.Workers)
	for i := range next.Workers {
		if w := &next.Workers[i]; w.Release {
			w.ReleaseDir = filepath.Join(ReleasesDir(old.DataDir), w.Name)
		}
	}
	return next
}
