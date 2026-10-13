package config

import (
	"maps"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
)

// Diff — что изменилось между применёнными настройками и прочитанными заново
// (перечитывание agent.yaml по SIGHUP).
type Diff struct {
	// Restart — ключи, которые на ходу не меняются (нужен перезапуск агента).
	Restart []string
	// Log — уровень, формат лога или порог отправки серверу (log.forward).
	Log bool
	// Hello — имя или метки: агент переподключается с новым hello.
	Hello bool
	// Workers — набор воркеров или их настройки (в том числе lifecycle — он
	// применяется без перезапуска воркера).
	Workers bool
}

// Changed — есть что применять или о чём предупредить.
func (d Diff) Changed() bool {
	return len(d.Restart) > 0 || d.Log || d.Hello || d.Workers
}

// Compare — разница настроек old → next (обе после Validate).
func Compare(old, next Config) Diff {
	var d Diff
	restart := func(key string, changed bool) {
		if changed {
			d.Restart = append(d.Restart, key)
		}
	}
	// Разделы, которые KeepRestartOnly оставляет прежними целиком: каждое их
	// поле — ключ перезапуска.
	restartFields("server", old.Server, next.Server, restart)
	restart("dataDir", old.DataDir != next.DataDir)
	restartFields("update", old.Update, next.Update, restart)
	restartFields("enroll", old.Enroll, next.Enroll, restart)
	restartFields("outbox", old.Outbox, next.Outbox, restart)
	restart("log.buffer", old.Log.Buffer != next.Log.Buffer)

	oldLog, nextLog := old.Log, next.Log
	oldLog.Buffer, nextLog.Buffer = 0, 0
	d.Log = oldLog != nextLog
	d.Hello = old.Name != next.Name || !maps.Equal(old.Labels, next.Labels)
	d.Workers = !reflect.DeepEqual(old.Workers, next.Workers)
	return d
}

// restartFields — сравнить поля раздела old и next; ключ — <prefix>.<имя в yaml>.
func restartFields[T any](prefix string, old, next T, restart func(string, bool)) {
	o, n := reflect.ValueOf(old), reflect.ValueOf(next)
	for i := range o.NumField() {
		name, _, _ := strings.Cut(o.Type().Field(i).Tag.Get("yaml"), ",")
		restart(prefix+"."+name, !reflect.DeepEqual(o.Field(i).Interface(), n.Field(i).Interface()))
	}
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
