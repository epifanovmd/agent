//go:build unix

package app

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"

	"github.com/epifanovmd/agent/protocol"
)

// LocalDesiredDir — каталог желаемого без бэкенда (§18): desired/<воркер>/<ключ>.json рядом с
// файлом настроек.
const LocalDesiredDir = "desired"

// localDesiredDir — desired рядом с файлом настроек (без файла — в каталоге данных).
func (a *App) localDesiredDir() string {
	if a.configPath != "" {
		return filepath.Join(filepath.Dir(a.configPath), LocalDesiredDir)
	}
	return filepath.Join(a.config().DataDir, LocalDesiredDir)
}

// loadDesired — желаемое при запуске: последнее от бэкенда или из файлов.
func (a *App) loadDesired() {
	if a.backend == nil {
		a.setDesired(a.readLocalDesired())
		return
	}
	d, ok, err := a.store.LoadDesired()
	if err != nil {
		a.log.Error("сохранённое желаемое не прочитано", "err", err)
	}
	if ok {
		a.setDesired(d)
	}
}

// readLocalDesired — желаемое из файлов: версия ключа — время изменения файла (мс).
func (a *App) readLocalDesired() protocol.Desired {
	d := protocol.Desired{Workers: map[string]protocol.WorkerDesired{}}
	dir := a.localDesiredDir()
	workers, _ := os.ReadDir(dir)
	for _, w := range workers {
		if !w.IsDir() || !protocol.ValidName(w.Name()) {
			continue
		}
		files, _ := os.ReadDir(filepath.Join(dir, w.Name()))
		configs := map[string]protocol.ConfigValue{}
		for _, f := range files {
			key, ok := strings.CutSuffix(f.Name(), ".json")
			if !ok || f.IsDir() || !protocol.ValidName(key) {
				continue
			}
			path := filepath.Join(dir, w.Name(), f.Name())
			raw, err := os.ReadFile(path)
			info, ierr := f.Info()
			if err != nil || ierr != nil || !json.Valid(raw) {
				a.log.Warn("файл желаемого не прочитан — пропущен", "file", path, "err", err)
				continue
			}
			configs[key] = protocol.ConfigValue{Version: info.ModTime().UnixMilli(), Data: raw}
		}
		if len(configs) > 0 {
			d.Workers[w.Name()] = protocol.WorkerDesired{Configs: configs}
		}
	}
	return d
}
