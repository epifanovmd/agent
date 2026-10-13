package install

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/releases"
	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/protocol"
)

// installWorkers — воркеры со сборкой с сервера (--worker): сборка под эту машину из
// manifest.json (старшая версия; sha256 и, если есть ключ, подпись
// сверяются) кладётся в <dataDir>/workers/<name>/current (+ version); архив
// .tar.gz распаковывается в каталог current. Вернёт записи для agent.yaml.
func installWorkers(ctx context.Context, s *System, l Paths, o Options, user string) ([]newWorker, error) {
	if len(o.Workers) == 0 {
		return nil, nil
	}
	dir := strings.TrimRight(o.Releases, "/")
	if dir == "" {
		dir = o.Server + ReleasesPath
	}
	client := s.HTTP
	if client == nil {
		tlsCfg, err := config.Server{CAFile: caIfSet(s, l, o)}.TLSConfig()
		if err != nil {
			return nil, err
		}
		client = &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, TLSClientConfig: tlsCfg}}
	}
	manifest, err := releases.Manifest(ctx, client, dir)
	if err != nil {
		return nil, err
	}
	keys, err := update.ParseKeys(append([]string{o.BuiltinKey}, o.PublicKeys...)...)
	if err != nil {
		return nil, fmt.Errorf("--update-key: %w", err)
	}
	root := s.p(config.ReleasesDir(l.DataDir))
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	if err := s.own(root, user); err != nil {
		return nil, err
	}
	var out []newWorker
	for _, name := range o.Workers {
		releaseDir := filepath.Join(root, name)
		// Уже стоит та же или более новая сборка (например, после worker.update) — не трогать.
		if art := manifest.Worker(name, "linux", s.Arch); art != nil {
			cur := readVersion(filepath.Join(releaseDir, config.ReleaseVersion))
			if _, err := os.Stat(filepath.Join(releaseDir, config.ReleaseCurrent)); err == nil && cur != "" &&
				protocol.CompareVersions(cur, art.Version) >= 0 {
				s.say("Воркер %s %s уже стоит (в каталоге — %s) — не меняется", name, cur, art.Version)
				out = append(out, workerEntry(name, art.StopTimeout, art.Command))
				continue
			}
		}
		placed, err := releases.PlaceWorker(ctx, client, dir, manifest, name, "linux", s.Arch, releaseDir, keys)
		if err != nil {
			return nil, fmt.Errorf("--worker %s: %w", name, err)
		}
		if placed.Unsigned {
			s.warn("воркер %s: в manifest.json нет подписи сборки — сверена только контрольная сумма", name)
		}
		if err := s.own(releaseDir, user); err != nil {
			return nil, err
		}
		s.say("Воркер %s %s поставлен: %s", name, placed.Version, filepath.Join(config.ReleasesDir(l.DataDir), name, config.ReleaseCurrent))
		out = append(out, workerEntry(name, placed.StopTimeout, placed.Command))
	}
	return out, nil
}

// workerEntry — запись воркера со сборкой для agent.yaml.
func workerEntry(name, stopTimeout, command string) newWorker {
	w := newWorker{Name: name, Release: true}
	if stopTimeout != "" {
		w.Lifecycle = &newLifecycle{StopTimeout: stopTimeout}
	}
	if command != "" {
		w.Command = []string{"./" + strings.TrimPrefix(command, "./")}
	}
	return w
}

// readVersion — версия из файла ("" — нет файла).
func readVersion(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

func caIfSet(s *System, l Paths, o Options) string {
	if o.CAFile != "" {
		return s.p(l.CAFile)
	}
	return ""
}
