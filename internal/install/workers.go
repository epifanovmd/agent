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
)

// installWorkers — воркеры со сборкой с сервера (--worker): сборка под эту машину из
// manifest.json (старшая версия; sha256 и, если есть ключ, подпись
// сверяются) кладётся в <dataDir>/workers/<name>/current (+ version); архив
// .tar.gz распаковывается в каталог current. Вернёт записи для agent.yaml.
func installWorkers(ctx context.Context, s *System, l Paths, o Options, user string) ([]config.TemplateWorker, error) {
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
	if err := s.Chown(root, user); err != nil {
		return nil, err
	}
	var out []config.TemplateWorker
	for _, name := range o.Workers {
		releaseDir := filepath.Join(root, name)
		placed, err := releases.PlaceWorker(ctx, client, dir, manifest, name, "linux", s.Arch, releaseDir, keys)
		if err != nil {
			return nil, fmt.Errorf("--worker %s: %w", name, err)
		}
		if placed.Unsigned {
			s.warn("воркер %s: в manifest.json нет подписи сборки — сверена только контрольная сумма", name)
		}
		if err := s.Chown(releaseDir, user); err != nil {
			return nil, err
		}
		s.say("Воркер %s %s поставлен: %s", name, placed.Version, filepath.Join(config.ReleasesDir(l.DataDir), name, config.ReleaseCurrent))
		w := config.TemplateWorker{Name: name, Release: true, StopTimeout: placed.StopTimeout}
		if placed.Command != "" {
			w.Command = []string{"./" + strings.TrimPrefix(placed.Command, "./")}
		}
		out = append(out, w)
	}
	return out, nil
}

func caIfSet(s *System, l Paths, o Options) string {
	if o.CAFile != "" {
		return s.p(l.CAFile)
	}
	return ""
}
