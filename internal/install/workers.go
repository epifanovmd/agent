package install

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/update"
)

// reCommand — command сборки-архива: путь внутри архива.
var reCommand = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)

// installWorkers — воркеры из выпуска (--worker): сборка под эту машину из
// manifest.json (старшая версия; sha256 и, если есть ключ, подпись
// сверяются) кладётся в <dataDir>/workers/<name>/current (+ version); архив
// .tar.gz распаковывается в каталог current. Вернёт записи для agent.yaml.
func installWorkers(ctx context.Context, s *System, o Options, user string) ([]config.TemplateWorker, error) {
	if len(o.Workers) == 0 {
		return nil, nil
	}
	releases := strings.TrimRight(o.Releases, "/")
	if releases == "" {
		releases = o.Server + ReleasesPath
	}
	client := s.HTTP
	if client == nil {
		tlsCfg, err := config.Server{CAFile: caIfSet(s, o)}.TLSConfig()
		if err != nil {
			return nil, err
		}
		client = &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, TLSClientConfig: tlsCfg}}
	}
	manifest, err := fetchManifest(ctx, client, releases+"/manifest.json")
	if err != nil {
		return nil, err
	}
	var pub []byte
	if key := cmpOr(o.PublicKey, o.BuiltinKey); key != "" {
		k, err := update.ParsePublicKey(key)
		if err != nil {
			return nil, fmt.Errorf("--public-key: %w", err)
		}
		pub = k
	}
	root := s.p(config.ReleasesDir(DataDir))
	if err := os.MkdirAll(root, 0o755); err != nil {
		return nil, err
	}
	if err := s.Chown(root, user); err != nil {
		return nil, err
	}
	var out []config.TemplateWorker
	for _, name := range o.Workers {
		a := manifest.Worker(name, "linux", s.Arch)
		if a == nil {
			return nil, fmt.Errorf("--worker %s: в выпуске %s нет сборки воркера под linux/%s", name, releases, s.Arch)
		}
		if a.File == "" || a.SHA256 == "" || a.File != path.Base(a.File) {
			return nil, fmt.Errorf("--worker %s: запись выпуска неполная", name)
		}
		archive := strings.HasSuffix(a.File, ".tar.gz")
		if a.Command != "" && (!archive || !reCommand.MatchString(a.Command) || strings.HasPrefix(a.Command, "/") ||
			slices.Contains(strings.Split(a.Command, "/"), "..")) {
			return nil, fmt.Errorf("--worker %s: command в выпуске — не путь внутри архива: %q", name, a.Command)
		}
		dir := filepath.Join(root, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		next := filepath.Join(dir, config.ReleaseCurrent+".new")
		_ = os.RemoveAll(next)
		build := update.Build{Name: name, Version: a.Version, OS: "linux", Arch: s.Arch, SHA256: a.SHA256}
		if pub != nil && a.Signature != "" {
			if err := update.Verify(pub, build, a.Signature); err != nil {
				return nil, fmt.Errorf("--worker %s: %w", name, err)
			}
		} else if pub != nil {
			s.warn("воркер %s: в выпуске нет подписи сборки — сверена только контрольная сумма", name)
		}
		fileURL := releases + "/" + url.PathEscape(a.File)
		if archive {
			tmp := next + ".tar.gz"
			if err := update.Download(ctx, client, "", fileURL, tmp, a.SHA256); err != nil {
				return nil, fmt.Errorf("--worker %s: %w", name, err)
			}
			err := update.Extract(tmp, next)
			os.Remove(tmp)
			if err != nil {
				return nil, fmt.Errorf("--worker %s: %w", name, err)
			}
		} else if err := update.Download(ctx, client, "", fileURL, next, a.SHA256); err != nil {
			return nil, fmt.Errorf("--worker %s: %w", name, err)
		}
		cur := filepath.Join(dir, config.ReleaseCurrent)
		if err := os.RemoveAll(cur); err != nil {
			return nil, err
		}
		if err := os.Rename(next, cur); err != nil {
			return nil, err
		}
		if err := os.WriteFile(filepath.Join(dir, config.ReleaseVersion), []byte(a.Version+"\n"), 0o644); err != nil {
			return nil, err
		}
		if err := s.Chown(dir, user); err != nil {
			return nil, err
		}
		s.say("Воркер %s %s поставлен: %s", name, a.Version, filepath.Join(config.ReleasesDir(DataDir), name, config.ReleaseCurrent))
		w := config.TemplateWorker{Name: name, Release: true, StopTimeout: a.StopTimeout}
		if a.Command != "" {
			w.Command = []string{"./" + strings.TrimPrefix(a.Command, "./")}
		}
		out = append(out, w)
	}
	return out, nil
}

func caIfSet(s *System, o Options) string {
	if o.CAFile != "" {
		return s.p(CAFile)
	}
	return ""
}

func fetchManifest(ctx context.Context, client *http.Client, u string) (*message.Manifest, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("манифест выпуска %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("манифест выпуска %s: HTTP %d", u, resp.StatusCode)
	}
	var m message.Manifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, fmt.Errorf("манифест выпуска %s: %w", u, err)
	}
	return &m, nil
}
