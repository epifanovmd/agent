package install

import (
	"context"
	"encoding/json"
	"errors"
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

// installWorkers — воркеры со сборкой с сервера (--worker): сборка под эту машину из
// manifest.json (старшая версия; sha256 и, если есть ключ, подпись
// сверяются) кладётся в <dataDir>/workers/<name>/current (+ version); архив
// .tar.gz распаковывается в каталог current. Вернёт записи для agent.yaml.
func installWorkers(ctx context.Context, s *System, l Paths, o Options, user string) ([]config.TemplateWorker, error) {
	if len(o.Workers) == 0 {
		return nil, nil
	}
	releases := strings.TrimRight(o.Releases, "/")
	if releases == "" {
		releases = o.Server + ReleasesPath
	}
	client := s.HTTP
	if client == nil {
		tlsCfg, err := config.Server{CAFile: caIfSet(s, l, o)}.TLSConfig()
		if err != nil {
			return nil, err
		}
		client = &http.Client{Timeout: 10 * time.Minute, Transport: &http.Transport{Proxy: http.ProxyFromEnvironment, TLSClientConfig: tlsCfg}}
	}
	manifest, err := fetchManifest(ctx, client, releases+"/manifest.json")
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
		a := manifest.Worker(name, "linux", s.Arch)
		if a == nil {
			return nil, fmt.Errorf("--worker %s: в каталоге сборок %s нет сборки воркера под linux/%s", name, releases, s.Arch)
		}
		if a.SHA256 == "" {
			return nil, fmt.Errorf("--worker %s: запись о сборке в manifest.json неполная", name)
		}
		fileURL, err := buildURL(releases, a.File)
		if err != nil {
			return nil, fmt.Errorf("--worker %s: %w", name, err)
		}
		archive := isArchive(fileURL)
		if a.Command != "" && (!archive || !reCommand.MatchString(a.Command) || strings.HasPrefix(a.Command, "/") ||
			slices.Contains(strings.Split(a.Command, "/"), "..")) {
			return nil, fmt.Errorf("--worker %s: command в manifest.json — не путь внутри архива: %q", name, a.Command)
		}
		dir := filepath.Join(root, name)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return nil, err
		}
		next := filepath.Join(dir, config.ReleaseCurrent+".new")
		_ = os.RemoveAll(next)
		build := update.Build{Name: name, Version: a.Version, OS: "linux", Arch: s.Arch, SHA256: a.SHA256}
		if len(keys) > 0 && a.Signature != "" {
			if err := keys.Verify(build, a.Signature); err != nil {
				return nil, fmt.Errorf("--worker %s: %w", name, err)
			}
		} else if len(keys) > 0 {
			s.warn("воркер %s: в manifest.json нет подписи сборки — сверена только контрольная сумма", name)
		}
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
		s.say("Воркер %s %s поставлен: %s", name, a.Version, filepath.Join(config.ReleasesDir(l.DataDir), name, config.ReleaseCurrent))
		w := config.TemplateWorker{Name: name, Release: true, StopTimeout: a.StopTimeout}
		if a.Command != "" {
			w.Command = []string{"./" + strings.TrimPrefix(a.Command, "./")}
		}
		out = append(out, w)
	}
	return out, nil
}

// buildURL — где скачать сборку по записи manifest.json (§11): имя файла — в
// каталоге сборок releases; абсолютная ссылка https:// — как есть; http:// —
// только если и каталог сборок http://.
func buildURL(releases, file string) (string, error) {
	if file == "" {
		return "", errors.New("запись о сборке в manifest.json неполная")
	}
	if file == path.Base(file) && !strings.Contains(file, ":") {
		return releases + "/" + url.PathEscape(file), nil
	}
	u, err := url.Parse(file)
	if err != nil || u.Host == "" || u.User != nil {
		return "", fmt.Errorf("файл сборки %q: нужно имя файла или ссылка https://", file)
	}
	base, _ := url.Parse(releases)
	if !strings.EqualFold(u.Scheme, "https") && (!strings.EqualFold(u.Scheme, "http") || base == nil || !strings.EqualFold(base.Scheme, "http")) {
		return "", fmt.Errorf("файл сборки %q: нужна ссылка https:// (http:// — только если и каталог сборок http://)", file)
	}
	return u.String(), nil
}

// isArchive — сборка — архив .tar.gz (по пути ссылки).
func isArchive(rawURL string) bool {
	if u, err := url.Parse(rawURL); err == nil {
		return strings.HasSuffix(u.Path, ".tar.gz")
	}
	return strings.HasSuffix(rawURL, ".tar.gz")
}

func caIfSet(s *System, l Paths, o Options) string {
	if o.CAFile != "" {
		return s.p(l.CAFile)
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
		return nil, fmt.Errorf("манифест сборок %s: %w", u, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("манифест сборок %s: HTTP %d", u, resp.StatusCode)
	}
	var m message.Manifest
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, fmt.Errorf("манифест сборок %s: %w", u, err)
	}
	return &m, nil
}
