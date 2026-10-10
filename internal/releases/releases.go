// Package releases — каталог сборок (manifest.json и файлы сборок, §11): с
// сервера, из релиза агента на GitHub или из локальной папки; установка
// сборки воркера в его каталог <dataDir>/workers/<name>.
package releases

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/update"
)

// DefaultBase — каталог сборок агента: его релизы на GitHub.
const DefaultBase = "https://github.com/epifanovmd/agent/releases"

// ManifestFile — список сборок каталога.
const ManifestFile = "manifest.json"

// reCommand — command сборки-архива: путь внутри архива.
var reCommand = regexp.MustCompile(`^[A-Za-z0-9._/-]+$`)

// LatestDir — сборки последней версии агента в каталоге base ("" — DefaultBase),
// устроенном как релизы на GitHub.
func LatestDir(base string) string { return baseOf(base) + "/latest/download" }

// VersionDir — сборки версии version агента в каталоге base ("" — DefaultBase).
func VersionDir(base, version string) string {
	return baseOf(base) + "/download/v" + strings.TrimPrefix(version, "v")
}

func baseOf(base string) string {
	if base == "" {
		base = DefaultBase
	}
	return strings.TrimRight(base, "/")
}

// remote — каталог по адресу http(s)://, иначе — локальная папка.
func remote(dir string) bool {
	return strings.HasPrefix(dir, "http://") || strings.HasPrefix(dir, "https://")
}

// Manifest — manifest.json каталога dir.
func Manifest(ctx context.Context, client *http.Client, dir string) (*message.Manifest, error) {
	where := strings.TrimRight(dir, "/") + "/" + ManifestFile
	var body io.ReadCloser
	if remote(dir) {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, where, nil)
		if err != nil {
			return nil, err
		}
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("манифест сборок %s: %w", where, err)
		}
		if resp.StatusCode != http.StatusOK {
			resp.Body.Close()
			return nil, fmt.Errorf("манифест сборок %s: HTTP %d", where, resp.StatusCode)
		}
		body = resp.Body
	} else {
		where = filepath.Join(dir, ManifestFile)
		f, err := os.Open(where)
		if err != nil {
			return nil, fmt.Errorf("манифест сборок: %w", err)
		}
		body = f
	}
	defer body.Close()
	var m message.Manifest
	if err := json.NewDecoder(body).Decode(&m); err != nil {
		return nil, fmt.Errorf("манифест сборок %s: %w", where, err)
	}
	return &m, nil
}

// BuildURL — где взять сборку по записи manifest.json (§11): имя файла — в
// каталоге dir; абсолютная ссылка https:// — как есть; http:// — только если
// и каталог http://. У локальной папки — только имя файла в ней.
func BuildURL(dir, file string) (string, error) {
	if file == "" {
		return "", errors.New("запись о сборке в manifest.json неполная")
	}
	if file == path.Base(file) && !strings.Contains(file, ":") {
		if !remote(dir) {
			return filepath.Join(dir, file), nil
		}
		return strings.TrimRight(dir, "/") + "/" + url.PathEscape(file), nil
	}
	u, err := url.Parse(file)
	if err != nil || u.Host == "" || u.User != nil {
		return "", fmt.Errorf("файл сборки %q: нужно имя файла или ссылка https://", file)
	}
	base, _ := url.Parse(dir)
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

// fetch — сборка src (ссылка или локальный файл) в dst со сверкой sha256.
func fetch(ctx context.Context, client *http.Client, src, dst, sha256hex string) error {
	if remote(src) {
		return update.Download(ctx, client, "", src, dst, sha256hex)
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(out, h), in); err != nil {
		out.Close()
		os.Remove(dst)
		return err
	}
	if err := out.Close(); err != nil {
		os.Remove(dst)
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); !update.SameHash(got, sha256hex) {
		os.Remove(dst)
		return fmt.Errorf("%s: sha256 не сходится: %s", filepath.Base(src), got)
	}
	return nil
}

// Placed — поставленная сборка воркера.
type Placed struct {
	Version string
	// Command — что запускать в архиве (из manifest.json), пусто — ./run или сам файл.
	Command string
	// StopTimeout — из manifest.json.
	StopTimeout string
	// Unsigned — подписи в manifest.json нет, сверена только контрольная сумма.
	Unsigned bool
}

// PlaceWorker — сборка воркера name под goos/arch из каталога dir (старшая
// версия; sha256 и, если есть ключи, подпись сверяются) в releaseDir/current
// (+ version); архив .tar.gz распаковывается в каталог current.
func PlaceWorker(ctx context.Context, client *http.Client, dir string, m *message.Manifest, name, goos, arch, releaseDir string, keys update.Keys) (Placed, error) {
	a := m.Worker(name, goos, arch)
	if a == nil {
		return Placed{}, fmt.Errorf("в каталоге сборок %s нет сборки воркера %s под %s/%s", dir, name, goos, arch)
	}
	if a.SHA256 == "" {
		return Placed{}, errors.New("запись о сборке в manifest.json неполная")
	}
	src, err := BuildURL(dir, a.File)
	if err != nil {
		return Placed{}, err
	}
	archive := isArchive(src)
	if a.Command != "" && (!archive || !reCommand.MatchString(a.Command) || strings.HasPrefix(a.Command, "/") ||
		slices.Contains(strings.Split(a.Command, "/"), "..")) {
		return Placed{}, fmt.Errorf("command в manifest.json — не путь внутри архива: %q", a.Command)
	}
	res := Placed{Version: a.Version, Command: a.Command, StopTimeout: a.StopTimeout}
	build := update.Build{Name: name, Version: a.Version, OS: goos, Arch: arch, SHA256: a.SHA256}
	switch {
	case len(keys) > 0 && a.Signature != "":
		if err := keys.Verify(build, a.Signature); err != nil {
			return Placed{}, err
		}
	case len(keys) > 0:
		res.Unsigned = true
	}
	if err := os.MkdirAll(releaseDir, 0o755); err != nil {
		return Placed{}, err
	}
	next := filepath.Join(releaseDir, config.ReleaseCurrent+".new")
	_ = os.RemoveAll(next)
	if archive {
		tmp := next + ".tar.gz"
		if err := fetch(ctx, client, src, tmp, a.SHA256); err != nil {
			return Placed{}, err
		}
		err := update.Extract(tmp, next)
		os.Remove(tmp)
		if err != nil {
			return Placed{}, err
		}
	} else if err := fetch(ctx, client, src, next, a.SHA256); err != nil {
		return Placed{}, err
	}
	cur := filepath.Join(releaseDir, config.ReleaseCurrent)
	if err := os.RemoveAll(cur); err != nil {
		return Placed{}, err
	}
	if err := os.Rename(next, cur); err != nil {
		return Placed{}, err
	}
	if err := os.WriteFile(filepath.Join(releaseDir, config.ReleaseVersion), []byte(a.Version+"\n"), 0o644); err != nil {
		return Placed{}, err
	}
	return res, nil
}

// AgentBuild — сборка агента версии version под goos/arch из каталога base
// ("" — релизы на GitHub): ссылка, sha256 и подпись из manifest.json версии.
func AgentBuild(ctx context.Context, client *http.Client, base, version, goos, arch string) (update.Release, error) {
	version = strings.TrimPrefix(version, "v")
	dir := VersionDir(base, version)
	m, err := Manifest(ctx, client, dir)
	if err != nil {
		return update.Release{}, err
	}
	if m.Version != "" && message.CompareVersions(m.Version, version) != 0 {
		return update.Release{}, fmt.Errorf("в каталоге %s версия %s, а не %s", dir, m.Version, version)
	}
	for _, a := range m.Artifacts {
		if a.OS != goos || a.Arch != arch {
			continue
		}
		src, err := BuildURL(dir, a.File)
		if err != nil {
			return update.Release{}, err
		}
		return update.Release{Version: version, URL: src, SHA256: a.SHA256, Signature: a.Signature}, nil
	}
	return update.Release{}, fmt.Errorf("в каталоге %s нет сборки агента под %s/%s", dir, goos, arch)
}
