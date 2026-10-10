// Package bundle — архив папки агента для узла (agent pack) и установка из
// него (agent install): программа под платформу узла, файлы настроек, воркеры
// из папки и сборки воркеров в каталоге release/ (manifest.json, §11).
package bundle

import (
	"context"
	"crypto/ed25519"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/releases"
	"github.com/epifanovmd/agent/internal/update"
)

// Файлы архива.
const (
	// Dir — папка в архиве: распаковка даёт agent/.
	Dir = "agent"
	// InfoFile — что в архиве: версия агента, файл настроек, платформа, ключи.
	InfoFile = "bundle.json"
	// ReleaseDir — сборки воркеров архива с manifest.json.
	ReleaseDir = "release"
	// Binary — программа агента в папке.
	Binary = "agent"
)

// Info — bundle.json.
type Info struct {
	// Version — версия агента в архиве.
	Version string `json:"version"`
	// Config — главный файл настроек (путь в папке), Env — окружение (--env).
	Config string `json:"config"`
	Env    string `json:"env,omitempty"`
	OS     string `json:"os"`
	Arch   string `json:"arch"`
	// PublicKeys — открытые ключи, которыми подписаны сборки воркеров архива:
	// установка добавляет их к ключам проверки агента.
	PublicKeys []string `json:"publicKeys,omitempty"`
	// Workers — воркеры, чьи сборки лежат в release/.
	Workers   []string `json:"workers"`
	CreatedAt int64    `json:"createdAt"`
}

// Platform — ОС и архитектура узла.
type Platform struct{ OS, Arch string }

func (p Platform) String() string { return p.OS + "/" + p.Arch }

// ParsePlatforms — "linux/amd64,linux/arm64".
func ParsePlatforms(s string) ([]Platform, error) {
	var out []Platform
	for _, item := range strings.Split(s, ",") {
		goos, arch, ok := strings.Cut(strings.TrimSpace(item), "/")
		if !ok || !slices.Contains([]string{"linux", "darwin"}, goos) || !slices.Contains([]string{"amd64", "arm64"}, arch) {
			return nil, fmt.Errorf("--platform: %q — linux|darwin/amd64|arm64, например linux/amd64", item)
		}
		out = append(out, Platform{goos, arch})
	}
	return out, nil
}

// Options — что и как упаковать.
type Options struct {
	// Config — главный файл настроек папки агента (agent.prod.yaml).
	Config string
	// Env — окружение (для имени архива и bundle.json), "" — по имени файла.
	Env       string
	Platforms []Platform
	// Out — каталог для архивов.
	Out string
	// WithEnv — положить в архив файлы переменных (envFiles): в них секреты.
	WithEnv bool
	// ReleaseOut — ещё и общий каталог сборок воркеров из папки под все платформы
	// (manifest.json и архивы): его раздаёт бэкенд для worker.update ("" — не нужен).
	ReleaseOut string
	// Version — версия этой программы; Self — её файл.
	Version, Self string
	// Keys — ключи проверки сборок, скачанных из каталога сборок агента.
	Keys update.Keys
	// Signing — ключ подписи сборок воркеров (nil — без подписи).
	Signing ed25519.PrivateKey
	Client  *http.Client
	// Warn — замечание для человека.
	Warn func(string)

	share func(a message.WorkerArtifact, file string) error
}

// Pack — архивы папки агента, по одному на платформу. Вернёт их пути.
func Pack(ctx context.Context, o Options) ([]string, error) {
	src := config.ReadSource(o.Config)
	if len(src.Errors) > 0 {
		return nil, problems(src.Errors)
	}
	cfg := config.Defaults()
	if err := src.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("%s: %w", o.Config, err)
	}
	if v := src.Env["AGENT_UPDATE_RELEASES"]; v != "" {
		cfg.Update.Releases = v
	}
	if v := os.Getenv("AGENT_UPDATE_RELEASES"); v != "" {
		cfg.Update.Releases = v
	}
	if err := cfg.ValidateWorkers(); err != nil {
		return nil, err
	}
	root := filepath.Dir(src.Files[len(src.Files)-1])
	files := slices.Clone(src.Files)
	if o.WithEnv {
		files = append(files, src.EnvFiles...)
	}
	rels := make([]string, len(files))
	for i, f := range files {
		rel, err := inside(root, f)
		if err != nil {
			return nil, fmt.Errorf("файл %s — вне папки агента %s: в архив попадает только папка", f, root)
		}
		rels[i] = rel
	}
	if o.Version == "dev" && slices.ContainsFunc(o.Platforms, func(p Platform) bool { return p.OS != runtime.GOOS || p.Arch != runtime.GOARCH }) {
		return nil, errors.New("агент без версии (dev) упаковывает только себя: --platform " + runtime.GOOS + "/" + runtime.GOARCH)
	}
	label := o.Env
	if label == "" {
		label = strings.TrimSuffix(filepath.Base(o.Config), filepath.Ext(o.Config))
	}
	if err := os.MkdirAll(o.Out, 0o755); err != nil {
		return nil, err
	}
	var out []string
	var shared []message.WorkerArtifact
	if o.ReleaseOut != "" {
		if err := os.MkdirAll(o.ReleaseOut, 0o755); err != nil {
			return nil, err
		}
		o.share = func(a message.WorkerArtifact, file string) error {
			shared = append(shared, a)
			return copyFile(file, filepath.Join(o.ReleaseOut, a.File))
		}
	}
	for _, p := range o.Platforms {
		archive := filepath.Join(o.Out, fmt.Sprintf("agent-%s-%s-%s-%s.tar.gz", label, strings.TrimPrefix(o.Version, "v"), p.OS, p.Arch))
		if err := packOne(ctx, o, cfg, root, rels, rels[len(src.Files)-1], p, archive); err != nil {
			return out, fmt.Errorf("%s: %w", p, err)
		}
		out = append(out, archive)
	}
	if o.ReleaseOut != "" {
		m := message.Manifest{Version: strings.TrimPrefix(o.Version, "v"), Artifacts: []message.Artifact{}, Workers: shared}
		if o.Signing != nil {
			m.PublicKey = releases.PublicKeyOf(o.Signing)
		}
		if err := writeJSON(filepath.Join(o.ReleaseOut, releases.ManifestFile), m); err != nil {
			return out, err
		}
	}
	return out, nil
}

func packOne(ctx context.Context, o Options, cfg config.Config, root string, rels []string, main string, p Platform, archive string) error {
	tmp, err := os.MkdirTemp("", "agent-pack-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	stage := filepath.Join(tmp, Dir)
	release := filepath.Join(stage, ReleaseDir)
	if err := os.MkdirAll(release, 0o755); err != nil {
		return err
	}
	if err := binary(ctx, o, cfg, p, filepath.Join(stage, Binary)); err != nil {
		return err
	}
	for _, rel := range rels {
		if err := copyFile(filepath.Join(root, rel), filepath.Join(stage, rel)); err != nil {
			return err
		}
	}
	m := message.Manifest{Version: strings.TrimPrefix(o.Version, "v"), Artifacts: []message.Artifact{}}
	info := Info{Version: m.Version, Config: main, Env: o.Env, OS: p.OS, Arch: p.Arch, CreatedAt: time.Now().UnixMilli()}
	if o.Signing != nil {
		m.PublicKey = releases.PublicKeyOf(o.Signing)
		info.PublicKeys = []string{m.PublicKey}
	}
	var agentManifest *message.Manifest
	for _, w := range cfg.Workers {
		switch {
		case w.Path != "":
			rel, err := inside(root, w.Path)
			if err != nil {
				return fmt.Errorf("воркер %s: папка %s — вне папки агента", w.Name, w.Path)
			}
			version, err := readVersion(w.Path)
			if err != nil {
				return fmt.Errorf("воркер %s: %w", w.Name, err)
			}
			src, skip := w.Path, releases.ReadIgnore(w.Path)
			if isExecutable(filepath.Join(w.Path, "build")) {
				if src, err = buildWorker(ctx, w.Path, filepath.Join(tmp, "build-"+w.Name), version, p); err != nil {
					return fmt.Errorf("воркер %s: %w", w.Name, err)
				}
				skip = nil
			}
			if err := copyDir(src, filepath.Join(stage, rel), skip); err != nil {
				return err
			}
			file := fmt.Sprintf("%s-%s-%s-%s.tar.gz", w.Name, version, p.OS, p.Arch)
			hash, err := releases.TarDirSkip(src, filepath.Join(release, file), "", skip)
			if err != nil {
				return fmt.Errorf("воркер %s: %w", w.Name, err)
			}
			a := message.WorkerArtifact{Name: w.Name, Version: version, OS: p.OS, Arch: p.Arch, File: file, SHA256: hash}
			if o.Signing != nil {
				a.Signature = update.Sign(o.Signing, update.Build{Name: w.Name, Version: version, OS: p.OS, Arch: p.Arch, SHA256: hash})
			}
			m.Workers = append(m.Workers, a)
			if o.share != nil {
				if err := o.share(a, filepath.Join(release, file)); err != nil {
					return err
				}
			}
		case w.From == config.FromAgent:
			if agentManifest == nil {
				am, err := releases.Manifest(ctx, o.Client, releases.VersionDir(cfg.Update.Releases, o.Version))
				if err != nil {
					return fmt.Errorf("воркер %s: %w", w.Name, err)
				}
				agentManifest = am
			}
			a := agentManifest.Worker(w.Name, p.OS, p.Arch)
			if a == nil {
				return fmt.Errorf("воркер %s: в релизе агента %s нет его сборки под %s", w.Name, o.Version, p)
			}
			src, err := releases.BuildURL(releases.VersionDir(cfg.Update.Releases, o.Version), a.File)
			if err != nil {
				return err
			}
			entry := *a
			entry.File = filepath.Base(a.File)
			if err := download(ctx, o, src, filepath.Join(release, entry.File), entry.SHA256); err != nil {
				return fmt.Errorf("воркер %s: %w", w.Name, err)
			}
			m.Workers = append(m.Workers, entry)
		case w.Release:
			o.Warn(fmt.Sprintf("воркер %s со сборкой с сервера (release: true) — в архив не попадает: его ставит сервер", w.Name))
			continue
		default:
			continue
		}
		info.Workers = append(info.Workers, w.Name)
	}
	if err := writeJSON(filepath.Join(release, releases.ManifestFile), m); err != nil {
		return err
	}
	if err := writeJSON(filepath.Join(stage, InfoFile), info); err != nil {
		return err
	}
	_, err = releases.TarDir(stage, archive, Dir)
	return err
}

// binary — программа агента под платформу p: эта же, если платформа и есть
// её; иначе — из каталога сборок агента той же версии (подпись сверяется).
func binary(ctx context.Context, o Options, cfg config.Config, p Platform, dst string) error {
	if p.OS == runtime.GOOS && p.Arch == runtime.GOARCH {
		return copyFile(o.Self, dst)
	}
	rel, err := releases.AgentBuild(ctx, o.Client, cfg.Update.Releases, o.Version, p.OS, p.Arch)
	if err != nil {
		return err
	}
	if len(o.Keys) == 0 {
		o.Warn("ключей проверки подписи нет — у программы агента под " + p.String() + " сверена только контрольная сумма")
	} else if err := o.Keys.Verify(update.Build{Name: update.AgentName, Version: rel.Version, OS: p.OS, Arch: p.Arch, SHA256: rel.SHA256}, rel.Signature); err != nil {
		return err
	}
	return download(ctx, o, rel.URL, dst, rel.SHA256)
}

func download(ctx context.Context, o Options, src, dst, sha string) error {
	if err := update.Download(ctx, o.Client, "", src, dst, sha); err != nil {
		return err
	}
	return os.Chmod(dst, 0o755)
}

// readVersion — версия воркера из файла VERSION в его папке.
func readVersion(dir string) (string, error) {
	raw, err := os.ReadFile(filepath.Join(dir, "VERSION"))
	if err != nil {
		return "", errors.New("нет файла VERSION в папке воркера — версия нужна для сборки")
	}
	v := strings.TrimSpace(string(raw))
	if v == "" || strings.ContainsAny(v, "/ \t") {
		return "", fmt.Errorf("VERSION: %q — нужна версия вида 1.0.0", v)
	}
	return v, nil
}

// inside — путь file относительно root; вне root — ошибка.
func inside(root, file string) (string, error) {
	rel, err := filepath.Rel(root, file)
	if err != nil || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("%s вне %s", file, root)
	}
	return rel, nil
}

func problems(list []config.Problem) error {
	errs := make([]error, len(list))
	for i, p := range list {
		errs[i] = errors.New(p.String())
	}
	return errors.Join(errs...)
}

func writeJSON(path string, v any) error {
	raw, err := json.MarshalIndent(v, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(raw, '\n'), 0o644)
}

func copyFile(src, dst string) error {
	info, err := os.Stat(src)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return err
	}
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, info.Mode().Perm())
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

// isExecutable — обычный файл с правом запуска.
func isExecutable(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.Mode().IsRegular() && st.Mode().Perm()&0o111 != 0
}

// buildWorker — сборка воркера под платформу p: его build запускается в папке воркера с
// GOOS, GOARCH (и AGENT_OS, AGENT_ARCH) и OUT — каталогом итога; в OUT должен появиться
// исполняемый run, VERSION кладётся сюда же. Вернёт OUT.
func buildWorker(ctx context.Context, dir, out, version string, p Platform) (string, error) {
	if err := os.MkdirAll(out, 0o755); err != nil {
		return "", err
	}
	cmd := exec.CommandContext(ctx, filepath.Join(dir, "build"))
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOOS="+p.OS, "GOARCH="+p.Arch, "AGENT_OS="+p.OS, "AGENT_ARCH="+p.Arch, "OUT="+out)
	if raw, err := cmd.CombinedOutput(); err != nil {
		return "", fmt.Errorf("build под %s: %v\n%s", p, err, strings.TrimSpace(string(raw)))
	}
	if !isExecutable(filepath.Join(out, "run")) {
		return "", fmt.Errorf("build под %s не оставил исполняемый $OUT/run", p)
	}
	return out, os.WriteFile(filepath.Join(out, "VERSION"), []byte(version+"\n"), 0o644)
}

// copyDir — каталог src в dst без путей skip: файлы с правами, ссылки — ссылками.
func copyDir(src, dst string, skip releases.Skip) error {
	return filepath.Walk(src, func(p string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(src, p)
		if rel != "." && skip != nil && skip(filepath.ToSlash(rel), info.IsDir()) {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		target := filepath.Join(dst, rel)
		switch {
		case info.IsDir():
			if info.Name() == "__pycache__" || info.Name() == ".git" {
				return filepath.SkipDir
			}
			return os.MkdirAll(target, 0o755)
		case info.Mode()&os.ModeSymlink != 0:
			link, err := os.Readlink(p)
			if err != nil {
				return err
			}
			return os.Symlink(link, target)
		case info.Mode().IsRegular():
			if info.Name() == ".DS_Store" || strings.HasSuffix(info.Name(), ".pyc") {
				return nil
			}
			return copyFile(p, target)
		}
		return nil
	})
}
