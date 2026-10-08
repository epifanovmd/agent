// Package update — самообновление исполняемого файла агента: загрузка,
// проверка sha256 и подписи Ed25519, замена с копией .prev, откат версии,
// не дошедшей до связи с сервером за несколько запусков (boot guard); сборки
// воркеров из выпуска — файлом или архивом .tar.gz.
package update

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"runtime"
	"strings"
	"syscall"
)

// MaxBootAttempts — запусков новой версии без связи до отката.
const MaxBootAttempts = 3

// Release — что ставить: из команды agent.update.
type Release struct {
	Version   string `json:"version"`
	URL       string `json:"url"`
	SHA256    string `json:"sha256"`
	Signature string `json:"signature"`
}

type marker struct {
	Version  string `json:"version"`
	SHA256   string `json:"sha256"`
	Attempts int    `json:"attempts"`
}

// Paths — исполняемый файл и отметка обновления.
type Paths struct {
	Binary string
	Marker string
}

// NewPaths — пути для бинаря; отметка обновления — рядом с ним: Boot
// работает до загрузки конфигурации (новая версия может упасть раньше).
func NewPaths(binary string) Paths {
	return Paths{Binary: binary, Marker: binary + ".update.json"}
}

// AgentName — имя сборки агента в подписи.
const AgentName = "agent"

// Build — что подписывается: сборка Name (agent или имя воркера) версии
// Version под OS/Arch с содержимым SHA256 (hex).
type Build struct {
	Name, Version, OS, Arch, SHA256 string
}

// Local — сборка name версии version под эту машину.
func Local(name, version, sha256hex string) Build {
	return Build{Name: name, Version: version, OS: runtime.GOOS, Arch: runtime.GOARCH, SHA256: sha256hex}
}

// Payload — подписываемая строка (§7): agent-release/1, имя, версия, os,
// arch, sha256 строчными — через \n.
func (b Build) Payload() string {
	return strings.Join([]string{"agent-release/1", b.Name, b.Version, b.OS, b.Arch, strings.ToLower(b.SHA256)}, "\n")
}

// Sign — подпись сборки: Ed25519 над Payload, base64.
func Sign(priv ed25519.PrivateKey, b Build) string {
	return base64.StdEncoding.EncodeToString(ed25519.Sign(priv, []byte(b.Payload())))
}

// Verify — подпись сборки ключом выпуска: подпись другой сборки (версии,
// платформы, имени) не подходит.
func Verify(pub ed25519.PublicKey, b Build, signature string) error {
	sig, err := base64.StdEncoding.DecodeString(signature)
	if err != nil || !ed25519.Verify(pub, []byte(b.Payload()), sig) {
		return fmt.Errorf("update: подпись сборки %s %s (%s/%s) не сходится", b.Name, b.Version, b.OS, b.Arch)
	}
	return nil
}

// ParsePublicKey — ключ проверки из base64.
func ParsePublicKey(b64 string) (ed25519.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(b64)
	if err != nil || len(raw) != ed25519.PublicKeySize {
		return nil, errors.New("update: publicKey — base64 32 байт Ed25519")
	}
	return ed25519.PublicKey(raw), nil
}

// FileHash — sha256 файла, hex.
func FileHash(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// Install — скачать, проверить и подменить исполняемый файл (без перезапуска).
func Install(ctx context.Context, client *http.Client, auth string, p Paths, pub ed25519.PublicKey, rel Release) error {
	if pub == nil {
		return ErrNotVerified
	}
	if err := Verify(pub, Local(AgentName, rel.Version, rel.SHA256), rel.Signature); err != nil {
		return err
	}
	if current, err := FileHash(p.Binary); err == nil && current == rel.SHA256 {
		return nil
	}
	next := p.Binary + ".new"
	if err := Download(ctx, client, auth, rel.URL, next, rel.SHA256); err != nil {
		return err
	}
	if err := copyFile(p.Binary, p.Binary+".prev"); err != nil {
		os.Remove(next)
		return fmt.Errorf("update: копия текущей версии: %w", err)
	}
	if err := writeMarker(p.Marker, marker{Version: rel.Version, SHA256: rel.SHA256}); err != nil {
		os.Remove(next)
		return err
	}
	return os.Rename(next, p.Binary)
}

// ErrNotVerified — ключа проверки подписи (update.publicKey) нет: сборку
// нельзя проверить, ставить её агент не будет.
var ErrNotVerified = errors.New("update: не задан ключ проверки релизов (update.publicKey)")

// Fetch — проверить подпись сборки b ключом выпуска и скачать файл в dst
// (0755) со сверкой sha256. Ключа нет — ErrNotVerified.
func Fetch(ctx context.Context, client *http.Client, auth string, pub ed25519.PublicKey, b Build, url, signature, dst string) error {
	if pub == nil {
		return ErrNotVerified
	}
	if err := Verify(pub, b, signature); err != nil {
		return err
	}
	return Download(ctx, client, auth, url, dst, b.SHA256)
}

// FetchArchive — как Fetch, но сборка — архив .tar.gz: после проверки он
// распаковывается в новый каталог dst (Extract), сам архив удаляется.
func FetchArchive(ctx context.Context, client *http.Client, auth string, pub ed25519.PublicKey, b Build, url, signature, dst string) error {
	archive := dst + ".tar.gz"
	if err := Fetch(ctx, client, auth, pub, b, url, signature, archive); err != nil {
		return err
	}
	defer os.Remove(archive)
	return Extract(archive, dst)
}

// Download — скачать url в dst (0755) и сверить sha256; при ошибке dst удаляется.
func Download(ctx context.Context, client *http.Client, auth, url, dst, sha256hex string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("update: загрузка: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("update: загрузка: HTTP %d", resp.StatusCode)
	}
	f, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return fmt.Errorf("update: %w", err)
	}
	h := sha256.New()
	if _, err := io.Copy(io.MultiWriter(f, h), resp.Body); err != nil {
		f.Close()
		os.Remove(dst)
		return fmt.Errorf("update: загрузка: %w", err)
	}
	if err := f.Close(); err != nil {
		os.Remove(dst)
		return err
	}
	if got := hex.EncodeToString(h.Sum(nil)); got != sha256hex {
		os.Remove(dst)
		return fmt.Errorf("update: sha256 не сходится: %s", got)
	}
	return nil
}

// Guard — перед каждым запуском: считать запуски новой версии; не дошла до
// связи за MaxBootAttempts — вернуть .prev. true — откат выполнен.
func Guard(p Paths) (bool, error) {
	m, err := readMarker(p.Marker)
	if err != nil || m == nil {
		return false, nil
	}
	m.Attempts++
	if m.Attempts <= MaxBootAttempts {
		return false, writeMarker(p.Marker, *m)
	}
	prev := p.Binary + ".prev"
	if _, err := os.Stat(prev); err != nil {
		_ = os.Remove(p.Marker)
		return false, nil
	}
	if err := copyFile(prev, p.Binary+".rollback"); err != nil {
		return false, fmt.Errorf("update: откат: %w", err)
	}
	if err := os.Rename(p.Binary+".rollback", p.Binary); err != nil {
		return false, fmt.Errorf("update: откат: %w", err)
	}
	_ = os.Remove(p.Marker)
	return true, nil
}

// Boot — Guard в самом процессе: после отката — перезапуск прежней версии
// (exec). Под systemd проверку делает прежняя версия до запуска новой
// (`agent.prev boot-guard`, AGENT_BOOT_GUARD=external) — так откатывается и
// версия, падающая до main.
func Boot(p Paths) error {
	if os.Getenv("AGENT_BOOT_GUARD") == "external" {
		return nil
	}
	rolledBack, err := Guard(p)
	if err != nil || !rolledBack {
		return err
	}
	return syscall.Exec(p.Binary, os.Args, os.Environ())
}

// Healthy — новая версия связалась с сервером: отметка снимается.
func Healthy(p Paths) {
	_ = os.Remove(p.Marker)
}

func readMarker(path string) (*marker, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var m marker
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, err
	}
	return &m, nil
}

func writeMarker(path string, m marker) error {
	raw, _ := json.Marshal(m)
	return os.WriteFile(path, raw, 0o600)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}
