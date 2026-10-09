package message

import (
	"cmp"
	"strconv"
	"strings"
)

// ─── Сборки (manifest.json, §11) ───────────────────────────────────────

// Manifest — manifest.json каталога сборок (`agent-release manifest`):
// сборки агента и (необязательно) сборки воркеров. PublicKey —
// открытый ключ, которым подписаны сборки (справочно: агент проверяет подписи
// своими ключами). Artifacts пуст — только сборки воркеров. File сборки —
// имя файла в каталоге сборок или абсолютная ссылка https://.
type Manifest struct {
	Version   string           `json:"version"`
	PublicKey string           `json:"publicKey,omitempty"`
	Artifacts []Artifact       `json:"artifacts"`
	Workers   []WorkerArtifact `json:"workers,omitempty"`
}

// Artifact — сборка агента под ОС и архитектуру. Signature — Ed25519 ключом
// публикации над подписываемой строкой (§11), base64.
type Artifact struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	File      string `json:"file"`
	SHA256    string `json:"sha256"`
	Signature string `json:"signature,omitempty"`
}

// WorkerArtifact — сборка воркера (файл `<name>-<version>-<os>-<arch>`).
// StopTimeout — значение по умолчанию для записи воркера в agent.yaml при
// установке (agent install --worker).
type WorkerArtifact struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	OS          string `json:"os"`
	Arch        string `json:"arch"`
	File        string `json:"file"`
	SHA256      string `json:"sha256"`
	Signature   string `json:"signature,omitempty"`
	StopTimeout string `json:"stopTimeout,omitempty"`
	// Command — для сборки-архива: что в нём запускать (путь внутри архива,
	// например bin/report); установщик вписывает его в command воркера.
	// Нет — агент запускает файл run архива.
	Command string `json:"command,omitempty"`
}

// Worker — сборка воркера name под os/arch (nil — нет). Записей одного
// воркера под одну платформу может быть несколько — берётся старшая по
// версии (CompareVersions).
func (m *Manifest) Worker(name, goos, arch string) *WorkerArtifact {
	var best *WorkerArtifact
	for i := range m.Workers {
		w := &m.Workers[i]
		if w.Name == name && w.OS == goos && w.Arch == arch && (best == nil || CompareVersions(w.Version, best.Version) > 0) {
			best = w
		}
	}
	return best
}

// CompareVersions — сравнение версий по числовым частям через «.» (префикс
// «v» не учитывается, нечисловые части — строками; при равных числовых
// частях — сравнение строк целиком): -1, 0, 1.
func CompareVersions(a, b string) int {
	trim := func(v string) []string {
		v = strings.TrimPrefix(v, "v")
		if i := strings.IndexAny(v, "-+"); i >= 0 {
			v = v[:i]
		}
		return strings.Split(v, ".")
	}
	pa, pb := trim(a), trim(b)
	for i := 0; i < max(len(pa), len(pb)); i++ {
		var x, y string
		if i < len(pa) {
			x = pa[i]
		}
		if i < len(pb) {
			y = pb[i]
		}
		nx, ex := strconv.Atoi(x)
		ny, ey := strconv.Atoi(y)
		switch {
		case ex == nil && ey == nil && nx != ny:
			return cmp.Compare(nx, ny)
		case (ex != nil || ey != nil) && x != y:
			return strings.Compare(x, y)
		}
	}
	return strings.Compare(a, b)
}
