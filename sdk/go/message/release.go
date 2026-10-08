package message

import (
	"cmp"
	"strconv"
	"strings"
)

// ─── Выпуск (manifest.json, §7) ────────────────────────────────────────

// Manifest — manifest.json каталога выпуска (`agent release-manifest`):
// сборки агента и (необязательно) сборки воркеров из выпуска.
type Manifest struct {
	Version   string           `json:"version"`
	Artifacts []Artifact       `json:"artifacts"`
	Workers   []WorkerArtifact `json:"workers,omitempty"`
}

// Artifact — сборка агента под ОС и архитектуру. Signature — Ed25519 ключом
// выпуска над hex sha256 файла (base64).
type Artifact struct {
	OS        string `json:"os"`
	Arch      string `json:"arch"`
	File      string `json:"file"`
	SHA256    string `json:"sha256"`
	Signature string `json:"signature,omitempty"`
}

// WorkerArtifact — сборка воркера из выпуска (файл `<name>-<version>-<os>-<arch>`).
// Restart и StopTimeout — значения по умолчанию для записи воркера в
// agent.yaml при установке (install.sh --worker).
type WorkerArtifact struct {
	Name        string `json:"name"`
	Version     string `json:"version"`
	OS          string `json:"os"`
	Arch        string `json:"arch"`
	File        string `json:"file"`
	SHA256      string `json:"sha256"`
	Signature   string `json:"signature,omitempty"`
	Restart     string `json:"restart,omitempty"`
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

// ─── Обновление воркера из выпуска ─────────────────────────────────────

// Встроенные команды агента для воркеров (§6.4).
const (
	// CommandWorkerRestart — заменить экземпляры воркера: args `{name?}`.
	CommandWorkerRestart = "worker.restart"
	// CommandWorkerUpdate — обновить воркер из выпуска (release: true):
	// args WorkerUpdate, итог WorkerUpdateResult.
	CommandWorkerUpdate = "worker.update"
)

// WorkerUpdate — args команды worker.update. URL от корня («/api/…») агент
// дополняет адресом своего сервера и скачивает со своим ключом.
type WorkerUpdate struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	URL       string `json:"url"`
	SHA256    string `json:"sha256"`
	Signature string `json:"signature"`
}

// WorkerUpdateResult — итог worker.update: Previous — версия до обновления
// (пусто — не была известна).
type WorkerUpdateResult struct {
	Name     string `json:"name"`
	Version  string `json:"version"`
	Previous string `json:"previous"`
}

// Коды ошибок команд обновления (cmd.done.error.code).
const (
	// ErrUpdateNotVerified — у агента нет ключа проверки подписи (update.publicKey).
	ErrUpdateNotVerified = "UPDATE_NOT_VERIFIED"
	// ErrWorkerNotReleased — воркер не из выпуска (нет release: true) или его нет.
	ErrWorkerNotReleased = "WORKER_NOT_RELEASED"
	// ErrWorkerUpdateFailed — загрузка, проверка или запуск новой сборки не
	// удались; агент вернул прежнюю сборку.
	ErrWorkerUpdateFailed = "WORKER_UPDATE_FAILED"
	// ErrWorkerUpdateInProgress — этот воркер уже обновляется.
	ErrWorkerUpdateInProgress = "WORKER_UPDATE_IN_PROGRESS"
)
