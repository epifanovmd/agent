package server

import (
	"bytes"
	"cmp"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Пути раздачи релизов агента (спецификация §7) — публичные: сборки
// подписаны, секретов в них нет.
const (
	ReleasesPath = message.LinkPath + "/releases/"
	InstallPath  = message.LinkPath + "/install.sh"
)

// updateTimeout — срок команд agent.update и worker.update.
const updateTimeout = 300

// Release — manifest.json каталога выпуска (make release).
type Release struct {
	Version   string            `json:"version"`
	Artifacts []ReleaseArtifact `json:"artifacts"`
	// Workers — подписанные сборки воркеров (необязательно).
	Workers []WorkerArtifact `json:"workers,omitempty"`
}

// WorkerArtifact — сборка воркера под ОС и архитектуру (message.WorkerArtifact).
type WorkerArtifact = message.WorkerArtifact

// ReleaseArtifact — сборка агента под ОС и архитектуру (message.Artifact).
type ReleaseArtifact = message.Artifact

// artifact — сборка под os/arch (nil — нет).
func (r *Release) artifact(goos, arch string) *ReleaseArtifact {
	for i := range r.Artifacts {
		if a := &r.Artifacts[i]; a.OS == goos && a.Arch == arch {
			return a
		}
	}
	return nil
}

// worker — сборка воркера name под os/arch; записей несколько — старшая
// версия (nil — нет).
func (r *Release) worker(name, goos, arch string) *WorkerArtifact {
	var best *WorkerArtifact
	for i := range r.Workers {
		w := &r.Workers[i]
		if w.Name == name && w.OS == goos && w.Arch == arch && (best == nil || compareVersions(w.Version, best.Version) > 0) {
			best = w
		}
	}
	return best
}

// releaseFile — файл есть в манифесте (сборка агента или воркера).
func (r *Release) releaseFile(name string) bool {
	return slices.ContainsFunc(r.Artifacts, func(art ReleaseArtifact) bool { return art.File == name }) ||
		slices.ContainsFunc(r.Workers, func(w WorkerArtifact) bool { return w.File == name })
}

// compareVersions — сравнение версий по числовым частям через «.» (пре-релиз
// после «-» и «+» не учитывается; нечисловые части — строками).
func compareVersions(a, b string) int {
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

// UpdateCandidate — агент, которому есть обновление: Current — его версия,
// Target — версия выпуска.
type UpdateCandidate struct {
	AgentID string `json:"agentId"`
	Name    string `json:"name"`
	Online  bool   `json:"online"`
	Current string `json:"current"`
	Target  string `json:"target"`
	OS      string `json:"os"`
	Arch    string `json:"arch"`
}

// Release — манифест выпуска из Options.ReleasesDir (nil — выпуска нет или
// манифест не читается).
func (a *Agents) Release() *Release {
	if a.opts.ReleasesDir == "" {
		return nil
	}
	raw, err := os.ReadFile(filepath.Join(a.opts.ReleasesDir, "manifest.json"))
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			a.log.Warn("манифест выпуска не прочитан", "err", err)
		}
		return nil
	}
	var r Release
	if err := json.Unmarshal(raw, &r); err != nil || r.Version == "" {
		a.log.Warn("манифест выпуска некорректен", "err", err)
		return nil
	}
	return &r
}

// capabilities — возможности агента (из последнего hello, если отдельно нет).
func capabilities(a *Agent) *message.Capabilities {
	if a.Capabilities != nil {
		return a.Capabilities
	}
	if a.Hello != nil {
		return &a.Hello.Capabilities
	}
	return nil
}

// selfUpdate — агент обновляет себя сам (capabilities.update.mode = self).
func selfUpdate(a *Agent) bool {
	caps := capabilities(a)
	return caps != nil && caps.Update != nil && caps.Update.Mode == "self"
}

// UpdateCandidates — агенты с update.mode = self, чья версия
// (hello.agent.version) не совпадает с выпуском и для чьих hello.host.os/arch
// есть сборка.
func (a *Agents) UpdateCandidates() ([]UpdateCandidate, error) {
	out := []UpdateCandidate{}
	rel := a.Release()
	if rel == nil {
		return out, nil
	}
	list, err := a.List()
	if err != nil {
		return nil, err
	}
	for _, agent := range list {
		if agent.Revoked || agent.Hello == nil || !selfUpdate(agent) || agent.Hello.Agent.Version == rel.Version {
			continue
		}
		host := agent.Hello.Host
		if rel.artifact(host.OS, host.Arch) == nil {
			continue
		}
		out = append(out, UpdateCandidate{
			AgentID: agent.ID, Name: agent.Name, Online: agent.Online,
			Current: agent.Hello.Agent.Version, Target: rel.Version, OS: host.OS, Arch: host.Arch,
		})
	}
	return out, nil
}

// UpdateAgent — команда agent.update {version, url, sha256, signature} со
// сборкой выпуска под ОС и архитектуру агента, срок 300 с. Нет выпуска или
// сборки, агент не self — UPDATE_NOT_AVAILABLE.
func (a *Agents) UpdateAgent(agentID string) (*Command, error) { return a.updateAgent("", agentID) }

func (a *Agents) updateAgent(actor, agentID string) (*Command, error) {
	agent, err := a.Agent(agentID)
	if err != nil {
		return nil, protoErr("AGENT_NOT_FOUND", "Агент не найден")
	}
	unavailable := func(msg string) error { return protoErr("UPDATE_NOT_AVAILABLE", msg) }
	rel := a.Release()
	switch {
	case rel == nil:
		return nil, unavailable("Выпуска агента нет")
	case agent.Hello == nil || !selfUpdate(agent):
		return nil, unavailable("Агент не обновляет себя сам (update.mode ≠ self)")
	}
	art := rel.artifact(agent.Hello.Host.OS, agent.Hello.Host.Arch)
	if art == nil {
		return nil, unavailable("Нет сборки под " + agent.Hello.Host.OS + "/" + agent.Hello.Host.Arch)
	}
	return a.newCommand(actor, CommandRequest{
		AgentID: agentID, Name: message.CommandUpdate, TimeoutSec: updateTimeout,
		Args: map[string]string{
			"version": rel.Version, "url": ReleasesPath + art.File,
			"sha256": art.SHA256, "signature": art.Signature,
		},
	}, func(cmd *Command) {
		a.audit(actor, AuditAgentUpdate, agentID, agentID, map[string]any{"commandId": cmd.ID, "version": rel.Version})
	})
}

// WorkerUpdateCandidate — воркер из выпуска (status.workers[].release) на
// агенте, которому есть обновление: Current — версия воркера на агенте,
// Target — версия в манифесте.
type WorkerUpdateCandidate struct {
	AgentID   string `json:"agentId"`
	AgentName string `json:"agentName"`
	Online    bool   `json:"online"`
	Worker    string `json:"worker"`
	Current   string `json:"current"`
	Target    string `json:"target"`
	OS        string `json:"os"`
	Arch      string `json:"arch"`
}

// WorkerUpdateCandidates — воркеры с release: true в status.workers агентов
// (не отозванных, объявивших worker.update), чья версия отличается от старшей
// версии этого воркера в манифесте под hello.host.os/arch. update.mode не
// учитывается: он про обновление самого агента.
func (a *Agents) WorkerUpdateCandidates() ([]WorkerUpdateCandidate, error) {
	out := []WorkerUpdateCandidate{}
	rel := a.Release()
	if rel == nil || len(rel.Workers) == 0 {
		return out, nil
	}
	list, err := a.List()
	if err != nil {
		return nil, err
	}
	for _, agent := range list {
		if agent.Revoked || agent.Hello == nil || agent.Status == nil ||
			!declaresCommand(capabilities(agent), message.CommandWorkerUpdate) {
			continue
		}
		host := agent.Hello.Host
		for _, w := range agent.Status.Workers {
			if !w.Release {
				continue
			}
			art := rel.worker(w.Name, host.OS, host.Arch)
			if art == nil || art.Version == w.Version {
				continue
			}
			out = append(out, WorkerUpdateCandidate{
				AgentID: agent.ID, AgentName: agent.Name, Online: agent.Online, Worker: w.Name,
				Current: w.Version, Target: art.Version, OS: host.OS, Arch: host.Arch,
			})
		}
	}
	return out, nil
}

// UpdateWorker — команда worker.update {name, version, url, sha256, signature}
// со сборкой воркера name из выпуска под ОС и архитектуру агента, срок 300 с.
// Имя не по правилу — MESSAGE_INVALID; нет выпуска или сборки, воркер не из
// выпуска (status.workers[].release), агент не объявил worker.update —
// UPDATE_NOT_AVAILABLE. Переустановка той же версии разрешена.
func (a *Agents) UpdateWorker(agentID, name string) (*Command, error) {
	return a.updateWorker("", agentID, name)
}

func (a *Agents) updateWorker(actor, agentID, name string) (*Command, error) {
	agent, err := a.Agent(agentID)
	if err != nil {
		return nil, protoErr("AGENT_NOT_FOUND", "Агент не найден")
	}
	if !message.ValidName(name) {
		return nil, invalidName("name", name)
	}
	unavailable := func(msg string) error { return protoErr("UPDATE_NOT_AVAILABLE", msg) }
	rel := a.Release()
	switch {
	case rel == nil:
		return nil, unavailable("Выпуска нет")
	case agent.Hello == nil || !declaresCommand(capabilities(agent), message.CommandWorkerUpdate):
		return nil, unavailable("Агент не объявил команду " + message.CommandWorkerUpdate)
	case agent.Status == nil || !slices.ContainsFunc(agent.Status.Workers, func(w message.StatusWorker) bool { return w.Name == name && w.Release }):
		return nil, unavailable("Воркер " + name + " не из выпуска (release: true)")
	}
	art := rel.worker(name, agent.Hello.Host.OS, agent.Hello.Host.Arch)
	if art == nil {
		return nil, unavailable("Нет сборки воркера " + name + " под " + agent.Hello.Host.OS + "/" + agent.Hello.Host.Arch)
	}
	return a.newCommand(actor, CommandRequest{
		AgentID: agentID, Name: message.CommandWorkerUpdate, TimeoutSec: updateTimeout,
		Args: message.WorkerUpdate{
			Name: name, Version: art.Version, URL: ReleasesPath + art.File, SHA256: art.SHA256, Signature: art.Signature,
		},
	}, func(cmd *Command) {
		a.audit(actor, AuditWorkerUpdate, agentID, agentID, map[string]any{"worker": name, "version": art.Version, "commandId": cmd.ID})
	})
}

// handleRelease — GET …/releases/<file>: manifest.json и только файлы из
// манифеста (сборки агента и воркеров) (имя без каталогов).
func (a *Agents) handleRelease(w http.ResponseWriter, r *http.Request) {
	name := r.PathValue("file")
	rel := a.Release()
	if rel == nil || name == "" || filepath.Base(name) != name || name == "." || name == ".." {
		http.NotFound(w, r)
		return
	}
	if name != "manifest.json" && !rel.releaseFile(name) {
		http.NotFound(w, r)
		return
	}
	f, err := os.Open(filepath.Join(a.opts.ReleasesDir, name))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.IsDir() {
		http.NotFound(w, r)
		return
	}
	if name == "manifest.json" {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "no-cache")
	} else {
		w.Header().Set("Content-Type", "application/octet-stream")
	}
	http.ServeContent(w, r, name, st.ModTime(), f)
}

var (
	// Адрес сервера: хост и необязательный путь без символов shell (" $ ` \)
	// и переводов строк.
	safeServer = regexp.MustCompile("^https?://[A-Za-z0-9.\\-:\\[\\]]+(/[^\"$`\\\\\\r\\n]*)?$")
	safeKey    = regexp.MustCompile(`^[A-Za-z0-9+/=]*$`)
)

// handleInstall — GET …/install.sh: установщик из каталога выпуска с адресом
// сервера (PublicURL, иначе из запроса) и ключом проверки релизов.
func (a *Agents) handleInstall(w http.ResponseWriter, r *http.Request) {
	if a.opts.ReleasesDir == "" {
		http.NotFound(w, r)
		return
	}
	script, err := os.ReadFile(filepath.Join(a.opts.ReleasesDir, "install.sh"))
	if err != nil {
		http.NotFound(w, r)
		return
	}
	// Значения попадают в строку shell в кавычках — только безопасные символы.
	a.mu.Lock()
	server := a.serverURL(a.requestBase(r))
	a.mu.Unlock()
	if !safeServer.MatchString(server) {
		writeError(w, http.StatusBadRequest, "MESSAGE_INVALID", "Некорректный адрес сервера")
		return
	}
	key := a.opts.PublicKey
	if !safeKey.MatchString(key) {
		a.log.Warn("PublicKey не base64: в install.sh не подставлен")
		key = ""
	}
	script = bytes.Replace(script, []byte(`DEFAULT_SERVER=""`), []byte(`DEFAULT_SERVER="`+server+`"`), 1)
	script = bytes.Replace(script, []byte(`DEFAULT_PUBLIC_KEY=""`), []byte(`DEFAULT_PUBLIC_KEY="`+key+`"`), 1)
	w.Header().Set("Content-Type", "text/x-shellscript; charset=utf-8")
	w.Header().Set("Cache-Control", "no-cache")
	http.ServeContent(w, r, "install.sh", time.Time{}, bytes.NewReader(script))
}
