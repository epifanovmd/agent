package server

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Revoke: сессия закрывается 4401, дальше WebSocket и HTTP sync — 401.
func TestRevoke(t *testing.T) {
	s := newStand(t)
	id, auth := s.enroll("a")
	a := s.dial(auth)
	a.send(helloEnv("boot", message.Capabilities{}))
	a.expect(message.TypeWelcome, nil)
	if err := s.agents.Revoke(id); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-a.done:
		if websocket.CloseStatus(err) != message.CloseUnauthorized {
			t.Fatalf("закрытие: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("сессия не закрыта")
	}
	agent, _ := s.agents.Agent(id)
	if !agent.Revoked || agent.Online {
		t.Fatalf("агент: revoked=%v online=%v", agent.Revoked, agent.Online)
	}
	raw, _ := json.Marshal(agent)
	if !strings.Contains(string(raw), `"revoked":true`) {
		t.Fatalf("JSON: %s", raw)
	}

	url := "ws" + strings.TrimPrefix(s.srv.URL, "http") + message.LinkPath
	_, resp, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{
		Subprotocols: []string{message.WSChannel},
		HTTPHeader:   http.Header{"Authorization": {auth}},
	})
	if err == nil || resp == nil || resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("повторное подключение: %v %v", resp, err)
	}
	if code, _, _ := s.sync(context.Background(), auth, nil, 0, helloEnv("boot", message.Capabilities{})); code != http.StatusUnauthorized {
		t.Fatalf("sync: %d", code)
	}
	var pe *message.Error
	if err := s.agents.Revoke("nobody"); !errors.As(err, &pe) || pe.Code != "AGENT_NOT_FOUND" {
		t.Fatalf("Revoke неизвестного: %v", err)
	}
}

// Revoke во время long-poll HTTP sync — 401.
func TestRevokeHTTPSync(t *testing.T) {
	s := newStand(t)
	id, auth := s.enroll("a")
	ctx := context.Background()
	_, resp, _ := s.sync(ctx, auth, nil, 0, helloEnv("boot", message.Capabilities{}))
	sid := resp.SessionID
	polled := make(chan int, 1)
	go func() {
		code, _, _ := s.sync(ctx, auth, &sid, 10)
		polled <- code
	}()
	time.Sleep(100 * time.Millisecond)
	_ = s.agents.Revoke(id)
	select {
	case code := <-polled:
		if code != http.StatusUnauthorized {
			t.Fatalf("long-poll: %d", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long-poll не вернулся")
	}
}

const installScript = `#!/bin/sh
DEFAULT_SERVER=""
DEFAULT_PUBLIC_KEY=""
SERVER="$DEFAULT_SERVER"
`

// releaseStand — сервер агентов с каталогом выпуска: сборка агента linux/amd64
// версии 2.0.0, воркер sysinfo 1.3.0 (и старая 1.2.0) под linux/amd64.
func releaseStand(t *testing.T) *stand { return releaseStandWith(t, Options{}) }

// releaseStandWith — стенд с выпуском и своими опциями (EnrollToken, ReleasesDir, PublicKey — свои).
func releaseStandWith(t *testing.T, opts Options) *stand {
	dir := t.TempDir()
	manifest := Release{Version: "2.0.0", Artifacts: []ReleaseArtifact{
		{OS: "linux", Arch: "amd64", File: "agent-linux-amd64", SHA256: "abc", Signature: "c2ln"},
	}, Workers: []WorkerArtifact{
		{Name: "sysinfo", Version: "1.2.0", OS: "linux", Arch: "amd64", File: "sysinfo-1.2.0-linux-amd64", SHA256: "old", Signature: "b2xk"},
		{Name: "sysinfo", Version: "1.3.0", OS: "linux", Arch: "amd64", File: "sysinfo-1.3.0-linux-amd64",
			SHA256: "def", Signature: "d3Jr", Restart: "stop-first", StopTimeout: "30s"},
	}}
	raw, _ := json.Marshal(manifest)
	files := map[string]string{
		"manifest.json": string(raw), "agent-linux-amd64": "BINARY", "install.sh": installScript, "secret.txt": "секрет",
		"sysinfo-1.3.0-linux-amd64": "WORKER", "sysinfo-1.4.0-linux-amd64": "НЕ В МАНИФЕСТЕ",
	}
	for name, content := range files {
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	// Файл вне каталога выпуска.
	_ = os.WriteFile(filepath.Join(filepath.Dir(dir), "outside"), []byte("x"), 0o644)
	opts.EnrollToken, opts.ReleasesDir, opts.PublicKey = "tok", dir, "UFVCS0VZ"
	agents := newTestAgents(t, opts)
	srv := httptest.NewServer(agents.Handler())
	t.Cleanup(srv.Close)
	return &stand{t: t, agents: agents, srv: srv}
}

func (s *stand) get(path string, header http.Header) (int, string) {
	s.t.Helper()
	req, _ := http.NewRequest(http.MethodGet, s.srv.URL+path, nil)
	if header != nil {
		req.Header = header
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, string(body)
}

func TestReleasesServed(t *testing.T) {
	s := releaseStand(t)
	code, body := s.get(ReleasesPath+"manifest.json", nil)
	var m Release
	if code != http.StatusOK || json.Unmarshal([]byte(body), &m) != nil || m.Version != "2.0.0" {
		t.Fatalf("manifest: %d %s", code, body)
	}
	if code, body := s.get(ReleasesPath+"agent-linux-amd64", nil); code != http.StatusOK || body != "BINARY" {
		t.Fatalf("сборка: %d %q", code, body)
	}
	if code, body := s.get(ReleasesPath+"sysinfo-1.3.0-linux-amd64", nil); code != http.StatusOK || body != "WORKER" {
		t.Fatalf("сборка воркера: %d %q", code, body)
	}
	// Только файлы из манифеста, без выхода из каталога.
	for _, path := range []string{"secret.txt", "install.sh", "..%2Foutside", "%2E%2E", "agent-linux-arm64", "sysinfo-1.4.0-linux-amd64", ""} {
		if code, _ := s.get(ReleasesPath+path, nil); code != http.StatusNotFound {
			t.Fatalf("%s: %d", path, code)
		}
	}
	if rel := s.agents.Release(); rel == nil || rel.Version != "2.0.0" || len(rel.Artifacts) != 1 || len(rel.Workers) != 2 ||
		rel.Workers[1] != (WorkerArtifact{Name: "sysinfo", Version: "1.3.0", OS: "linux", Arch: "amd64", File: "sysinfo-1.3.0-linux-amd64",
			SHA256: "def", Signature: "d3Jr", Restart: "stop-first", StopTimeout: "30s"}) {
		t.Fatalf("Release: %+v", rel)
	}

	// install.sh — адрес сервера из запроса (за прокси — X-Forwarded-Proto) и ключ.
	code, body = s.get(InstallPath, http.Header{"X-Forwarded-Proto": {"https"}})
	host := strings.TrimPrefix(s.srv.URL, "http://")
	if code != http.StatusOK || !strings.Contains(body, `DEFAULT_SERVER="https://`+host+`"`) ||
		!strings.Contains(body, `DEFAULT_PUBLIC_KEY="UFVCS0VZ"`) || !strings.Contains(body, `SERVER="$DEFAULT_SERVER"`) {
		t.Fatalf("install.sh: %d\n%s", code, body)
	}
	if _, body := s.get(InstallPath, nil); !strings.Contains(body, `DEFAULT_SERVER="http://`+host+`"`) {
		t.Fatalf("install.sh без прокси:\n%s", body)
	}
	if code, _ := s.get(InstallPath, http.Header{"X-Forwarded-Proto": {`https"; rm -rf /; "`}}); code != http.StatusBadRequest {
		t.Fatalf("небезопасный адрес: %d", code)
	}

	// PublicURL важнее адреса из запроса; с путём — можно, символы shell — 400.
	s.agents.SetPublicURL("https://api.example.com:8443/agents")
	if code, body := s.get(InstallPath, nil); code != http.StatusOK || !strings.Contains(body, `DEFAULT_SERVER="https://api.example.com:8443/agents"`) {
		t.Fatalf("install.sh с PublicURL: %d\n%s", code, body)
	}
	for _, bad := range []string{"https://example.com/$(id)", "https://example.com/a\"b", "https://example.com/`id`", "https://example.com/a\\b", "https://example.com/a\nb", "ftp://example.com", "https://exa mple.com"} {
		s.agents.SetPublicURL(bad)
		if code, _ := s.get(InstallPath, nil); code != http.StatusBadRequest {
			t.Fatalf("небезопасный PublicURL %q: %d", bad, code)
		}
	}
	s.agents.SetPublicURL("")

	// Без каталога выпуска — 404.
	plain := newStand(t)
	for _, path := range []string{ReleasesPath + "manifest.json", InstallPath} {
		if code, _ := plain.get(path, nil); code != http.StatusNotFound {
			t.Fatalf("без выпуска %s: %d", path, code)
		}
	}
}

func TestUpdateCandidatesAndUpdateAgent(t *testing.T) {
	s := releaseStand(t)
	hello := func(version, goos, arch, mode string) message.Envelope {
		h := message.Hello{
			Versions: []int{1}, Agent: message.HelloAgent{Name: "a", Version: version, BootID: "b"},
			Host:         message.Host{OS: goos, Arch: arch},
			Capabilities: message.Capabilities{Commands: &message.CommandsCapability{Names: []string{"agent.update"}}},
			Jobs:         []message.JobRef{},
		}
		if mode != "" {
			h.Capabilities.Update = &message.UpdateCapability{Mode: mode}
		}
		return message.MustNew(message.TypeHello, h)
	}
	hellos := map[string]message.Envelope{
		"old":      hello("1.0.0", "linux", "amd64", "self"),
		"no-build": hello("1.0.0", "darwin", "arm64", "self"),
		"external": hello("1.0.0", "linux", "amd64", "external"),
		"current":  hello("2.0.0", "linux", "amd64", "self"),
	}
	ids := map[string]string{}
	var dialed *wsAgent
	for _, name := range []string{"old", "no-build", "external", "current"} {
		id, auth := s.enroll(name)
		ids[name] = id
		a := s.dial(auth)
		a.send(hellos[name])
		a.expect(message.TypeWelcome, nil)
		if name == "old" {
			dialed = a
		}
	}

	cands, err := s.agents.UpdateCandidates()
	if err != nil {
		t.Fatal(err)
	}
	want := UpdateCandidate{AgentID: ids["old"], Name: "old", Online: true, Current: "1.0.0", Target: "2.0.0", OS: "linux", Arch: "amd64"}
	if len(cands) != 1 || cands[0] != want {
		t.Fatalf("кандидаты: %+v", cands)
	}

	cmd, err := s.agents.UpdateAgent(ids["old"])
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Name != "agent.update" || cmd.TimeoutSec != 300 || cmd.AgentID != ids["old"] {
		t.Fatalf("команда: %+v", cmd)
	}
	var run message.CommandRun
	dialed.expect(message.TypeCmdRun, &run)
	var args map[string]string
	_ = json.Unmarshal(run.Args, &args)
	if run.Name != "agent.update" || run.TimeoutSec != 300 || args["version"] != "2.0.0" ||
		args["url"] != "/api/v1/agent-link/releases/agent-linux-amd64" || args["sha256"] != "abc" || args["signature"] != "c2ln" {
		t.Fatalf("cmd.run: %+v %v", run, args)
	}

	for _, name := range []string{"no-build", "external"} {
		var pe *message.Error
		if _, err := s.agents.UpdateAgent(ids[name]); !errors.As(err, &pe) || pe.Code != "UPDATE_NOT_AVAILABLE" {
			t.Fatalf("%s: %v", name, err)
		}
	}
}

// workerHello — hello агента с update.mode и объявленными командами.
func workerHello(goos, arch, mode string, commands ...string) message.Envelope {
	h := message.Hello{
		Versions: []int{1}, Agent: message.HelloAgent{Name: "a", Version: "2.0.0", BootID: "b"},
		Host:         message.Host{OS: goos, Arch: arch},
		Capabilities: message.Capabilities{Commands: &message.CommandsCapability{Names: commands}},
		Jobs:         []message.JobRef{},
	}
	if mode != "" {
		h.Capabilities.Update = &message.UpdateCapability{Mode: mode}
	}
	return message.MustNew(message.TypeHello, h)
}

// workerStatus — status с воркерами.
func workerStatus(workers ...message.StatusWorker) message.Envelope {
	return streamEnv(message.TypeStatus, message.Status{
		State: message.StateIdle, Slots: map[string]int{}, Jobs: []message.StatusJob{}, Workers: workers,
	})
}

func TestWorkerUpdateCandidatesAndUpdateWorker(t *testing.T) {
	var audits collector[AuditEntry]
	s := releaseStandWith(t, Options{OnAudit: audits.add})
	sysinfo := func(version string, release bool) message.StatusWorker {
		return message.StatusWorker{Name: "sysinfo", State: "running", Instances: 1, Version: version, Release: release}
	}
	type node struct {
		hello  message.Envelope
		status message.Envelope
	}
	nodes := map[string]node{
		// Кандидат: воркер из выпуска 1.0.0, выпуск 1.3.0 (старшая из двух).
		"old": {workerHello("linux", "amd64", "self", message.CommandWorkerUpdate),
			workerStatus(sysinfo("1.0.0", true), message.StatusWorker{Name: "echo", State: "running", Instances: 1, Release: true})},
		// Воркер не из выпуска — не кандидат.
		"local": {workerHello("linux", "amd64", "self", message.CommandWorkerUpdate), workerStatus(sysinfo("1.0.0", false))},
		// Версия совпадает с выпуском.
		"current": {workerHello("linux", "amd64", "self", message.CommandWorkerUpdate), workerStatus(sysinfo("1.3.0", true))},
		// Нет сборки под darwin/arm64.
		"no-build": {workerHello("darwin", "arm64", "self", message.CommandWorkerUpdate), workerStatus(sysinfo("1.0.0", true))},
		// update.mode = external (агент в контейнере) — воркеры всё равно обновляет.
		"external": {workerHello("linux", "amd64", "external", message.CommandWorkerUpdate), workerStatus(sysinfo("1.0.0", true))},
		// Не объявил worker.update — не кандидат, команда недоступна.
		"no-command": {workerHello("linux", "amd64", "self"), workerStatus(sysinfo("1.0.0", true))},
	}
	ids := map[string]string{}
	conns := map[string]*wsAgent{}
	for _, name := range []string{"old", "local", "current", "no-build", "external", "no-command"} {
		id, auth := s.enroll(name)
		ids[name] = id
		a := s.dial(auth)
		a.send(nodes[name].hello)
		a.expect(message.TypeWelcome, nil)
		a.send(nodes[name].status)
		conns[name] = a
		eventually(t, "status "+name, func() bool {
			ag, err := s.agents.Agent(id)
			return err == nil && ag.Status != nil
		})
	}

	cands, err := s.agents.WorkerUpdateCandidates()
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]WorkerUpdateCandidate{}
	for _, name := range []string{"old", "external"} {
		want[ids[name]] = WorkerUpdateCandidate{AgentID: ids[name], AgentName: name, Online: true, Worker: "sysinfo",
			Current: "1.0.0", Target: "1.3.0", OS: "linux", Arch: "amd64"}
	}
	if len(cands) != len(want) {
		t.Fatalf("кандидаты: %+v", cands)
	}
	for _, c := range cands {
		if want[c.AgentID] != c {
			t.Fatalf("кандидат: %+v", c)
		}
	}

	cmd, err := s.agents.By("ivan").UpdateWorker(ids["old"], "sysinfo")
	if err != nil {
		t.Fatal(err)
	}
	if cmd.Name != message.CommandWorkerUpdate || cmd.TimeoutSec != 300 || cmd.AgentID != ids["old"] || cmd.Actor != "ivan" {
		t.Fatalf("команда: %+v", cmd)
	}
	var run message.CommandRun
	conns["old"].expect(message.TypeCmdRun, &run)
	var args map[string]string
	_ = json.Unmarshal(run.Args, &args)
	wantArgs := map[string]string{
		"name": "sysinfo", "version": "1.3.0", "url": "/api/v1/agent-link/releases/sysinfo-1.3.0-linux-amd64",
		"sha256": "def", "signature": "d3Jr",
	}
	if run.Name != "worker.update" || run.TimeoutSec != 300 || len(args) != len(wantArgs) {
		t.Fatalf("cmd.run: %+v %v", run, args)
	}
	for k, v := range wantArgs {
		if args[k] != v {
			t.Fatalf("cmd.run %s: %q, ждали %q", k, args[k], v)
		}
	}
	got := audits.take()
	if len(got) != 1 || got[0].Action != AuditWorkerUpdate || AuditWorkerUpdate != "worker.update" || got[0].Target != ids["old"] ||
		got[0].AgentID != ids["old"] || got[0].Actor != "ivan" || got[0].Details["worker"] != "sysinfo" ||
		got[0].Details["version"] != "1.3.0" || got[0].Details["commandId"] != cmd.ID {
		t.Fatalf("аудит: %+v", got)
	}

	// update.mode = external и переустановка той же версии — команда отправляется.
	for _, name := range []string{"external", "current"} {
		if _, err := s.agents.UpdateWorker(ids[name], "sysinfo"); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		var run message.CommandRun
		conns[name].expect(message.TypeCmdRun, &run)
		if run.Name != message.CommandWorkerUpdate {
			t.Fatalf("%s: cmd.run %+v", name, run)
		}
	}
	if got := audits.take(); len(got) != 2 {
		t.Fatalf("аудит external/current: %+v", got)
	}

	unavailable := map[string][2]string{
		"не из выпуска":         {ids["local"], "sysinfo"},
		"нет сборки":            {ids["no-build"], "sysinfo"},
		"нет воркера в выпуске": {ids["old"], "echo"},
		"нет такого воркера":    {ids["old"], "absent"},
		"не объявил команду":    {ids["no-command"], "sysinfo"},
	}
	for what, c := range unavailable {
		var pe *message.Error
		if _, err := s.agents.UpdateWorker(c[0], c[1]); !errors.As(err, &pe) || pe.Code != "UPDATE_NOT_AVAILABLE" {
			t.Fatalf("%s: %v", what, err)
		}
	}
	var pe *message.Error
	if _, err := s.agents.UpdateWorker("nobody", "sysinfo"); !errors.As(err, &pe) || pe.Code != "AGENT_NOT_FOUND" {
		t.Fatalf("неизвестный агент: %v", err)
	}
	if _, err := s.agents.UpdateWorker(ids["old"], "Bad Name"); !errors.As(err, &pe) || pe.Code != "MESSAGE_INVALID" {
		t.Fatalf("неверное имя: %v", err)
	}
	if got := audits.take(); len(got) != 0 {
		t.Fatalf("аудит отказов: %+v", got)
	}

	// Без выпуска — кандидатов нет, обновить нечего.
	plain := newStand(t)
	if c, err := plain.agents.WorkerUpdateCandidates(); err != nil || len(c) != 0 {
		t.Fatalf("без выпуска: %+v %v", c, err)
	}
}

func TestCompareVersions(t *testing.T) {
	for _, c := range []struct {
		a, b string
		want int
	}{
		{"1.3.0", "1.2.0", 1}, {"1.10.0", "1.9.9", 1}, {"1.2", "1.2.0", -1}, {"v2.0.0", "1.9.0", 1},
		{"1.2.0", "1.2.0", 0}, {"1.2.0-rc1", "1.1.0", 1},
	} {
		if got := compareVersions(c.a, c.b); got != c.want {
			t.Errorf("compareVersions(%q, %q) = %d, ждали %d", c.a, c.b, got, c.want)
		}
	}
}
