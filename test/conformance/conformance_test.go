//go:build unix

// Package conformance — один сценарий для серверов на всех SDK: настоящий
// агент (internal/app) с Python-воркерами против сервера на sdk/go/server (test/testserver), бэкенда
// examples/server (agent-sdk/server, Node) и examples/server-python
// (agent_sdk.server). Сервер без нужного окружения (node, python3, собранный
// sdk/node) пропускается.
package conformance

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/identity"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/sdk/go/message"
	"github.com/epifanovmd/agent/sdk/go/server"
	"github.com/epifanovmd/agent/test/testserver"
)

const token = "conformance-token"

// control — что сценарий делает с сервером (API приложения).
type control interface {
	enqueue(t *testing.T, queue string, data any) string
	job(t *testing.T, id string) (status string, result json.RawMessage)
	command(t *testing.T, name string, args any) string
	commandDone(t *testing.T, id string) (status string, result json.RawMessage)
	// lists — id задач и команд в порядке списков SDK (новые первыми).
	lists(t *testing.T) (jobs, commands []string)
	// setState — снимок домена: общий (agentID == "") или личный агента.
	setState(t *testing.T, domain, agentID string, spec any)
	// deleteState — удалить снимок; версия возвращённого общего (0 — нет).
	deleteState(t *testing.T, domain, agentID string) int64
	// agent — агент conformance (JSON модели контракта; nil — ещё нет).
	agent(t *testing.T) json.RawMessage
	declares(t *testing.T, command, domain string) bool
	// observed — история метрик и inventory агента conformance.
	observed(t *testing.T) (points []point, inventory bool)
	// rotateKey — сменить ключ агента (agent.rotateKey); id команды.
	rotateKey(t *testing.T, agentID string) string
	// url — адрес сервера (транспорт агента и раздача выпуска).
	url() string
}

// point — точка истории метрик (MetricsPoint контракта SDK).
type point struct {
	At       int64 `json:"at"`
	Backfill bool  `json:"backfill"`
	Metrics  struct {
		Host *struct {
			MemTotalBytes uint64 `json:"memTotalBytes"`
		} `json:"host"`
	} `json:"metrics"`
}

const publicKey = "Y29uZm9ybWFuY2Uta2V5LWJhc2U2NA=="

// releasesDir — каталог выпуска: сборка и manifest.json (без подписи) и install.sh из deploy.
func releasesDir(t *testing.T) string {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "agent-linux-amd64"), []byte("сборка"), 0o755); err != nil {
		t.Fatal(err)
	}
	manifest := `{"version":"9.9.9","artifacts":[{"os":"linux","arch":"amd64","file":"agent-linux-amd64","sha256":"00"}]}`
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	script, err := os.ReadFile(filepath.Join(root(t), "deploy/install/install.sh"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "install.sh"), script, 0o644); err != nil {
		t.Fatal(err)
	}
	return dir
}

func root(t *testing.T) string {
	dir, err := filepath.Abs("../..")
	if err != nil {
		t.Fatal(err)
	}
	return dir
}

func TestServersOnAllSDKs(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("нет python3: воркеры сценария — на agent_sdk.worker")
	}
	t.Run("go", func(t *testing.T) {
		srv := testserver.New(testserver.Config{
			EnrollToken: token, StatusInterval: 200 * time.Millisecond, MetricsInterval: 200 * time.Millisecond,
			ReleasesDir: releasesDir(t), PublicKey: publicKey, Log: logx.Discard(),
		})
		hs := httptest.NewServer(srv.Handler())
		t.Cleanup(func() { hs.Close(); srv.Close() })
		scenario(t, python, hs.URL, goControl{srv.Agents(), hs.URL})
	})
	t.Run("node", func(t *testing.T) {
		node, err := exec.LookPath("node")
		if err != nil {
			t.Skip("нет node")
		}
		r := root(t)
		for _, need := range []string{"sdk/node/dist", "examples/server/node_modules/tsx"} {
			if _, err := os.Stat(filepath.Join(r, need)); err != nil {
				t.Skipf("нет %s: scripts/demo.sh build", need)
			}
		}
		// TypeScript — через tsx из node_modules примера (--import ищет его от cwd).
		url := start(t, releasesDir(t), filepath.Join(r, "examples/server"), node, "--import", "tsx", "src/main.ts")
		scenario(t, python, url, httpControl{base: url})
	})
	t.Run("python", func(t *testing.T) {
		url := start(t, releasesDir(t), root(t), python, "examples/server-python/main.py")
		scenario(t, python, url, httpControl{base: url})
	})
}

// scenario — агент с воркерами echo и kv: задача, состояние → команда читает его.
func scenario(t *testing.T, python, url string, c control) {
	r := root(t)
	cfg := config.Defaults()
	cfg.Server.URL = url
	cfg.DataDir = t.TempDir()
	cfg.Name = "conformance"
	cfg.Enroll.Token = token
	cfg.Update.Mode = "disabled"
	cfg.Telemetry.GPU = "off"
	cfg.Log.Level = "error"
	env := map[string]string{"PYTHONPATH": filepath.Join(r, "sdk/python"), "KV_DIR": t.TempDir(), "KV_TELEMETRY_INTERVAL": "0.2"}
	for _, name := range []string{"echo", "kv"} {
		cfg.Workers = append(cfg.Workers, config.Worker{
			Name: name, Command: []string{python, filepath.Join(r, "examples/workers", name+"_worker.py")},
			Env: env, Replicas: 1, StopTimeout: config.Duration(5 * time.Second),
		})
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	agent, err := app.New(cfg, "conformance")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = agent.Run(ctx) }()
	t.Cleanup(func() { cancel(); <-done })

	eventually(t, "сервер знает команду и домен воркера kv", 30*time.Second, func() bool {
		return c.declares(t, "example.kv.get", "example.kv")
	})

	job := c.enqueue(t, "example.echo", map[string]string{"text": "conformance"})
	if h, ok := c.(httpControl); ok {
		t.Cleanup(func() {
			if t.Failed() {
				var snap json.RawMessage
				h.call(t, "GET", "/api/snapshot", nil, &snap)
				t.Logf("снимок сервера: %s", snap)
			}
		})
	}
	eventually(t, "задача echo выполнена", 30*time.Second, func() bool {
		status, result := c.job(t, job)
		if status == "failed" || status == "cancelled" {
			t.Fatalf("задача %s: %s", status, result)
		}
		var echo struct {
			Echo string `json:"echo"`
		}
		return status == "completed" && json.Unmarshal(result, &echo) == nil && echo.Echo == "conformance"
	})

	// Списки задач и команд — новые первыми.
	second := c.enqueue(t, "example.echo", map[string]string{"text": "второй"})
	if jobs, _ := c.lists(t); len(jobs) < 2 || jobs[0] != second || jobs[1] != job {
		t.Fatalf("задачи не новые первыми: %v (первая %s, вторая %s)", jobs, job, second)
	}

	// История метрик: точки по часам сервера (collectedAt + clockOffsetMs) и inventory.
	eventually(t, "история метрик и inventory", 30*time.Second, func() bool {
		points, inventory := c.observed(t)
		if len(points) < 2 || !inventory {
			return false
		}
		last := points[len(points)-1]
		now := time.Now().UnixMilli()
		return last.At > now-60_000 && last.At <= now+5_000 && last.Metrics.Host != nil && last.Metrics.Host.MemTotalBytes > 0 &&
			points[0].At <= last.At
	})

	// Выпуск: манифест и install.sh с подставленными адресом сервера и ключом.
	manifest := get(t, c.url()+"/api/v1/agent-link/releases/manifest.json")
	if !strings.Contains(manifest, `"9.9.9"`) {
		t.Fatalf("манифест: %s", manifest)
	}
	if got := get(t, c.url()+"/api/v1/agent-link/releases/agent-linux-amd64"); got != "сборка" {
		t.Fatalf("сборка из манифеста: %q", got)
	}
	script := get(t, c.url()+"/api/v1/agent-link/install.sh")
	if !strings.Contains(script, `DEFAULT_SERVER="`+c.url()+`"`) || !strings.Contains(script, `DEFAULT_PUBLIC_KEY="`+publicKey+`"`) {
		t.Fatalf("install.sh без подстановок:\n%s", head(script))
	}

	value := fmt.Sprintf("v-%d", time.Now().UnixNano())
	c.setState(t, "example.kv", "", map[string]string{"key": value})
	reads := func(want string) func() bool {
		return func() bool {
			id := c.command(t, "example.kv.get", map[string]string{"key": "key"})
			for range 100 {
				status, result := c.commandDone(t, id)
				switch status {
				case "succeeded":
					return strings.Contains(string(result), want)
				case "failed":
					return false // снимок ещё не применён — повтор
				}
				time.Sleep(50 * time.Millisecond)
			}
			return false
		}
	}
	first := c.command(t, "example.kv.get", map[string]string{"key": "key"})
	next := c.command(t, "example.kv.get", map[string]string{"key": "key"})
	if _, cmds := c.lists(t); len(cmds) < 2 || cmds[0] != next || cmds[1] != first {
		t.Fatalf("команды не новые первыми: %v (первая %s, вторая %s)", cmds, first, next)
	}
	var shared int64
	eventually(t, "состояние применено и читается командой", 30*time.Second, func() bool {
		shared = applied(t, c, "example.kv")
		return shared > 0 && reads(value)()
	})

	// Личный снимок агента перекрывает общий; его удаление возвращает агенту
	// общий с версией новее личной.
	var model struct {
		ID string `json:"id"`
	}
	_ = json.Unmarshal(c.agent(t), &model)
	own := value + "-own"
	c.setState(t, "example.kv", model.ID, map[string]string{"key": own})
	var ownVersion int64
	eventually(t, "личный снимок применён", 30*time.Second, func() bool {
		ownVersion = applied(t, c, "example.kv")
		return ownVersion > shared && reads(own)()
	})
	version := c.deleteState(t, "example.kv", model.ID)
	if version <= ownVersion {
		t.Fatalf("общий после удаления личного: версия %d, личная %d", version, ownVersion)
	}
	eventually(t, "после удаления личного применён общий", 30*time.Second, func() bool {
		return applied(t, c, "example.kv") == version && reads(value)()
	})

	// Смена ключа: команда agent.rotateKey с хешем нового секрета; сервер
	// закрывает сессию (1012), агент переподключается с новым секретом, старый
	// больше не принимается.
	old := credentials(t, cfg.DataDir)
	rotate := c.rotateKey(t, model.ID)
	var secretHash string
	eventually(t, "команда agent.rotateKey выполнена", 30*time.Second, func() bool {
		status, result := c.commandDone(t, rotate)
		if status == "failed" || status == "cancelled" {
			t.Fatalf("agent.rotateKey %s: %s", status, result)
		}
		var r struct {
			SecretHash string `json:"secretHash"`
		}
		_ = json.Unmarshal(result, &r)
		secretHash = r.SecretHash
		return status == "succeeded" && len(secretHash) == 64
	})
	eventually(t, "агент подключился с новым секретом", 30*time.Second, func() bool {
		now := credentials(t, cfg.DataDir)
		return now.PendingSecret == "" && now.Secret != old.Secret && identity.SecretHash(now.Secret) == secretHash
	})
	if status := syncStatus(t, c.url(), old.Authorization()); status != http.StatusUnauthorized {
		t.Fatalf("старый секрет: HTTP %d, нужен 401", status)
	}
	eventually(t, "после смены ключа агент на связи и выполняет команду", 30*time.Second, func() bool {
		return c.declares(t, "example.kv.get", "example.kv") && reads(value)()
	})
}

// credentials — файл учётных данных агента в каталоге данных.
func credentials(t *testing.T, dataDir string) identity.Credentials {
	creds, ok, err := identity.NewStore(dataDir).Load()
	if err != nil || !ok {
		t.Fatalf("учётные данные агента: ok=%v err=%v", ok, err)
	}
	return creds
}

// syncStatus — HTTP-код открытия сессии HTTP sync с заголовком authorization.
func syncStatus(t *testing.T, base, authorization string) int {
	req, _ := http.NewRequest("POST", base+message.SyncPath, strings.NewReader(`{"sessionId":null,"messages":[],"waitSeconds":0}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authorization)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// applied — версия домена, применённая агентом conformance (state.applied ok).
func applied(t *testing.T, c control, domain string) int64 {
	var a struct {
		StateApplied map[string]struct {
			Version int64 `json:"version"`
			OK      bool  `json:"ok"`
		} `json:"stateApplied"`
	}
	_ = json.Unmarshal(c.agent(t), &a)
	if st, ok := a.StateApplied[domain]; ok && st.OK {
		return st.Version
	}
	return 0
}

// start — сервер примера дочерним процессом на свободном порту.
func start(t *testing.T, releases, dir, bin string, args ...string) string {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	cmd := exec.Command(bin, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), fmt.Sprintf("PORT=%d", port), "ENROLL_TOKEN="+token, "STATUS_INTERVAL_MS=200",
		"METRICS_INTERVAL_MS=200", "METRICS_STORE_INTERVAL_MS=0", "RELEASES_DIR="+releases, "PUBLIC_KEY="+publicKey)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
		if t.Failed() {
			t.Logf("вывод сервера:\n%s", out.String())
		}
	})
	url := fmt.Sprintf("http://127.0.0.1:%d", port)
	eventually(t, "сервер запущен", 15*time.Second, func() bool {
		resp, err := http.Get(url + "/api/snapshot")
		if err != nil {
			return false
		}
		resp.Body.Close()
		return resp.StatusCode == http.StatusOK
	})
	return url
}

func get(t *testing.T, url string) string {
	t.Helper()
	resp, err := http.Get(url)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("GET %s: HTTP %d %s", url, resp.StatusCode, raw)
	}
	return string(raw)
}

func head(s string) string {
	if lines := strings.SplitN(s, "\n", 30); len(lines) == 30 {
		return strings.Join(lines[:29], "\n")
	}
	return s
}

func eventually(t *testing.T, what string, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// ─── Go: API Agents напрямую ────────────────────────────────────────────

type goControl struct {
	agents *server.Agents
	base   string
}

func (g goControl) url() string { return g.base }

func (g goControl) observed(t *testing.T) ([]point, bool) {
	list, err := g.agents.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range list {
		if a.Name != "conformance" {
			continue
		}
		metrics, err := g.agents.Metrics(a.ID, 0)
		if err != nil {
			t.Fatal(err)
		}
		raw, _ := json.Marshal(metrics)
		var points []point
		_ = json.Unmarshal(raw, &points)
		return points, a.Inventory != nil
	}
	return nil, false
}

func (g goControl) enqueue(t *testing.T, queue string, data any) string {
	job, err := g.agents.Enqueue(server.JobRequest{Queue: queue, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	return job.ID
}

func (g goControl) job(t *testing.T, id string) (string, json.RawMessage) {
	job, err := g.agents.Job(id)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(job.Result)
	return job.Status, raw
}

func (g goControl) command(t *testing.T, name string, args any) string {
	cmd, err := g.agents.Command(server.CommandRequest{Name: name, Args: args, TimeoutSec: 10})
	if err != nil {
		t.Fatal(err)
	}
	return cmd.ID
}

func (g goControl) commandDone(t *testing.T, id string) (string, json.RawMessage) {
	cmd, err := g.agents.CommandByID(id)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(cmd.Result)
	return cmd.Status, raw
}

func (g goControl) rotateKey(t *testing.T, agentID string) string {
	cmd, err := g.agents.RotateKey(agentID)
	if err != nil {
		t.Fatal(err)
	}
	return cmd.ID
}

func (g goControl) setState(t *testing.T, domain, agentID string, spec any) {
	if _, err := g.agents.SetState(domain, spec, agentID); err != nil {
		t.Fatal(err)
	}
}

func (g goControl) deleteState(t *testing.T, domain, agentID string) int64 {
	st, err := g.agents.DeleteState(domain, agentID)
	if err != nil {
		t.Fatal(err)
	}
	if st == nil {
		return 0
	}
	return st.Version
}

func (g goControl) lists(t *testing.T) (jobs, commands []string) {
	js, err := g.agents.Jobs()
	if err != nil {
		t.Fatal(err)
	}
	cs, err := g.agents.Commands()
	if err != nil {
		t.Fatal(err)
	}
	for _, j := range js {
		jobs = append(jobs, j.ID)
	}
	for _, c := range cs {
		commands = append(commands, c.ID)
	}
	return jobs, commands
}

func (g goControl) agent(t *testing.T) json.RawMessage {
	list, err := g.agents.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range list {
		if a.Name == "conformance" {
			raw, _ := json.Marshal(a)
			return raw
		}
	}
	return nil
}

func (g goControl) declares(t *testing.T, command, domain string) bool {
	list, err := g.agents.List()
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range list {
		raw, _ := json.Marshal(a)
		if a.Online && declared(raw, command, domain) {
			return true
		}
	}
	return false
}

// ─── Node и Python: API приложения примеров (/api/…) ───────────────────

type httpControl struct{ base string }

func (h httpControl) url() string { return h.base }

func (h httpControl) observed(t *testing.T) ([]point, bool) {
	var snap struct {
		Agents []struct {
			ID        string          `json:"id"`
			Name      string          `json:"name"`
			Inventory json.RawMessage `json:"inventory"`
		} `json:"agents"`
	}
	h.call(t, "GET", "/api/snapshot", nil, &snap)
	for _, a := range snap.Agents {
		if a.Name != "conformance" {
			continue
		}
		var points []point
		h.call(t, "GET", "/api/agents/"+a.ID+"/metrics", nil, &points)
		return points, len(a.Inventory) > 0 && string(a.Inventory) != "null"
	}
	return nil, false
}

func (h httpControl) call(t *testing.T, method, path string, body any, out any) {
	var r io.Reader
	if body != nil {
		raw, _ := json.Marshal(body)
		r = bytes.NewReader(raw)
	}
	req, _ := http.NewRequest(method, h.base+path, r)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		t.Fatalf("%s %s: HTTP %d %s", method, path, resp.StatusCode, raw)
	}
	if out != nil {
		if err := json.Unmarshal(raw, out); err != nil {
			t.Fatalf("%s %s: %v: %s", method, path, err, raw)
		}
	}
}

type record struct {
	ID     string          `json:"id"`
	Status string          `json:"status"`
	Result json.RawMessage `json:"result"`
}

func (h httpControl) enqueue(t *testing.T, queue string, data any) string {
	var job record
	h.call(t, "POST", "/api/jobs", map[string]any{"queue": queue, "data": data}, &job)
	return job.ID
}

func (h httpControl) job(t *testing.T, id string) (string, json.RawMessage) {
	var job record
	h.call(t, "GET", "/api/jobs/"+id, nil, &job)
	return job.Status, job.Result
}

func (h httpControl) command(t *testing.T, name string, args any) string {
	var cmd record
	h.call(t, "POST", "/api/commands", map[string]any{"name": name, "args": args, "timeoutSec": 10}, &cmd)
	return cmd.ID
}

func (h httpControl) commandDone(t *testing.T, id string) (string, json.RawMessage) {
	var cmd record
	h.call(t, "GET", "/api/commands/"+id, nil, &cmd)
	return cmd.Status, cmd.Result
}

func (h httpControl) rotateKey(t *testing.T, agentID string) string {
	var cmd record
	h.call(t, "POST", "/api/agents/"+agentID+"/rotate-key", nil, &cmd)
	return cmd.ID
}

func (h httpControl) setState(t *testing.T, domain, agentID string, spec any) {
	h.call(t, "PUT", "/api/state/"+domain+"?agentId="+url.QueryEscape(agentID), spec, nil)
}

func (h httpControl) deleteState(t *testing.T, domain, agentID string) int64 {
	var out struct {
		State *struct {
			Version int64 `json:"version"`
		} `json:"state"`
	}
	h.call(t, "DELETE", "/api/state/"+domain+"?agentId="+url.QueryEscape(agentID), nil, &out)
	if out.State == nil {
		return 0
	}
	return out.State.Version
}

func (h httpControl) lists(t *testing.T) (jobs, commands []string) {
	var snap struct {
		Jobs     []record `json:"jobs"`
		Commands []record `json:"commands"`
	}
	h.call(t, "GET", "/api/snapshot", nil, &snap)
	for _, j := range snap.Jobs {
		jobs = append(jobs, j.ID)
	}
	for _, c := range snap.Commands {
		commands = append(commands, c.ID)
	}
	return jobs, commands
}

func (h httpControl) agent(t *testing.T) json.RawMessage {
	var snap struct {
		Agents []json.RawMessage `json:"agents"`
	}
	h.call(t, "GET", "/api/snapshot", nil, &snap)
	for _, raw := range snap.Agents {
		var a struct {
			Name string `json:"name"`
		}
		if json.Unmarshal(raw, &a) == nil && a.Name == "conformance" {
			return raw
		}
	}
	return nil
}

func (h httpControl) declares(t *testing.T, command, domain string) bool {
	var snap struct {
		Agents []json.RawMessage `json:"agents"`
	}
	h.call(t, "GET", "/api/snapshot", nil, &snap)
	for _, raw := range snap.Agents {
		var a struct {
			Online bool `json:"online"`
		}
		_ = json.Unmarshal(raw, &a)
		if a.Online && declared(raw, command, domain) {
			return true
		}
	}
	return false
}

// declared — агент (JSON модели контракта) объявил команду и домен.
func declared(raw json.RawMessage, command, domain string) bool {
	var a struct {
		Capabilities struct {
			Commands struct {
				Names []string `json:"names"`
			} `json:"commands"`
			State struct {
				Domains map[string]any `json:"domains"`
			} `json:"state"`
		} `json:"capabilities"`
	}
	if json.Unmarshal(raw, &a) != nil {
		return false
	}
	_, hasDomain := a.Capabilities.State.Domains[domain]
	for _, n := range a.Capabilities.Commands.Names {
		if n == command && hasDomain {
			return true
		}
	}
	return false
}
