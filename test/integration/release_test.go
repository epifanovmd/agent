//go:build unix

package integration

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/internal/worker"
	"github.com/epifanovmd/agent/test/testserver"
)

// releaseKit — каталог сборок на сервере: сборки, подписанные своим ключом, и manifest.json.
type releaseKit struct {
	t       *testing.T
	dir     string
	priv    ed25519.PrivateKey
	pub     string
	agent   map[string]any
	workers []map[string]any
}

func newReleaseKit(t *testing.T) *releaseKit {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return &releaseKit{t: t, dir: t.TempDir(), priv: priv, pub: base64.StdEncoding.EncodeToString(pub)}
}

// build — сборка name версии version в каталоге сборок; запись для manifest.json.
func (k *releaseKit) build(name, version string, content []byte) map[string]any {
	sum := sha256.Sum256(content)
	hash := hex.EncodeToString(sum[:])
	file := fmt.Sprintf("%s-%s-%s-%s", name, version, runtime.GOOS, runtime.GOARCH)
	if err := os.WriteFile(filepath.Join(k.dir, file), content, 0o644); err != nil {
		k.t.Fatal(err)
	}
	sig := update.Sign(k.priv, update.Build{Name: name, Version: version, OS: runtime.GOOS, Arch: runtime.GOARCH, SHA256: hash})
	return map[string]any{"os": runtime.GOOS, "arch": runtime.GOARCH, "file": file, "sha256": hash, "signature": sig}
}

// setAgent, setWorker — сборка агента или воркера в каталоге сборок (заменяет прежнюю).
func (k *releaseKit) setAgent(version string, content []byte) {
	k.agent = k.build(update.AgentName, version, content)
	k.agent["version"] = version
	k.write()
}

func (k *releaseKit) setWorker(name, version string, content []byte, tune ...func(map[string]any)) {
	w := k.build(name, version, content)
	w["name"], w["version"] = name, version
	for _, fn := range tune {
		fn(w)
	}
	k.workers = []map[string]any{w}
	k.write()
}

func (k *releaseKit) write() {
	version := "0.0.0"
	artifacts := []map[string]any{}
	if k.agent != nil {
		version = k.agent["version"].(string)
		artifacts = append(artifacts, k.agent)
	}
	raw, _ := json.Marshal(map[string]any{"version": version, "artifacts": artifacts, "workers": k.workers})
	if err := os.WriteFile(filepath.Join(k.dir, "manifest.json"), raw, 0o644); err != nil {
		k.t.Fatal(err)
	}
}

// testBinary — копия тестового бинаря с меткой сборки tag (версия или broken).
func testBinary(t *testing.T, tag string) []byte {
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, []byte(tailMark+tag+"\n")...)
}

// Воркер со сборкой с сервера: первая установка по worker.update, обновление (previous — прежняя
// версия), новая сборка не запускается — возвращается прежняя (UPDATE_FAILED), подпись
// другой сборки — UPDATE_FAILED, воркер без release: true — WORKER_NOT_RELEASED.
func TestWorkerUpdate(t *testing.T) {
	t.Parallel()
	kit := newReleaseKit(t)
	kit.write()
	s := newStand(t, func(c *testserver.Config) { c.ReleasesDir, c.PublicKey = kit.dir, kit.pub })
	rel := s.worker("rel")
	rel.Command, rel.Release = nil, true
	cfg := s.config(rel, s.worker("w"))
	cfg.Update = config.Update{Mode: config.UpdateSelf, PublicKey: kit.pub}
	s.start(cfg)
	a := s.running("w")

	build := func() string {
		res, err := s.server.Fetch(a.ID, "rel", "/build", testserver.FetchInit{})
		if err != nil {
			return ""
		}
		return res.Body
	}
	install := func(version string, tag string) (testserver.UpdateResult, error) {
		kit.setWorker("rel", version, testBinary(t, tag))
		var res testserver.UpdateResult
		err := s.server.Action("updateWorker", a.ID, &res, "rel", map[string]any{"timeoutMs": 120_000})
		return res, err
	}

	res, err := install("1.0.0", "w-1.0.0")
	if err != nil || res.Version != "1.0.0" || res.Previous != "" {
		t.Fatalf("первая установка: %+v %v", res, err)
	}
	if got := build(); got != "w-1.0.0" {
		t.Fatalf("работает сборка %q", got)
	}
	res, err = install("1.1.0", "w-1.1.0")
	if err != nil || res.Version != "1.1.0" || res.Previous != "1.0.0" {
		t.Fatalf("обновление: %+v %v", res, err)
	}
	if _, err := install("1.2.0", broken); errorCode(err) != "UPDATE_FAILED" {
		t.Fatalf("сборка, которая не запускается: %v", err)
	}
	eventually(t, "возвращена прежняя сборка", func() bool {
		b, _ := s.server.Agent(agentName)
		w, _ := b.Worker("rel")
		return build() == "w-1.1.0" && w.Version == "1.1.0" && w.State == "running"
	})

	kit.setWorker("rel", "1.3.0", testBinary(t, "w-1.3.0"), func(w map[string]any) {
		w["signature"] = update.Sign(kit.priv, update.Build{Name: "rel", Version: "9.9.9", OS: runtime.GOOS, Arch: runtime.GOARCH, SHA256: w["sha256"].(string)})
	})
	if err := s.server.Action("updateWorker", a.ID, nil, "rel"); errorCode(err) != "UPDATE_FAILED" {
		t.Fatalf("подпись другой сборки: %v", err)
	}
	if got := build(); got != "w-1.1.0" {
		t.Fatalf("после отклонённой сборки работает %q", got)
	}
	if err := s.server.Action("updateWorker", a.ID, nil, "w"); errorCode(err) != "WORKER_NOT_RELEASED" {
		t.Fatalf("воркер без release: true: %v", err)
	}
}

// agentRunner — агент отдельным процессом под «менеджером» (как systemd Restart=always):
// завершился — запускается снова.
type agentRunner struct {
	t    *testing.T
	bin  string
	conf string
	// data — каталог данных агента: после теста останавливаются и воркеры, пережившие агента.
	data string

	mu      sync.Mutex
	cmd     *exec.Cmd
	stopped bool
	out     bytes.Buffer
	done    chan struct{}
}

func runAgent(t *testing.T, bin, conf, data string) *agentRunner {
	r := &agentRunner{t: t, bin: bin, conf: conf, data: data, done: make(chan struct{})}
	go r.loop()
	t.Cleanup(func() {
		r.mu.Lock()
		r.stopped = true
		if r.cmd != nil && r.cmd.Process != nil {
			_ = r.cmd.Process.Signal(syscall.SIGTERM)
		}
		r.mu.Unlock()
		<-r.done
		worker.StopProcesses(r.data)
		if t.Failed() {
			r.mu.Lock()
			t.Logf("вывод агента:\n%s", r.out.String())
			r.mu.Unlock()
		}
	})
	return r
}

func (r *agentRunner) loop() {
	defer close(r.done)
	for {
		r.mu.Lock()
		if r.stopped {
			r.mu.Unlock()
			return
		}
		cmd := exec.Command(r.bin)
		cmd.Env = append(os.Environ(), "IT_AGENT=1", "IT_AGENT_CONFIG="+r.conf)
		cmd.Stdout, cmd.Stderr = &lockedWriter{r}, &lockedWriter{r}
		err := cmd.Start()
		r.cmd = cmd
		r.mu.Unlock()
		if err == nil {
			_ = cmd.Wait()
		}
		time.Sleep(200 * time.Millisecond)
	}
}

type lockedWriter struct{ r *agentRunner }

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.r.mu.Lock()
	defer w.r.mu.Unlock()
	return w.r.out.Write(p)
}

// Самообновление агента: agent.update ставит новую версию, итог приходит от неё после
// welcome; версия, которая не выходит на связь, через три запуска заменяется прежней (boot
// guard), итог — UPDATE_FAILED.
func TestAgentUpdate(t *testing.T) {
	t.Parallel()
	kit := newReleaseKit(t)
	kit.write()
	s := newStand(t, func(c *testserver.Config) {
		c.ReleasesDir, c.PublicKey = kit.dir, kit.pub
		c.Options = map[string]any{"offlineGraceMs": 0}
	})
	dir := t.TempDir()
	bin := filepath.Join(dir, "agent")
	if err := os.WriteFile(bin, testBinary(t, "1.0.0"), 0o755); err != nil {
		t.Fatal(err)
	}
	conf := filepath.Join(dir, "agent.yaml")
	writeFile(t, conf, fmt.Sprintf(`server: {url: %q}
dataDir: %q
name: %s
enroll: {token: %s}
log: {level: error}
telemetry: {metrics: []}
update: {mode: self, publicKey: %q}
`, s.http.URL, filepath.Join(dir, "data"), agentName, token, kit.pub))
	runAgent(t, bin, conf, filepath.Join(dir, "data"))

	version := func(want string) testserver.Agent {
		t.Helper()
		var a testserver.Agent
		within(t, 60*time.Second, "агент "+want+" на связи", func() bool {
			var ok bool
			a, ok = s.server.Agent(agentName)
			return ok && a.Online && a.Version == want
		})
		return a
	}
	a := version("1.0.0")

	kit.setAgent("1.1.0", testBinary(t, "1.1.0"))
	var res testserver.UpdateResult
	if err := s.server.Action("updateAgent", a.ID, &res, map[string]any{"timeoutMs": 120_000}); err != nil {
		t.Fatal(err)
	}
	if res.Version != "1.1.0" || res.Previous != "1.0.0" {
		t.Fatalf("итог agent.update: %+v", res)
	}
	a = version("1.1.0")
	if _, err := os.Stat(bin + ".prev"); err != nil {
		t.Fatalf("копия прежней версии: %v", err)
	}

	kit.setAgent("1.2.0", testBinary(t, broken))
	err := s.server.Action("updateAgent", a.ID, nil, map[string]any{"timeoutMs": 120_000})
	if errorCode(err) != "UPDATE_FAILED" {
		t.Fatalf("версия, не вышедшая на связь: %v", err)
	}
	version("1.1.0")
}

// signal — сигнал работающему процессу агента (менеджер запустит его снова).
func (r *agentRunner) signal(sig syscall.Signal) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.cmd == nil || r.cmd.Process == nil {
		r.t.Fatal("агент не запущен")
	}
	_ = r.cmd.Process.Signal(sig)
	return r.cmd.Process.Pid
}
