//go:build unix

package integration

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	goruntime "runtime"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/sdk/go/message"
	"github.com/epifanovmd/agent/sdk/go/worker"
	"github.com/epifanovmd/agent/test/testserver"
)

// releaseWorker — воркер из выпуска: версию берёт из
// AGENT_WORKER_RELEASE_VERSION (файл version сборки, его передаёт агент).
func releaseWorker() {
	version := os.Getenv("AGENT_WORKER_RELEASE_VERSION")
	w := worker.New("itrel", version)
	w.Command("it.rel", func(context.Context, *worker.Command) (any, error) { return version, nil })
	if err := w.Run(context.Background()); err != nil {
		os.Exit(1)
	}
}

// releaseKit — каталог выпуска с подписанным manifest.json и ключ.
type releaseKit struct {
	t    *testing.T
	dir  string
	pub  string
	priv ed25519.PrivateKey
	m    message.Manifest
}

func newReleaseKit(t *testing.T) *releaseKit {
	pub, priv, _ := ed25519.GenerateKey(nil)
	k := &releaseKit{t: t, dir: t.TempDir(), pub: base64.StdEncoding.EncodeToString(pub), priv: priv,
		m: message.Manifest{Version: "9.9.9", Artifacts: []message.Artifact{}}}
	k.write()
	return k
}

// addWorker — сборка воркера в выпуске: content — содержимое файла.
func (k *releaseKit) addWorker(name, version string, content []byte) {
	file := name + "-" + version + "-" + goruntime.GOOS + "-" + goruntime.GOARCH
	path := filepath.Join(k.dir, file)
	if err := os.WriteFile(path, content, 0o755); err != nil {
		k.t.Fatal(err)
	}
	hash, _ := update.FileHash(path)
	k.m.Workers = append(k.m.Workers, message.WorkerArtifact{
		Name: name, Version: version, OS: goruntime.GOOS, Arch: goruntime.GOARCH,
		File: file, SHA256: hash, Signature: update.Sign(k.priv, update.Local(name, version, hash)),
	})
	k.write()
}

func (k *releaseKit) write() {
	raw, _ := json.Marshal(k.m)
	if err := os.WriteFile(filepath.Join(k.dir, "manifest.json"), raw, 0o644); err != nil {
		k.t.Fatal(err)
	}
}

// testBinary — тестовый бинарь (он же воркер) с приписанным хвостом: сборки
// «разных версий» различаются sha256.
func testBinary(t *testing.T, tail string) []byte {
	exe, _ := os.Executable()
	raw, err := os.ReadFile(exe)
	if err != nil {
		t.Fatal(err)
	}
	return append(raw, []byte("\n"+tail)...)
}

func releaseSpec() config.Worker {
	return config.Worker{
		Name: "itrel", Release: true, Env: map[string]string{"IT_WORKER": "1", "IT_WORKER_KIND": "release"},
		Replicas: 1, StopTimeout: config.Duration(10 * time.Second),
	}
}

func declared(a testserver.Agent, name string) bool {
	return a.Capabilities != nil && a.Capabilities.Commands != nil && slices.Contains(a.Capabilities.Commands.Names, name)
}

func waitCommand(t *testing.T, s *stand, id string) testserver.Command {
	t.Helper()
	var c testserver.Command
	deadline := time.Now().Add(60 * time.Second)
	for {
		c, _ = s.server.CommandSnapshot(id)
		if c.Finished() {
			return c
		}
		if time.Now().After(deadline) {
			t.Fatalf("команда %s не завершилась: %+v", id, c)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Воркер из выпуска: сервер обновляет его (UpdateWorker → worker.update) —
// новая версия в status; сборка, которая не регистрируется, — откат,
// WORKER_UPDATE_FAILED, работает прежняя.
func TestWorkerUpdateFromRelease(t *testing.T) {
	rel := newReleaseKit(t)
	s := newStand(t, func(c *testserver.Config) { c.ReleasesDir = rel.dir })
	dataDir := t.TempDir()
	relDir := filepath.Join(dataDir, "workers", "itrel")
	_ = os.MkdirAll(relDir, 0o755)
	_ = os.WriteFile(filepath.Join(relDir, "current"), testBinary(t, "1.0.0"), 0o755)
	_ = os.WriteFile(filepath.Join(relDir, "version"), []byte("1.0.0\n"), 0o644)

	s.agent("ws", []config.Worker{releaseSpec()}, func(c *config.Config) {
		c.DataDir = dataDir
		c.Update.Mode = message.UpdateExternal // worker.update не зависит от самообновления агента
		c.Update.PublicKey = rel.pub
	})
	var agent testserver.Agent
	itrel := func() message.StatusWorker {
		agent, _ = s.server.Agent("it-agent")
		return workersOf(agent)["itrel"]
	}
	eventually(t, "воркер из выпуска 1.0.0", func() bool {
		w := itrel()
		return w.Release && w.Version == "1.0.0" && w.State == "running" && declared(agent, message.CommandWorkerUpdate)
	})
	if workersOf(agent)["go"].Release {
		t.Fatal("обычный воркер — не из выпуска")
	}

	// Новая версия.
	rel.addWorker("itrel", "2.0.0", testBinary(t, "2.0.0"))
	cmd, err := s.server.Agents().UpdateWorker(agent.ID, "itrel")
	if err != nil {
		t.Fatal(err)
	}
	done := waitCommand(t, s, cmd.ID)
	var res message.WorkerUpdateResult
	_ = json.Unmarshal(done.Result, &res)
	if done.Status != testserver.CommandSucceeded || res != (message.WorkerUpdateResult{Name: "itrel", Version: "2.0.0", Previous: "1.0.0"}) {
		t.Fatalf("обновление: %+v", done)
	}
	eventually(t, "в status новая версия", func() bool {
		w := itrel()
		return w.Version == "2.0.0" && w.State == "running" && w.Instances == 1
	})
	ping, merr := s.server.Command(testserver.CommandRequest{AgentID: agent.ID, Name: "it.rel"})
	if merr != nil {
		t.Fatal(merr)
	}
	if c := waitCommand(t, s, ping.ID); string(c.Result) != `"2.0.0"` {
		t.Fatalf("работает новая сборка: %+v", c)
	}

	// Сборка, которая не регистрируется, — откат на 2.0.0.
	rel.addWorker("itrel", "3.0.0", []byte("#!/bin/sh\necho сломанная сборка >&2\nexit 3\n"))
	cmd, err = s.server.Agents().UpdateWorker(agent.ID, "itrel")
	if err != nil {
		t.Fatal(err)
	}
	done = waitCommand(t, s, cmd.ID)
	if done.Status != testserver.CommandFailed || done.Error == nil || done.Error.Code != message.ErrWorkerUpdateFailed {
		t.Fatalf("откат: %+v", done)
	}
	if v, _ := os.ReadFile(filepath.Join(relDir, "version")); string(v) != "2.0.0\n" {
		t.Fatalf("версия после отката: %q", v)
	}
	eventually(t, "работает прежняя", func() bool {
		w := itrel()
		return w.Version == "2.0.0" && w.State == "running" && w.Instances == 1
	})
	ping, merr = s.server.Command(testserver.CommandRequest{AgentID: agent.ID, Name: "it.rel"})
	if merr != nil {
		t.Fatal(merr)
	}
	if c := waitCommand(t, s, ping.ID); string(c.Result) != `"2.0.0"` {
		t.Fatalf("после отката: %+v", c)
	}
}

// Перечитывание: появился воркер из выпуска — агент объявляет worker.update
// (новый hello); сборки ещё нет — воркер в backoff с понятной причиной.
func TestWorkerUpdateDeclaredOnReload(t *testing.T) {
	s := newStand(t)
	var cfg *config.Config
	a, _ := s.agentApp("ws", nil, func(c *config.Config) {
		c.Update.Mode = message.UpdateExternal
		cfg = c
	})
	var agent testserver.Agent
	eventually(t, "агент на связи", func() bool {
		agent, _ = s.server.Agent("it-agent")
		return agent.Capabilities != nil && declared(agent, message.CommandWorkerRestart)
	})
	if declared(agent, message.CommandWorkerUpdate) {
		t.Fatal("без воркеров из выпуска worker.update не объявляется")
	}
	next := *cfg
	next.Workers = append(slices.Clone(cfg.Workers), releaseSpec())
	if err := next.Validate(); err != nil {
		t.Fatal(err)
	}
	a.Reload(next)
	eventually(t, "worker.update объявлен, воркер без сборки — backoff", func() bool {
		agent, _ = s.server.Agent("it-agent")
		w := workersOf(agent)["itrel"]
		return declared(agent, message.CommandWorkerUpdate) && w.Release && w.State == "backoff" &&
			agent.Status.State == message.StateDegraded
	})
	if msg := agent.Status.Message; !strings.Contains(msg, "нет сборки") || !strings.Contains(msg, "install.sh --worker itrel") {
		t.Fatalf("причина: %q", msg)
	}
}
