//go:build unix && e2e

package e2e

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/protocol"
)

// ready — агент зарегистрирован, оба воркера зарегистрированы и их манифесты в hello.
func ready(t *testing.T, b *backend) *agentRec {
	t.Helper()
	a := b.agent()
	b.wait("воркеры в hello с манифестами", func() bool {
		if a.hello == nil || len(a.hello.Workers) != 2 {
			return false
		}
		for _, w := range a.hello.Workers {
			if w.Manifest == nil {
				return false
			}
		}
		return true
	})
	return a
}

// Регистрация по токену, hello (узел, воркеры с манифестами), состояние, метрики узла и
// воркеров, журнал.
func TestHelloStateMetrics(t *testing.T) {
	b := newBackend(t)
	newNode(t, b.srv.URL)
	a := ready(t, b)
	b.mu.Lock()
	h := *a.hello
	b.mu.Unlock()
	if h.Name != "node-e2e" || h.Labels["zone"] != "test" || h.Host.OS == "" || h.Agent.BootID == "" || h.Agent.UpdateMode != "disabled" {
		t.Fatalf("hello: %+v", h)
	}
	if !h.Workers[0].Manifest.DeclaresCommand("echo.say") || !h.Workers[1].Manifest.DeclaresCommand("sys.info") {
		t.Fatalf("манифесты: %+v", h.Workers)
	}
	b.wait("состояние: воркеры running", func() bool {
		e, s := workerState(a, "echo"), workerState(a, "sysinfo")
		return e != nil && s != nil && e.State == protocol.WorkerRunning && s.State == protocol.WorkerRunning &&
			e.Version == "1.0.0" && e.Health != nil && e.Health.OK
	})
	b.wait("метрики узла и воркеров", func() bool {
		for _, m := range a.metrics {
			if m.Host != nil && m.Host.MemTotalBytes != nil && len(m.Workers["sysinfo"]) > 0 && len(m.Workers["echo"]) > 0 {
				return true
			}
		}
		return false
	})
	b.wait("журнал агента у бэкенда", func() bool {
		for _, e := range a.logs {
			if e.Source == protocol.LogSourceAgent && strings.Contains(e.Msg, "воркер зарегистрирован") {
				return true
			}
		}
		return false
	})
	b.wait("событие sys.started", func() bool { return len(eventsOf(a, "sys.started")) > 0 })
}

// Желаемое (§7, §14): настройка доходит до воркера, итог применения — в состоянии; неверное
// значение — ok: false; ключ не из манифеста — ошибка в состоянии, воркеру не передаётся.
func TestDesiredConfigs(t *testing.T) {
	b := newBackend(t)
	newNode(t, b.srv.URL)
	a := ready(t, b)
	v := b.setConfig(a, "echo", "settings", map[string]any{"prefix": "> ", "upper": true})
	b.wait("settings применены", func() bool {
		w := workerState(a, "echo")
		if w == nil {
			return false
		}
		st, ok := w.Configs["settings"]
		return ok && st.Version == v && st.OK != nil && *st.OK
	})
	r := b.workerCommand(a, "echo", "echo.say", map[string]any{"text": "привет"})
	var said struct{ Text string }
	if !r.OK || json.Unmarshal(r.Result, &said) != nil || said.Text != "> ПРИВЕТ" {
		t.Fatalf("настройка действует: %+v %s", r, r.Result)
	}
	v = b.setConfig(a, "sysinfo", "banner", map[string]any{"text": strings.Repeat("я", 300)})
	b.setConfig(a, "sysinfo", "nope", 1)
	b.wait("неверный banner и чужой ключ — ошибки", func() bool {
		w := workerState(a, "sysinfo")
		if w == nil {
			return false
		}
		banner, nope := w.Configs["banner"], w.Configs["nope"]
		return banner.Version == v && banner.OK != nil && !*banner.OK && banner.Error.Code == protocol.CodeConfigRejected &&
			nope.OK != nil && !*nope.OK
	})
}

// Наблюдение (§7, §16): воркеры получают его в потоке и шлют пульс с частотой метрик — метрики
// воркера в точках обновляются раз в секунду, а не раз в 10 с.
func TestWatchReachesWorkers(t *testing.T) {
	b := newBackend(t)
	newNode(t, b.srv.URL)
	a := ready(t, b)
	b.wait("воркер sysinfo работает", func() bool {
		w := workerState(a, "sysinfo")
		return w != nil && w.State == protocol.WorkerRunning
	})
	var from int
	start := time.Now()
	b.set(func() {
		from = len(a.metrics)
		a.desired.Watch = &protocol.Watch{MetricsIntervalMs: 1000}
		a.desired.Rev++
	})
	b.wait("метрики sysinfo каждую секунду", func() bool {
		seen := map[string]bool{}
		for _, m := range a.metrics[from:] {
			var v struct{ UptimeSec int }
			if json.Unmarshal(m.Workers["sysinfo"], &v) == nil {
				seen[strconv.Itoa(v.UptimeSec)] = true
			}
		}
		return len(seen) >= 4
	})
	if d := time.Since(start); d > 8*time.Second {
		t.Fatalf("4 разных значения метрик воркера — за %s: пульс не участился", d)
	}
}

// Команды воркеру (§15): итог, событие, запрос к бэкенду; ход и отмена долгой команды; тип не
// из манифеста — COMMAND_UNDECLARED; неизвестный воркер — WORKER_UNKNOWN; повтор команды с тем
// же id не выполняет её снова.
func TestWorkerCommands(t *testing.T) {
	b := newBackend(t)
	newNode(t, b.srv.URL)
	a := ready(t, b)
	b.set(func() {
		b.requests["echo.lookup"] = protocol.RequestReply{OK: true, Data: json.RawMessage(`{"prefix":"node: "}`)}
	})

	r := b.workerCommand(a, "echo", "echo.say", map[string]any{"text": "a", "lookup": true})
	var said struct{ Text string }
	if !r.OK || json.Unmarshal(r.Result, &said) != nil || said.Text != "node: a" {
		t.Fatalf("echo.say с запросом к бэкенду: %+v %s", r, r.Result)
	}
	b.wait("событие echo.said", func() bool {
		ev := eventsOf(a, "echo.said")
		return len(ev) == 1 && ev[0].Worker == "echo" && strings.Contains(string(ev[0].Data), "node: a")
	})
	if r := b.workerCommand(a, "sysinfo", "sys.info", nil); !r.OK || !strings.Contains(string(r.Result), `"pid"`) {
		t.Fatalf("sys.info: %+v", r)
	}

	long := b.command(a, protocol.CmdWorkerCommand, protocol.WorkerCommandArgs{Worker: "echo", Type: "echo.long",
		Args: json.RawMessage(`{"steps": 3, "delayMs": 200}`)})
	var done struct{ Steps int }
	if r := b.result(a, long); !r.OK || json.Unmarshal(r.Result, &done) != nil || done.Steps != 3 {
		t.Fatalf("echo.long: %+v %s", r, r.Result)
	}
	b.wait("ход команды", func() bool {
		n := 0
		for _, e := range eventsOf(a, protocol.EventCommandProgress) {
			var p protocol.Progress
			if json.Unmarshal(e.Data, &p) == nil && p.Re == long && p.Worker == "echo" {
				n++
			}
		}
		return n == 3
	})

	slow := b.command(a, protocol.CmdWorkerCommand, protocol.WorkerCommandArgs{Worker: "echo", Type: "echo.long",
		Args: json.RawMessage(`{"steps": 100, "delayMs": 200}`)})
	b.wait("долгая команда идёт", func() bool {
		for _, e := range eventsOf(a, protocol.EventCommandProgress) {
			if strings.Contains(string(e.Data), slow) {
				return true
			}
		}
		return false
	})
	if r := b.result(a, b.command(a, protocol.CmdWorkerCancel, protocol.WorkerCancelArgs{Worker: "echo", Re: slow})); !r.OK {
		t.Fatalf("worker.cancel: %+v", r)
	}
	if r := b.result(a, slow); r.OK || r.Error.Code != protocol.CodeCancelled {
		t.Fatalf("отменённая: %+v", r)
	}

	if r := b.workerCommand(a, "echo", "echo.nope", nil); r.OK || r.Error.Code != protocol.CodeCommandUndeclared {
		t.Fatalf("не из манифеста: %+v", r)
	}
	if r := b.workerCommand(a, "ghost", "x", nil); r.OK || r.Error.Code != protocol.CodeWorkerUnknown {
		t.Fatalf("неизвестный воркер: %+v", r)
	}
	if r := b.result(a, b.command(a, "agent.nope", nil)); r.OK || r.Error.Code != protocol.CodeUnknownCommand {
		t.Fatalf("незнакомая команда: %+v", r)
	}

	// Повтор: итог тот же, событие — одно.
	id := b.command(a, protocol.CmdWorkerCommand, protocol.WorkerCommandArgs{Worker: "echo", Type: "echo.say", Args: json.RawMessage(`{"text":"once"}`)})
	first := b.result(a, id)
	b.set(func() { delete(a.results, id) })
	b.commandID(a, id, protocol.CmdWorkerCommand, protocol.WorkerCommandArgs{Worker: "echo", Type: "echo.say", Args: json.RawMessage(`{"text":"once"}`)})
	if again := b.result(a, id); string(again.Result) != string(first.Result) {
		t.Fatalf("повтор: %+v / %+v", again, first)
	}
	time.Sleep(time.Second)
	b.mu.Lock()
	n := 0
	for _, e := range eventsOf(a, "echo.said") {
		if strings.Contains(string(e.Data), "once") {
			n++
		}
	}
	b.mu.Unlock()
	if n != 1 {
		t.Fatalf("повтор выполнил команду снова: событий %d", n)
	}
}

// Команды агенту (§8): журнал, перезапуск воркера, смена ключа, обновление выключено.
func TestAgentCommands(t *testing.T) {
	b := newBackend(t)
	newNode(t, b.srv.URL)
	a := ready(t, b)

	r := b.result(a, b.command(a, protocol.CmdAgentLogs, protocol.AgentLogsArgs{Lines: 50}))
	var logs protocol.LogsResult
	if !r.OK || json.Unmarshal(r.Result, &logs) != nil || len(logs.Entries) == 0 {
		t.Fatalf("agent.logs: %+v", r)
	}

	if r := b.result(a, b.command(a, protocol.CmdAgentLogs, protocol.AgentLogsArgs{Worker: "ghost"})); r.OK || r.Error.Code != protocol.CodeWorkerUnknown {
		t.Fatalf("agent.logs неизвестного воркера: %+v", r)
	}
	if r := b.result(a, b.command(a, protocol.CmdWorkerRestart, protocol.WorkerRestartArgs{Worker: "echo"})); !r.OK {
		t.Fatalf("worker.restart: %+v", r)
	}
	b.wait("echo снова running", func() bool {
		w := workerState(a, "echo")
		return w != nil && w.State == protocol.WorkerRunning
	})

	r = b.result(a, b.command(a, protocol.CmdAgentRotateKey, nil))
	var rk protocol.RotateKeyResult
	if !r.OK || json.Unmarshal(r.Result, &rk) != nil || len(rk.SecretHash) != 64 {
		t.Fatalf("agent.rotateKey: %+v", r)
	}
	b.wait("агент перешёл на новый секрет", func() bool { return a.pendingHash == "" && a.secretHash == rk.SecretHash })

	if r := b.result(a, b.command(a, protocol.CmdAgentUpdate, protocol.AgentUpdateArgs{Version: "9.9.9"})); r.OK || r.Error.Code != protocol.CodeUpdateUnsupported {
		t.Fatalf("agent.update при update.mode: disabled: %+v", r)
	}
}

// Перезапуск агента: воркеры работают дальше и переподключаются; долгая команда, шедшая во
// время перезапуска, завершается с итогом; ключ и seq команд сохраняются.
func TestAgentRestart(t *testing.T) {
	b := newBackend(t)
	n := newNode(t, b.srv.URL)
	a := ready(t, b)
	long := b.command(a, protocol.CmdWorkerCommand, protocol.WorkerCommandArgs{Worker: "echo", Type: "echo.long",
		Args: json.RawMessage(`{"steps": 15, "delayMs": 200}`)})
	b.wait("долгая команда идёт", func() bool {
		for _, e := range eventsOf(a, protocol.EventCommandProgress) {
			if strings.Contains(string(e.Data), long) {
				return true
			}
		}
		return false
	})
	n.stop()
	n.start()
	if r := b.result(a, long); !r.OK {
		t.Fatalf("команда через перезапуск агента: %+v", r)
	}
	b.mu.Lock()
	agents := len(b.agents)
	starts := len(eventsOf(a, "sys.started"))
	b.mu.Unlock()
	if agents != 1 || starts != 1 {
		t.Fatalf("агент тот же, воркеры не перезапускались: агентов %d, запусков sysinfo %d", agents, starts)
	}
	if r := b.workerCommand(a, "sysinfo", "sys.info", nil); !r.OK {
		t.Fatalf("после перезапуска: %+v", r)
	}
}

// Без связи события копятся в очереди на диске и доходят, когда бэкенд снова доступен, — и
// через перезапуск агента.
func TestOutboxOffline(t *testing.T) {
	b := newBackend(t)
	n := newNode(t, b.srv.URL)
	a := ready(t, b)
	c := n.admin()
	b.set(func() { b.down = true })
	b.srv.CloseClientConnections()
	var res protocol.Result
	if err := c.Do(context.Background(), http.MethodPost, "/workers/echo/command", app.CommandRequest{Type: "echo.say",
		Args: json.RawMessage(`{"text":"offline"}`)}, &res); err != nil || !res.OK {
		t.Fatalf("команда через локальный API: %+v %v", res, err)
	}
	var st app.AgentStatus
	eventually(t, "событие в очереди", func() bool {
		return c.Do(context.Background(), http.MethodGet, "/status", nil, &st) == nil && st.Outbox >= 1 && !st.Online
	})
	n.stop()
	n.start()
	b.set(func() { b.down = false })
	b.wait("событие дошло", func() bool {
		for _, e := range eventsOf(a, "echo.said") {
			if strings.Contains(string(e.Data), "offline") {
				return true
			}
		}
		return false
	})
}

// Прокси буферизует поток (нет «: ok») — агент переходит на длинный опрос и получает команды.
func TestPollFallback(t *testing.T) {
	b := newBackend(t)
	b.buffered = true
	newNode(t, b.srv.URL)
	a := ready(t, b)
	if r := b.workerCommand(a, "sysinfo", "sys.info", nil); !r.OK {
		t.Fatalf("команда через опрос: %+v", r)
	}
	b.mu.Lock()
	polls := a.polls
	b.mu.Unlock()
	if polls == 0 {
		t.Fatal("длинного опроса не было")
	}
}

// Ключ не известен бэкенду — регистрация заново по токену, воркеры работают дальше; отозванный
// агент (revoked) прекращает связь, останавливает воркеры и остаётся отозванным после
// перезапуска.
func TestReenrollAndRevoke(t *testing.T) {
	b := newBackend(t)
	n := newNode(t, b.srv.URL)
	a := ready(t, b)
	// Бэкенд потерял агента (новая база): seq команд у новой записи — снова с начала.
	b.command(a, protocol.CmdAgentLogs, protocol.AgentLogsArgs{Lines: 1})
	b.set(func() { delete(b.agents, a.id); b.seq = 0 })
	b.srv.CloseClientConnections()
	var second *agentRec
	b.wait("регистрация заново", func() bool {
		for _, rec := range b.agents {
			second = rec
		}
		return second != nil && second.hello != nil
	})
	if r := b.result(second, b.command(second, protocol.CmdAgentLogs, protocol.AgentLogsArgs{Lines: 1})); !r.OK {
		t.Fatalf("команда новой записи после регистрации заново: %+v", r)
	}
	b.set(func() { second.revoked = true })
	b.srv.CloseClientConnections()
	eventually(t, "агент прекратил связь", func() bool { return n.output("агент отозван бэкендом") })
	dataDir := filepath.Join(n.dir, "data")
	eventually(t, "воркеры остановлены", func() bool {
		st, err := app.Status(dataDir)
		if err != nil || st.Status != protocol.StatusRevoked {
			return false
		}
		for _, w := range st.Workers {
			if w.State == protocol.WorkerRunning {
				return false
			}
		}
		return true
	})
	b.mu.Lock()
	hellos := second.helloCount
	b.mu.Unlock()
	n.stop()
	<-n.done
	n.start()
	eventually(t, "после перезапуска — отозван", func() bool {
		st, err := app.Status(dataDir)
		return err == nil && st.Status == protocol.StatusRevoked && st.StatusReason != ""
	})
	time.Sleep(2 * time.Second)
	b.mu.Lock()
	again := second.helloCount
	b.mu.Unlock()
	if again != hellos {
		t.Fatalf("отозванный агент снова выходил на связь: hello %d → %d", hellos, again)
	}
	if st, _ := app.Status(dataDir); len(st.Workers) > 0 && st.Workers[0].State == protocol.WorkerRunning {
		t.Fatalf("отозванный агент запустил воркеры: %+v", st.Workers)
	}
}

// Бэкенд ещё не подтвердил агента (hello: status pending): воркеры не запускаются; подтвердил —
// запускаются.
func TestPendingUntilActive(t *testing.T) {
	b := newBackend(t)
	b.set(func() { b.helloStatus = protocol.StatusPending })
	n := newNode(t, b.srv.URL)
	a := b.agent()
	b.wait("hello", func() bool { return a.hello != nil })
	dataDir := filepath.Join(n.dir, "data")
	time.Sleep(2 * time.Second)
	st, err := app.Status(dataDir)
	if err != nil || st.Status != protocol.StatusPending {
		t.Fatalf("pending: %+v %v", st, err)
	}
	for _, w := range st.Workers {
		if w.State == protocol.WorkerRunning {
			t.Fatalf("воркер %s запущен у неподтверждённого агента", w.Name)
		}
	}
	b.set(func() { b.helloStatus = protocol.StatusActive })
	b.srv.CloseClientConnections()
	ready(t, b)
	if st, _ := app.Status(dataDir); st.Status != protocol.StatusActive {
		t.Fatalf("после подтверждения: %+v", st)
	}
}

// Без бэкенда (§18): желаемое — из файлов desired/<воркер>/<ключ>.json, перечитываются по
// SIGHUP; команды — через локальный API.
func TestWithoutBackend(t *testing.T) {
	n := newNode(t, "")
	dir := filepath.Join(n.dir, "desired", "echo")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	c := n.admin()
	say := func() string {
		var res protocol.Result
		if err := c.Do(context.Background(), http.MethodPost, "/workers/echo/command", app.CommandRequest{Type: "echo.say",
			Args: json.RawMessage(`{"text":"x"}`)}, &res); err != nil || !res.OK {
			t.Fatalf("команда: %+v %v", res, err)
		}
		var v struct{ Text string }
		_ = json.Unmarshal(res.Result, &v)
		return v.Text
	}
	eventually(t, "echo зарегистрирован", func() bool {
		var ws []protocol.WorkerState
		return c.Do(context.Background(), http.MethodGet, "/workers", nil, &ws) == nil && len(ws) == 2 && ws[0].State == protocol.WorkerRunning
	})
	if got := say(); got != "x" {
		t.Fatalf("без настройки: %q", got)
	}
	if err := os.WriteFile(filepath.Join(dir, "settings.json"), []byte(`{"prefix":"local: "}`), 0o600); err != nil {
		t.Fatal(err)
	}
	_ = n.cmd.Process.Signal(syscall.SIGHUP)
	eventually(t, "настройка из файла", func() bool { return say() == "local: x" })
	var st app.AgentStatus
	if err := c.Do(context.Background(), http.MethodGet, "/status", nil, &st); err != nil || st.Server != "" || st.Online {
		t.Fatalf("status без бэкенда: %+v %v", st, err)
	}
	out, err := runAgent("command", "-config", n.cfg, "sysinfo", "sys.info")
	if err != nil || !strings.Contains(out, `"pid"`) {
		t.Fatalf("agent command: %v\n%s", err, out)
	}
	out, err = runAgent("status", "-config", n.cfg)
	if err != nil || !strings.Contains(out, "без бэкенда") || !strings.Contains(out, "echo") {
		t.Fatalf("agent status: %v\n%s", err, out)
	}
}

func runAgent(args ...string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	out, err := execCommand(ctx, agentBin, args...).CombinedOutput()
	return string(out), err
}

// worker.update (§10): подписанная сборка с бэкенда заменяет воркер, итог — {version, previous};
// неверная подпись — UPDATE_FAILED, воркер работает на прежней сборке.
func TestWorkerUpdate(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	b := newBackend(t)
	newNodeWith(t, func(dir string) string {
		return fmt.Sprintf(`server: { url: %s, allowInsecure: true }
enroll: { token: e2e-token }
dataDir: %s/data
name: node-e2e
update: { mode: self, publicKey: %s }
workers:
  - name: sysinfo
    release: true
    lifecycle: { stopTimeout: 5s }
`, b.srv.URL, dir, base64.StdEncoding.EncodeToString(pub))
	}, func(dir string) {
		rel := filepath.Join(dir, "data", "workers", "sysinfo")
		_ = os.MkdirAll(rel, 0o755)
		raw, _ := os.ReadFile(sysinfoBin)
		_ = os.WriteFile(filepath.Join(rel, "current"), raw, 0o755)
		_ = os.WriteFile(filepath.Join(rel, "version"), []byte("1.0.0\n"), 0o644)
	})
	a := b.agent()
	b.wait("sysinfo 1.0.0 в hello", func() bool {
		return a.hello != nil && len(a.hello.Workers) == 1 && a.hello.Workers[0].Version == "1.0.0" && a.hello.Workers[0].Manifest != nil
	})
	sum, _ := update.FileHash(sysinfoNext)
	b.set(func() { b.files["sysinfo-2"] = sysinfoNext })
	build := update.Build{Name: "sysinfo", Version: "2.0.0", OS: runtime.GOOS, Arch: runtime.GOARCH, SHA256: sum}
	args := protocol.WorkerUpdateArgs{Worker: "sysinfo", Version: "2.0.0", URL: protocol.BackendPrefix + "/files/sysinfo-2",
		SHA256: sum, Signature: update.Sign(priv, build)}

	bad := args
	_, other, _ := ed25519.GenerateKey(nil)
	bad.Signature = update.Sign(other, build)
	if r := b.result(a, b.command(a, protocol.CmdWorkerUpdate, bad)); r.OK || r.Error.Code != protocol.CodeUpdateFailed {
		t.Fatalf("чужая подпись: %+v", r)
	}
	r := b.result(a, b.command(a, protocol.CmdWorkerUpdate, args))
	var res protocol.UpdateResult
	if !r.OK || json.Unmarshal(r.Result, &res) != nil || res != (protocol.UpdateResult{Version: "2.0.0", Previous: "1.0.0"}) {
		t.Fatalf("worker.update: %+v %s", r, r.Result)
	}
	info := b.workerCommand(a, "sysinfo", "sys.info", nil)
	if !info.OK || !strings.Contains(string(info.Result), `"version":"2.0.0"`) {
		t.Fatalf("работает новая сборка: %+v %s", info, info.Result)
	}
	b.wait("версия 2.0.0 в hello", func() bool { return a.hello.Workers[0].Version == "2.0.0" })
}
