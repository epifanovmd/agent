//go:build unix

// Package integration — настоящий агент (internal/app) против эталонного сервера
// (test/testserver на sdk/go/server): Go-воркер (задачи, команда, состояние), обрыв связи, HTTP sync,
// Python-воркеры (если есть python3).
package integration

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	stdlog "log"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/identity"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/sdk/go/message"
	"github.com/epifanovmd/agent/sdk/go/worker"
	"github.com/epifanovmd/agent/test/testserver"
)

const token = "it-token"

type stand struct {
	t      *testing.T
	server *testserver.Server
	http   *httptest.Server
	// dials — сколько раз агент открывал WebSocket (переподключения).
	dials atomic.Int64
}

func newStand(t *testing.T, tune ...func(*testserver.Config)) *stand {
	s := newUnstartedStand(t, tune...)
	s.http.Start()
	s.server.SetPublicURL(s.http.URL)
	return s
}

// newUnstartedStand — стенд, сервер которого запускает тест (Start или StartTLS).
func newUnstartedStand(t *testing.T, tune ...func(*testserver.Config)) *stand {
	cfg := testserver.Config{
		EnrollToken: token, StatusInterval: 200 * time.Millisecond, MetricsInterval: 200 * time.Millisecond,
		Log: logx.Discard(),
	}
	for _, fn := range tune {
		fn(&cfg)
	}
	srv := testserver.New(cfg)
	s := &stand{t: t, server: srv}
	handler := srv.Handler()
	s.http = httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == message.LinkPath && r.Method == http.MethodGet {
			s.dials.Add(1)
		}
		handler.ServeHTTP(w, r)
	}))
	s.http.Config.ErrorLog = stdlog.New(io.Discard, "", 0) // отказы TLS в тестах ожидаемы
	t.Cleanup(func() {
		s.http.Close()
		srv.Close()
	})
	return s
}

// TestMain — тестовый бинарь служит и Go-воркером (IT_WORKER=1).
func TestMain(m *testing.M) {
	if os.Getenv("IT_WORKER") == "1" {
		goWorker()
		return
	}
	os.Exit(m.Run())
}

// goWorker — Go-воркер на sdk/go/worker: очередь go.echo, команда it.ping, домен it.
// IT_WORKER_KIND=extra — другой воркер: только команда it.extra.
func goWorker() {
	if os.Getenv("IT_WORKER_KIND") == "release" {
		releaseWorker()
		return
	}
	if os.Getenv("IT_WORKER_KIND") == "control" {
		controlWorker()
		return
	}
	if os.Getenv("IT_WORKER_KIND") == "extra" {
		w := worker.New("extra", "it")
		w.Command("it.extra", func(context.Context, *worker.Command) (any, error) { return "extra", nil })
		if err := w.Run(context.Background()); err != nil {
			os.Exit(1)
		}
		return
	}
	w := worker.New("go", "it")
	w.Job("go.echo", 2, func(ctx context.Context, job *worker.Job) (any, error) {
		var data struct {
			Text  string  `json:"text"`
			Sleep float64 `json:"sleep"`
		}
		_ = json.Unmarshal(job.Data, &data)
		job.Progress(0.5, "половина")
		_ = job.Event("step", map[string]int{"n": 1})
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-job.StopRequested():
			return map[string]any{"echo": data.Text, "stopped": true}, nil
		case <-time.After(time.Duration(data.Sleep * float64(time.Second))):
		}
		return map[string]string{"echo": data.Text}, nil
	})
	w.Command("it.ping", func(_ context.Context, cmd *worker.Command) (any, error) {
		_, _ = io.WriteString(cmd, "pong\n")
		return map[string]bool{"ok": true}, nil
	})
	w.Command("it.print", func(_ context.Context, cmd *worker.Command) (any, error) {
		// Вывод воркера агент пишет в свой лог (stdout — info) и отправляет
		// серверу по порогу.
		var text string
		_ = json.Unmarshal(cmd.Args, &text)
		fmt.Fprintln(os.Stdout, text)
		return nil, nil
	})
	w.State("it", func(_ context.Context, version int64, spec json.RawMessage) (any, error) {
		// В отчёте — снимок, каким его получил воркер (секреты — раскрытыми).
		return map[string]any{"version": version, "spec": spec}, nil
	})
	if err := w.Run(context.Background()); err != nil {
		os.Exit(1)
	}
}

// agent — агент с Go-воркером (go.echo, it.ping, домен it) и workers;
// остановка — по завершении теста. Возвращает каталог данных агента.
func (s *stand) agent(transport string, workers []config.Worker, tune ...func(*config.Config)) string {
	_, dir := s.agentApp(transport, workers, tune...)
	return dir
}

// agentApp — как agent, но возвращает и сам агент (перечитывание настроек).
func (s *stand) agentApp(transport string, workers []config.Worker, tune ...func(*config.Config)) (*app.App, string) {
	exe, err := os.Executable()
	if err != nil {
		s.t.Fatal(err)
	}
	workers = append([]config.Worker{{
		Name: "go", Command: []string{exe}, Env: map[string]string{"IT_WORKER": "1"},
		Replicas: 1, StopTimeout: config.Duration(10 * time.Second),
	}}, workers...)
	cfg := config.Defaults()
	cfg.Server.URL = s.http.URL
	cfg.Server.Transport = transport
	cfg.DataDir = s.t.TempDir()
	cfg.Name = "it-agent"
	cfg.Enroll.Token = token
	cfg.Update.Mode = "disabled"
	cfg.Telemetry.GPU = "off"
	cfg.Log.Level = "error"
	cfg.Workers = workers
	for _, fn := range tune {
		fn(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		s.t.Fatal(err)
	}
	agent, err := app.New(cfg, "it")
	if err != nil {
		s.t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = agent.Run(ctx)
	}()
	s.t.Cleanup(func() {
		cancel()
		<-done
	})
	return agent, cfg.DataDir
}

func (s *stand) waitJob(id string, status string) testserver.Job {
	s.t.Helper()
	var job testserver.Job
	eventually(s.t, "задача "+status, func() bool {
		job, _ = s.server.Job(id)
		return job.Status == status
	})
	return job
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func data(v any) json.RawMessage {
	raw, _ := json.Marshal(v)
	return raw
}

func TestJobsCommandsAndState(t *testing.T) {
	for _, transport := range []string{"ws", "http"} {
		t.Run(transport, func(t *testing.T) {
			s := newStand(t)
			s.agent(transport, nil)

			job := s.server.Enqueue(testserver.EnqueueRequest{Queue: "go.echo", Data: data(map[string]string{"text": "привет"})})
			done := s.waitJob(job.ID, testserver.JobCompleted)
			if string(done.Result) != `{"echo":"привет"}` || len(done.Events) != 1 || done.Progress != 1 {
				t.Fatalf("итог: %+v", done)
			}

			cmd, err := s.server.Command(testserver.CommandRequest{Name: "it.ping"})
			if err != nil {
				t.Fatal(err)
			}
			eventually(t, "команда", func() bool {
				c, _ := s.server.CommandSnapshot(cmd.ID)
				return c.Status == testserver.CommandSucceeded && c.Output == "pong\n"
			})

			version := s.server.SetState("it", data(map[string]int{"replicas": 3}))
			eventually(t, "состояние применено Go-воркером", func() bool {
				a, _ := s.server.Agent("it-agent")
				ap, ok := a.StateApplied["it"]
				return ok && ap.OK && ap.Version == version
			})

			stopping := s.server.Enqueue(testserver.EnqueueRequest{Queue: "go.echo", Data: data(map[string]any{"text": "долго", "sleep": 30})})
			s.waitJob(stopping.ID, testserver.JobRunning)
			if err := s.server.Agents().StopJob(stopping.ID); err != nil {
				t.Fatal(err)
			}
			stopped := s.waitJob(stopping.ID, testserver.JobCompleted)
			if !strings.Contains(string(stopped.Result), `"stopped":true`) {
				t.Fatalf("досрочно: %s", stopped.Result)
			}
		})
	}
}

func TestOfflineCompletionDeliveredAfterReconnect(t *testing.T) {
	s := newStand(t)
	s.agent("ws", nil)
	job := s.server.Enqueue(testserver.EnqueueRequest{Queue: "go.echo", Data: data(map[string]any{"text": "без связи", "sleep": 1}), LeaseSeconds: 30})
	s.waitJob(job.ID, testserver.JobRunning)

	// Обрыв: все соединения закрыты, сервер какое-то время недоступен.
	s.http.CloseClientConnections()
	s.http.Config.SetKeepAlivesEnabled(false)
	time.Sleep(1500 * time.Millisecond)
	s.http.Config.SetKeepAlivesEnabled(true)

	done := s.waitJob(job.ID, testserver.JobCompleted)
	if done.Attempt != 0 {
		t.Fatalf("итог без связи — та же попытка: %+v", done)
	}
}

func TestPythonWorker(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("нет python3")
	}
	root, _ := filepath.Abs("../..")
	s := newStand(t)
	s.agent("ws", []config.Worker{{
		Name:        "echo",
		Command:     []string{python, filepath.Join(root, "examples/workers/echo_worker.py")},
		Env:         map[string]string{"PYTHONPATH": filepath.Join(root, "sdk/python")},
		Replicas:    1,
		StopTimeout: config.Duration(10 * time.Second),
	}})

	job := s.server.Enqueue(testserver.EnqueueRequest{
		Queue:   "example.echo",
		Data:    data(map[string]string{"text": "из python"}),
		Inputs:  map[string]string{"source": "вход"},
		Outputs: []string{"echo"},
	})
	done := s.waitJob(job.ID, testserver.JobCompleted)
	if !strings.Contains(string(done.Result), "из python | вход") {
		t.Fatalf("итог: %s", done.Result)
	}
	if out, ok := s.server.File(job.ID + "/out/echo"); !ok || string(out) != "из python | вход" {
		t.Fatalf("выходной файл: %q", out)
	}
}

// Воркер-сервис на Python: команды, домен состояния, телеметрия и события
// объявляются после hello — сервер узнаёт о них из capabilities.
func TestPythonServiceWorker(t *testing.T) {
	python, err := exec.LookPath("python3")
	if err != nil {
		t.Skip("нет python3")
	}
	root, _ := filepath.Abs("../..")
	kvDir := t.TempDir()
	for _, transport := range []string{"ws", "http"} {
		t.Run(transport, func(t *testing.T) {
			s := newStand(t)
			s.agent(transport, []config.Worker{{
				Name:    "kv",
				Command: []string{python, filepath.Join(root, "examples/workers/kv_worker.py")},
				Env: map[string]string{
					"PYTHONPATH": filepath.Join(root, "sdk/python"), "KV_DIR": filepath.Join(kvDir, transport),
					"KV_TELEMETRY_INTERVAL": "0.1",
				},
				Replicas:    1,
				StopTimeout: config.Duration(10 * time.Second),
			}})

			eventually(t, "возможности воркера у сервера", func() bool {
				a, ok := s.server.Agent("it-agent")
				if !ok || a.Capabilities == nil || a.Capabilities.Commands == nil || a.Capabilities.State == nil {
					return false
				}
				_, domain := a.Capabilities.State.Domains["example.kv"]
				return domain && slices.Contains(a.Capabilities.Commands.Names, "example.kv.get")
			})

			version := s.server.SetState("example.kv", data(map[string]string{"greeting": "привет"}))
			eventually(t, "снимок применён воркером", func() bool {
				a, _ := s.server.Agent("it-agent")
				applied, ok := a.StateApplied["example.kv"]
				return ok && applied.OK && applied.Version == version && string(applied.Report) == `{"keys":1}`
			})

			cmd, cerr := s.server.Command(testserver.CommandRequest{Name: "example.kv.get", Args: data(map[string]string{"key": "greeting"})})
			if cerr != nil {
				t.Fatal(cerr)
			}
			eventually(t, "команда воркера", func() bool {
				c, _ := s.server.CommandSnapshot(cmd.ID)
				return c.Status == testserver.CommandSucceeded && c.Output == "читаю greeting\n" &&
					strings.Contains(string(c.Result), `"value":"привет"`)
			})
			missing, _ := s.server.Command(testserver.CommandRequest{Name: "example.kv.get", Args: data(map[string]string{"key": "nope"})})
			eventually(t, "ошибка команды с кодом", func() bool {
				c, _ := s.server.CommandSnapshot(missing.ID)
				return c.Status == testserver.CommandFailed && c.Error != nil && c.Error.Code == "KEY_NOT_FOUND"
			})

			eventually(t, "телеметрия и событие воркера", func() bool {
				a, _ := s.server.Agent("it-agent")
				if a.Metrics == nil || string(a.Metrics.Channels["example.kv"]) != `{"keys":1}` {
					return false
				}
				for _, e := range a.Events {
					if e.Source == "kv" && e.Type == "kv.applied" {
						return true
					}
				}
				return false
			})
		})
	}
}

// Смена ключа: сервер поручает agent.rotateKey, агент отвечает хешем нового
// секрета, сервер закрывает сессию (1012), агент переподключается с новым
// секретом; старый больше не принимается, агент работает дальше.
func TestRotateKey(t *testing.T) {
	for _, transport := range []string{"ws", "http"} {
		t.Run(transport, func(t *testing.T) {
			s := newStand(t)
			dir := s.agent(transport, nil)
			store := identity.NewStore(dir)

			var agent testserver.Agent
			eventually(t, "агент на связи и объявил agent.rotateKey", func() bool {
				a, ok := s.server.Agent("it-agent")
				agent = a
				return ok && a.Online && a.Capabilities != nil && a.Capabilities.Commands != nil &&
					slices.Contains(a.Capabilities.Commands.Names, message.CommandRotateKey)
			})
			old, ok, err := store.Load()
			if err != nil || !ok {
				t.Fatalf("учётные данные: %v %v", ok, err)
			}

			cmd, err := s.server.Agents().RotateKey(agent.ID)
			if err != nil {
				t.Fatal(err)
			}
			var result message.RotateKeyResult
			eventually(t, "agent.rotateKey выполнена", func() bool {
				c, _ := s.server.CommandSnapshot(cmd.ID)
				if c.Status == testserver.CommandFailed {
					t.Fatalf("команда: %+v", c.Error)
				}
				raw, _ := json.Marshal(c.Result)
				return c.Status == testserver.CommandSucceeded && json.Unmarshal(raw, &result) == nil
			})
			if len(result.SecretHash) != 64 {
				t.Fatalf("secretHash: %q", result.SecretHash)
			}

			// Новый секрет признан сервером (повышение — только после welcome).
			eventually(t, "агент переподключился с новым секретом", func() bool {
				c, ok, _ := store.Load()
				return ok && c.PendingSecret == "" && c.Secret != old.Secret &&
					identity.SecretHash(c.Secret) == result.SecretHash
			})
			if c, _, _ := store.Load(); c.AgentID != old.AgentID {
				t.Fatalf("id агента сменился: %s → %s", old.AgentID, c.AgentID)
			}
			if status := syncStatus(t, s.http.URL, old.Authorization()); status != http.StatusUnauthorized {
				t.Fatalf("старый секрет: HTTP %d, нужен 401", status)
			}
			if list, _ := s.server.Agents().List(); len(list) != 1 {
				t.Fatalf("агентов %d — повторной регистрации быть не должно", len(list))
			}

			// Агент на связи: команда и задача.
			eventually(t, "агент снова на связи", func() bool {
				a, ok := s.server.Agent("it-agent")
				return ok && a.Online
			})
			ping, perr := s.server.Command(testserver.CommandRequest{Name: "it.ping"})
			if perr != nil {
				t.Fatal(perr)
			}
			eventually(t, "команда после смены ключа", func() bool {
				c, _ := s.server.CommandSnapshot(ping.ID)
				return c.Status == testserver.CommandSucceeded
			})
			job := s.server.Enqueue(testserver.EnqueueRequest{Queue: "go.echo", Data: data(map[string]string{"text": "после"})})
			s.waitJob(job.ID, testserver.JobCompleted)
		})
	}
}

// syncStatus — HTTP-код открытия сессии HTTP sync с заголовком authorization
// (только для отклонённых учётных данных: принятые вытеснили бы сессию агента).
func syncStatus(t *testing.T, base, authorization string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodPost, base+message.SyncPath, strings.NewReader(`{"sessionId":null,"messages":[],"waitSeconds":0}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", authorization)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}
