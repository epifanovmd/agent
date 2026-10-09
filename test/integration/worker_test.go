//go:build unix

package integration

import (
	"bytes"
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/update"
)

// Метка сборки в конце копии тестового бинаря: версия агента или воркера из выпуска.
// «broken» — сборка завершается сразу после запуска.
const (
	tailMark = "\nIT-BUILD:"
	broken   = "broken"
)

// buildTag — метка сборки этого процесса ("" — без метки).
func buildTag() string {
	exe, err := os.Executable()
	if err != nil {
		return ""
	}
	f, err := os.Open(exe)
	if err != nil {
		return ""
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil || st.Size() < 64 {
		return ""
	}
	buf := make([]byte, 64)
	if _, err := f.ReadAt(buf, st.Size()-64); err != nil {
		return ""
	}
	i := bytes.LastIndex(buf, []byte(tailMark))
	if i < 0 {
		return ""
	}
	return strings.TrimSpace(string(buf[i+len(tailMark):]))
}

// testWorker — воркер для тестов: HTTP на unix-сокете AGENT_WORKER_SOCKET без SDK. PUT
// /config/{key} отвечает 200 {"applied": data} — подробный итог применения, отказ — 422 с
// data в тексте. Каталог
// IT_STATE хранит то, что переживает перезапуск: журнал настроек (configs.log), отказ в
// настройках (файл reject), отметку уборки (cleanup), отменённые запросы (cancelled.log),
// pid дочернего процесса (child.pid).
func testWorker() {
	tag := buildTag()
	if tag == broken {
		fmt.Fprintln(os.Stderr, "сборка не запускается")
		os.Exit(3)
	}
	state := os.Getenv("IT_STATE")
	record := func(file, line string) {
		f, err := os.OpenFile(filepath.Join(state, file), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
		if err != nil {
			return
		}
		defer f.Close()
		fmt.Fprintln(f, line)
	}
	toAgent := &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", os.Getenv(message.EnvSocket))
	}}}
	agent := func(method, path string, body []byte) (int, []byte, error) {
		req, _ := http.NewRequest(method, "http://agent"+path, bytes.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+os.Getenv(message.EnvWorkerToken))
		resp, err := toAgent.Do(req)
		if err != nil {
			return 0, nil, err
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, raw, nil
	}
	var hang atomic.Bool
	var busy atomic.Int32
	var metricsCalls atomic.Int64
	var childMu sync.Mutex

	mux := http.NewServeMux()
	mux.HandleFunc("PUT /config/{key}", func(w http.ResponseWriter, r *http.Request) {
		var v message.ConfigValue
		if err := json.NewDecoder(r.Body).Decode(&v); err != nil {
			http.Error(w, "неверное тело", http.StatusBadRequest)
			return
		}
		key := r.PathValue("key")
		if _, err := os.Stat(filepath.Join(state, "reject")); err == nil {
			record("configs.log", fmt.Sprintf("reject %s %d", key, v.Version))
			w.WriteHeader(http.StatusUnprocessableEntity)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": "значение не подходит: " + string(v.Data)})
			return
		}
		record("configs.log", fmt.Sprintf("put %s %d %s", key, v.Version, v.Data))
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprintf(w, `{"applied":%s}`, cmp.Or(string(v.Data), "null"))
	})
	mux.HandleFunc("DELETE /config/{key}", func(w http.ResponseWriter, r *http.Request) {
		record("configs.log", "delete "+r.PathValue("key"))
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		fmt.Fprintf(w, `{"calls":%d,"build":%q}`, metricsCalls.Add(1), tag)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		if hang.Load() {
			<-r.Context().Done()
			return
		}
		fmt.Fprintf(w, `{"ok":true,"busy":%v,"info":{"pid":%d,"build":%q}}`, busy.Load() > 0, os.Getpid(), tag)
	})
	mux.HandleFunc("POST /cleanup", func(w http.ResponseWriter, _ *http.Request) {
		record("cleanup", "done")
		w.WriteHeader(http.StatusNoContent)
	})
	// /manifest — версия: метка сборки или 0.1.0; все ключи, маршруты, события и запросы к
	// серверу, которые используют тесты. Файл no-manifest в IT_STATE — манифеста нет (404):
	// воркер не зарегистрирован; файл jobs — манифест объявляет задачи example.quick и
	// example.long.
	mux.HandleFunc("GET /manifest", func(w http.ResponseWriter, r *http.Request) {
		if _, err := os.Stat(filepath.Join(state, "no-manifest")); err == nil {
			http.NotFound(w, r)
			return
		}
		jobs := ""
		if _, err := os.Stat(filepath.Join(state, "jobs")); err == nil {
			jobs = `,"jobs":[{"type":"example.quick"},{"type":"example.long"}]`
		}
		fmt.Fprintf(w, `{"version":%q,"configs":[{"key":"main","schema":{"type":"object"}},{"key":"limits"}],`+
			`"routes":%s,"events":[{"type":"example.done"},{"type":"example.later"},`+
			`{"type":"example.offline"},{"type":"example.progress"}],`+
			`"requests":[{"type":"example.ask"},{"type":"example.slow"},{"type":"example.deny"}]%s}`, cmp.Or(tag, "0.1.0"), testRoutes, jobs)
	})
	mux.HandleFunc("GET /build", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, tag) })
	mux.HandleFunc("GET /pid", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, os.Getpid()) })
	mux.HandleFunc("/echo", func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", r.Header.Get("Content-Type"))
		w.Header().Set("X-Method", r.Method)
		w.Header().Set("X-Query", r.URL.RawQuery)
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write(body)
	})
	// /stream — тело двумя частями с паузой 1 с.
	mux.HandleFunc("GET /stream", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		fmt.Fprintln(w, "первая часть")
		w.(http.Flusher).Flush()
		time.Sleep(time.Second)
		fmt.Fprintln(w, "вторая часть")
	})
	// /big — 300 000 байт: все значения байтов по кругу.
	mux.HandleFunc("GET /big", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(bigBody())
	})
	// /sleep — ответ через 10 с; запрос прерван раньше — запись в cancelled.log.
	mux.HandleFunc("GET /sleep", func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
			record("cancelled.log", r.URL.Query().Get("id"))
		case <-time.After(10 * time.Second):
			fmt.Fprint(w, "проснулся")
		}
	})
	// /emit?type=…&n=…&data=… — n событий через сокет агента (data — JSON, по умолчанию
	// {"i": номер}); ответ — коды ответов агента.
	mux.HandleFunc("POST /emit", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		from, _ := strconv.Atoi(r.URL.Query().Get("from"))
		var codes []string
		for i := from; i < from+max(n, 1); i++ {
			data := cmp.Or(r.URL.Query().Get("data"), fmt.Sprintf(`{"i":%d}`, i))
			body, _ := json.Marshal(message.EventPost{Type: r.URL.Query().Get("type"), Data: json.RawMessage(data)})
			code, _, err := agent(http.MethodPost, message.EventsPath, body)
			if err != nil {
				codes = append(codes, err.Error())
				continue
			}
			codes = append(codes, strconv.Itoa(code))
		}
		fmt.Fprint(w, strings.Join(codes, ","))
	})
	// /agent/… — GET к сокету агента: статус и тело ответа.
	mux.HandleFunc("GET /agent/{path...}", func(w http.ResponseWriter, r *http.Request) {
		code, body, err := agent(http.MethodGet, "/"+r.PathValue("path"), nil)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("X-Agent-Status", strconv.Itoa(code))
		_, _ = w.Write(body)
	})
	// /work?steps=N&ms=M — долгая работа: busy, событие example.progress {step} на каждый шаг
	// и example.done в конце. Агент недоступен (перезапускается) — событие повторяется, пока
	// не будет принято.
	mux.HandleFunc("POST /work", func(w http.ResponseWriter, r *http.Request) {
		steps, _ := strconv.Atoi(r.URL.Query().Get("steps"))
		ms, _ := strconv.Atoi(r.URL.Query().Get("ms"))
		id := r.URL.Query().Get("id")
		busy.Add(1)
		emit := func(typ string, data string) {
			body, _ := json.Marshal(message.EventPost{Type: typ, Data: json.RawMessage(data)})
			for {
				if code, _, err := agent(http.MethodPost, message.EventsPath, body); err == nil && code == http.StatusAccepted {
					return
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
		go func() {
			defer busy.Add(-1)
			for i := 1; i <= steps; i++ {
				time.Sleep(time.Duration(ms) * time.Millisecond)
				emit("example.progress", fmt.Sprintf(`{"id":%q,"step":%d,"pid":%d}`, id, i, os.Getpid()))
			}
			emit("example.done", fmt.Sprintf(`{"id":%q,"pid":%d}`, id, os.Getpid()))
		}()
		w.WriteHeader(http.StatusAccepted)
	})
	// /jobs — задачи (§12): example.quick — итог сразу (200), example.long — 202, затем
	// job.progress и job.done через агента.
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var req message.JobRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, `{"message":"неверное тело"}`, http.StatusBadRequest)
			return
		}
		switch req.Type {
		case "example.quick":
			fmt.Fprintf(w, `{"result":{"echo":%s}}`, cmp.Or(string(req.Data), "null"))
		case "example.long":
			id := "w-" + req.JobID
			go func() {
				for _, ev := range []message.EventPost{
					{Type: message.JobEventProgress, Data: json.RawMessage(fmt.Sprintf(`{"jobId":%q,"id":%q,"progress":0.5}`, req.JobID, id))},
					{Type: message.JobEventDone, Data: json.RawMessage(fmt.Sprintf(`{"jobId":%q,"id":%q,"result":{"pid":%d}}`, req.JobID, id, os.Getpid()))},
				} {
					time.Sleep(100 * time.Millisecond)
					body, _ := json.Marshal(ev)
					_, _, _ = agent(http.MethodPost, message.EventsPath, body)
				}
			}()
			w.WriteHeader(http.StatusAccepted)
			fmt.Fprintf(w, `{"id":%q}`, id)
		default:
			w.WriteHeader(http.StatusBadRequest)
			fmt.Fprintf(w, `{"message":"нет задачи %s"}`, req.Type)
		}
	})
	// /ask?type=…&timeoutMs=…&data=… — запрос к серверу через сокет агента (data — JSON, по
	// умолчанию {"n": 1}): статус ответа агента — в X-Agent-Status, тело — как есть.
	mux.HandleFunc("POST /ask", func(w http.ResponseWriter, r *http.Request) {
		ms, _ := strconv.Atoi(r.URL.Query().Get("timeoutMs"))
		data := cmp.Or(r.URL.Query().Get("data"), `{"n":1}`)
		body, _ := json.Marshal(message.RequestPost{Type: r.URL.Query().Get("type"), Data: json.RawMessage(data), TimeoutMs: int64(ms)})
		code, raw, err := agent(http.MethodPost, message.RequestsPath, body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		w.Header().Set("X-Agent-Status", strconv.Itoa(code))
		_, _ = w.Write(raw)
	})
	mux.HandleFunc("POST /hang", func(w http.ResponseWriter, _ *http.Request) {
		hang.Store(true)
		w.WriteHeader(http.StatusNoContent)
	})
	// /print?text=…&stderr=1 — строка в stdout (или stderr).
	mux.HandleFunc("POST /print", func(w http.ResponseWriter, r *http.Request) {
		out := os.Stdout
		if r.URL.Query().Get("stderr") != "" {
			out = os.Stderr
		}
		fmt.Fprintln(out, r.URL.Query().Get("text"))
		w.WriteHeader(http.StatusNoContent)
	})
	// /child — дочерний процесс; его pid — в child.pid.
	mux.HandleFunc("POST /child", func(w http.ResponseWriter, _ *http.Request) {
		childMu.Lock()
		defer childMu.Unlock()
		exe, _ := os.Executable()
		cmd := exec.Command(exe)
		cmd.Env = append(os.Environ(), "IT_WORKER=", "IT_CHILD=1")
		if err := cmd.Start(); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		go func() { _ = cmd.Wait() }()
		record("child.pid", strconv.Itoa(cmd.Process.Pid))
		fmt.Fprint(w, cmd.Process.Pid)
	})

	ln, err := net.Listen("unix", os.Getenv(message.EnvWorkerSocket))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	_ = http.Serve(ln, mux)
}

// testRoutes — маршруты тестового воркера в манифесте (routes, §7).
const testRoutes = `[{"method":"POST","path":"/echo"},{"method":"PUT","path":"/echo"},{"method":"GET","path":"/build"},` +
	`{"method":"GET","path":"/pid"},{"method":"GET","path":"/stream"},{"method":"GET","path":"/big"},` +
	`{"method":"GET","path":"/sleep"},{"method":"GET","path":"/agent/{key}"},{"method":"GET","path":"/agent/{key}/{name}"},` +
	`{"method":"POST","path":"/work"},{"method":"POST","path":"/hang"},{"method":"POST","path":"/print"},` +
	`{"method":"POST","path":"/child"},{"method":"POST","path":"/ask"}]`

// bigBody — тело /big.
func bigBody() []byte {
	b := make([]byte, 300_000)
	for i := range b {
		b[i] = byte(i)
	}
	return b
}

// agentProcess — агент отдельным процессом (IT_AGENT): самообновление заменяет исполняемый
// файл, поэтому агент — копия тестового бинаря, а не код в процессе теста. Версия — метка
// сборки; сборка «broken» завершается после проверки обновления (boot guard), как версия,
// которая не может выйти на связь. Настройки — файл IT_AGENT_CONFIG.
func agentProcess() {
	if exe, err := os.Executable(); err == nil {
		if err := update.Boot(update.NewPaths(exe)); err != nil {
			fmt.Fprintln(os.Stderr, "проверка обновления:", err)
		}
	}
	version := buildTag()
	if version == broken {
		fmt.Fprintln(os.Stderr, "сборка не запускается")
		os.Exit(3)
	}
	cfg, err := config.Load(os.Getenv("IT_AGENT_CONFIG"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	a, err := app.New(cfg, version)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	// Сигналы — как у agent run: SIGTERM — остановка (воркеры работают дальше), SIGUSR1 — перезапуск.
	if err := a.Serve(os.Getenv("IT_AGENT_CONFIG")); err != nil && !errors.Is(err, app.ErrRestart) && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
