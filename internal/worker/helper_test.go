//go:build unix

package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/backoff"
	"github.com/epifanovmd/agent/protocol"
)

// Тестовый воркер — этот же тестовый файл, запущенный с TEST_WORKER=режим:
//
//	ok         — регистрация (версия — TEST_VERSION, описание — «pid <pid>») и пульс раз в 50 мс;
//	noregister — работает, но не регистрируется;
//	unhealthy  — как ok, но пульс ok: false;
//	crash      — сразу выход с кодом 1;
//	stubborn   — как ok, но SIGTERM не завершает;
//	children   — как ok и дочерний процесс sleep (pid — в TEST_CHILD_PID);
//	exit0      — как ok, но через 200 мс выход с кодом 0.
//
// Файл TEST_BUSY есть — пульс с busy: true; файл TEST_HANG содержит pid процесса — пульса нет.
// Файл TEST_CMD: строки «out <текст>» и «err <текст>» — вывод в stdout и stderr (файл
// удаляется после чтения). Окружение воркер записывает в $WORKER_STATE_DIR/env.json.
// Пульс не принят (агент перезапущен) — регистрация заново.
//
// TEST_EVENTS — файл, куда пишутся строки «start <pid>» и «stop <pid>».
func TestMain(m *testing.M) {
	if mode := os.Getenv("TEST_WORKER"); mode != "" {
		testWorker(mode)
		return
	}
	// Короткие сроки жизни воркеров.
	restartBackoff = backoff.Policy{Min: 20 * time.Millisecond, Max: 50 * time.Millisecond}
	watchEvery, startTimeout = 20*time.Millisecond, 5*time.Second
	registerTimeout, healthSilence, healthRestart = 2*time.Second, 400*time.Millisecond, 400*time.Millisecond
	os.Exit(m.Run())
}

func event(text string) {
	if path := os.Getenv("TEST_EVENTS"); path != "" {
		f, err := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err == nil {
			fmt.Fprintf(f, "%s %d\n", text, os.Getpid())
			f.Close()
		}
	}
}

func exists(env string) bool {
	path := os.Getenv(env)
	if path == "" {
		return false
	}
	_, err := os.Stat(path)
	return err == nil
}

func testWorker(mode string) {
	if mode == "crash" {
		fmt.Fprintln(os.Stderr, "падаю")
		os.Exit(1)
	}
	event("start")
	fmt.Println("воркер запущен")
	env := map[string]string{}
	for _, kv := range os.Environ() {
		k, v, _ := strings.Cut(kv, "=")
		env[k] = v
	}
	raw, _ := json.Marshal(env)
	_ = os.WriteFile(filepath.Join(os.Getenv(protocol.EnvStateDir), "env.json"), raw, 0o600)
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM)
	if mode == "children" {
		child := exec.Command("sleep", "60")
		if err := child.Start(); err == nil {
			_ = os.WriteFile(os.Getenv("TEST_CHILD_PID"), []byte(fmt.Sprint(child.Process.Pid)), 0o600)
		}
	}
	if mode == "exit0" {
		go func() {
			time.Sleep(200 * time.Millisecond)
			os.Exit(0)
		}()
	}
	client := &http.Client{Timeout: 2 * time.Second, Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", os.Getenv(protocol.EnvAgentSocket))
	}}}
	post := func(path string, body any) bool {
		raw, _ := json.Marshal(body)
		req, _ := http.NewRequest(http.MethodPost, "http://agent"+path, bytes.NewReader(raw))
		req.Header.Set("Authorization", protocol.AuthWorkerScheme+" "+os.Getenv(protocol.EnvWorkerToken))
		resp, err := client.Do(req)
		if err != nil {
			return false
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		return resp.StatusCode/100 == 2
	}
	if mode != "noregister" {
		go func() {
			registered := false
			for {
				if !registered {
					registered = post(protocol.WorkerPathRegister, protocol.Register{Version: os.Getenv("TEST_VERSION"),
						Manifest: protocol.WorkerManifest{Description: fmt.Sprint("pid ", os.Getpid())}})
				}
				hang, _ := os.ReadFile(os.Getenv("TEST_HANG"))
				if registered && strings.TrimSpace(string(hang)) != fmt.Sprint(os.Getpid()) {
					registered = post(protocol.WorkerPathHealth, protocol.Health{OK: mode != "unhealthy", Busy: exists("TEST_BUSY"), Message: mode,
						Metrics: json.RawMessage(`{"n":1}`)})
				}
				time.Sleep(50 * time.Millisecond)
			}
		}()
	}
	go func() {
		for {
			if path := os.Getenv("TEST_CMD"); path != "" {
				if raw, err := os.ReadFile(path); err == nil {
					_ = os.Remove(path)
					for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
						kind, text, _ := strings.Cut(line, " ")
						if kind == "err" {
							fmt.Fprintln(os.Stderr, text)
						} else {
							fmt.Println(text)
						}
					}
				}
			}
			time.Sleep(20 * time.Millisecond)
		}
	}()
	for range term {
		if mode == "stubborn" {
			fmt.Fprintln(os.Stderr, "SIGTERM пропущен")
			continue
		}
		event("stop")
		os.Exit(0)
	}
}

// eventually — дождаться условия (не дольше 10 с).
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// readEvents — строки файла событий по порядку.
func readEvents(path string) []string {
	raw, _ := os.ReadFile(path)
	text := strings.TrimSpace(string(raw))
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}
