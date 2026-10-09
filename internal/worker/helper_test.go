//go:build unix

package worker

import (
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strings"
	"syscall"
	"testing"
	"time"
)

// Тестовый воркер — этот же тестовый файл, запущенный с TEST_WORKER=режим:
//
//	ok        — HTTP на AGENT_WORKER_SOCKET: /health, /metrics, /env, PUT /config/{key}, POST /cleanup,
//	            GET /manifest — тело из TEST_MANIFEST (нет — {"version":"1.0.0"}, none — 404) или
//	            из файла TEST_MANIFEST_FILE (нет файла — 404);
//	nohealth  — как ok, но без GET /health (404);
//	crash     — сразу выход с кодом 1;
//	deaf      — сокет слушает, но не отвечает (GET /health без ответа);
//	unhealthy — GET /health → ok: false;
//	stubborn  — как ok, но SIGTERM не завершает;
//	children  — как ok и дочерний процесс sleep (pid — в TEST_CHILD_PID);
//	exit0     — как ok, но через 200 мс выход с кодом 0.
//
// В режиме ok: файл TEST_BUSY есть — GET /health отвечает busy: true; файл
// TEST_HANG содержит pid процесса — GET /health не отвечает; GET /pid — pid процесса;
// POST /print?text=…&stderr=1 — строка в stdout (stderr).
//
// TEST_EVENTS — файл, куда пишутся строки «start <pid>» и «stop <pid>».
func TestMain(m *testing.M) {
	if mode := os.Getenv("TEST_WORKER"); mode != "" {
		testWorker(mode)
		return
	}
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

func testWorker(mode string) {
	if mode == "crash" {
		fmt.Fprintln(os.Stderr, "падаю")
		os.Exit(1)
	}
	event("start")
	fmt.Println("воркер слушает сокет")
	term := make(chan os.Signal, 1)
	signal.Notify(term, syscall.SIGTERM)
	if mode == "children" {
		child := exec.Command("sleep", "60")
		child.Stdout, child.Stderr = nil, nil
		if err := child.Start(); err == nil {
			_ = os.WriteFile(os.Getenv("TEST_CHILD_PID"), []byte(fmt.Sprint(child.Process.Pid)), 0o600)
		}
	}
	ln, err := net.Listen("unix", os.Getenv("AGENT_WORKER_SOCKET"))
	if err != nil {
		os.Exit(3)
	}
	if mode == "exit0" {
		go func() {
			time.Sleep(200 * time.Millisecond)
			os.Exit(0)
		}()
	}
	exists := func(env string) bool {
		path := os.Getenv(env)
		if path == "" {
			return false
		}
		_, err := os.Stat(path)
		return err == nil
	}
	if mode == "deaf" {
		go func() {
			for {
				c, err := ln.Accept()
				if err != nil {
					return
				}
				go func() { _, _ = io.Copy(io.Discard, c) }()
			}
		}()
	} else {
		mux := http.NewServeMux()
		mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
			if raw, _ := os.ReadFile(os.Getenv("TEST_HANG")); strings.TrimSpace(string(raw)) == fmt.Sprint(os.Getpid()) {
				<-r.Context().Done()
				return
			}
			if mode == "nohealth" {
				http.NotFound(w, r)
				return
			}
			ok := mode != "unhealthy"
			fmt.Fprintf(w, `{"ok":%v,"busy":%v,"message":"%s","info":{"version":"%s"}}`, ok, exists("TEST_BUSY"), mode, os.Getenv("TEST_VERSION"))
		})
		mux.HandleFunc("GET /pid", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, os.Getpid()) })
		mux.HandleFunc("POST /print", func(w http.ResponseWriter, r *http.Request) {
			out := os.Stdout
			if r.URL.Query().Get("stderr") != "" {
				out = os.Stderr
			}
			fmt.Fprintln(out, r.URL.Query().Get("text"))
			w.WriteHeader(http.StatusNoContent)
		})
		mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) { fmt.Fprint(w, `{"n":1}`) })
		manifest := os.Getenv("TEST_MANIFEST")
		if manifest == "" {
			manifest = `{"version":"1.0.0"}`
		}
		mux.HandleFunc("GET /manifest", func(w http.ResponseWriter, r *http.Request) {
			body := manifest
			if file := os.Getenv("TEST_MANIFEST_FILE"); file != "" {
				raw, err := os.ReadFile(file)
				if err != nil {
					http.NotFound(w, r)
					return
				}
				body = string(raw)
			}
			if body == "none" {
				http.NotFound(w, r)
				return
			}
			fmt.Fprint(w, body)
		})
		mux.HandleFunc("GET /env", func(w http.ResponseWriter, _ *http.Request) {
			env := map[string]string{}
			for _, kv := range os.Environ() {
				k, v, _ := strings.Cut(kv, "=")
				env[k] = v
			}
			_ = json.NewEncoder(w).Encode(env)
		})
		mux.HandleFunc("POST /cleanup", func(w http.ResponseWriter, _ *http.Request) {
			event("cleanup")
			w.WriteHeader(http.StatusNoContent)
		})
		go func() { _ = http.Serve(ln, mux) }()
	}
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
