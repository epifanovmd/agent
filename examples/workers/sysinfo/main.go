//go:build unix

// Пример воркера на Go без SDK: HTTP-сервис на unix-сокете AGENT_WORKER_SOCKET
// (sdk/spec §12).
//
//   - GET /info — сведения о процессе и баннер (запрос сервера fetch);
//   - PUT /config/banner {version, data: {text}} — баннер; DELETE /config/banner — сброс;
//   - GET /metrics — горутины, память, аптайм;
//   - GET /health — {ok, info: {version}};
//   - POST /cleanup — уборка перед удалением агента: сброс баннера;
//   - GET /manifest — что воркер умеет: версия, ключ banner со схемой, маршрут, событие;
//   - событие sys.started через сокет агента после запуска.
//
// Запускает агент (workers). Сборка под эту машину (Go — в контейнере):
//
//	scripts/go.sh build "" "" sysinfo ./examples/workers/sysinfo
package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	goruntime "runtime"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"
)

const version = "1.0.0"

// maxBanner — длина баннера, символов.
const maxBanner = 200

// manifest — самоописание воркера (sdk/spec §12).
var manifest = map[string]any{
	"version":     version,
	"description": "Сведения о процессе воркера и баннер",
	"configs": []any{map[string]any{
		"key":         "banner",
		"description": "Текст баннера в ответе GET /info",
		"schema": map[string]any{
			"type":                 "object",
			"properties":           map[string]any{"text": map[string]any{"type": "string", "maxLength": maxBanner}},
			"additionalProperties": false,
		},
	}},
	"routes": []any{map[string]any{"method": "GET", "path": "/info", "description": "Процесс, узел и баннер"}},
	"events": []any{map[string]any{"type": "sys.started", "description": "Воркер запущен"}},
}

type sysinfo struct {
	started time.Time
	mu      sync.Mutex
	banner  string
	applied int64
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (s *sysinfo) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /info", func(w http.ResponseWriter, _ *http.Request) {
		s.mu.Lock()
		defer s.mu.Unlock()
		host, _ := os.Hostname()
		writeJSON(w, http.StatusOK, map[string]any{
			"pid": os.Getpid(), "host": host, "go": goruntime.Version(), "version": version,
			"banner": s.banner, "bannerVersion": s.applied,
		})
	})
	mux.HandleFunc("PUT /config/banner", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Version int64 `json:"version"`
			Data    struct {
				Text string `json:"text"`
			} `json:"data"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"message": err.Error()})
			return
		}
		if utf8.RuneCountInString(body.Data.Text) > maxBanner {
			writeJSON(w, http.StatusUnprocessableEntity, map[string]string{"message": fmt.Sprintf("text: не длиннее %d символов", maxBanner)})
			return
		}
		s.mu.Lock()
		s.banner, s.applied = body.Data.Text, body.Version
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /config/banner", func(w http.ResponseWriter, _ *http.Request) {
		s.reset()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		var m goruntime.MemStats
		goruntime.ReadMemStats(&m)
		writeJSON(w, http.StatusOK, map[string]any{
			"goroutines": goruntime.NumGoroutine(), "heapBytes": m.HeapAlloc, "uptimeSec": int(time.Since(s.started).Seconds()),
		})
	})
	mux.HandleFunc("GET /manifest", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, manifest)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "info": map[string]string{"version": version}})
	})
	mux.HandleFunc("POST /cleanup", func(w http.ResponseWriter, _ *http.Request) {
		s.reset()
		w.WriteHeader(http.StatusNoContent)
	})
	return mux
}

func (s *sysinfo) reset() {
	s.mu.Lock()
	s.banner, s.applied = "", 0
	s.mu.Unlock()
}

// event — событие серверу через сокет агента (POST /events).
func event(typ string, data any) error {
	agent := &http.Client{Timeout: 5 * time.Second, Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", os.Getenv("AGENT_SOCKET"))
		}}}
	body, _ := json.Marshal(map[string]any{"type": typ, "data": data})
	req, _ := http.NewRequest(http.MethodPost, "http://agent/events", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer "+os.Getenv("AGENT_WORKER_TOKEN"))
	req.Header.Set("Content-Type", "application/json")
	resp, err := agent.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusAccepted {
		return fmt.Errorf("POST /events: HTTP %d", resp.StatusCode)
	}
	return nil
}

func main() {
	socket := os.Getenv("AGENT_WORKER_SOCKET")
	if socket == "" {
		fmt.Fprintln(os.Stderr, "sysinfo: нет AGENT_WORKER_SOCKET — воркер запускает агент")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	s := &sysinfo{started: time.Now()}
	_ = os.Remove(socket)
	ln, err := net.Listen("unix", socket)
	if err != nil {
		fmt.Fprintln(os.Stderr, "sysinfo:", err)
		os.Exit(2)
	}
	srv := &http.Server{Handler: s.handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	go func() {
		if err := event("sys.started", map[string]any{"pid": os.Getpid(), "version": version}); err != nil {
			fmt.Fprintln(os.Stderr, "sysinfo: событие не отправлено:", err)
		}
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, "sysinfo:", err)
		os.Exit(2)
	}
}
