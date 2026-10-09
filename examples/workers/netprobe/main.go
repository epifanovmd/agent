//go:build unix

// Воркер netprobe — пример воркера на Go без SDK: HTTP-сервис на unix-сокете
// AGENT_WORKER_SOCKET (sdk/spec §12). Проверяет связность узла с целями:
// потери и время ответа (RTT) по ICMP или TCP.
//
//   - PUT /config/targets {version, data: {targets: [{id, host, port?, method}], intervalSec, count, timeoutMs}} —
//     цели этого узла; неверные — 422 {message};
//   - DELETE /config/targets — целей нет;
//   - GET /metrics — итог последнего круга {at, results: […]};
//   - GET /health — {ok, info: {targets}};
//   - POST /jobs {type: "netprobe.run", jobId, data: {targets?, count?, timeoutMs?}} — задача
//     (sdk/spec §12, быстрая): проверка сейчас, итог сразу — 200 {result: {at, results}};
//   - GET /manifest — что воркер умеет: версия, ключ targets со схемой, задача netprobe.run.
//
// ICMP без прав root: «ping»-сокет (SOCK_DGRAM, IPPROTO_ICMP; на Linux — если группа процесса
// входит в net.ipv4.ping_group_range), иначе системная команда ping, иначе TCP до порта цели.
//
// Сборка под эту машину (Go — в контейнере):
//
//	scripts/go.sh build "" "" netprobe ./examples/workers/netprobe
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
	"sync"
	"syscall"
	"time"
)

// configKey — ключ настроек воркера.
const configKey = "targets"

func main() {
	socket := os.Getenv("AGENT_WORKER_SOCKET")
	if socket == "" {
		fmt.Fprintln(os.Stderr, "netprobe: нет AGENT_WORKER_SOCKET — воркер запускает агент")
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	np := &netprobe{prober: newProber(newMethods()), changed: make(chan struct{}, 1)}
	np.report = np.setLast
	go np.loop(ctx)

	_ = os.Remove(socket)
	ln, err := net.Listen("unix", socket)
	if err != nil {
		fmt.Fprintln(os.Stderr, "netprobe:", err)
		os.Exit(2)
	}
	srv := &http.Server{Handler: np.handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		fmt.Fprintln(os.Stderr, "netprobe:", err)
		os.Exit(2)
	}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func reject(w http.ResponseWriter, status int, err error) {
	writeJSON(w, status, map[string]string{"message": err.Error()})
}

// handler — маршруты воркера.
func (n *netprobe) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /config/{key}", func(w http.ResponseWriter, r *http.Request) {
		if r.PathValue("key") != configKey {
			reject(w, http.StatusNotFound, fmt.Errorf("ключ настроек — %s", configKey))
			return
		}
		var body struct {
			Version int64           `json:"version"`
			Data    json.RawMessage `json:"data"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&body); err != nil {
			reject(w, http.StatusBadRequest, err)
			return
		}
		if err := n.apply(body.Data); err != nil {
			reject(w, http.StatusUnprocessableEntity, err)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("DELETE /config/{key}", func(w http.ResponseWriter, r *http.Request) {
		n.mu.Lock()
		n.spec = nil
		n.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	})
	mux.HandleFunc("GET /metrics", func(w http.ResponseWriter, _ *http.Request) {
		n.mu.Lock()
		last := n.last
		n.mu.Unlock()
		if last == nil {
			writeJSON(w, http.StatusOK, map[string]any{})
			return
		}
		writeJSON(w, http.StatusOK, last)
	})
	mux.HandleFunc("GET /manifest", func(w http.ResponseWriter, _ *http.Request) {
		writeJSON(w, http.StatusOK, manifest)
	})
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, _ *http.Request) {
		targets := 0
		if s := n.current(); s != nil {
			targets = len(s.Targets)
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "info": map[string]int{"targets": targets}})
	})
	mux.HandleFunc("POST /jobs", func(w http.ResponseWriter, r *http.Request) {
		var job struct {
			Type string          `json:"type"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&job); err != nil {
			reject(w, http.StatusBadRequest, err)
			return
		}
		if job.Type != jobRun {
			reject(w, http.StatusBadRequest, fmt.Errorf("задачи %q нет: есть %s", job.Type, jobRun))
			return
		}
		rep, err := n.run(r.Context(), job.Data)
		if err != nil {
			reject(w, http.StatusBadRequest, err)
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"result": rep})
	})
	return mux
}

// netprobe — текущие цели (настройки targets), итог последнего круга и цикл проверок.
type netprobe struct {
	prober  *prober
	report  func(Report)
	changed chan struct{} // новые цели: начать круг сразу

	mu   sync.Mutex
	spec *Spec // nil — целей ещё нет
	last *Report
}

func (n *netprobe) setLast(r Report) {
	n.mu.Lock()
	n.last = &r
	n.mu.Unlock()
}

// apply — новые настройки: проверить, запомнить, начать новый круг.
func (n *netprobe) apply(raw json.RawMessage) error {
	spec, err := parseSpec(raw)
	if err != nil {
		return err
	}
	n.mu.Lock()
	n.spec = &spec
	n.mu.Unlock()
	select {
	case n.changed <- struct{}{}:
	default:
	}
	return nil
}

func (n *netprobe) current() *Spec {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.spec
}

// loop — круг проверок раз в intervalSec (круг дольше интервала — следующий сразу после него).
func (n *netprobe) loop(ctx context.Context) {
	var next <-chan time.Time
	for {
		select {
		case <-ctx.Done():
			return
		case <-n.changed:
		case <-next:
		}
		spec := n.current()
		if spec == nil {
			next = nil
			continue
		}
		start := time.Now()
		rep := n.prober.round(ctx, spec.Targets, spec.Count, spec.timeout())
		if ctx.Err() != nil {
			return
		}
		n.report(rep)
		wait := time.Duration(spec.IntervalSec)*time.Second - time.Since(start)
		next = time.After(max(wait, 0))
	}
}

// run — задача netprobe.run: проверка сейчас. Без targets — цели из настроек (итог становится и
// последним для GET /metrics), с targets — разовая проверка этих целей (count и timeoutMs — из
// тела или настроек).
func (n *netprobe) run(ctx context.Context, raw []byte) (Report, error) {
	var args struct {
		Targets   json.RawMessage `json:"targets"`
		Count     int             `json:"count"`
		TimeoutMs int             `json:"timeoutMs"`
	}
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &args); err != nil {
			return Report{}, fmt.Errorf("тело: %w", err)
		}
	}
	base := Spec{Count: defaultCount, TimeoutMs: defaultTimeoutMs, IntervalSec: defaultIntervalSec}
	if cur := n.current(); cur != nil {
		base = *cur
	}
	own := len(args.Targets) == 0 || string(args.Targets) == "null"
	if !own {
		base.Targets = nil
		if err := json.Unmarshal(args.Targets, &base.Targets); err != nil {
			return Report{}, fmt.Errorf("targets: %w", err)
		}
	}
	if args.Count != 0 {
		base.Count = args.Count
	}
	if args.TimeoutMs != 0 {
		base.TimeoutMs = args.TimeoutMs
	}
	if err := base.normalize(); err != nil {
		return Report{}, err
	}
	rep := n.prober.round(ctx, base.Targets, base.Count, base.timeout())
	if ctx.Err() != nil {
		return Report{}, ctx.Err()
	}
	if own {
		n.report(rep)
	}
	return rep, nil
}
