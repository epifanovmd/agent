// Package sysmetrics — встроенный воркер агента: метрики узла (§9). Агент
// запускает его сам — своей же программой в режиме `agent sysmetrics` — как
// обычный воркер: HTTP на unix-сокете AGENT_WORKER_SOCKET, GET /metrics →
// message.HostMetrics (агент кладёт ответ в metrics.host), GET /health и
// GET /manifest (обязательный минимум воркера, версия — версия агента).
// Какие группы собирать — переменная EnvSettings (JSON Settings) из
// настроек агента telemetry. Сбор идёт в отдельном процессе: сбой или
// зависание чтения системы не задевает агента.
package sysmetrics

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/signal"
	"slices"
	"sync"
	"syscall"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

// Служебные режимы программы агента.
const (
	// Command — `agent sysmetrics`: работа воркера.
	Command = "sysmetrics"
	// SensorsCommand — `agent sensors`: показания датчиков (JSON) и выход; на
	// macOS датчики читаются только в этом отдельном процессе.
	SensorsCommand = "sensors"
)

// EnvSettings — переменная окружения воркера с настройками сбора (JSON Settings).
const EnvSettings = "AGENT_SYSMETRICS"

// Settings — что собирать: группы (message.MetricGroups), точки
// монтирования для группы disk (["all"] — все), исключённые интерфейсы.
type Settings struct {
	Metrics           []string `json:"metrics"`
	Disks             []string `json:"disks,omitempty"`
	ExcludeInterfaces []string `json:"excludeInterfaces,omitempty"`
}

// Dispatch — служебные режимы программы агента: args[0] — Command или
// SensorsCommand. false — это не они.
func Dispatch(args []string) (bool, error) {
	if len(args) == 0 {
		return false, nil
	}
	switch args[0] {
	case Command:
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
		defer stop()
		return true, Run(ctx)
	case SensorsCommand:
		return true, PrintSensors(context.Background())
	}
	return false, nil
}

// Run — воркер sysmetrics до отмены ctx: сокет — AGENT_WORKER_SOCKET,
// настройки — EnvSettings.
func Run(ctx context.Context) error {
	var set Settings
	if raw := os.Getenv(EnvSettings); raw != "" {
		if err := json.Unmarshal([]byte(raw), &set); err != nil {
			return fmt.Errorf("sysmetrics: %s: %w", EnvSettings, err)
		}
	}
	socket := os.Getenv(message.EnvWorkerSocket)
	if socket == "" {
		return errors.New("sysmetrics: нет " + message.EnvWorkerSocket + " — воркер запускает агент")
	}
	_ = os.Remove(socket)
	ln, err := net.Listen("unix", socket)
	if err != nil {
		return fmt.Errorf("sysmetrics: %w", err)
	}
	srv := &http.Server{Handler: Handler(newCollector(), set, os.Getenv(message.EnvVersion)), ReadHeaderTimeout: 5 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	if err := srv.Serve(ln); !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

// Handler — HTTP воркера: GET /metrics — метрики по set, GET /health,
// GET /manifest с версией агента version.
func Handler(c *collector, set Settings, version string) http.Handler {
	groups := slices.DeleteFunc(slices.Clone(set.Metrics), func(g string) bool { return !slices.Contains(message.MetricGroups, g) })
	var mu sync.Mutex
	mux := http.NewServeMux()
	mux.HandleFunc("GET "+message.MetricsPath, func(w http.ResponseWriter, r *http.Request) {
		// Скорости — по разнице с прошлым сбором: сборы идут по одному.
		mu.Lock()
		h := c.host(r.Context(), groups, set.Disks, set.ExcludeInterfaces)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(h)
	})
	mux.HandleFunc("GET "+message.HealthPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(message.Health{OK: true})
	})
	manifest := message.WorkerManifest{Version: cmp.Or(version, "dev"), Description: "Метрики узла"}
	mux.HandleFunc("GET "+message.WorkerManifestPath, func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(manifest)
	})
	return mux
}
