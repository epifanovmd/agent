//go:build unix

// Пример воркера на Go с клиентом github.com/epifanovmd/agent/worker (docs/PROTOCOL.md, §11–§17):
//
//   - настройка banner {text} — текст в итоге команды sys.info;
//   - команда sys.info — процесс, узел, версия, баннер;
//   - событие sys.started после запуска;
//   - метрики процесса (горутины, память, время работы) — в пульсе.
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
	"os"
	"os/signal"
	goruntime "runtime"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/epifanovmd/agent/protocol"
	"github.com/epifanovmd/agent/worker"
)

// version — версия воркера; сборка может задать свою: -ldflags "-X main.version=…".
var version = "1.0.0"

// maxBanner — длина баннера, символов.
const maxBanner = 200

func schema(raw string) json.RawMessage { return json.RawMessage(raw) }

// manifest — что воркер умеет (§13).
var manifest = protocol.WorkerManifest{
	Description: "Сведения о процессе воркера и баннер",
	Configs: []protocol.ManifestConfig{{Key: "banner", Description: "Текст баннера в итоге sys.info",
		Schema: schema(fmt.Sprintf(`{"type":"object","properties":{"text":{"type":"string","maxLength":%d}},"additionalProperties":false}`, maxBanner))}},
	Commands: []protocol.ManifestCommand{{Type: "sys.info", Description: "Процесс, узел и баннер",
		Schema: schema(`{"type":"object"}`),
		Result: schema(`{"type":"object","properties":{"pid":{"type":"integer"},"host":{"type":"string"},"version":{"type":"string"},"banner":{"type":"string"}}}`)}},
	Events: []protocol.ManifestEvent{{Type: "sys.started", Description: "Воркер запущен", Schema: schema(`{"type":"object"}`)}},
}

type sysinfo struct {
	started time.Time
	mu      sync.Mutex
	banner  string
}

// configure — ключ banner: {text} не длиннее maxBanner; удалён — пустой баннер.
func (s *sysinfo) configure(_ context.Context, key string, data json.RawMessage) error {
	if key != "banner" {
		return nil
	}
	var v struct {
		Text string `json:"text"`
	}
	if data != nil {
		if err := json.Unmarshal(data, &v); err != nil {
			return protocol.NewError(protocol.CodeConfigRejected, "banner: нужен объект {text}")
		}
		if utf8.RuneCountInString(v.Text) > maxBanner {
			return protocol.NewError(protocol.CodeConfigRejected, fmt.Sprintf("banner.text: не длиннее %d символов", maxBanner))
		}
	}
	s.mu.Lock()
	s.banner = v.Text
	s.mu.Unlock()
	return nil
}

func (s *sysinfo) info(context.Context, *worker.Call) (any, error) {
	host, _ := os.Hostname()
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{"pid": os.Getpid(), "host": host, "version": version, "go": goruntime.Version(), "banner": s.banner}, nil
}

func (s *sysinfo) health() protocol.Health {
	var mem goruntime.MemStats
	goruntime.ReadMemStats(&mem)
	metrics, _ := json.Marshal(map[string]any{
		"goroutines": goruntime.NumGoroutine(), "heapBytes": mem.HeapAlloc,
		"uptimeSec": int(time.Since(s.started).Seconds()),
	})
	return protocol.Health{OK: true, Metrics: metrics}
}

func main() {
	s := &sysinfo{started: time.Now()}
	w, err := worker.New(worker.Options{
		Version: version, Manifest: manifest, Configure: s.configure, Health: s.health,
		Commands: map[string]worker.CommandFunc{"sys.info": s.info},
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, "sysinfo:", err)
		os.Exit(1)
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM, syscall.SIGINT)
	defer stop()
	go func() {
		// Событие — когда агент примет (он может ещё запускаться).
		for ctx.Err() == nil {
			err := w.Emit(ctx, "sys.started", map[string]any{"pid": os.Getpid(), "version": version})
			var ei *protocol.ErrorInfo
			if err == nil || errors.As(err, &ei) && ei.Code == protocol.CodeEventUndeclared {
				return
			}
			time.Sleep(time.Second)
		}
	}()
	fmt.Println("sysinfo", version, "запущен")
	if err := w.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, "sysinfo:", err)
		os.Exit(1)
	}
}
