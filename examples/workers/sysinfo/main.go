//go:build unix

// Эталонный воркер на Go — на SDK воркеров (sdk/go/worker); формат сообщений
// воркера без SDK описан в sdk/spec §10 и sdk/README.md. Показывает всё, что воркер может
// объявить, кроме очередей:
//
//   - команда example.sys.info — сведения о процессе и баннер;
//   - команда example.sys.count {to, delaySeconds} — счёт с выводом потоком
//     (проверка вывода, срока и отмены);
//   - домен example.sys.banner {text} — желаемое состояние в памяти: после
//     перезапуска агент присылает снимок заново;
//   - канал телеметрии example.sys — горутины, память, аптайм раз в 5 с;
//   - событие sys.started после регистрации;
//   - уборка (agent cleanup при удалении агента) — сброс баннера.
//
// Запускает агент (workers). Сборка под эту машину (Go — в контейнере):
//
//	scripts/go.sh build "" "" sysinfo ./examples/workers/sysinfo
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	goruntime "runtime"
	"sync"
	"time"

	"github.com/epifanovmd/agent/sdk/go/worker"
)

const version = "1.0.0"

type sysinfo struct {
	started time.Time

	mu      sync.Mutex
	banner  string
	applied int64
}

func main() {
	s := &sysinfo{started: time.Now()}
	w := worker.New("sysinfo", version)
	w.Command("example.sys.info", s.info)
	w.Command("example.sys.count", s.count)
	w.State("example.sys.banner", s.applyBanner)
	// Уборка перед удалением агента: снять то, что воркер поставил на узле.
	// Здесь баннер только в памяти — сбросить; настоящий воркер удалил бы свои
	// файлы, правила, интерфейсы.
	w.Cleanup(s.cleanup)
	// Опрос телеметрии стартует после worker.ready: первый опрос — и событие
	// о запуске (регистрация принята).
	var started sync.Once
	w.Telemetry("example.sys", 5*time.Second, func() any {
		started.Do(func() {
			_ = w.Event("sys.started", map[string]any{"pid": os.Getpid(), "go": goruntime.Version()})
		})
		return s.telemetry()
	})

	// SIGTERM — SDK дорабатывает команды и выходит; состояние на узле (здесь —
	// в памяти) подхватит следующий запуск.
	if err := w.Run(context.Background()); err != nil {
		fmt.Fprintln(os.Stderr, "sysinfo:", err)
		os.Exit(2)
	}
}

func (s *sysinfo) info(context.Context, *worker.Command) (any, error) {
	host, _ := os.Hostname()
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]any{
		"hostname": host, "pid": os.Getpid(), "go": goruntime.Version(),
		"os": goruntime.GOOS, "arch": goruntime.GOARCH, "cpus": goruntime.NumCPU(),
		"uptimeSec": int(time.Since(s.started).Seconds()),
		"banner":    s.banner, "bannerVersion": s.applied,
	}, nil
}

func (s *sysinfo) count(ctx context.Context, cmd *worker.Command) (any, error) {
	var args struct {
		To           int     `json:"to"`
		DelaySeconds float64 `json:"delaySeconds"`
	}
	_ = json.Unmarshal(cmd.Args, &args)
	if args.To <= 0 || args.To > 1000 {
		return nil, worker.CommandError("BAD_ARGS", "to — от 1 до 1000")
	}
	delay := time.Duration(args.DelaySeconds * float64(time.Second))
	for i := 1; i <= args.To; i++ {
		select {
		case <-ctx.Done():
			// Срок истёк (cmd.cancel): итог агенту уже не нужен, SDK его не шлёт.
			return nil, ctx.Err()
		case <-time.After(delay):
		}
		fmt.Fprintf(cmd, "%d\n", i)
	}
	return map[string]int{"counted": args.To}, nil
}

func (s *sysinfo) applyBanner(_ context.Context, version int64, raw json.RawMessage) (any, error) {
	var spec struct {
		Text string `json:"text"`
	}
	if err := json.Unmarshal(raw, &spec); err != nil {
		return nil, fmt.Errorf("spec: %w", err)
	}
	if len(spec.Text) > 200 {
		return nil, fmt.Errorf("text — не длиннее 200 символов")
	}
	s.mu.Lock()
	s.banner, s.applied = spec.Text, version
	s.mu.Unlock()
	return map[string]int{"length": len(spec.Text)}, nil
}

func (s *sysinfo) cleanup(context.Context) error {
	s.mu.Lock()
	s.banner, s.applied = "", 0
	s.mu.Unlock()
	return nil
}

func (s *sysinfo) telemetry() any {
	var mem goruntime.MemStats
	goruntime.ReadMemStats(&mem)
	return map[string]any{
		"goroutines": goruntime.NumGoroutine(),
		"heapBytes":  mem.HeapAlloc,
		"uptimeSec":  int(time.Since(s.started).Seconds()),
	}
}
