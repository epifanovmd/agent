//go:build unix

package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/jobs"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// CleanupResult — итог уборки одного воркера; Err == nil — убрано.
type CleanupResult struct {
	Worker string
	Err    error
}

// Cleanup — уборка при удалении агента с узла (`agent cleanup`, без связи с
// сервером): каждый воркер конфигурации по очереди запускается одной копией,
// после регистрации получает worker.cleanup, ответ worker.cleaned ждётся до
// его stopTimeout, затем воркер останавливается. До worker.cleanup воркер
// получает контекст wctx с mode cleanup.
func Cleanup(ctx context.Context, specs []config.Worker, log *slog.Logger, wctx message.WorkerContext) []CleanupResult {
	sup := New(nil, jobs.New(noSender{}, log, func() {}), log, func() {}, wctx.Agent.Version)
	sup.cleaning = true
	wctx.Mode = message.WorkerModeCleanup
	sup.SetContext(wctx)
	results := make([]CleanupResult, 0, len(specs))
	for _, spec := range specs {
		spec.Replicas = 1
		// Уборка идёт вне службы агента (из install.sh): группы cgroup
		// службы нет — без ограничений.
		spec.Limits = config.Limits{}
		err := sup.cleanupOne(ctx, &worker{spec: spec})
		if err != nil {
			log.Error("уборка воркера не удалась", "worker", spec.Name, "err", err)
		} else {
			log.Info("воркер убрал за собой", "worker", spec.Name)
		}
		results = append(results, CleanupResult{Worker: spec.Name, Err: err})
	}
	return results
}

func (s *Supervisor) cleanupOne(ctx context.Context, w *worker) error {
	timeout := w.spec.StopTimeout.Std()
	inst, err := startInstance(s, w, w.spec.Name+"#cleanup")
	if err != nil {
		return err
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		inst.terminate(stopCtx)
	}()
	select {
	case <-inst.ready:
	case <-inst.exited:
		return fmt.Errorf("завершился до регистрации: %s", inst.exitReason())
	case <-ctx.Done():
		return ctx.Err()
	}
	callCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	env := message.MustNew(message.TypeWorkerCleanup, struct{}{})
	env.ID = message.NewID()
	reply, err := inst.await(callCtx, "cleanup:"+env.ID, env, nil)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
			return fmt.Errorf("нет ответа на worker.cleanup за %s", timeout)
		}
		return err
	}
	var cleaned message.WorkerCleaned
	if err := reply.Decode(&cleaned); err != nil {
		return err
	}
	if !cleaned.OK {
		if cleaned.Error == "" {
			cleaned.Error = "уборка не удалась"
		}
		return errors.New(cleaned.Error)
	}
	return nil
}

// noSender — сервера при уборке нет: задач воркеру не выдаётся.
type noSender struct{}

func (noSender) Stream(string, any)         {}
func (noSender) Reliable(string, any) error { return nil }
func (noSender) Request(context.Context, string, any, any) error {
	return errors.New("нет связи с сервером")
}
