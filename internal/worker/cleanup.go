//go:build unix

package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
)

// CleanupResult — итог уборки одного воркера; Err == nil — убрано.
type CleanupResult struct {
	Worker string
	Err    error
}

// Cleanup — уборка перед удалением агента (`agent uninstall`, §13): каждый
// воркер по очереди подхватывается (если работает после остановки агента)
// или запускается, получает POST /cleanup (срок — stopTimeout) и
// останавливается. Процессы воркеров, которых нет в настройках,
// останавливаются. Токены воркеров — у Supervisor s (сокет агента на это
// время тоже работает).
func (s *Supervisor) Cleanup(ctx context.Context) []CleanupResult {
	known := map[string]bool{}
	for _, name := range s.Names() {
		known[name] = true
	}
	s.stopOrphans(known)
	s.wg.Wait()
	var out []CleanupResult
	for _, name := range s.Names() {
		w := s.get(name)
		w.mu.Lock()
		spec := w.spec
		w.mu.Unlock()
		err := w.cleanup(ctx, spec)
		if err != nil {
			s.opts.Log.Error("уборка воркера не удалась", "worker", name, "err", err)
		} else {
			s.opts.Log.Info("воркер убрал за собой", "worker", name)
		}
		out = append(out, CleanupResult{Worker: name, Err: err})
	}
	return out
}

func (w *Worker) cleanup(ctx context.Context, spec config.Worker) error {
	p, _ := w.adopt(ctx, spec)
	if p == nil {
		var err error
		if p, err = w.start(ctx, spec); err != nil {
			return err
		}
	}
	defer func() {
		p.stop()
		removeState(w.sup.stateDir, w.name)
	}()
	stopTimeout := spec.Lifecycle.StopTimeout.Std()
	cctx, cancel := context.WithTimeout(ctx, stopTimeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(cctx, http.MethodPost, "http://worker"+message.CleanupPath, nil)
	resp, err := p.client.Do(req)
	if err != nil {
		if cctx.Err() != nil && ctx.Err() == nil {
			return fmt.Errorf("нет ответа на POST /cleanup за %s", stopTimeout)
		}
		return err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
	switch {
	case resp.StatusCode == http.StatusNotFound:
		return nil // убирать нечего: воркер уборку не поддерживает
	case resp.StatusCode/100 != 2:
		return fmt.Errorf("POST /cleanup: HTTP %d %s", resp.StatusCode, ErrorText(body))
	}
	return nil
}

// ErrorText — текст ошибки из ответа воркера: поле message JSON или тело
// как есть (до 1000 символов).
func ErrorText(body []byte) string {
	var e message.WorkerError
	if json.Unmarshal(body, &e) == nil && e.Message != "" {
		return e.Message
	}
	text := strings.TrimSpace(string(body))
	if r := []rune(text); len(r) > 1000 {
		text = string(r[:1000]) + "…"
	}
	return text
}

// StopProcesses — `agent stop-workers`: остановить процессы воркеров,
// оставшиеся работать после остановки агента (файлы dataDir/processes):
// SIGTERM, не вышел за stopTimeout — SIGKILL. Итог — имена остановленных.
func StopProcesses(dataDir string) []string {
	dir := filepath.Join(dataDir, ProcessesDir)
	var out []string
	for _, name := range stateNames(dir) {
		if st, ok := readState(dir, name); ok && stopOrphan(st) {
			out = append(out, name)
		}
		removeState(dir, name)
	}
	return out
}
