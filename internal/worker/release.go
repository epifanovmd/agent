//go:build unix

package worker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/fsutil"
	"github.com/epifanovmd/agent/protocol"
)

// Update — worker.update (§10): fetch кладёт проверенную сборку в dst
// (файл или каталог распакованного архива); воркер заменяется (сначала
// уходит прежний процесс), текущая сборка откладывается как previous, новая
// становится current. Новая не зарегистрировалась с пульсом ok: true за
// startTimeout — previous возвращается, воркер перезапускается с ней, итог —
// UPDATE_FAILED. Без force замена ждёт, пока воркер занят (§16). Ошибки —
// *protocol.ErrorInfo.
func (s *Supervisor) Update(ctx context.Context, name, version string, force bool, fetch func(dst string) error) (protocol.UpdateResult, error) {
	var res protocol.UpdateResult
	w := s.get(name)
	if w == nil {
		return res, protocol.NewError(protocol.CodeWorkerUnknown, fmt.Sprintf("воркера %q нет в настройках агента", name))
	}
	w.mu.Lock()
	spec := w.spec
	switch {
	case !spec.Release:
		w.mu.Unlock()
		return res, protocol.NewError(protocol.CodeWorkerNotReleased, fmt.Sprintf("у воркера %q нет сборки с сервера (release: true не задан)", name))
	case w.updating:
		w.mu.Unlock()
		return res, protocol.NewError(protocol.CodeBusy, fmt.Sprintf("воркер %q уже обновляется", name))
	}
	w.updating = true
	w.mu.Unlock()
	defer func() {
		w.mu.Lock()
		w.updating = false
		w.mu.Unlock()
	}()
	failed := func(format string, args ...any) error {
		return protocol.NewError(protocol.CodeUpdateFailed, fmt.Sprintf(format, args...))
	}

	dir := spec.ReleaseDir
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return res, failed("каталог сборки: %v", err)
	}
	current := filepath.Join(dir, config.ReleaseCurrent)
	next := current + ".new"
	_ = os.RemoveAll(next)
	if err := fetch(next); err != nil {
		_ = os.RemoveAll(next)
		var ei *protocol.ErrorInfo
		if errors.As(err, &ei) {
			return res, err
		}
		return res, failed("%v", err)
	}
	res.Version = version
	res.Previous = releaseVersion(spec)
	_, statErr := os.Stat(current)
	hadCurrent := statErr == nil
	log := s.opts.Log.With("worker", name)

	install := func() error {
		if hadCurrent {
			previous := filepath.Join(dir, config.ReleasePrevious)
			if err := os.RemoveAll(previous); err != nil {
				return err
			}
			if err := os.Rename(current, previous); err != nil {
				return err
			}
			if err := writeFileAtomic(filepath.Join(dir, config.ReleasePreviousVersion), res.Previous); err != nil {
				return err
			}
		}
		if err := os.Rename(next, current); err != nil {
			return err
		}
		return writeFileAtomic(filepath.Join(dir, config.ReleaseVersion), version)
	}
	rollback := func() error {
		if !hadCurrent {
			_ = os.RemoveAll(current)
			_ = os.Remove(filepath.Join(dir, config.ReleaseVersion))
			return nil
		}
		if err := os.RemoveAll(current); err != nil {
			return err
		}
		if err := os.Rename(filepath.Join(dir, config.ReleasePrevious), current); err != nil {
			return err
		}
		return writeFileAtomic(filepath.Join(dir, config.ReleaseVersion), res.Previous)
	}

	log.Info("воркер: замена на новую сборку", "version", version, "previous", res.Previous)
	var installErr error
	startErr := w.replace(ctx, func() error {
		installErr = install()
		return installErr
	}, protocol.PendingUpdate, force)
	if installErr != nil {
		_ = os.RemoveAll(next)
		return res, failed("замена сборки: %v", installErr)
	}
	cause := startErr
	if cause == nil {
		cause = s.waitHealthy(ctx, name, startTimeout)
	}
	if cause == nil {
		log.Info("воркер работает на новой сборке", "version", version)
		s.opts.OnChange()
		return res, nil
	}
	msg := cause.Error()
	var rbErr error
	if err := w.replace(context.WithoutCancel(ctx), func() error {
		rbErr = rollback()
		return rbErr
	}, protocol.PendingUpdate, true); err != nil && rbErr == nil {
		log.Error("воркер: прежняя сборка не запустилась", "err", err)
	}
	switch {
	case rbErr != nil:
		msg += "; прежняя сборка не возвращена: " + rbErr.Error()
	case hadCurrent:
		msg += "; возвращена прежняя сборка " + res.Previous
	default:
		msg += "; прежней сборки не было"
	}
	log.Error("воркер: обновление не удалось", "err", msg)
	s.opts.OnChange()
	return res, failed("%s", msg)
}

func writeFileAtomic(path, text string) error {
	return fsutil.WriteFile(path, []byte(text+"\n"), 0o644)
}
