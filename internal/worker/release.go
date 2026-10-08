//go:build unix

package worker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/epifanovmd/agent/internal/commands"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// updateWait — не меньше стольки ждать регистрации новой сборки
// (max(stopTimeout, updateWait)); переменная — для тестов.
var updateWait = 60 * time.Second

// Released — воркер name из выпуска (release: true).
func (s *Supervisor) Released(name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	w, ok := s.workers[name]
	return ok && w.spec.Release
}

// AnyReleased — есть хотя бы один воркер из выпуска.
func (s *Supervisor) AnyReleased() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, w := range s.workers {
		if w.spec.Release {
			return true
		}
	}
	return false
}

// Update — worker.update: fetch кладёт проверенную сборку в dst (рядом с
// current) — файл или каталог распакованного архива; текущая откладывается
// как previous, новая становится current (файл — атомарно, каталог —
// переименованием), воркер заменяется своим способом (rolling | stop-first) и
// должен зарегистрироваться с версией version не позже max(stopTimeout,
// updateWait). Иначе previous возвращается на место, воркер перезапускается
// с ней, итог — ошибка WORKER_UPDATE_FAILED. Ошибка fetch типа
// *commands.Error возвращается как есть.
func (s *Supervisor) Update(ctx context.Context, name, version string, fetch func(dst string) error, out io.Writer) (message.WorkerUpdateResult, error) {
	res := message.WorkerUpdateResult{Name: name, Version: version}
	s.mu.Lock()
	w, ok := s.workers[name]
	switch {
	case !ok || !w.spec.Release:
		s.mu.Unlock()
		return res, commands.Errorf(message.ErrWorkerNotReleased, "воркер %q не из выпуска (нет в настройках или release: true не задан)", name)
	case s.stopping:
		s.mu.Unlock()
		return res, commands.Errorf(message.ErrWorkerUpdateFailed, "агент останавливается")
	case w.updating:
		s.mu.Unlock()
		return res, commands.Errorf(message.ErrWorkerUpdateInProgress, "воркер %q уже обновляется", name)
	}
	w.updating = true
	spec := w.spec
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		w.updating = false
		s.mu.Unlock()
	}()
	failed := func(format string, args ...any) error {
		return commands.Errorf(message.ErrWorkerUpdateFailed, format, args...)
	}

	dir := spec.ReleaseDir
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return res, failed("каталог сборки: %v", err)
	}
	current := filepath.Join(dir, config.ReleaseCurrent)
	next := current + ".new"
	_ = os.RemoveAll(next) // остаток прерванного обновления
	fmt.Fprintf(out, "загрузка %s %s…\n", name, version)
	if err := fetch(next); err != nil {
		_ = os.RemoveAll(next)
		var ce *commands.Error
		if errors.As(err, &ce) {
			return res, err
		}
		return res, failed("%v", err)
	}

	res.Previous = readVersion(filepath.Join(dir, config.ReleaseVersion))
	_, statErr := os.Stat(current)
	hadCurrent := statErr == nil
	previous := filepath.Join(dir, config.ReleasePrevious)
	dirs := isDir(current) || isDir(next)
	if hadCurrent {
		var err error
		if dirs {
			// Каталог: текущая сборка переезжает в previous целиком.
			if err = os.RemoveAll(previous); err == nil {
				err = os.Rename(current, previous)
			}
		} else {
			err = keepAside(current, previous)
		}
		if err != nil {
			_ = os.RemoveAll(next)
			return res, failed("копия текущей сборки: %v", err)
		}
		if err := writeFileAtomic(filepath.Join(dir, config.ReleasePreviousVersion), res.Previous); err != nil {
			_ = os.RemoveAll(next)
			return res, failed("%v", err)
		}
	}
	if err := os.Rename(next, current); err != nil {
		_ = os.RemoveAll(next)
		if dirs && hadCurrent {
			_ = os.Rename(previous, current)
		}
		return res, failed("замена сборки: %v", err)
	}
	if err := writeFileAtomic(filepath.Join(dir, config.ReleaseVersion), version); err != nil {
		return res, s.rollback(w, spec, hadCurrent, res.Previous, fmt.Errorf("версия: %w", err), nil, 0)
	}
	s.mu.Lock()
	w.build++
	build := w.build
	slots := append([]*slot(nil), w.slots...)
	s.mu.Unlock()
	s.log.Info("воркер: новая сборка установлена — замена", "worker", name, "version", version, "previous", res.Previous)
	fmt.Fprintf(out, "сборка %s установлена, замена воркера (%s)…\n", version, restartMode(spec))

	if err := s.rollout(ctx, w, slots, spec, version); err != nil {
		fmt.Fprintf(out, "новая сборка не заработала: %v — возврат прежней\n", err)
		return res, s.rollback(w, spec, hadCurrent, res.Previous, err, slots, build)
	}
	fmt.Fprintf(out, "воркер %s работает на версии %s\n", name, version)
	s.changed()
	return res, nil
}

func restartMode(spec config.Worker) string {
	if spec.Restart == config.RestartStopFirst {
		return config.RestartStopFirst
	}
	return config.RestartRolling
}

// rollout — заменить экземпляры мест на новую сборку и проверить, что каждый
// зарегистрировался с версией version.
func (s *Supervisor) rollout(ctx context.Context, w *worker, slots []*slot, spec config.Worker, version string) error {
	wait := max(spec.StopTimeout.Std(), updateWait)
	if spec.Restart == config.RestartStopFirst {
		wait += spec.StopTimeout.Std() // старый сначала дорабатывает
	}
	rctx, cancel := context.WithTimeout(ctx, wait)
	defer cancel()
	if err := s.replace(rctx, w, slots); err != nil {
		if rctx.Err() != nil && ctx.Err() == nil {
			return fmt.Errorf("новая сборка не зарегистрировалась за %s", wait)
		}
		return err
	}
	for _, sl := range slots {
		s.mu.Lock()
		inst, removed := sl.current, sl.removed
		s.mu.Unlock()
		if removed {
			continue
		}
		if inst == nil || !inst.registered() {
			return errors.New("новая сборка не зарегистрировалась")
		}
		inst.mu.Lock()
		got := inst.version
		inst.mu.Unlock()
		if got != version {
			return fmt.Errorf("новая сборка зарегистрировалась с версией %q, а не %q", got, version)
		}
	}
	return nil
}

// rollback — вернуть прежнюю сборку (previous) и перезапустить экземпляры
// мест slots с новой (поколение failedBuild и новее). Итог —
// WORKER_UPDATE_FAILED с причиной.
func (s *Supervisor) rollback(w *worker, spec config.Worker, hadCurrent bool, previous string, cause error, slots []*slot, failedBuild int) error {
	dir := spec.ReleaseDir
	current := filepath.Join(dir, config.ReleaseCurrent)
	var errs []string
	if hadCurrent {
		prevBuild := filepath.Join(dir, config.ReleasePrevious)
		var err error
		if isDir(prevBuild) || isDir(current) {
			if err = os.RemoveAll(current); err == nil {
				err = os.Rename(prevBuild, current)
			}
		} else {
			err = keepAside(prevBuild, current)
		}
		if err != nil {
			errs = append(errs, "возврат сборки: "+err.Error())
		}
		if err := writeFileAtomic(filepath.Join(dir, config.ReleaseVersion), previous); err != nil {
			errs = append(errs, err.Error())
		}
	} else {
		_ = os.RemoveAll(current)
		_ = os.Remove(filepath.Join(dir, config.ReleaseVersion))
	}
	s.mu.Lock()
	w.build++
	s.mu.Unlock()
	if len(slots) > 0 {
		s.restore(w, spec, slots, failedBuild)
	}
	msg := cause.Error()
	switch {
	case len(errs) > 0:
		msg += "; прежняя сборка не возвращена: " + strings.Join(errs, "; ")
	case hadCurrent:
		msg += "; возвращена прежняя сборка " + previous
	default:
		msg += "; прежней сборки не было"
	}
	s.log.Error("воркер: обновление не удалось", "worker", spec.Name, "err", msg)
	s.changed()
	return commands.Errorf(message.ErrWorkerUpdateFailed, "%s", msg)
}

// restore — после отката: зарегистрированные экземпляры новой сборки
// заменяются (прежней), незарегистрированные останавливаются, и цикл места
// сразу поднимает прежнюю сборку.
func (s *Supervisor) restore(w *worker, spec config.Worker, slots []*slot, failedBuild int) {
	var replace []*slot
	for _, sl := range slots {
		s.mu.Lock()
		inst, removed := sl.current, sl.removed
		if !removed {
			sl.failures = 0
		}
		s.mu.Unlock()
		switch {
		case removed:
		case inst == nil:
		case inst.build < failedBuild:
			continue // прежняя сборка так и работает (rolling: замена не дошла)
		case inst.registered():
			replace = append(replace, sl)
			continue
		default:
			inst.retired.Store(true)
			go func() {
				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				inst.terminate(ctx)
			}()
		}
		select {
		case sl.wake <- struct{}{}:
		default:
		}
	}
	if len(replace) == 0 {
		return
	}
	s.mu.Lock()
	ctx := s.ctx
	s.mu.Unlock()
	if ctx == nil {
		ctx = context.Background()
	}
	ctx, cancel := context.WithTimeout(ctx, max(spec.StopTimeout.Std(), updateWait))
	defer cancel()
	if err := s.replace(ctx, w, replace); err != nil {
		s.log.Error("воркер: прежняя сборка не запустилась", "worker", spec.Name, "err", err)
	}
}

// isDir — путь есть и это каталог.
func isDir(path string) bool {
	info, err := os.Stat(path)
	return err == nil && info.IsDir()
}

// keepAside — dst становится копией src атомарно: жёсткая ссылка (или копия)
// во временный файл и rename.
func keepAside(src, dst string) error {
	tmp := dst + ".tmp"
	_ = os.Remove(tmp)
	if err := os.Link(src, tmp); err != nil {
		if err := copyFile(src, tmp); err != nil {
			return err
		}
	}
	return os.Rename(tmp, dst)
}

func copyFile(src, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o755)
	if err != nil {
		return err
	}
	if _, err := io.Copy(out, in); err != nil {
		out.Close()
		return err
	}
	return out.Close()
}

func writeFileAtomic(path, text string) error {
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, []byte(text+"\n"), 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// readVersion — версия сборки из файла version ("" — нет).
func readVersion(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}
