//go:build unix

package worker

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/protocol"
)

// lineMax — строка вывода воркера длиннее обрезается.
const lineMax = 16 << 10

// adoptPoll — как часто агент проверяет, жив ли подхваченный процесс
// (своего потомка он ждёт через wait); переменная — для тестов.
var adoptPoll = 250 * time.Millisecond

// env — что агент передаёт процессу воркера (§11).
type env struct {
	agentSocket string
	token       string
	// stateDir — каталог своих данных воркера.
	stateDir string
	// extra — переменные агента для всех воркеров (KEY=VALUE).
	extra []string
}

// process — процесс воркера: запущенный этим агентом или подхваченный после
// перезапуска агента (§11).
type process struct {
	name string
	log  *slog.Logger
	// version — версия сборки воркера на момент запуска.
	version string
	pid     int
	// start — время запуска процесса по данным системы (procStart).
	start string
	// adopted — подхвачен: не потомок агента, код выхода неизвестен.
	adopted bool
	// life — текущая жизнь воркера (меняется перечитыванием настроек).
	life func() config.Lifecycle
	out  *output

	exited   chan struct{}
	mu       sync.Mutex
	exitErr  error
	detached bool
	// reason — почему процесс завершён агентом (завис, не запустился).
	reason string
}

// argv — команда запуска и рабочий каталог: command + args; у воркера со
// сборкой с сервера — его сборка (файл или ./run архива), command — в каталоге сборки.
func argv(spec config.Worker) ([]string, string, error) {
	args := append(slices.Clone(spec.Command), spec.Args...)
	dir := spec.Dir
	if !spec.Release {
		return args, dir, nil
	}
	info, err := os.Stat(spec.Current())
	if err != nil {
		return nil, "", fmt.Errorf("нет сборки воркера (%s): поставьте её — agent install --worker %s — или обновите действием worker.update",
			spec.Current(), spec.Name)
	}
	switch {
	case info.IsDir() && len(spec.Command) == 0:
		return append([]string{"./" + config.ReleaseRun}, spec.Args...), spec.Current(), nil
	case info.IsDir():
		return args, spec.Current(), nil
	case len(spec.Command) == 0:
		return append([]string{spec.Current()}, spec.Args...), dir, nil
	}
	return args, spec.ReleaseDir, nil
}

// readVersion — версия сборки из файла version ("" — нет).
func readVersion(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}

// releaseVersion — версия текущей сборки воркера ("" — воркер без release: true).
func releaseVersion(spec config.Worker) string {
	if !spec.Release {
		return ""
	}
	return readVersion(filepath.Join(spec.ReleaseDir, config.ReleaseVersion))
}

// startProcess — запустить процесс воркера: сначала, если нужно, ./setup (установка
// зависимостей), затем сам воркер — свой сеанс и группа процессов (воркер не получает
// сигналов терминала и переживает выход агента), вывод — в файлы logDir.
func startProcess(ctx context.Context, spec config.Worker, logDir string, e env, log *slog.Logger, life func() config.Lifecycle) (*process, error) {
	args, dir, err := argv(spec)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(e.stateDir, 0o700); err != nil {
		return nil, fmt.Errorf("каталог данных воркера: %w", err)
	}
	set := map[string]string{
		protocol.EnvWorkerName:  spec.Name,
		protocol.EnvAgentSocket: e.agentSocket,
		protocol.EnvWorkerToken: e.token,
		protocol.EnvStateDir:    e.stateDir,
		"PYTHONUNBUFFERED":      "1",
	}
	environ := workerEnv(os.Environ(), spec, e.extra, set)
	if err := runSetup(ctx, spec.Name, dir, environ, life().SetupTimeout.Std(), log); err != nil {
		return nil, err
	}
	out, files, err := openOutput(logDir, spec, nil, log)
	if err != nil {
		return nil, err
	}
	p := &process{
		name:    spec.Name,
		log:     log.With("worker", spec.Name),
		version: releaseVersion(spec),
		life:    life,
		out:     out,
		exited:  make(chan struct{}),
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = dir
	cmd.Env = environ
	cmd.Stdout, cmd.Stderr = files[0], files[1]
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	err = cmd.Start()
	files[0].Close()
	files[1].Close()
	if err != nil {
		out.close(true)
		return nil, fmt.Errorf("запуск: %w", err)
	}
	p.pid = cmd.Process.Pid
	p.start, _ = procStart(p.pid)
	p.log.Info("воркер запущен", "pid", p.pid)
	go func() {
		err := cmd.Wait()
		p.mu.Lock()
		p.exitErr = err
		detached := p.detached
		p.mu.Unlock()
		if detached {
			return // за процессом следит другой агент
		}
		p.exit()
	}()
	return p, nil
}

// adoptProcess — процесс воркера из файла st, запущенный прежним агентом:
// агент следит за ним, как за своим, и читает вывод с мест из файла.
func adoptProcess(st procState, spec config.Worker, logDir string, log *slog.Logger, life func() config.Lifecycle) (*process, error) {
	out, files, err := openOutput(logDir, spec, &st.Output, log)
	if err != nil {
		return nil, err
	}
	files[0].Close()
	files[1].Close()
	p := &process{
		name: spec.Name, log: log.With("worker", spec.Name),
		version: st.Version, pid: st.PID, start: st.Start, adopted: true, life: life, out: out,
		exited: make(chan struct{}),
	}
	go func() {
		tick := time.NewTicker(adoptPoll)
		defer tick.Stop()
		for range tick.C {
			p.mu.Lock()
			detached := p.detached
			p.mu.Unlock()
			if detached {
				return
			}
			if !p.alive() {
				p.exit()
				return
			}
		}
	}()
	return p, nil
}

// exit — процесс завершился: его потомки завершаются вместе с ним, вывод
// дочитывается.
func (p *process) exit() {
	_ = syscall.Kill(-p.pid, syscall.SIGKILL)
	p.out.close(true)
	close(p.exited)
}

// alive — процесс жив и это он (pid не выдан другому процессу).
func (p *process) alive() bool {
	start, ok := procStart(p.pid)
	return ok && start == p.start
}

// state — файл процесса для следующего запуска агента.
func (p *process) state(token, spec string, startedAt time.Time, output [2]int64) procState {
	return procState{PID: p.pid, Start: p.start, Token: token, Version: p.version, Spec: spec,
		StartedAt: startedAt.UnixMilli(), Output: output, StopTimeout: p.life().StopTimeout}
}

// detach — агент уходит, процесс работает дальше: дочитать вывод и
// перестать следить. Итог — места в файлах вывода для следующего агента.
func (p *process) detach() [2]int64 {
	p.mu.Lock()
	p.detached = true
	p.mu.Unlock()
	return p.out.close(false)
}

// signal — сигнал всей группе процессов воркера. Подхваченному — только
// если pid всё ещё его.
func (p *process) signal(sig syscall.Signal) {
	if p.adopted && !p.alive() {
		return
	}
	if err := syscall.Kill(-p.pid, sig); err == nil {
		return
	}
	_ = syscall.Kill(p.pid, sig)
}

// stop — SIGTERM, не вышел за stopTimeout — SIGKILL; ждёт выхода.
func (p *process) stop() {
	select {
	case <-p.exited:
		return
	default:
	}
	p.signal(syscall.SIGTERM)
	stopTimeout := p.life().StopTimeout.Std()
	timer := time.NewTimer(stopTimeout)
	defer timer.Stop()
	select {
	case <-p.exited:
	case <-timer.C:
		p.log.Warn("воркер не завершился за stopTimeout — SIGKILL", "stopTimeout", stopTimeout)
		p.signal(syscall.SIGKILL)
		<-p.exited
	}
}

// kill — завершить сразу (завис): SIGKILL с причиной.
func (p *process) kill(reason string) {
	p.mu.Lock()
	p.reason = reason
	p.mu.Unlock()
	p.signal(syscall.SIGKILL)
}

func (p *process) exitReason() string {
	p.mu.Lock()
	defer p.mu.Unlock()
	switch {
	case p.reason != "":
		return p.reason
	case p.adopted:
		return "процесс завершился (подхвачен после перезапуска агента — код выхода неизвестен)"
	case p.exitErr == nil:
		return "exit 0"
	}
	return p.exitErr.Error()
}

// failed — выход — сбой (воркер перезапускается): код не 0, сигнал,
// агент убил зависший; у подхваченного код неизвестен — тоже сбой.
func (p *process) failed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.adopted || p.reason != "" || p.exitErr != nil
}

// lineLog — вывод воркера построчно в лог агента (stdout — info, stderr —
// warn); строка длиннее lineMax обрезается с пометкой.
type lineLog struct {
	log   *slog.Logger
	level slog.Level
	mu    sync.Mutex
	buf   []byte
	skip  bool
}

const truncatedMark = " …[строка обрезана]"

func (l *lineLog) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := len(p)
	for len(p) > 0 {
		idx := slices.Index(p, '\n')
		part := p
		if idx >= 0 {
			part = p[:idx]
		}
		if !l.skip {
			room := lineMax - len(l.buf)
			if len(part) > room {
				l.buf = append(l.buf, part[:room]...)
				l.emit(string(trimRune(l.buf)) + truncatedMark)
				l.skip = true
			} else {
				l.buf = append(l.buf, part...)
			}
		}
		if idx < 0 {
			break
		}
		if !l.skip {
			l.emit(string(l.buf))
		}
		l.buf, l.skip = l.buf[:0], false
		p = p[idx+1:]
	}
	return n, nil
}

func (l *lineLog) emit(line string) {
	if line = strings.TrimRight(line, "\r"); line != "" {
		l.log.Log(context.Background(), l.level, line)
	}
}

// trimRune — без неполного символа UTF-8 в конце.
func trimRune(b []byte) []byte {
	for k := 0; k < utf8.UTFMax && len(b) > 0; k++ {
		if utf8.Valid(b) {
			return b
		}
		b = b[:len(b)-1]
	}
	return b
}

// Flush — недописанная последняя строка.
func (l *lineLog) Flush() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.skip {
		if rest := strings.TrimSpace(string(l.buf)); rest != "" {
			l.log.Log(context.Background(), l.level, rest)
		}
	}
	l.buf, l.skip = l.buf[:0], false
}

// pending — байт недописанной строки (её дочитает следующий читатель файла).
func (l *lineLog) pending() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.skip {
		return 0
	}
	return len(l.buf)
}
