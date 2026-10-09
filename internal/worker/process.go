//go:build unix

package worker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
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
	"github.com/epifanovmd/agent/internal/message"
)

// SocketName — файл сокета воркера в его каталоге.
const SocketName = "http.sock"

// lineMax — строка вывода воркера длиннее обрезается.
const lineMax = 16 << 10

// adoptPoll — как часто агент проверяет, жив ли подхваченный процесс
// (своего потомка он ждёт через wait); переменная — для тестов.
var adoptPoll = 250 * time.Millisecond

// env — что агент передаёт процессу воркера (§12).
type env struct {
	agentSocket  string
	agentVersion string
	token        string
	// extra — переменные агента для всех воркеров (KEY=VALUE).
	extra []string
}

// process — процесс воркера: запущенный этим агентом или подхваченный после
// перезапуска агента (§13).
type process struct {
	name   string
	socket string
	log    *slog.Logger
	client *http.Client
	// version — версия сборки воркера из выпуска на момент запуска.
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

// argv — команда запуска и рабочий каталог: command + args; у воркера из
// выпуска — его сборка (файл или ./run архива), command — в каталоге сборки.
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

// releaseVersion — версия текущей сборки воркера из выпуска ("" — не из выпуска).
func releaseVersion(spec config.Worker) string {
	if !spec.Release {
		return ""
	}
	return readVersion(filepath.Join(spec.ReleaseDir, config.ReleaseVersion))
}

// socketDir — каталог сокета воркера: доступен только пользователю воркера
// (агенту-root он доступен и так).
func socketDir(runDir string, spec config.Worker) (string, error) {
	dir := filepath.Join(runDir, spec.Name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return "", err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return "", err
	}
	if spec.User != "" {
		cred, _, err := lookupUser(spec.User)
		if err != nil {
			return "", err
		}
		if err := os.Chown(dir, int(cred.Uid), int(cred.Gid)); err != nil {
			return "", err
		}
	}
	return dir, nil
}

// unixClient — HTTP-клиент к сокету воркера; адрес запроса — http://worker/путь.
func unixClient(socket string) *http.Client {
	return &http.Client{Transport: &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			var d net.Dialer
			return d.DialContext(ctx, "unix", socket)
		},
		MaxIdleConnsPerHost: 8,
		IdleConnTimeout:     30 * time.Second,
	}}
}

// startProcess — запустить процесс воркера (не дожидаясь сокета): свой сеанс
// и группа процессов (воркер не получает сигналов терминала и переживает
// выход агента), вывод — в файлы logDir.
func startProcess(spec config.Worker, runDir, logDir string, e env, log *slog.Logger, life func() config.Lifecycle, logs func() config.Logs) (*process, error) {
	args, dir, err := argv(spec)
	if err != nil {
		return nil, err
	}
	sockDir, err := socketDir(runDir, spec)
	if err != nil {
		return nil, fmt.Errorf("каталог сокета: %w", err)
	}
	socket := filepath.Join(sockDir, SocketName)
	_ = os.Remove(socket)
	set := map[string]string{
		message.EnvWorker:       spec.Name,
		message.EnvWorkerSocket: socket,
		message.EnvSocket:       e.agentSocket,
		message.EnvWorkerToken:  e.token,
		message.EnvVersion:      e.agentVersion,
		"PYTHONUNBUFFERED":      "1",
	}
	var cred *syscall.Credential
	var userEnv map[string]string
	if spec.User != "" {
		if cred, userEnv, err = lookupUser(spec.User); err != nil {
			return nil, err
		}
	}
	out, files, err := openOutput(logDir, spec, nil, log, logs)
	if err != nil {
		return nil, err
	}
	p := &process{
		name:    spec.Name,
		socket:  socket,
		log:     log.With("worker", spec.Name),
		client:  unixClient(socket),
		version: releaseVersion(spec),
		life:    life,
		out:     out,
		exited:  make(chan struct{}),
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Dir = dir
	cmd.Env = workerEnv(os.Environ(), spec, e.extra, set, userEnv)
	cmd.Stdout, cmd.Stderr = files[0], files[1]
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true, Credential: cred}
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
func adoptProcess(st procState, spec config.Worker, logDir string, log *slog.Logger, life func() config.Lifecycle, logs func() config.Logs) (*process, error) {
	out, files, err := openOutput(logDir, spec, &st.Output, log, logs)
	if err != nil {
		return nil, err
	}
	files[0].Close()
	files[1].Close()
	p := &process{
		name: spec.Name, socket: st.Socket, log: log.With("worker", spec.Name), client: unixClient(st.Socket),
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

// exit — процесс завершился: потомки без keepChildren завершаются вместе с
// ним, вывод дочитывается.
func (p *process) exit() {
	if !p.life().KeepChildren {
		_ = syscall.Kill(-p.pid, syscall.SIGKILL)
	}
	p.out.close(true)
	p.client.CloseIdleConnections()
	close(p.exited)
}

// alive — процесс жив и это он (pid не выдан другому процессу).
func (p *process) alive() bool {
	start, ok := procStart(p.pid)
	return ok && start == p.start
}

// state — файл процесса для следующего запуска агента.
func (p *process) state(token, spec string, startedAt time.Time, output [2]int64) procState {
	life := p.life()
	return procState{PID: p.pid, Start: p.start, Socket: p.socket, Token: token, Version: p.version, Spec: spec,
		StartedAt: startedAt.UnixMilli(), Output: output, StopTimeout: life.StopTimeout, KeepChildren: life.KeepChildren}
}

// detach — агент уходит, процесс работает дальше: дочитать вывод и
// перестать следить. Итог — места в файлах вывода для следующего агента.
func (p *process) detach() [2]int64 {
	p.mu.Lock()
	p.detached = true
	p.mu.Unlock()
	return p.out.close(false)
}

// waitReady — ждать, пока сокет воркера примет соединение (не дольше
// wait); процесс завершился раньше — ошибка.
func (p *process) waitReady(ctx context.Context, wait time.Duration) error {
	deadline := time.NewTimer(wait)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		if c, err := net.DialTimeout("unix", p.socket, time.Second); err == nil {
			c.Close()
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-p.exited:
			return fmt.Errorf("завершился до готовности: %s", p.exitReason())
		case <-deadline.C:
			return fmt.Errorf("сокет %s не ответил за %s", p.socket, wait)
		case <-tick.C:
		}
	}
}

// signal — сигнал воркеру: с keepChildren — только основному процессу,
// иначе всей группе. Подхваченному — только если pid всё ещё его.
func (p *process) signal(sig syscall.Signal) {
	if p.adopted && !p.alive() {
		return
	}
	if !p.life().KeepChildren {
		if err := syscall.Kill(-p.pid, sig); err == nil {
			return
		}
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

// failed — выход — сбой (lifecycle.restart: on-failure): код не 0, сигнал,
// агент убил зависший; у подхваченного код неизвестен — тоже сбой.
func (p *process) failed() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.adopted || p.reason != "" || p.exitErr != nil
}

// errNoResponse — воркер не ответил (нет соединения, срок истёк).
var errNoResponse = errors.New("воркер не ответил")

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
