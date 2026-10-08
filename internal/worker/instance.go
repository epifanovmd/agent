//go:build unix

package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
	"unicode/utf8"

	"github.com/epifanovmd/agent/internal/cgroup"
	"github.com/epifanovmd/agent/internal/commands"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/jobs"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// defaultRegisterTimeout — воркер должен объявить очереди за это время
// (импорт тяжёлых библиотек), если в настройках не задано registerTimeout.
const defaultRegisterTimeout = 120 * time.Second

// Проверка «жив ли воркер» (§10, «Проверка»): worker.ping раз в pingEvery,
// ответ — за pingWait, pingMisses запросов подряд без ответа — копия зависла.
// Переменные — для тестов.
var (
	pingEvery  = 30 * time.Second
	pingWait   = 10 * time.Second
	pingMisses = 3
)

// lineMax — строка вывода воркера длиннее обрезается: строка без перевода
// строки не копится в памяти без предела.
const lineMax = 16 << 10

// ipcFD — номер унаследованного дескриптора канала (первый из ExtraFiles).
const ipcFD = 3

// instance — один процесс воркера; исполнитель задач для jobs.Manager,
// команд и доменов состояния, которые он объявил.
type instance struct {
	id       string
	spec     config.Worker
	sup      *Supervisor
	w        *worker
	reporter jobs.Reporter
	log      *slog.Logger
	agentVer string
	// build — поколение сборки воркера (worker.build) на момент запуска.
	build int

	cmd     *exec.Cmd
	ipc     net.Conn
	writeMu sync.Mutex
	// group — подгруппа cgroup этой копии (nil — без ограничений).
	group *cgroup.Group
	// hung — копия не отвечала на worker.ping и завершена принудительно.
	hung atomic.Bool
	// pingEvery, pingWait — сроки проверки на момент запуска.
	pingEvery, pingWait time.Duration

	mu       sync.Mutex
	queues   map[string]int
	version  string
	retired  atomic.Bool
	ready    chan struct{}
	exited   chan struct{}
	exitErr  error
	started  time.Time
	jobs     map[string]message.JobRef
	stopping atomic.Bool
	// accepted — имена, принятые при регистрации (команды, домены, каналы).
	accepted map[owned]bool
	// calls — вызовы, ждущие ответа воркера: команды и применения состояния.
	calls map[string]*call
	// health — последнее worker.health (nil — не сообщал).
	health *message.WorkerHealth
	// selfPause — пауза очередей, выставленная самим воркером (worker.pause).
	selfPause pauseSet
	// ctxWake — контекст агента изменился; sentCtx — последний отправленный.
	ctxWake chan struct{}
	sentCtx *message.WorkerContext
}

// call — вызов воркера, ждущий итога.
type call struct {
	out  io.Writer // вывод команды
	done chan message.Envelope
}

func startInstance(sup *Supervisor, w *worker, id string) (*instance, error) {
	sup.mu.Lock()
	spec, build := w.spec, w.build
	extra := slices.Clone(sup.env)
	sup.mu.Unlock()
	reporter, log, agentVersion := jobs.Reporter(sup.jobs), sup.log, sup.agentVer
	argv := spec.Argv()
	dir := spec.Dir
	var releaseVersion string
	if spec.Release {
		info, err := os.Stat(spec.Current())
		if err != nil {
			return nil, fmt.Errorf("worker %s: нет сборки (%s): поставьте её install.sh --worker %s или обновите воркер командой worker.update",
				spec.Name, spec.Current(), spec.Name)
		}
		switch {
		case info.IsDir() && len(spec.Command) == 0:
			// Архив без command в настройках — его файл run.
			argv = append([]string{"./" + config.ReleaseRun}, spec.Args...)
			dir = spec.Current()
		case info.IsDir():
			dir = spec.Current() // command выполняется в каталоге сборки
		case len(spec.Command) > 0:
			dir = spec.ReleaseDir // ./current — сама сборка
		}
		releaseVersion = readVersion(filepath.Join(spec.ReleaseDir, config.ReleaseVersion))
	}
	set := map[string]string{
		"AGENT_IPC_FD":     fmt.Sprint(ipcFD),
		"AGENT_WORKER":     spec.Name,
		"AGENT_VERSION":    agentVersion,
		"PYTHONUNBUFFERED": "1",
	}
	if spec.Release {
		// Версия установленной сборки (файл version) — воркер может сообщить
		// её в worker.register.
		set["AGENT_WORKER_RELEASE_VERSION"] = releaseVersion
	}
	var cred *syscall.Credential
	var userEnv map[string]string
	if spec.User != "" {
		var err error
		if cred, userEnv, err = lookupUser(spec.User); err != nil {
			return nil, fmt.Errorf("worker %s: user: %w", spec.Name, err)
		}
	}
	env := workerEnv(os.Environ(), spec, extra, set, userEnv)

	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_STREAM, 0)
	if err != nil {
		return nil, fmt.Errorf("worker %s: socketpair: %w", spec.Name, err)
	}
	parent := os.NewFile(uintptr(fds[0]), "agent-ipc")
	child := os.NewFile(uintptr(fds[1]), "agent-ipc-child")
	ipc, err := net.FileConn(parent)
	parent.Close()
	if err != nil {
		child.Close()
		return nil, fmt.Errorf("worker %s: ipc: %w", spec.Name, err)
	}
	defer child.Close()

	inst := &instance{
		id:       id,
		spec:     spec,
		sup:      sup,
		w:        w,
		reporter: reporter,
		log:      log.With("worker", spec.Name, "instance", id),
		agentVer: agentVersion,
		build:    build,
		ipc:      ipc,
		ready:    make(chan struct{}),
		exited:   make(chan struct{}),
		jobs:     map[string]message.JobRef{},
		calls:    map[string]*call{},
		ctxWake:  make(chan struct{}, 1),

		pingEvery: pingEvery,
		pingWait:  pingWait,
	}
	// Подгруппа cgroup копии — до запуска: процесс попадает туда сразу.
	if group := sup.workerGroup(spec); group != nil {
		if inst.group, err = group.Instance(id); err != nil {
			inst.log.Warn("ограничения воркера не применены: подгруппа копии не создана", "err", err)
		}
	}
	stdout := &lineLog{log: inst.log, level: slog.LevelInfo}
	stderr := &lineLog{log: inst.log, level: slog.LevelWarn}
	newCmd := func() *exec.Cmd {
		cmd := exec.Command(argv[0], argv[1:]...)
		cmd.Dir = dir
		cmd.Env = env
		cmd.ExtraFiles = []*os.File{child}
		cmd.Stdout, cmd.Stderr = stdout, stderr
		// Своя группа процессов: сигнал остановки получают и потомки воркера.
		cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Credential: cred}
		return cmd
	}
	cmd := newCmd()
	inCgroup := false
	if inst.group != nil {
		if release, ok := useCgroupFD(cmd, inst.group.Dir()); ok {
			defer release()
			inCgroup = true
		}
	}
	err = cmd.Start()
	if err != nil && inCgroup {
		// Ядро не умеет запуск сразу в группе — обычный запуск и перенос.
		cmd, inCgroup = newCmd(), false
		err = cmd.Start()
	}
	if err != nil {
		ipc.Close()
		inst.removeGroup()
		return nil, fmt.Errorf("worker %s: запуск: %w", spec.Name, err)
	}
	inst.cmd = cmd
	inst.started = time.Now()
	inst.log.Info("воркер запущен", "pid", cmd.Process.Pid)
	if inst.group != nil && !inCgroup {
		if err := inst.group.Add(cmd.Process.Pid); err != nil {
			inst.log.Warn("ограничения воркера не применены: процесс не перенесён в cgroup", "err", err)
		}
	}

	go inst.readLoop()
	go func() {
		err := cmd.Wait()
		// Потомки, пережившие воркер, завершаются вместе с ним.
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		if inst.group != nil {
			_ = inst.group.Kill()
		}
		inst.mu.Lock()
		inst.exitErr = err
		inst.mu.Unlock()
		ipc.Close()
		stdout.Flush()
		stderr.Flush()
		inst.removeGroup()
		close(inst.exited)
	}()
	wait := spec.RegisterTimeout.Std()
	if wait <= 0 {
		wait = defaultRegisterTimeout
	}
	go func() {
		timer := time.NewTimer(wait)
		defer timer.Stop()
		select {
		case <-inst.ready:
		case <-inst.exited:
		case <-timer.C:
			inst.log.Error("воркер не зарегистрировался вовремя — перезапуск", "registerTimeout", wait)
			inst.kill()
		}
	}()
	return inst, nil
}

// removeGroup — убрать подгруппу копии (процессов в ней уже нет; ядру нужно
// немного времени, чтобы их забыть).
func (i *instance) removeGroup() {
	if i.group == nil {
		return
	}
	var err error
	for range 20 {
		if err = i.group.Remove(); err == nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	i.log.Debug("подгруппа копии воркера не удалена", "err", err)
}

// ─── jobs.Runner ───────────────────────────────────────────────────────

func (i *instance) ID() string { return i.id }

func (i *instance) Queues() map[string]int {
	i.mu.Lock()
	defer i.mu.Unlock()
	out := make(map[string]int, len(i.queues))
	for q, n := range i.queues {
		out[q] = n
	}
	return out
}

func (i *instance) Accepting() bool {
	return !i.retired.Load() && !i.stopping.Load()
}

func (i *instance) Run(a message.JobAssign) error {
	i.mu.Lock()
	i.jobs[a.JobID] = a.Ref()
	i.mu.Unlock()
	return i.send(message.MustNew(message.TypeJobAssign, a))
}

func (i *instance) Cancel(ref message.JobRef) {
	i.forgetRef(ref)
	_ = i.send(message.MustNew(message.TypeJobCancel, ref))
}

// ReplaceStuck — jobs.StuckReplacer: отменённая задача не завершилась за
// срок — копия заменяется новой.
func (i *instance) ReplaceStuck(ref message.JobRef) {
	go i.sup.replaceStuck(i, fmt.Sprintf("отменённая задача %s (попытка %d) не завершилась", ref.JobID, ref.Attempt))
}

func (i *instance) Stop(ref message.JobRef) {
	_ = i.send(message.MustNew(message.TypeJobStop, ref))
}

// ─── жизненный цикл ────────────────────────────────────────────────────

// retire — перестать брать задачи и завершиться после текущих (заменён новым).
func (i *instance) retire() {
	if i.retired.Swap(true) {
		return
	}
	_ = i.send(message.MustNew(message.TypeWorkerDrain, struct{}{}))
}

// terminate — SIGTERM всей группе процессов воркера, по отмене ctx — SIGKILL.
func (i *instance) terminate(ctx context.Context) {
	i.stopping.Store(true)
	if i.cmd == nil || i.cmd.Process == nil {
		return
	}
	i.signal(syscall.SIGTERM)
	select {
	case <-i.exited:
	case <-ctx.Done():
		i.log.Warn("воркер не завершился вовремя — SIGKILL")
		i.kill()
		<-i.exited
	}
}

// signal — сигнал группе процессов воркера (он и его потомки).
func (i *instance) signal(sig syscall.Signal) {
	if err := syscall.Kill(-i.cmd.Process.Pid, sig); err != nil {
		_ = i.cmd.Process.Signal(sig)
	}
}

// kill — завершить копию принудительно: SIGKILL группе процессов и
// cgroup.kill её подгруппы (потомки, сменившие группу процессов).
func (i *instance) kill() {
	i.signal(syscall.SIGKILL)
	if i.group != nil {
		_ = i.group.Kill()
	}
}

func (i *instance) exitReason() string {
	if i.hung.Load() {
		return "воркер завис: нет ответа на worker.ping"
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if i.exitErr == nil {
		return "exit 0"
	}
	return i.exitErr.Error()
}

func (i *instance) runningJobs() int {
	i.mu.Lock()
	defer i.mu.Unlock()
	return len(i.jobs)
}

// forgetRef — задача ref завершена или отменена (другая попытка того же
// jobId остаётся).
func (i *instance) forgetRef(ref message.JobRef) {
	i.mu.Lock()
	if i.jobs[ref.JobID] == ref {
		delete(i.jobs, ref.JobID)
	}
	i.mu.Unlock()
}

func (i *instance) send(env message.Envelope) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	i.writeMu.Lock()
	defer i.writeMu.Unlock()
	_ = i.ipc.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = i.ipc.Write(append(raw, '\n'))
	return err
}

func (i *instance) readLoop() {
	scanner := bufio.NewScanner(i.ipc)
	scanner.Buffer(make([]byte, 64*1024), 16<<20)
	for scanner.Scan() {
		var env message.Envelope
		if err := json.Unmarshal(scanner.Bytes(), &env); err != nil {
			i.log.Warn("воркер: сообщение не JSON", "err", err)
			continue
		}
		if err := i.handle(env); err != nil {
			i.log.Warn("воркер: сообщение не обработано", "type", env.Type, "err", err)
		}
	}
}

func (i *instance) handle(env message.Envelope) error {
	switch env.Type {
	case message.TypeWorkerRegister:
		var reg message.WorkerRegister
		if err := env.Decode(&reg); err != nil {
			return err
		}
		rejected := i.register(reg)
		err := i.send(message.MustNew(message.TypeWorkerReady, message.WorkerReady{AgentVersion: i.agentVer, Rejected: rejected}))
		if !i.sup.cleaning {
			// Имена, которые воркер больше не объявляет, снимаются.
			i.sup.prune(i.w, i)
		}
		if i.registered() {
			return err // повторная регистрация: контекст уже идёт
		}
		// Контекст — сразу после worker.ready и до всего остального (задачи,
		// уборка): экземпляр готов только после него.
		i.pushContext()
		close(i.ready)
		go i.contextLoop()
		if reg.Ping {
			go i.pingLoop()
		}
		return err
	case message.TypeWorkerPong:
		i.resolve("pong:"+env.Re, env)
	case message.TypeWorkerHealth, message.TypeWorkerPause, message.TypeWorkerResume, message.TypeWorkerRestart:
		return i.control(env)
	case message.TypeJobProgress:
		var p message.JobProgress
		if err := env.Decode(&p); err != nil {
			return err
		}
		i.reporter.Progress(p)
	case message.TypeJobEvent:
		var e message.JobEvent
		if err := env.Decode(&e); err != nil {
			return err
		}
		i.reporter.Event(e)
	case message.TypeJobComplete:
		var c message.JobComplete
		if err := env.Decode(&c); err != nil {
			return err
		}
		i.forgetRef(c.JobRef)
		i.reporter.Complete(c)
	case message.TypeJobFail:
		var f message.JobFail
		if err := env.Decode(&f); err != nil {
			return err
		}
		i.forgetRef(f.JobRef)
		i.reporter.Fail(f)
	case message.TypeJobURLs:
		var req message.JobURLsRequest
		if err := env.Decode(&req); err != nil {
			return err
		}
		go i.answerURLs(env.ID, req)
	case message.TypeCmdOutput:
		var out message.CommandOutput
		if err := env.Decode(&out); err != nil {
			return err
		}
		i.mu.Lock()
		c := i.calls["cmd:"+out.CommandID]
		i.mu.Unlock()
		if c != nil && c.out != nil {
			_, _ = io.WriteString(c.out, out.Chunk)
		}
	case message.TypeCmdDone:
		var done message.CommandDone
		if err := env.Decode(&done); err != nil {
			return err
		}
		i.resolve("cmd:"+done.CommandID, env)
	case message.TypeStateApplied:
		i.resolve("state:"+env.Re, env)
	case message.TypeWorkerCleaned:
		i.resolve("cleanup:"+env.Re, env)
	case message.TypeTelemetry, message.TypeEvent:
		if i.sup.cleaning {
			return nil // уборка без сервера: отправлять некуда
		}
		return i.report(env)
	default:
		return errors.New("неизвестный тип")
	}
	return nil
}

// report — показатели и события воркера серверу.
func (i *instance) report(env message.Envelope) error {
	switch env.Type {
	case message.TypeTelemetry:
		var t message.Telemetry
		if err := env.Decode(&t); err != nil {
			return err
		}
		if !i.has(kindChannel, t.Channel) {
			return fmt.Errorf("канал %q не объявлен воркером", t.Channel)
		}
		if !i.sup.owns(i.spec.Name, kindChannel, t.Channel) {
			return nil // воркер удалён из настроек и дорабатывает: канал снят
		}
		i.sup.bridge.Telemetry.Report(t.Channel, t.Data)
	case message.TypeEvent:
		var e message.Event
		if err := env.Decode(&e); err != nil {
			return err
		}
		if e.Type == "" || len(e.Type) > 50 {
			return errors.New("event: type — от 1 до 50 символов")
		}
		if i.sup.bridge.Events == nil {
			return errors.New("event: события не подключены")
		}
		e.Source = i.spec.Name
		if err := i.sup.bridge.Events.Reliable(message.TypeEvent, e); err != nil {
			i.log.Error("воркер: событие не записано в outbox", "err", err)
		}
	}
	return nil
}

func (i *instance) register(reg message.WorkerRegister) (rejected []string) {
	allowed := map[string]bool{}
	for _, q := range i.spec.Queues {
		allowed[q] = true
	}
	var badQueues []string
	queues := map[string]int{}
	for _, q := range reg.Queues {
		if q.Name != "" && !message.ValidName(q.Name) {
			if !slices.Contains(badQueues, q.Name) {
				badQueues = append(badQueues, q.Name)
			}
			continue
		}
		if q.Concurrency <= 0 || (len(allowed) > 0 && !allowed[q.Name]) {
			continue
		}
		queues[q.Name] = q.Concurrency
	}
	accepted := map[owned]bool{}
	if !i.sup.cleaning {
		accepted, rejected = i.sup.claim(i.w, reg)
	}
	rejected = append(badQueues, rejected...)
	if bad := invalidNames(reg); len(bad) > 0 {
		i.log.Warn("воркер: имена не по правилу — отклонены (латиница, цифры, «.», «_», «-», начало — буква или цифра, до 64 символов)",
			"invalid", bad, "pattern", message.NamePattern)
	}
	i.mu.Lock()
	i.queues = queues
	i.version = reg.Version
	i.accepted = accepted
	i.mu.Unlock()
	i.log.Info("воркер зарегистрирован", "version", reg.Version, "sdk", reg.SDK, "queues", queues,
		"commands", reg.Commands, "domains", reg.Domains, "channels", reg.Channels)
	if len(rejected) > 0 {
		i.log.Warn("воркер: имена отклонены — не по правилу, зарезервированы или заняты", "rejected", rejected)
	}
	return rejected
}

// hasExited — процесс завершился.
func (i *instance) hasExited() bool {
	select {
	case <-i.exited:
		return true
	default:
		return false
	}
}

// registered — воркер зарегистрировался.
func (i *instance) registered() bool {
	select {
	case <-i.ready:
		return true
	default:
		return false
	}
}

func (i *instance) has(kind, name string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()
	return i.accepted[owned{kind, name}]
}

// ─── вызовы воркера ───────────────────────────────────────────────────

// await — отправить вызов и ждать итога: ответ воркера, его завершение или
// отмена ctx.
func (i *instance) await(ctx context.Context, key string, env message.Envelope, out io.Writer) (message.Envelope, error) {
	c := &call{out: out, done: make(chan message.Envelope, 1)}
	i.mu.Lock()
	i.calls[key] = c
	i.mu.Unlock()
	defer func() {
		i.mu.Lock()
		delete(i.calls, key)
		i.mu.Unlock()
	}()
	if err := i.send(env); err != nil {
		return message.Envelope{}, &message.Error{Code: "WORKER_UNAVAILABLE", Message: "воркер не принимает вызов: " + err.Error(), Retryable: true}
	}
	select {
	case reply := <-c.done:
		return reply, nil
	case <-i.exited:
		return message.Envelope{}, &message.Error{Code: "WORKER_EXITED", Message: "воркер завершился: " + i.exitReason(), Retryable: true}
	case <-ctx.Done():
		return message.Envelope{}, ctx.Err()
	}
}

func (i *instance) resolve(key string, env message.Envelope) {
	i.mu.Lock()
	c := i.calls[key]
	i.mu.Unlock()
	if c == nil {
		return
	}
	select {
	case c.done <- env:
	default:
	}
}

// command — команда воркера; вывод — в out, по истечении срока — cmd.cancel.
func (i *instance) command(ctx context.Context, name string, args json.RawMessage, out io.Writer) (any, error) {
	id := commands.ID(ctx)
	if id == "" {
		id = message.NewID()
	}
	run := message.CommandRun{CommandID: id, Name: name, Args: args, TimeoutSec: 60}
	if deadline, ok := ctx.Deadline(); ok {
		run.TimeoutSec = max(int(time.Until(deadline).Seconds()), 1)
	}
	reply, err := i.await(ctx, "cmd:"+id, message.MustNew(message.TypeCmdRun, run), out)
	if err != nil {
		if ctx.Err() != nil {
			_ = i.send(message.MustNew(message.TypeCmdCancel, message.CommandRef{CommandID: id}))
			return nil, ctx.Err()
		}
		var ae *message.Error
		if errors.As(err, &ae) {
			return nil, &commands.Error{Code: ae.Code, Message: ae.Message}
		}
		return nil, err
	}
	var done message.CommandDone
	if err := reply.Decode(&done); err != nil {
		return nil, err
	}
	if !done.OK {
		e := commands.Error{Code: "COMMAND_FAILED", Message: "команда воркера завершилась ошибкой"}
		if done.Error != nil {
			e.Code, e.Message = done.Error.Code, done.Error.Message
		}
		return nil, &e
	}
	if len(done.Result) == 0 {
		return nil, nil
	}
	return done.Result, nil
}

// applyState — применить снимок домена воркером; отчёт — его report.
func (i *instance) applyState(ctx context.Context, put message.StatePut) (any, error) {
	env := message.MustNew(message.TypeStatePut, put)
	env.ID = message.NewID()
	reply, err := i.await(ctx, "state:"+env.ID, env, nil)
	if err != nil {
		return nil, err
	}
	var applied message.StateApplied
	if err := reply.Decode(&applied); err != nil {
		return nil, err
	}
	if !applied.OK {
		msg := applied.Error
		if msg == "" {
			msg = "воркер не применил снимок"
		}
		if len(applied.Report) > 0 {
			// Отчёт при ошибке тоже доходит до сервера (state.applied ok:false, report).
			return applied.Report, errors.New(msg)
		}
		return nil, errors.New(msg)
	}
	if len(applied.Report) == 0 {
		return nil, nil
	}
	return applied.Report, nil
}

// pingLoop — проверка, что копия жива (§10, «Проверка»): pingMisses запросов
// подряд без ответа — предупреждение и принудительное завершение; цикл места
// перезапустит копию как упавшую.
func (i *instance) pingLoop() {
	misses := 0
	for {
		timer := time.NewTimer(i.pingEvery)
		select {
		case <-i.exited:
			timer.Stop()
			return
		case <-timer.C:
		}
		env := message.MustNew(message.TypeWorkerPing, struct{}{})
		env.ID = message.NewID()
		ctx, cancel := context.WithTimeout(context.Background(), i.pingWait)
		_, err := i.await(ctx, "pong:"+env.ID, env, nil)
		cancel()
		if i.hasExited() {
			return
		}
		if err == nil {
			misses = 0
			continue
		}
		misses++
		i.log.Warn("воркер не ответил на worker.ping", "misses", misses, "err", err)
		if misses >= pingMisses {
			i.log.Warn("воркер завис: нет ответа на worker.ping — перезапуск", "misses", misses)
			i.hung.Store(true)
			i.kill()
			return
		}
	}
}

// urlsWait — сколько воркер ждёт ответа на job.urls: агент за это время
// повторяет запрос серверу (jobs.Manager.URLs).
var urlsWait = 2 * time.Minute

func (i *instance) answerURLs(id string, req message.JobURLsRequest) {
	ctx, cancel := context.WithTimeout(context.Background(), urlsWait)
	defer cancel()
	urls, err := i.reporter.URLs(ctx, req)
	var reply message.Envelope
	if err != nil {
		e := message.Error{Code: "URLS_UNAVAILABLE", Message: err.Error(), Retryable: true}
		var ae *message.Error
		if errors.As(err, &ae) {
			e = *ae
		}
		reply = message.MustNew(message.TypeError, e)
	} else {
		reply = message.MustNew(message.TypeJobURLs, urls)
	}
	reply.Re = id
	if err := i.send(reply); err != nil {
		i.log.Warn("воркер: ответ job.urls не отправлен", "err", err)
	}
}

// lineLog — вывод воркера построчно в лог агента с уровнем level (stdout —
// info, stderr — warn). Строка длиннее lineMax обрезается с пометкой,
// остаток до перевода строки отбрасывается.
type lineLog struct {
	log   *slog.Logger
	level slog.Level
	mu    sync.Mutex
	buf   []byte
	// skip — отбрасывать до конца строки (её начало уже записано).
	skip bool
}

// truncatedMark — пометка обрезанной строки.
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

var _ io.Writer = (*lineLog)(nil)
