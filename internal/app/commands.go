//go:build unix

package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/releases"
	"github.com/epifanovmd/agent/internal/store"
	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/internal/worker"
	"github.com/epifanovmd/agent/protocol"
)

// command — команда бэкенда (§8): в журнал; новая — выполняется, выполненная — её итог снова,
// идущая — ничего.
func (a *App) command(cmd protocol.Command) {
	state, res, err := a.store.StartCommand(cmd)
	switch {
	case err != nil:
		a.log.Error("команда не записана в журнал — не выполняется", "id", cmd.ID, "type", cmd.Type, "err", err)
	case state == store.CommandDone && res != nil:
		if err := a.store.RequeueResult(*res); err != nil {
			a.log.Error("итог команды не записан в очередь", "id", cmd.ID, "err", err)
		}
	case state == store.CommandNew:
		a.log.Info("команда бэкенда", "id", cmd.ID, "type", cmd.Type)
		go a.execute(cmd)
	}
}

// resumeCommands — команды, которые шли при прошлом запуске агента: продолжить.
func (a *App) resumeCommands() {
	running, err := a.store.Running()
	if err != nil {
		a.log.Error("журнал команд не прочитан", "err", err)
		return
	}
	pending := a.pendingUpdate()
	for _, cmd := range running {
		if cmd.Type == protocol.CmdAgentUpdate {
			if pending == nil || pending.ID != cmd.ID {
				a.finishErr(cmd.ID, protocol.NewError(protocol.CodeUpdateFailed, "обновление прервано перезапуском агента"))
			}
			continue
		}
		a.log.Info("команда прошлого запуска агента — продолжение", "id", cmd.ID, "type", cmd.Type)
		go a.execute(cmd)
	}
}

// execute — выполнить команду; итог — в очередь (worker.command — итог пришлёт воркер).
func (a *App) execute(cmd protocol.Command) {
	if cmd.Deadline > 0 && time.Now().UnixMilli() > cmd.Deadline {
		a.finishErr(cmd.ID, protocol.NewError(protocol.CodeExpired, "срок команды истёк до начала"))
		return
	}
	switch cmd.Type {
	case protocol.CmdWorkerCommand:
		var args protocol.WorkerCommandArgs
		if err := decodeArgs(cmd, &args); err != nil {
			a.finishErr(cmd.ID, err)
			return
		}
		if args.Worker == "" || args.Type == "" {
			a.finishErr(cmd.ID, invalid("нужны worker и type"))
			return
		}
		a.dispatch(cmd.ID, cmd.Deadline, args, nil)
		return
	}
	ctx := worker.WithDeferred(context.Background(), func(pending string) {
		a.log.Info("воркер занят — замена отложена до окончания его работы", "id", cmd.ID, "type", cmd.Type, "pending", pending)
	})
	result, err := a.run(ctx, cmd)
	if errors.Is(err, errDeferred) {
		return
	}
	if err != nil {
		a.finishErr(cmd.ID, err)
		return
	}
	r := protocol.Result{Re: cmd.ID, OK: true, Result: json.RawMessage("{}")}
	if result != nil {
		raw, merr := json.Marshal(result)
		if merr != nil {
			a.finishErr(cmd.ID, merr)
			return
		}
		r.Result = raw
	}
	a.finish(r)
}

// errDeferred — итог команды отправит следующий запуск агента (agent.update).
var errDeferred = errors.New("итог — после перезапуска")

// finish — итог команды бэкенда в журнал и очередь.
func (a *App) finish(r protocol.Result) {
	if err := a.store.FinishCommand(r); err != nil {
		a.log.Error("итог команды не записан", "id", r.Re, "err", err)
	}
	if !r.OK && r.Error != nil {
		a.log.Warn("команда не выполнена", "id", r.Re, "code", r.Error.Code, "err", r.Error.Message)
	}
	a.statusChanged()
}

// finishErr — итог-ошибка: *protocol.ErrorInfo — как есть, другая — INTERNAL.
func (a *App) finishErr(id string, err error) {
	var ei *protocol.ErrorInfo
	if !errors.As(err, &ei) {
		ei = protocol.NewError(protocol.CodeInternal, err.Error())
	}
	a.finish(protocol.Result{Re: id, Error: ei})
}

func invalid(format string, args ...any) error {
	return protocol.NewError(protocol.CodeInvalid, fmt.Sprintf(format, args...))
}

// decodeArgs — args команды в v; нет args — пустой объект.
func decodeArgs(cmd protocol.Command, v any) error {
	raw := cmd.Args
	if len(raw) == 0 {
		raw = json.RawMessage("{}")
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return invalid("args: %v", err)
	}
	return nil
}

func unknownWorker(name string) error {
	return protocol.NewError(protocol.CodeWorkerUnknown, fmt.Sprintf("воркера %q нет в настройках агента", name))
}

// run — команды, которые агент выполняет сам.
func (a *App) run(ctx context.Context, cmd protocol.Command) (any, error) {
	switch cmd.Type {
	case protocol.CmdWorkerCancel:
		var args protocol.WorkerCancelArgs
		if err := decodeArgs(cmd, &args); err != nil {
			return nil, err
		}
		return nil, a.cancelCall(args)
	case protocol.CmdWorkerRestart:
		var args protocol.WorkerRestartArgs
		if err := decodeArgs(cmd, &args); err != nil {
			return nil, err
		}
		if !a.workers.Has(args.Worker) {
			return nil, unknownWorker(args.Worker)
		}
		if err := a.workers.Restart(ctx, args.Worker, args.Force); err != nil {
			return nil, protocol.NewError(protocol.CodeWorkerUnavailable, fmt.Sprintf("%s: %v", args.Worker, err))
		}
		return nil, nil
	case protocol.CmdWorkerUpdate:
		var args protocol.WorkerUpdateArgs
		if err := decodeArgs(cmd, &args); err != nil {
			return nil, err
		}
		if args.Worker == "" || args.Version == "" || args.URL == "" || args.SHA256 == "" {
			return nil, invalid("нужны worker, version, url, sha256, signature")
		}
		return a.workerUpdate(ctx, args)
	case protocol.CmdAgentUpdate:
		var args protocol.AgentUpdateArgs
		if err := decodeArgs(cmd, &args); err != nil {
			return nil, err
		}
		if args.Version == "" {
			return nil, invalid("нужна version")
		}
		return a.agentUpdate(ctx, cmd.ID, args)
	case protocol.CmdAgentRotateKey:
		return a.auth.rotate()
	case protocol.CmdAgentLogs:
		var args protocol.AgentLogsArgs
		if err := decodeArgs(cmd, &args); err != nil {
			return nil, err
		}
		return a.logs(args)
	}
	return nil, protocol.NewError(protocol.CodeUnknownCommand, fmt.Sprintf("команда %q агенту не известна", cmd.Type))
}

// logs — agent.logs: последние записи журнала агента или воркера, не больше предела итога.
func (a *App) logs(args protocol.AgentLogsArgs) (protocol.LogsResult, error) {
	source := protocol.LogSourceAgent
	if args.Worker != "" {
		if !a.workers.Has(args.Worker) {
			return protocol.LogsResult{}, unknownWorker(args.Worker)
		}
		source = args.Worker
	}
	lines := args.Lines
	if lines <= 0 {
		lines = protocol.DefaultLogLines
	}
	entries := a.journal.Tail(source, min(lines, protocol.MaxLogLines))
	for len(entries) > 0 {
		raw, _ := json.Marshal(entries)
		if len(raw) <= protocol.MaxResultBytes-64 {
			break
		}
		entries = entries[len(entries)/4+1:]
	}
	if entries == nil {
		entries = []protocol.LogEntry{}
	}
	return protocol.LogsResult{Entries: entries}, nil
}

// source — откуда скачать сборку по url из worker.update (§10): путь от корня (/…) — с бэкенда,
// с ключом агента; абсолютная https:// — с любого хоста (ключ агента — только своему бэкенду);
// http:// — только если и бэкенд http://. auth — заголовок Authorization ("" — без).
func (a *App) source(raw string) (src, auth string, err error) {
	server := strings.TrimRight(a.config().Server.URL, "/")
	if strings.HasPrefix(raw, "/") && !strings.HasPrefix(raw, "//") {
		if server == "" {
			return "", "", protocol.NewError(protocol.CodeUpdateFailed, "путь сборки без бэкенда: нужна ссылка https://")
		}
		return server + raw, a.auth.session(), nil
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" || u.User != nil {
		return "", "", protocol.NewError(protocol.CodeUpdateFailed, fmt.Sprintf("адрес сборки %q: нужен путь от корня (/…) или ссылка https://", raw))
	}
	if !releases.SchemeAllowed(u, server) {
		return "", "", protocol.NewError(protocol.CodeUpdateFailed, fmt.Sprintf("адрес сборки %q: нужна ссылка https:// (http:// — только если и бэкенд http://)", raw))
	}
	s, _ := url.Parse(server)
	if s != nil && strings.EqualFold(u.Scheme, s.Scheme) && strings.EqualFold(u.Host, s.Host) {
		return u.String(), a.auth.session(), nil
	}
	return u.String(), "", nil
}

// workerUpdate — worker.update (§10): сборка скачивается, сверяются sha256 и подпись, воркер
// заменяется с возвратом прежней сборки.
func (a *App) workerUpdate(ctx context.Context, args protocol.WorkerUpdateArgs) (any, error) {
	if !a.workers.Has(args.Worker) {
		return nil, unknownWorker(args.Worker)
	}
	if a.config().Update.Mode == config.UpdateDisabled {
		return nil, protocol.NewError(protocol.CodeUpdateUnsupported, "обновления выключены настройкой update.mode: disabled")
	}
	src, auth, err := a.source(args.URL)
	if err != nil {
		return nil, err
	}
	build := update.Local(args.Worker, args.Version, args.SHA256)
	fetch := func(dst string) error {
		if len(a.keys) == 0 {
			return protocol.NewError(protocol.CodeUpdateNotVerified, update.ErrNotVerified.Error())
		}
		if releases.IsArchive(src) {
			return update.FetchArchive(ctx, a.client, auth, a.keys, build, src, args.Signature, dst)
		}
		return update.Fetch(ctx, a.client, auth, a.keys, build, src, args.Signature, dst)
	}
	return a.workers.Update(ctx, args.Worker, args.Version, args.Force, fetch)
}

// pendingUpdate — agent.update ждёт запуска новой версии; файл в каталоге данных.
type pendingUpdate struct {
	ID       string `json:"id"`
	Version  string `json:"version"`
	Previous string `json:"previous"`
}

const pendingUpdateFile = "agent-update.json"

func (a *App) pendingUpdate() *pendingUpdate {
	raw, err := os.ReadFile(filepath.Join(a.config().DataDir, pendingUpdateFile))
	if err != nil {
		return nil
	}
	var p pendingUpdate
	if json.Unmarshal(raw, &p) != nil || p.ID == "" {
		return nil
	}
	return &p
}

// restartable — запустится ли агент после перезапуска: его файл настроек на месте и читается.
// Нет — обновление не начинается: агент с новой версией не стартовал бы, а откатить её без
// менеджера службы некому.
func (a *App) restartable() error {
	if _, err := config.Load(a.configPath); err != nil {
		return fmt.Errorf("после перезапуска агент не запустится — настройки %s не читаются: %w", a.configPath, err)
	}
	return nil
}

// agentUpdate — agent.update (§10): взять сборку версии из своего каталога сборок
// (update.releases), проверить, заменить файл и перезапуститься; итог отправит новая версия
// после hello.
func (a *App) agentUpdate(ctx context.Context, id string, args protocol.AgentUpdateArgs) (any, error) {
	switch a.config().Update.Mode {
	case config.UpdateExternal:
		return nil, protocol.NewError(protocol.CodeUpdateUnsupported, "агент в контейнере обновляется новым образом (update.mode: external)")
	case config.UpdateDisabled:
		return nil, protocol.NewError(protocol.CodeUpdateUnsupported, "обновления выключены настройкой update.mode: disabled")
	}
	if len(a.keys) == 0 {
		return nil, protocol.NewError(protocol.CodeUpdateNotVerified, update.ErrNotVerified.Error())
	}
	if args.Version == a.version {
		return protocol.UpdateResult{Version: a.version, Previous: a.version}, nil
	}
	if err := a.restartable(); err != nil {
		return nil, protocol.NewError(protocol.CodeUpdateFailed, err.Error())
	}
	rel, err := releases.AgentBuild(ctx, releasesClient, a.config().Update.Releases, args.Version, runtime.GOOS, runtime.GOARCH)
	if err != nil {
		return nil, protocol.NewError(protocol.CodeUpdateFailed, err.Error())
	}
	raw, _ := json.Marshal(pendingUpdate{ID: id, Version: args.Version, Previous: a.version})
	path := filepath.Join(a.config().DataDir, pendingUpdateFile)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return nil, err
	}
	if err := update.Install(ctx, a.client, "", a.update, a.keys, rel); err != nil {
		_ = os.Remove(path)
		if errors.Is(err, update.ErrNotVerified) {
			return nil, protocol.NewError(protocol.CodeUpdateNotVerified, err.Error())
		}
		return nil, protocol.NewError(protocol.CodeUpdateFailed, err.Error())
	}
	a.log.Info("новая версия агента установлена — перезапуск", "version", args.Version)
	time.AfterFunc(time.Second, a.requestRestart)
	return nil, errDeferred
}

// finishAgentUpdate — после hello: итог agent.update, начатого прежним запуском. Работает
// другая версия — новая не вышла на связь и возвращена прежняя (boot guard).
func (a *App) finishAgentUpdate() {
	p := a.pendingUpdate()
	if p == nil {
		return
	}
	_ = os.Remove(filepath.Join(a.config().DataDir, pendingUpdateFile))
	if p.Version == a.version {
		raw, _ := json.Marshal(protocol.UpdateResult{Version: p.Version, Previous: p.Previous})
		a.finish(protocol.Result{Re: p.ID, OK: true, Result: raw})
		return
	}
	a.finishErr(p.ID, protocol.NewError(protocol.CodeUpdateFailed,
		fmt.Sprintf("версия %s не вышла на связь — работает %s", p.Version, a.version)))
}
