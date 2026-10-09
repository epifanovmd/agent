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
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/update"
	"github.com/epifanovmd/agent/internal/worker"
)

// actions — встроенные действия, которые выполняются сейчас (по id).
type actions struct {
	mu      sync.Mutex
	running map[string]bool
}

// errDeferred — итог действия отправит следующий запуск агента (agent.update).
var errDeferred = errors.New("итог — после перезапуска")

// action — сообщение action (§10): один раз на id, итог — action.result.
func (a *App) action(env message.Envelope) {
	if env.ID == "" {
		a.log.Warn("action без id — пропущено")
		return
	}
	a.act.mu.Lock()
	if a.act.running == nil {
		a.act.running = map[string]bool{}
	}
	if a.act.running[env.ID] {
		a.act.mu.Unlock()
		return // повтор, пока действие идёт
	}
	a.act.running[env.ID] = true
	a.act.mu.Unlock()
	go func() {
		defer func() {
			a.act.mu.Lock()
			delete(a.act.running, env.ID)
			a.act.mu.Unlock()
		}()
		var act message.Action
		var result any
		// deferred — замена занятого воркера отложена: action.result уже ушёл
		// ({deferred, pending}), итог — action.done (§10).
		var deferred atomic.Bool
		ctx := worker.WithDeferred(context.Background(), func(pending string) {
			deferred.Store(true)
			a.log.Info("воркер занят — замена отложена, итог придёт в action.done", "action", act.Name, "id", env.ID)
			a.actionResult(env.ID, message.DeferredResult{Deferred: true, Pending: pending}, nil)
		})
		err := env.Decode(&act)
		if err != nil {
			err = message.NewError(message.CodeMessageInvalid, err.Error())
		} else {
			a.log.Info("действие сервера", "action", act.Name, "id", env.ID)
			result, err = a.runAction(ctx, env.ID, act)
		}
		switch {
		case errors.Is(err, errDeferred):
		case deferred.Load():
			a.actionDone(env.ID, act, result, err)
		default:
			a.actionResult(env.ID, result, err)
		}
	}()
}

// outcome — ok, result и error итога действия.
func (a *App) outcome(id string, result any, err error) (bool, json.RawMessage, *message.ErrorInfo) {
	if err != nil {
		var ei *message.ErrorInfo
		if !errors.As(err, &ei) {
			ei = message.NewError(message.CodeActionFailed, err.Error())
		}
		a.log.Warn("действие не выполнено", "id", id, "code", ei.Code, "err", ei.Message)
		return false, nil, ei
	}
	if result == nil {
		return true, nil, nil
	}
	raw, merr := json.Marshal(result)
	if merr != nil {
		return true, nil, nil
	}
	return true, raw, nil
}

// actionDone — action.done с re = id действия: итог отложенной замены
// воркера (важное сообщение).
func (a *App) actionDone(id string, act message.Action, result any, err error) {
	var args struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(act.Args, &args)
	done := message.ActionDone{Name: act.Name, Worker: args.Name}
	done.OK, done.Result, done.Error = a.outcome(id, result, err)
	env := message.MustNew(message.TypeActionDone, done)
	env.Re = id
	if err := a.link.Important(env); err != nil {
		a.log.Error("action.done не записан в outbox", "err", err)
	}
}

// actionResult — action.result с re = id действия (важное сообщение).
func (a *App) actionResult(id string, result any, err error) {
	var res message.ActionResult
	res.OK, res.Result, res.Error = a.outcome(id, result, err)
	env := message.MustNew(message.TypeActionResult, res)
	env.Re = id
	if err := a.link.Important(env); err != nil {
		a.log.Error("action.result не записан в outbox", "err", err)
	}
}

func invalid(format string, args ...any) error {
	return message.NewError(message.CodeMessageInvalid, fmt.Sprintf(format, args...))
}

// decodeArgs — args действия в v; нет args — пустой объект.
func decodeArgs(act message.Action, v any) error {
	raw := act.Args
	if len(raw) == 0 {
		raw = json.RawMessage("{}")
	}
	if err := json.Unmarshal(raw, v); err != nil {
		return invalid("args: %v", err)
	}
	return nil
}

func (a *App) runAction(ctx context.Context, id string, act message.Action) (any, error) {
	switch act.Name {
	case message.ActionWorkerRestart:
		var args message.WorkerRestartArgs
		if err := decodeArgs(act, &args); err != nil {
			return nil, err
		}
		if args.Name == "" {
			return nil, invalid("нужно name — имя воркера")
		}
		if !a.serverWorker(args.Name) {
			return nil, unknownWorker(args.Name)
		}
		if err := a.workers.Restart(ctx, args.Name, args.Force); err != nil {
			return nil, message.NewError(message.CodeActionFailed, fmt.Sprintf("%s: %v", args.Name, err))
		}
		return nil, nil
	case message.ActionWorkerUpdate:
		var args message.WorkerUpdateArgs
		if err := decodeArgs(act, &args); err != nil {
			return nil, err
		}
		if args.Name == "" || args.Version == "" || args.URL == "" || args.SHA256 == "" {
			return nil, invalid("нужны name, version, url, sha256, signature")
		}
		return a.workerUpdate(ctx, args)
	case message.ActionAgentUpdate:
		var args message.AgentUpdateArgs
		if err := decodeArgs(act, &args); err != nil {
			return nil, err
		}
		if args.Version == "" || args.URL == "" || args.SHA256 == "" {
			return nil, invalid("нужны version, url, sha256, signature")
		}
		return a.agentUpdate(ctx, id, args)
	case message.ActionAgentRotateKey:
		return a.auth.rotate()
	case message.ActionAgentLogs:
		var args message.AgentLogsArgs
		if err := decodeArgs(act, &args); err != nil {
			return nil, err
		}
		return a.logs(args)
	}
	return nil, message.NewError(message.CodeActionUnknown, fmt.Sprintf("действие %q не встроено в агента", act.Name))
}

func unknownWorker(name string) error {
	return message.NewError(message.CodeWorkerUnknown, fmt.Sprintf("воркера %q нет в настройках агента", name))
}

// logs — agent.logs: последние записи журнала агента или воркера, не больше
// предела action.result.
func (a *App) logs(args message.AgentLogsArgs) (message.LogsResult, error) {
	source := message.LogSourceAgent
	if args.Worker != "" {
		if !a.workers.Has(args.Worker) {
			return message.LogsResult{}, unknownWorker(args.Worker)
		}
		source = args.Worker
	}
	lines := args.Lines
	if lines <= 0 {
		lines = message.DefaultLogLines
	}
	entries := a.journal.Tail(source, min(lines, message.MaxLogLines))
	for len(entries) > 0 {
		raw, _ := json.Marshal(entries)
		if len(raw) <= message.MaxActionResultBytes-64 {
			break
		}
		entries = entries[len(entries)/4+1:]
	}
	return message.LogsResult{Entries: entries}, nil
}

// resolveURL — url от корня (/api/…) дополняется адресом сервера (§10).
func (a *App) resolveURL(u string) string {
	if strings.HasPrefix(u, "/") {
		return strings.TrimRight(a.link.ServerURL(), "/") + u
	}
	return u
}

// isArchive — сборка воркера — архив .tar.gz (по имени файла в url).
func isArchive(rawURL string) bool {
	p := rawURL
	if u, err := url.Parse(rawURL); err == nil {
		p = u.Path
	}
	return strings.HasSuffix(p, ".tar.gz")
}

// workerUpdate — worker.update (§11): сборка скачивается со своим ключом,
// сверяются sha256 и подпись, воркер заменяется с возвратом прежней сборки.
func (a *App) workerUpdate(ctx context.Context, args message.WorkerUpdateArgs) (any, error) {
	if !a.serverWorker(args.Name) {
		return nil, unknownWorker(args.Name)
	}
	if a.config().Update.Mode == config.UpdateDisabled {
		return nil, message.NewError(message.CodeUpdateNotSupported, "обновления выключены настройкой update.mode: disabled")
	}
	src := a.resolveURL(args.URL)
	build := update.Local(args.Name, args.Version, args.SHA256)
	fetch := func(dst string) error {
		if a.pubKey == nil {
			return message.NewError(message.CodeUpdateNotVerified, "не задан ключ проверки сборок (update.publicKey)")
		}
		var err error
		if isArchive(src) {
			err = update.FetchArchive(ctx, a.client, a.auth.Session(), a.pubKey, build, src, args.Signature, dst)
		} else {
			err = update.Fetch(ctx, a.client, a.auth.Session(), a.pubKey, build, src, args.Signature, dst)
		}
		return err
	}
	return a.workers.Update(ctx, args.Name, args.Version, args.Force, fetch)
}

// pendingUpdate — agent.update ждёт запуска новой версии; файл в каталоге данных.
type pendingUpdate struct {
	ID       string `json:"id"`
	Version  string `json:"version"`
	Previous string `json:"previous"`
}

const pendingUpdateFile = "agent-update.json"

// agentUpdate — agent.update (§11): скачать, проверить, заменить файл и
// перезапуститься; итог отправит новая версия после welcome.
func (a *App) agentUpdate(ctx context.Context, id string, args message.AgentUpdateArgs) (any, error) {
	switch a.config().Update.Mode {
	case config.UpdateExternal:
		return nil, message.NewError(message.CodeUpdateNotSupported, "агент в контейнере обновляется новым образом (update.mode: external)")
	case config.UpdateDisabled:
		return nil, message.NewError(message.CodeUpdateNotSupported, "обновления выключены настройкой update.mode: disabled")
	}
	if a.pubKey == nil {
		return nil, message.NewError(message.CodeUpdateNotVerified, "не задан ключ проверки сборок (update.publicKey)")
	}
	if args.Version == a.version {
		return message.UpdateResult{Version: a.version, Previous: a.version}, nil
	}
	rel := update.Release{Version: args.Version, URL: a.resolveURL(args.URL), SHA256: args.SHA256, Signature: args.Signature}
	pending := pendingUpdate{ID: id, Version: args.Version, Previous: a.version}
	raw, _ := json.Marshal(pending)
	path := filepath.Join(a.config().DataDir, pendingUpdateFile)
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		return nil, err
	}
	if err := update.Install(ctx, a.client, a.auth.Session(), a.update, a.pubKey, rel); err != nil {
		_ = os.Remove(path)
		if errors.Is(err, update.ErrNotVerified) {
			return nil, message.NewError(message.CodeUpdateNotVerified, err.Error())
		}
		return nil, message.NewError(message.CodeUpdateFailed, err.Error())
	}
	a.log.Info("новая версия агента установлена — перезапуск", "version", args.Version)
	time.AfterFunc(time.Second, a.requestRestart)
	return nil, errDeferred
}

// finishAgentUpdate — после welcome: итог agent.update, начатого прежним
// запуском. Работает другая версия — новая не вышла на связь и возвращена
// прежняя (boot guard).
func (a *App) finishAgentUpdate() {
	path := filepath.Join(a.config().DataDir, pendingUpdateFile)
	raw, err := os.ReadFile(path)
	if err != nil {
		return
	}
	_ = os.Remove(path)
	var p pendingUpdate
	if json.Unmarshal(raw, &p) != nil || p.ID == "" {
		return
	}
	if p.Version == a.version {
		a.actionResult(p.ID, message.UpdateResult{Version: p.Version, Previous: p.Previous}, nil)
		return
	}
	a.actionResult(p.ID, nil, message.NewError(message.CodeUpdateFailed,
		fmt.Sprintf("версия %s не вышла на связь — работает %s", p.Version, a.version)))
}

// serverWorker — воркер из настроек агента, видимый серверу (не встроенный).
func (a *App) serverWorker(name string) bool { return configWorkers{a.workers}.Has(name) }
