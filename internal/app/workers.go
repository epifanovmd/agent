//go:build unix

package app

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"sync"
	"time"

	"github.com/epifanovmd/agent/internal/store"
	"github.com/epifanovmd/agent/internal/worker"
	"github.com/epifanovmd/agent/protocol"
)

// ─── желаемое (§7, §14) ────────────────────────────────────────────────

// desiredState — последнее желаемое и что из него уже отправлено воркерам.
type desiredState struct {
	mu   sync.Mutex
	d    protocol.Desired
	sent map[string]string
}

// setDesired — новое желаемое целиком: воркерам — их части, наблюдение — сразу.
func (a *App) setDesired(d protocol.Desired) {
	a.desired.mu.Lock()
	a.desired.d = d
	a.desired.mu.Unlock()
	for _, name := range a.workers.Names() {
		a.pushDesired(name, false)
	}
	a.setWatch(d.Watch)
	a.statusChanged()
}

// currentDesired — последнее желаемое.
func (a *App) currentDesired() protocol.Desired {
	a.desired.mu.Lock()
	defer a.desired.mu.Unlock()
	return a.desired.d
}

// workerDesired — часть желаемого воркера name: только ключи из его manifest.configs.
func (a *App) workerDesired(name string) protocol.WorkerDesiredEvent {
	m := a.workers.Manifest(name)
	out := protocol.WorkerDesiredEvent{Configs: map[string]protocol.ConfigValue{}}
	a.desired.mu.Lock()
	defer a.desired.mu.Unlock()
	for key, v := range a.desired.d.Workers[name].Configs {
		if m.DeclaresConfig(key) {
			out.Configs[key] = v
		}
	}
	return out
}

// pushDesired — часть желаемого в поток воркера, если она изменилась (force — всегда).
func (a *App) pushDesired(name string, force bool) {
	if a.sock == nil || a.workers.Manifest(name) == nil {
		return
	}
	raw, _ := json.Marshal(a.workerDesired(name))
	a.desired.mu.Lock()
	if a.desired.sent == nil {
		a.desired.sent = map[string]string{}
	}
	same := a.desired.sent[name] == string(raw)
	a.desired.mu.Unlock()
	if same && !force {
		return
	}
	if a.sock.SendJSON(name, protocol.SSEDesired, json.RawMessage(raw)) {
		a.desired.mu.Lock()
		a.desired.sent[name] = string(raw)
		a.desired.mu.Unlock()
	}
}

// configsStatus — ключи желаемого воркера и итоги их применения (§9 state).
func (a *App) configsStatus(name string, applied map[string]protocol.AppliedStatus) map[string]protocol.AppliedStatus {
	m := a.workers.Manifest(name)
	a.desired.mu.Lock()
	want := maps.Clone(a.desired.d.Workers[name].Configs)
	a.desired.mu.Unlock()
	if len(want) == 0 {
		return nil
	}
	out := map[string]protocol.AppliedStatus{}
	for key, v := range want {
		switch st, ok := applied[key]; {
		case m != nil && !m.DeclaresConfig(key):
			no := false
			out[key] = protocol.AppliedStatus{Version: v.Version, OK: &no,
				Error: protocol.NewError(protocol.CodeConfigRejected, "ключ не объявлен в manifest.configs воркера")}
		case ok && st.Version == v.Version:
			out[key] = st
		default:
			out[key] = protocol.AppliedStatus{Version: v.Version}
		}
	}
	return out
}

// ─── команды воркерам (§15) ────────────────────────────────────────────

// calls — команды, переданные воркерам и ждущие итога (по id).
type calls struct {
	mu   sync.Mutex
	byID map[string]*call
}

func (c *calls) init() { c.byID = map[string]*call{} }

// call — команда воркеру: от бэкенда (итог — в очередь) или из локального API (итог — в done).
type call struct {
	worker string
	cmd    protocol.Command
	timer  *time.Timer
	// done — итог для локального API (nil — команда бэкенда).
	done chan protocol.Result
}

// dispatch — worker.command: команда воркеру с тем же id; итог придёт от воркера.
func (a *App) dispatch(id string, deadline int64, args protocol.WorkerCommandArgs, done chan protocol.Result) {
	if !a.workers.Has(args.Worker) {
		a.finishCall(&call{cmd: protocol.Command{ID: id}, done: done}, protocol.Result{Re: id,
			Error: protocol.NewError(protocol.CodeWorkerUnknown, fmt.Sprintf("воркера %q нет в настройках агента", args.Worker))})
		return
	}
	if args.TimeoutMs > 0 {
		limit := time.Now().Add(time.Duration(args.TimeoutMs) * time.Millisecond).UnixMilli()
		if deadline == 0 || limit < deadline {
			deadline = limit
		}
	}
	c := &call{worker: args.Worker, cmd: protocol.Command{ID: id, Type: args.Type, Args: args.Args, Deadline: deadline}, done: done}
	a.calls.mu.Lock()
	if _, ok := a.calls.byID[id]; ok {
		a.calls.mu.Unlock()
		return
	}
	a.calls.byID[id] = c
	if deadline > 0 {
		c.timer = time.AfterFunc(time.Until(time.UnixMilli(deadline)), func() {
			if a.takeCall(id, "") == c {
				a.sock.SendJSON(c.worker, protocol.SSECancel, protocol.Cancel{Re: id})
				a.finishCall(c, protocol.Result{Re: id, Error: protocol.NewError(protocol.CodeTimeout, "воркер не прислал итог в срок")})
			}
		})
	}
	a.calls.mu.Unlock()
	a.deliver(c)
}

// deliver — команда в поток воркера; тип не объявлен в манифесте — итог COMMAND_UNDECLARED.
// Воркер не подключён — команда уйдёт, когда он откроет поток.
func (a *App) deliver(c *call) {
	m := a.workers.Manifest(c.worker)
	if m == nil {
		return
	}
	if !m.DeclaresCommand(c.cmd.Type) {
		if a.takeCall(c.cmd.ID, c.worker) == c {
			a.finishCall(c, protocol.Result{Re: c.cmd.ID, Error: protocol.NewError(protocol.CodeCommandUndeclared,
				fmt.Sprintf("команда %q не объявлена в manifest.commands воркера %q", c.cmd.Type, c.worker))})
		}
		return
	}
	a.sock.SendJSON(c.worker, protocol.SSECommand, c.cmd)
}

// takeCall — убрать команду id (воркера worker; "" — любого); nil — её нет.
func (a *App) takeCall(id, worker string) *call {
	a.calls.mu.Lock()
	defer a.calls.mu.Unlock()
	c := a.calls.byID[id]
	if c == nil || (worker != "" && c.worker != worker) {
		return nil
	}
	delete(a.calls.byID, id)
	if c.timer != nil {
		c.timer.Stop()
	}
	return c
}

// workerCalls — команды воркера name, ждущие итога.
func (a *App) workerCalls(name string) []*call {
	a.calls.mu.Lock()
	defer a.calls.mu.Unlock()
	var out []*call
	for _, c := range a.calls.byID {
		if c.worker == name {
			out = append(out, c)
		}
	}
	return out
}

// finishCall — итог команды воркеру: бэкенду — через очередь, локальному API — в канал.
func (a *App) finishCall(c *call, r protocol.Result) {
	if !r.OK && r.Error == nil {
		r.Error = protocol.NewError(protocol.CodeInternal, "итог без ok и error")
	}
	if c.done != nil {
		c.done <- r
		return
	}
	a.finish(r)
}

// cancelCall — worker.cancel: отменить идущую команду воркера.
func (a *App) cancelCall(args protocol.WorkerCancelArgs) error {
	a.calls.mu.Lock()
	c := a.calls.byID[args.Re]
	a.calls.mu.Unlock()
	if c == nil || c.worker != args.Worker {
		return protocol.NewError(protocol.CodeInvalid, fmt.Sprintf("у воркера %q нет идущей команды %q", args.Worker, args.Re))
	}
	a.sock.SendJSON(c.worker, protocol.SSECancel, protocol.Cancel{Re: args.Re})
	return nil
}

// ─── сокет агента ──────────────────────────────────────────────────────

// sockAgent — что воркеры присылают через сокет агента (§12).
type sockAgent struct{ a *App }

func (s sockAgent) ByToken(token string) (string, bool) { return s.a.workers.ByToken(token) }

// workerError — ошибка Supervisor для воркера.
func workerError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, worker.ErrNotRegistered):
		return protocol.NewError(protocol.CodeWorkerInvalid, "воркер не зарегистрирован — нужен POST /register")
	case errors.Is(err, worker.ErrUnavailable):
		return protocol.NewError(protocol.CodeWorkerUnavailable, err.Error())
	case errors.Is(err, worker.ErrUnknown):
		return protocol.NewError(protocol.CodeWorkerUnknown, err.Error())
	}
	return err
}

func (s sockAgent) Register(name string, reg protocol.Register) error {
	return workerError(s.a.workers.Register(name, reg))
}

func (s sockAgent) Heartbeat(name string, h protocol.Health) error {
	return workerError(s.a.workers.Heartbeat(name, h))
}

// registered — воркер зарегистрирован; иначе ошибка для него.
func (s sockAgent) registered(name string) (*protocol.WorkerManifest, error) {
	if err := s.a.workers.Ready(name); err != nil {
		return nil, workerError(err)
	}
	return s.a.workers.Manifest(name), nil
}

// StreamOpened — поток воркера открыт: его часть желаемого и команды, ждущие итога.
func (s sockAgent) StreamOpened(name string) error {
	if _, err := s.registered(name); err != nil {
		return err
	}
	go func() {
		s.a.pushDesired(name, true)
		for _, c := range s.a.workerCalls(name) {
			s.a.deliver(c)
		}
	}()
	return nil
}

func (s sockAgent) Applied(name string, items []protocol.Applied) error {
	if _, err := s.registered(name); err != nil {
		return err
	}
	for _, it := range items {
		if it.Key == "" {
			return protocol.NewError(protocol.CodeInvalid, "applied: нужен key")
		}
		if err := s.a.store.SetApplied(name, it); err != nil {
			return err
		}
		if !it.OK {
			msg := ""
			if it.Error != nil {
				msg = it.Error.Message
			}
			s.a.log.Warn("воркер не применил настройки", "worker", name, "key", it.Key, "version", it.Version, "err", msg)
		}
	}
	s.a.statusChanged()
	return nil
}

func (s sockAgent) Results(name string, items []protocol.Result) error {
	for _, r := range items {
		c := s.a.takeCall(r.Re, name)
		if c == nil {
			s.a.log.Debug("итог команды, которую агент не ждёт, — пропущен", "worker", name, "re", r.Re)
			continue
		}
		if len(r.Result) > protocol.MaxResultBytes {
			r = protocol.Result{Re: r.Re, Error: protocol.NewError(protocol.CodeBodyTooLarge,
				fmt.Sprintf("итог воркера больше %d байт", protocol.MaxResultBytes))}
		}
		s.a.finishCall(c, r)
	}
	return nil
}

func (s sockAgent) Progress(name string, p protocol.Progress) error {
	s.a.calls.mu.Lock()
	c := s.a.calls.byID[p.Re]
	s.a.calls.mu.Unlock()
	if c == nil || c.worker != name || c.done != nil {
		return nil
	}
	p.Worker = name
	p.Progress = min(max(p.Progress, 0), 1)
	data, _ := json.Marshal(p)
	return s.a.appendEvents([]protocol.Event{{ID: protocol.NewID(), Worker: name, Type: protocol.EventCommandProgress,
		Data: data, At: time.Now().UnixMilli()}})
}

func (s sockAgent) Events(name string, items []protocol.WorkerEvent) error {
	m, err := s.registered(name)
	if err != nil {
		return err
	}
	if len(items) > protocol.MaxBatch {
		return protocol.NewError(protocol.CodeInvalid, fmt.Sprintf("в пачке больше %d событий", protocol.MaxBatch))
	}
	now := time.Now().UnixMilli()
	events := make([]protocol.Event, 0, len(items))
	for _, e := range items {
		switch {
		case e.ID == "" || len(e.ID) > 128:
			return protocol.NewError(protocol.CodeInvalid, "событие: нужен id до 128 символов")
		case !m.DeclaresEvent(e.Type):
			return protocol.NewError(protocol.CodeEventUndeclared, fmt.Sprintf("событие %q не объявлено в manifest.events", e.Type))
		case len(e.Data) > protocol.MaxEventDataBytes:
			return protocol.NewError(protocol.CodeBodyTooLarge, fmt.Sprintf("data события больше %d байт", protocol.MaxEventDataBytes))
		}
		events = append(events, protocol.Event{ID: e.ID, Worker: name, Type: e.Type, Data: e.Data, At: now})
	}
	return s.a.appendEvents(events)
}

// appendEvents — события в очередь на диске; без бэкенда — некому, не хранятся.
func (a *App) appendEvents(events []protocol.Event) error {
	if a.backend == nil {
		return nil
	}
	err := a.store.AppendEvents(events)
	if errors.Is(err, store.ErrFull) {
		return protocol.NewError(protocol.CodeOutboxFull, "очередь агента полна — повторите позже")
	}
	if err == nil {
		a.statusChanged()
	}
	return err
}

func (s sockAgent) Request(ctx context.Context, name string, r protocol.Request) protocol.RequestReply {
	m, err := s.registered(name)
	if err != nil {
		var ei *protocol.ErrorInfo
		errors.As(err, &ei)
		return protocol.RequestReply{Error: ei}
	}
	if !m.DeclaresRequest(r.Type) {
		return protocol.RequestReply{Error: protocol.NewError(protocol.CodeRequestUndeclared,
			fmt.Sprintf("запрос %q не объявлен в manifest.requests", r.Type))}
	}
	if len(r.Data) > protocol.MaxEventDataBytes {
		return protocol.RequestReply{Error: protocol.NewError(protocol.CodeBodyTooLarge,
			fmt.Sprintf("data запроса больше %d байт", protocol.MaxEventDataBytes))}
	}
	if s.a.backend == nil {
		return protocol.RequestReply{Error: protocol.NewError(protocol.CodeAgentOffline, "агент работает без бэкенда")}
	}
	r.ID, r.Worker = protocol.NewID(), name
	return s.a.backend.Request(ctx, r)
}
