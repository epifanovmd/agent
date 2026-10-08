package server

import (
	"errors"
	"maps"
	"reflect"
	"slices"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// session — сессия агента: исходящее копится в очереди; WebSocket пишет его
// сразу, HTTP sync отдаёт ответом. Все поля — под Agents.mu.
type session struct {
	id      string
	agentID string
	mode    string
	baseURL string // адрес сервера для ссылок на файлы
	address string // адрес агента в этом подключении (Agent.Address)
	outq    []message.Envelope
	notify  chan struct{} // закрывается при новом исходящем и закрытии
	closed  bool
	code    int
	greeted bool
	// pending — выданные задачи, ещё не перечисленные в status: jobId →
	// очередь (занимают слот).
	pending map[string]string
	sent    map[string]bool  // команды, отправленные в сессии
	known   map[string]int64 // домен → версия, известная агенту
	status  *message.Status
	caps    *message.Capabilities
	// sub — сводная подписка, заданная агенту (welcome, config); subUntil —
	// ближайший срок её подписок (0 — подписок нет).
	sub      message.Subscription
	subUntil int64
	// lastActive — последний запрос HTTP sync.
	lastActive time.Time
	// closeAfter — закрыть сессию этим кодом после ответа (ack) на текущее
	// сообщение (agent.rotateKey — 1012).
	closeAfter int
}

func (a *Agents) newSession(agentID, mode, baseURL string) *session {
	return &session{
		id: newID(), agentID: agentID, mode: mode, baseURL: baseURL,
		notify: make(chan struct{}), pending: map[string]string{}, sent: map[string]bool{},
		known: map[string]int64{}, lastActive: time.Now(),
	}
}

func (ss *session) wake() {
	close(ss.notify)
	ss.notify = make(chan struct{})
}

func (ss *session) send(typ string, data any, re string) {
	if ss.closed {
		return
	}
	env := message.MustNew(typ, data)
	env.Re = re
	ss.outq = append(ss.outq, env)
	ss.wake()
}

func (ss *session) take() []message.Envelope {
	out := ss.outq
	ss.outq = nil
	return out
}

func (ss *session) close(code int) {
	if ss.closed {
		return
	}
	ss.closed, ss.code = true, code
	ss.wake()
}

// Классы доставки агент → сервер (§4).
var (
	streamTypes = map[string]bool{
		message.TypeStatus: true, message.TypeMetrics: true, message.TypeCapabilities: true, message.TypeJobAccept: true,
		message.TypeJobProgress: true, message.TypeCmdAccept: true, message.TypeCmdOutput: true,
		message.TypeInventory: true, message.TypeLog: true,
	}
	reliableTypes = map[string]bool{
		message.TypeJobEvent: true, message.TypeJobComplete: true, message.TypeJobFail: true, message.TypeJobReject: true,
		message.TypeCmdDone: true, message.TypeStateApplied: true, message.TypeEvent: true,
	}
)

// ─── сессия (под a.mu) ─────────────────────────────────────────────────

// open — hello: новая сессия вытесняет прежнюю (4410), welcome, сверка задач,
// доставка команд и состояния.
func (a *Agents) open(ss *session, env message.Envelope) {
	var hello message.Hello
	if env.Decode(&hello) != nil || len(hello.Versions) == 0 {
		ss.close(message.CloseInvalid)
		return
	}
	if !slices.Contains(hello.Versions, 1) {
		ss.close(message.CloseUnsupported)
		return
	}
	if a.closed {
		ss.close(message.CloseRestart)
		return
	}
	caps := hello.Capabilities
	var ev alertEvents
	var revoked bool
	agent, _, err := a.mutateAgent(ss.agentID, func(ag *Agent) bool {
		if revoked = ag.Revoked; revoked {
			return false
		}
		ev = nil
		// Новый запуск агента — seq потока снова с начала.
		if ag.BootID != hello.Agent.BootID {
			ag.BootID, ag.LastSeq = hello.Agent.BootID, 0
		}
		ag.Hello, ag.Capabilities, ag.Status = &hello, &caps, nil
		ag.Labels = helloLabels(ag, hello.Labels)
		ag.Online, ag.Transport, ag.LastSeenAt = true, ss.mode, now()
		if ss.address != "" {
			ag.Address = ss.address
		}
		ev.set(ag, AlertOffline, "", false, "")
		return true
	})
	if err != nil || revoked {
		ss.close(message.CloseUnauthorized)
		return
	}
	if prev := a.sessions[agent.ID]; prev != nil && prev != ss {
		prev.close(message.CloseReplaced)
	}
	a.sessions[agent.ID] = ss
	// Переподключился в пределах отсрочки — online не менялся.
	if t := a.offline[agent.ID]; t != nil {
		t.timer.Stop()
		delete(a.offline, agent.ID)
	}
	ss.greeted, ss.caps = true, &caps
	a.emit(ChangeAgent, agent.ID)
	a.queueAlerts(ev)
	a.learnDomains(ss, &caps)
	cfg := message.SessionConfig{
		StatusIntervalMs:  a.opts.StatusInterval.Milliseconds(),
		MetricsIntervalMs: a.opts.MetricsInterval.Milliseconds(),
	}
	if ss.sub, ss.subUntil = summarize(agent.Subscriptions, now()); ss.subUntil != 0 {
		sum := ss.sub
		cfg.Subscription = &sum
	}
	ss.send(message.TypeWelcome, message.Welcome{
		Version: 1, AgentID: agent.ID, SessionID: ss.id, ServerTime: now(), Config: cfg,
	}, "")
	a.log.Info("агент на связи", "agent", agent.Name, "transport", ss.mode)
	a.reconcile(ss, hello.Jobs)
	a.trackRunning(ss)
	a.deliver(ss)
}

// trackRunning — выполняющиеся команды агента (приняты в прошлой сессии,
// может быть, другим процессом) отслеживаются как отправленные в этой: их
// отмену в другом процессе Refresh доставит агенту (cmd.cancel).
func (a *Agents) trackRunning(ss *session) {
	cmds, err := a.store.ListCommands(CommandFilter{Status: CommandRunning, AgentID: ss.agentID})
	if err != nil {
		a.log.Error("команды не прочитаны", "err", err)
	}
	for _, cmd := range cmds {
		ss.sent[cmd.ID] = true
	}
}

// helloLabels — метки агента после hello: hello.labels, поверх — выданные
// бэкендом при регистрации (GrantedLabels главнее — иначе узел выдал бы себя
// за другой).
func helloLabels(agent *Agent, labels map[string]string) map[string]string {
	out := map[string]string{}
	maps.Copy(out, labels)
	maps.Copy(out, agent.GrantedLabels)
	return out
}

// offlineTimer — отложенный переход агента в offline: since — когда закрылась
// его сессия в этом процессе.
type offlineTimer struct {
	timer *time.Timer
	since int64
}

// closeSession — сессия закрыта: если она текущая, агент без связи — после
// отсрочки OfflineGrace (переподключился раньше — online не менялся).
func (a *Agents) closeSession(ss *session, code int) {
	ss.close(code)
	if a.sessions[ss.agentID] != ss {
		return
	}
	delete(a.sessions, ss.agentID)
	agentID := ss.agentID
	if a.closed {
		a.setOffline(agentID, now())
		return
	}
	if t := a.offline[agentID]; t != nil {
		t.timer.Stop()
	}
	ot := &offlineTimer{since: now()}
	ot.timer = time.AfterFunc(a.offlineGrace, func() {
		a.mu.Lock()
		defer a.unlock()
		if a.offline[agentID] == ot && a.sessions[agentID] == nil {
			a.setOffline(agentID, ot.since)
		}
	})
	a.offline[agentID] = ot
}

// setOffline — агент без связи (отсрочка истекла или не нужна). Вести от
// агента позже since (он подключился к другому процессу) — не трогать.
func (a *Agents) setOffline(agentID string, since int64) {
	if t := a.offline[agentID]; t != nil {
		t.timer.Stop()
		delete(a.offline, agentID)
	}
	var ev alertEvents
	agent, written, _ := a.mutateAgent(agentID, func(ag *Agent) bool {
		if !ag.Online || ag.LastSeenAt > since {
			return false
		}
		ev = nil
		ag.Online = false
		if !a.closed {
			ev.set(ag, AlertOffline, "", true, "Агент без связи")
		}
		return true
	})
	if !written {
		return
	}
	a.emit(ChangeAgent, agentID)
	a.queueAlerts(ev)
	a.log.Info("агент без связи", "agent", agent.Name)
}

// sweepOffline — сверка (sweep): агент online без сессии в этом процессе и
// без вестей дольше OfflineAfter (процесс с его сессией упал) — offline и
// alert offline. Сессия здесь или отсрочка OfflineGrace — не трогаем. Истёкшие
// подписки агентов без связи — удалить. Под a.mu.
func (a *Agents) sweepOffline(ts int64) {
	list, err := a.store.ListAgents()
	if err != nil {
		a.log.Error("агенты не прочитаны", "err", err)
		return
	}
	limit := a.opts.OfflineAfter.Milliseconds()
	stale := func(agent *Agent) bool {
		return agent.Online && !agent.Revoked && a.sessions[agent.ID] == nil && a.offline[agent.ID] == nil &&
			ts-agent.LastSeenAt > limit
	}
	for _, agent := range list {
		if !stale(agent) {
			continue
		}
		var ev alertEvents
		// Условие — по свежей записи: другой процесс мог обновить её после ListAgents.
		cur, written, _ := a.mutateAgent(agent.ID, func(ag *Agent) bool {
			if !stale(ag) {
				return false
			}
			ev = nil
			ag.Online = false
			ev.set(ag, AlertOffline, "", true, "Агент без связи")
			return true
		})
		if !written {
			continue
		}
		a.emit(ChangeAgent, cur.ID)
		a.queueAlerts(ev)
		a.log.Info("агент без вестей — без связи", "agent", cur.Name, "lastSeenAt", cur.LastSeenAt)
	}
	a.sweepOfflineSubscriptions(list, ts)
}

// handle — сообщение открытой сессии; ack или error по классу доставки (§4).
func (a *Agents) handle(ss *session, env message.Envelope) {
	if ss.closed {
		return
	}
	isStream := streamTypes[env.Type] && env.Seq > 0
	dup, lastSeq, err := a.dispatch(ss, env, isStream)
	switch {
	case ss.closed:
	case dup:
		ss.send(message.TypeAck, message.Ack{Seq: lastSeq}, "")
	case err != nil:
		ss.send(message.TypeError, err, env.ID)
	case reliableTypes[env.Type] && env.ID != "":
		ss.send(message.TypeAck, message.Ack{IDs: []string{env.ID}}, "")
	case isStream:
		ss.send(message.TypeAck, message.Ack{Seq: env.Seq}, "")
	}
	if code := ss.closeAfter; code != 0 {
		ss.closeAfter = 0
		a.closeSession(ss, code)
	}
}

func invalid(typ string, err error) *message.Error {
	msg := "Некорректное " + typ
	if err != nil {
		msg += ": " + err.Error()
	}
	return &message.Error{Code: "MESSAGE_INVALID", Message: msg}
}

func leaseLost() *message.Error {
	return &message.Error{Code: "JOB_LEASE_LOST", Message: "Задача больше не за этим агентом"}
}

func internal(err error) *message.Error {
	if err == errBusy {
		return &message.Error{Code: errBusy.Code, Message: errBusy.Message, Retryable: true}
	}
	return &message.Error{Code: "INTERNAL", Message: err.Error(), Retryable: true}
}

// dispatch — сообщение агента: одна условная запись агента на сообщение
// (lastSeenAt, seq потока и поля, которые меняет сообщение), затем — то, что
// следует из записанного. Сообщение потока с seq ≤ Agent.LastSeq — повтор
// (dup): уже обработано, возможно другим процессом; ответ — ack {seq: lastSeq}.
func (a *Agents) dispatch(ss *session, env message.Envelope, isStream bool) (dup bool, lastSeq int64, perr *message.Error) {
	// change — изменение записи агента сообщением (может вызываться повторно);
	// after — действия после записи (запись — свежая).
	var change func(*Agent)
	var after func(*Agent) *message.Error
	switch env.Type {
	case message.TypeStatus:
		var st message.Status
		if err := env.Decode(&st); err != nil {
			return false, 0, invalid(env.Type, err)
		}
		var same bool
		var ev alertEvents
		change = func(ag *Agent) {
			ev = nil
			// Тот же status (пульс) — без уведомления: интерфейсу нечего обновлять.
			same = ag.Status != nil && reflect.DeepEqual(*ag.Status, st)
			ag.Status = &st
			if !same {
				ev.statusAlerts(ag, &st)
			}
		}
		after = func(ag *Agent) *message.Error {
			ss.status = &st
			if !same {
				a.emit(ChangeAgent, ag.ID)
				a.queueAlerts(ev)
			}
			return a.extendLeases(ss, st.Jobs)
		}
	case message.TypeMetrics:
		var m message.Metrics
		if err := env.Decode(&m); err != nil {
			return false, 0, invalid(env.Type, err)
		}
		at := metricsAt(m, now())
		change = func(ag *Agent) { currentMetrics(ag, &m, at) }
		after = func(ag *Agent) *message.Error {
			if err := a.storeMetrics(ag.ID, MetricsPoint{At: at, Backfill: m.Backfill, Metrics: m}); err != nil {
				return internal(err)
			}
			if !m.Backfill { // досланная точка не меняет текущие метрики агента
				a.emit(ChangeAgent, ag.ID)
			}
			return nil
		}
	case message.TypeLog:
		var l message.LogBatch
		if err := env.Decode(&l); err != nil {
			return false, 0, invalid(env.Type, err)
		}
		after = func(ag *Agent) *message.Error {
			a.receiveLog(ag.ID, l.Entries)
			return nil
		}
	case message.TypeInventory:
		var inv message.Inventory
		if err := env.Decode(&inv); err != nil {
			return false, 0, invalid(env.Type, err)
		}
		change = func(ag *Agent) { ag.Inventory = &inv }
		after = func(ag *Agent) *message.Error {
			a.emit(ChangeAgent, ag.ID)
			return nil
		}
	case message.TypeCapabilities:
		var caps message.Capabilities
		if err := env.Decode(&caps); err != nil {
			return false, 0, invalid(env.Type, err)
		}
		change = func(ag *Agent) { ag.Capabilities = mergeCapabilities(ag.Capabilities, caps) }
		after = func(ag *Agent) *message.Error {
			ss.caps = ag.Capabilities
			a.emit(ChangeAgent, ag.ID)
			a.learnDomains(ss, &caps)
			a.deliver(ss)
			return nil
		}
	case message.TypeEvent:
		var e message.Event
		if err := env.Decode(&e); err != nil || e.Type == "" {
			return false, 0, invalid(env.Type, err)
		}
		after = func(ag *Agent) *message.Error { return a.receiveEvent(ag, env, e) }
	case message.TypeStateApplied:
		var applied message.StateApplied
		if err := env.Decode(&applied); err != nil || applied.Domain == "" {
			return false, 0, invalid(env.Type, err)
		}
		var ev alertEvents
		change = func(ag *Agent) {
			ev = nil
			if ag.StateApplied == nil {
				ag.StateApplied = map[string]message.StateApplied{}
			}
			ag.StateApplied[applied.Domain] = applied
			ev.set(ag, AlertStateFailed, applied.Domain, !applied.OK, applied.Error)
		}
		after = func(ag *Agent) *message.Error {
			a.emit(ChangeAgent, ag.ID)
			a.queueAlerts(ev)
			if applied.OK && applied.Version > ss.known[applied.Domain] {
				ss.known[applied.Domain] = applied.Version
			}
			a.emit(ChangeState, applied.Domain)
			return nil
		}
	case message.TypeJobAccept, message.TypeJobProgress, message.TypeJobEvent, message.TypeJobURLs,
		message.TypeJobComplete, message.TypeJobFail, message.TypeJobReject:
		after = func(*Agent) *message.Error { return a.dispatchJob(ss, env) }
	case message.TypeCmdAccept, message.TypeCmdOutput, message.TypeCmdDone:
		after = func(*Agent) *message.Error { return a.dispatchCommand(ss, env) }
	default:
		after = func(*Agent) *message.Error {
			return &message.Error{Code: "UNKNOWN_TYPE", Message: "Неизвестный тип: " + env.Type}
		}
	}
	var revoked bool
	agent, _, err := a.mutateAgent(ss.agentID, func(ag *Agent) bool {
		if revoked = ag.Revoked; revoked {
			return false
		}
		if dup = isStream && env.Seq <= ag.LastSeq; dup {
			lastSeq = ag.LastSeq
			return false
		}
		ag.LastSeenAt = now()
		if isStream {
			ag.LastSeq = env.Seq
		}
		if change != nil {
			change(ag)
		}
		return true
	})
	switch {
	case revoked:
		// Отозван другим процессом: сессия — 4401.
		a.dropRevoked(ss.agentID)
		return false, 0, nil
	case err != nil:
		return false, 0, internal(err)
	case dup:
		return true, lastSeq, nil
	}
	return false, 0, after(agent)
}

// extendLeases — status перечислил задачи агента: продлить их аренду (у
// задач, всё ещё за агентом в этой попытке), выданные — больше не ждут
// перечисления; раздать ждущие задачи.
func (a *Agents) extendLeases(ss *session, jobs []message.StatusJob) *message.Error {
	for _, ref := range jobs {
		delete(ss.pending, ref.JobID)
		_, _, _, err := a.updateHeld(ss.agentID, message.JobRef{JobID: ref.JobID, Attempt: ref.Attempt}, func(j *Job) bool {
			j.LeaseUntil = time.Now().Add(time.Duration(j.LeaseSeconds) * time.Second).UnixMilli()
			return true
		})
		if err != nil {
			return internal(err)
		}
	}
	a.dispatchJobs()
	return nil
}

// receiveEvent — событие агента или воркера (важное): в Store; повтор по id
// — только ack.
func (a *Agents) receiveEvent(agent *Agent, env message.Envelope, e message.Event) *message.Error {
	if env.ID != "" && a.seen[env.ID] {
		return nil // повтор
	}
	source := e.Source
	if source == "" {
		source = "agent"
	}
	at := env.TS
	if at == 0 {
		at = now()
	}
	if err := a.store.AddEvent(AgentEvent{AgentID: agent.ID, AgentName: agent.Name, Source: source, Type: e.Type, Data: e.Data, At: at}); err != nil {
		return internal(err)
	}
	a.remember(env.ID)
	id := env.ID
	if id == "" {
		id = newID()
	}
	a.emit(ChangeEvent, id)
	return nil
}

// remember — id принятого события (повтор — только ack).
func (a *Agents) remember(id string) {
	if id == "" {
		return
	}
	a.seen[id] = true
	a.seenOrder = append(a.seenOrder, id)
	if len(a.seenOrder) > seenEventsKept {
		delete(a.seen, a.seenOrder[0])
		a.seenOrder = a.seenOrder[1:]
	}
}

func (a *Agents) dispatchJob(ss *session, env message.Envelope) *message.Error {
	agentID := ss.agentID
	switch env.Type {
	case message.TypeJobAccept:
		var ref message.JobRef
		if err := env.Decode(&ref); err != nil {
			return invalid(env.Type, err)
		}
		// Слот освобождается из pending, когда задачу перечислит status.
		job, _, written, err := a.updateHeld(agentID, ref, func(j *Job) bool {
			if j.Accepted {
				return false
			}
			j.Accepted = true
			return true
		})
		if err != nil {
			return internal(err)
		}
		if written {
			a.emit(ChangeJob, job.ID)
		}
	case message.TypeJobProgress:
		var p message.JobProgress
		if err := env.Decode(&p); err != nil {
			return invalid(env.Type, err)
		}
		job, _, written, err := a.updateHeld(agentID, p.JobRef, func(j *Job) bool {
			if p.Progress != nil {
				j.Progress = *p.Progress
			}
			if p.Text != nil {
				j.Text = *p.Text
			}
			j.Log = append(j.Log, p.Log...)
			if len(j.Log) > keepJobLog {
				j.Log = slices.Clone(j.Log[len(j.Log)-keepJobLog:])
			}
			return true
		})
		if err != nil {
			return internal(err)
		}
		if written {
			a.emit(ChangeJob, job.ID)
		}
	case message.TypeJobEvent:
		var e message.JobEvent
		if err := env.Decode(&e); err != nil {
			return invalid(env.Type, err)
		}
		at := env.TS
		if at == 0 {
			at = now()
		}
		job, held, written, err := a.updateHeld(agentID, e.JobRef, func(j *Job) bool {
			if e.Seq <= j.EventSeq {
				return false // повтор
			}
			j.EventSeq = e.Seq
			j.Events = append(j.Events, JobEvent{Seq: e.Seq, Type: e.Type, Data: e.Data, At: at})
			return true
		})
		switch {
		case err != nil:
			return internal(err)
		case !held:
			return leaseLost()
		case written:
			a.emit(ChangeJob, job.ID)
		}
	case message.TypeJobURLs:
		var req message.JobURLsRequest
		if err := env.Decode(&req); err != nil {
			return invalid(env.Type, err)
		}
		job := a.held(agentID, req.JobRef)
		if job == nil {
			return leaseLost()
		}
		urls, err := a.files.URLs(job, a.baseURL(ss))
		if err != nil {
			return &message.Error{Code: "URLS_UNAVAILABLE", Message: err.Error(), Retryable: true}
		}
		// Перечислены имена — только они; ничего не перечислено — все.
		if req.Inputs != nil || req.Outputs != nil {
			urls.Inputs, urls.Outputs = pick(urls.Inputs, req.Inputs), pick(urls.Outputs, req.Outputs)
		}
		ss.send(message.TypeJobURLs, urls, env.ID)
	case message.TypeJobComplete:
		var c message.JobComplete
		if err := env.Decode(&c); err != nil {
			return invalid(env.Type, err)
		}
		job, held, written, err := a.updateHeld(agentID, c.JobRef, func(j *Job) bool {
			j.Status, j.Result, j.Progress, j.Error, j.FinishedAt = JobCompleted, c.Result, 1, nil, now()
			return true
		})
		switch {
		case err != nil:
			return internal(err)
		case !held:
			if a.settled(agentID, c.JobRef, JobCompleted) {
				return nil // повтор итога
			}
			return leaseLost()
		case written:
			delete(ss.pending, job.ID)
			a.emit(ChangeJob, job.ID)
			a.log.Info("задача выполнена", "job", job.ID, "queue", job.Queue)
			a.dispatchJobs()
		}
	case message.TypeJobFail:
		var f message.JobFail
		if err := env.Decode(&f); err != nil {
			return invalid(env.Type, err)
		}
		held, _, requeued, err := a.failAttempt(agentID, f.JobRef, f.Code, f.Message, f.Retryable, nil)
		switch {
		case err != nil:
			return internal(err)
		case !held:
			if a.settled(agentID, f.JobRef, JobFailed) {
				return nil
			}
			return leaseLost()
		case requeued:
			a.dispatchJobs()
		}
	case message.TypeJobReject:
		var r message.JobReject
		if err := env.Decode(&r); err != nil {
			return invalid(env.Type, err)
		}
		delete(ss.pending, r.JobID)
		job, _, written, err := a.updateHeld(agentID, r.JobRef, func(j *Job) bool {
			j.Status, j.AgentID, j.Accepted, j.LeaseUntil = JobQueued, "", false, 0
			return true
		})
		if err != nil {
			return internal(err)
		}
		if written {
			a.emit(ChangeJob, job.ID)
			a.dispatchJobs()
		}
	}
	return nil
}

func pick[V any](all map[string]V, names []string) map[string]V {
	out := make(map[string]V, len(names))
	for _, n := range names {
		if v, ok := all[n]; ok {
			out[n] = v
		}
	}
	return out
}

func (a *Agents) dispatchCommand(ss *session, env message.Envelope) *message.Error {
	// update — изменить команду агента сессии; fn говорит, писать ли.
	update := func(id string, fn func(*Command) bool) (*Command, bool, *message.Error) {
		cmd, written, err := a.mutateCommand(id, func(c *Command) bool { return c.AgentID == ss.agentID && fn(c) })
		switch {
		case errors.Is(err, ErrNotFound):
			return nil, false, nil
		case err != nil:
			return nil, false, internal(err)
		}
		return cmd, written, nil
	}
	switch env.Type {
	case message.TypeCmdAccept:
		var ref message.CommandRef
		if err := env.Decode(&ref); err != nil {
			return invalid(env.Type, err)
		}
		cmd, written, perr := update(ref.CommandID, func(c *Command) bool {
			if c.Status != CommandPending {
				return false
			}
			c.Status = CommandRunning
			return true
		})
		if written {
			a.commandSaved(cmd)
		}
		return perr
	case message.TypeCmdOutput:
		var out message.CommandOutput
		if err := env.Decode(&out); err != nil {
			return invalid(env.Type, err)
		}
		cmd, written, perr := update(out.CommandID, func(c *Command) bool {
			if c.Finished() {
				return false
			}
			c.Output += out.Chunk
			if len(c.Output) > keepCmdOutput {
				c.Output = c.Output[len(c.Output)-keepCmdOutput:]
			}
			return true
		})
		if written {
			a.commandSaved(cmd)
		}
		return perr
	case message.TypeCmdDone:
		var done message.CommandDone
		if err := env.Decode(&done); err != nil {
			return invalid(env.Type, err)
		}
		delete(ss.sent, done.CommandID)
		// Итог только у незавершённой: после отмены (CancelCommand) и срока
		// (TIMEOUT) поздний итог не учитывается, ответ — обычный ack.
		cmd, written, perr := update(done.CommandID, func(c *Command) bool {
			if c.Finished() {
				return false
			}
			c.Status = CommandFailed
			if done.OK {
				c.Status = CommandSucceeded
			}
			c.Result, c.Error, c.ExitCode, c.FinishedAt = done.Result, done.Error, done.ExitCode, now()
			return true
		})
		if !written {
			return perr
		}
		a.commandSaved(cmd)
		// Новый секрет — переподключиться с ним сразу (1012) после ack.
		if done.OK && cmd.Name == message.CommandRotateKey && a.rotated(ss.agentID, done.Result) {
			ss.closeAfter = message.CloseRestart
		}
	}
	return nil
}

// held — задача за агентом в этой попытке и выполняется (чтение).
func (a *Agents) held(agentID string, ref message.JobRef) *Job {
	job, err := a.store.GetJob(ref.JobID)
	if err != nil || !heldBy(job, agentID, ref) {
		return nil
	}
	return job
}

// settled — итог этой попытки уже принят (повтор после потерянного ack).
func (a *Agents) settled(agentID string, ref message.JobRef, status string) bool {
	job, err := a.store.GetJob(ref.JobID)
	return err == nil && job.Status == status && job.AgentID == agentID && job.Attempt == ref.Attempt
}

// reconcile — сверка задач при hello (§6.3).
func (a *Agents) reconcile(ss *session, reported []message.JobRef) {
	listed := map[message.JobRef]bool{}
	for _, ref := range reported {
		listed[ref] = true
	}
	jobs, err := a.store.ListJobs(JobFilter{Status: JobRunning, AgentID: ss.agentID})
	if err != nil {
		a.log.Error("сверка задач: задачи не прочитаны", "err", err)
	}
	requeued := false
	for _, job := range jobs {
		switch {
		case listed[job.Ref()]:
			if job.StopRequested {
				ss.send(message.TypeJobStop, job.Ref(), "")
			}
		case !job.Accepted:
			// Выдана, но агент её не видел: выдать заново.
			fresh, _, written, _ := a.updateHeld(ss.agentID, job.Ref(), func(j *Job) bool {
				if j.Accepted {
					return false
				}
				j.LeaseUntil = time.Now().Add(time.Duration(j.LeaseSeconds) * time.Second).UnixMilli()
				return true
			})
			if written {
				a.emit(ChangeJob, fresh.ID)
				ss.pending[fresh.ID] = fresh.Queue
				ss.send(message.TypeJobAssign, a.assignment(ss, fresh), "")
			}
		default:
			_, _, again, _ := a.failAttempt(ss.agentID, job.Ref(), "AGENT_LOST", "Агент перезапустился и потерял задачу", true, nil)
			requeued = requeued || again
		}
	}
	for _, ref := range reported {
		if a.held(ss.agentID, ref) == nil {
			ss.send(message.TypeJobCancel, ref, "")
		}
	}
	if requeued {
		a.dispatchJobs()
	}
}

// learnDomains — применённые агентом версии доменов (hello, capabilities).
func (a *Agents) learnDomains(ss *session, caps *message.Capabilities) {
	if caps == nil || caps.State == nil {
		return
	}
	for domain, version := range caps.State.Domains {
		if version != nil && *version > ss.known[domain] {
			ss.known[domain] = *version
		}
	}
}

// deliver — ожидающие команды, которые агент объявил, и новые версии
// объявленных доменов.
func (a *Agents) deliver(ss *session) {
	if ss.closed || !ss.greeted {
		return
	}
	cmds, err := a.store.ListCommands(CommandFilter{Status: CommandPending, AgentID: ss.agentID})
	if err != nil {
		a.log.Error("команды не прочитаны", "err", err)
	}
	for _, cmd := range slices.Backward(cmds) { // старые первыми
		if ss.sent[cmd.ID] || !declaresCommand(ss.caps, cmd.Name) {
			continue
		}
		ss.sent[cmd.ID] = true
		ss.send(message.TypeCmdRun, message.CommandRun{CommandID: cmd.ID, Name: cmd.Name, Args: cmd.Args, TimeoutSec: cmd.TimeoutSec}, "")
	}
	if ss.caps == nil || ss.caps.State == nil {
		return
	}
	for _, domain := range slices.Sorted(maps.Keys(ss.caps.State.Domains)) {
		st := a.effectiveState(domain, ss.agentID)
		if st != nil && st.Version > ss.known[domain] {
			ss.known[domain] = st.Version
			ss.send(message.TypeStatePut, message.StatePut{Domain: domain, Version: st.Version, Spec: st.Spec}, "")
		}
	}
}

// effectiveState — снимок домена для агента: свой, иначе общий.
func (a *Agents) effectiveState(domain, agentID string) *DesiredState {
	st, err := a.store.GetState(domain, agentID)
	if err == nil {
		return st
	}
	if !errors.Is(err, ErrNotFound) {
		a.log.Error("состояние не прочитано", "domain", domain, "err", err)
		return nil
	}
	if st, err = a.store.GetState(domain, ""); err == nil {
		return st
	}
	return nil
}

// dispatchJobs — раздать ждущие задачи по свободным слотам (status.slots):
// каждую — наименее загруженному агенту (задач в работе и выданных),
// закреплённую — только своему. Задачу, которую тем временем взял другой
// процесс, условная запись не выдаст второй раз.
func (a *Agents) dispatchJobs() {
	queued, err := a.store.ListJobs(JobFilter{Status: JobQueued})
	if err != nil {
		a.log.Error("очередь не прочитана", "err", err)
		return
	}
	if len(queued) == 0 {
		return
	}
	var ready []*session
	for _, ss := range a.sortedSessions() {
		st := ss.status
		if ss.closed || !ss.greeted || st == nil ||
			st.State == message.StateDraining || st.State == message.StateUpdating || st.State == message.StateStarting {
			continue
		}
		ready = append(ready, ss)
	}
	if len(ready) == 0 {
		return
	}
	for _, job := range slices.Backward(queued) { // старые первыми
		var best *session
		bestLoad := 0
		for _, ss := range ready {
			if job.PinnedAgentID != "" && job.PinnedAgentID != ss.agentID {
				continue
			}
			if ss.free(job.Queue) <= 0 {
				continue
			}
			if load := len(ss.status.Jobs) + len(ss.pending); best == nil || load < bestLoad {
				best, bestLoad = ss, load
			}
		}
		if best != nil {
			a.assign(best, job)
		}
	}
}

// free — свободных мест очереди: status.slots минус выданные, но ещё не
// перечисленные в status.
func (ss *session) free(queue string) int {
	n := ss.status.Slots[queue]
	for _, q := range ss.pending {
		if q == queue {
			n--
		}
	}
	return n
}

// assign — выдать ждущую задачу агенту сессии: только если она всё ещё ждёт в
// той же попытке (иначе её взял другой процесс — пропустить).
func (a *Agents) assign(ss *session, job *Job) {
	fresh, written, _ := a.mutateJob(job.ID, func(j *Job) bool {
		if j.Status != JobQueued || j.Attempt != job.Attempt || j.PinnedAgentID != job.PinnedAgentID {
			return false
		}
		j.Status, j.AgentID, j.Accepted = JobRunning, ss.agentID, false
		j.LeaseUntil = time.Now().Add(time.Duration(j.LeaseSeconds) * time.Second).UnixMilli()
		return true
	})
	if !written {
		return
	}
	a.emit(ChangeJob, fresh.ID)
	ss.pending[fresh.ID] = fresh.Queue
	ss.send(message.TypeJobAssign, a.assignment(ss, fresh), "")
}

func (a *Agents) assignment(ss *session, job *Job) message.JobAssign {
	msg := message.JobAssign{
		JobID: job.ID, Attempt: job.Attempt, Queue: job.Queue, Data: job.Data, LeaseSeconds: job.LeaseSeconds,
		Inputs: map[string]string{}, Outputs: map[string]message.OutputURL{},
	}
	urls, err := a.files.URLs(job, a.baseURL(ss))
	if err != nil {
		a.log.Error("ссылки на файлы задачи не получены", "job", job.ID, "err", err)
		return msg
	}
	if urls.Inputs != nil {
		msg.Inputs = urls.Inputs
	}
	if urls.Outputs != nil {
		msg.Outputs = urls.Outputs
	}
	msg.URLsExpireAt = urls.ExpiresAt
	return msg
}

func (a *Agents) baseURL(ss *session) string { return a.serverURL(ss.baseURL) }

// failAttempt — провал попытки ref задачи агента (по свежей записи: задача
// выполняется у него в этой попытке и cond, если задано, верно): повтор, если
// попытки остались. held — задача была за агентом; failed — попытка
// записана проваленной; requeued — задача снова ждёт (раздать — dispatchJobs
// у вызывающего).
func (a *Agents) failAttempt(agentID string, ref message.JobRef, code, msg string, retryable bool,
	cond func(*Job) bool) (held, failed, requeued bool, err error) {
	job, held, written, err := a.updateHeld(agentID, ref, func(j *Job) bool {
		if cond != nil && !cond(j) {
			return false
		}
		j.Error = &JobError{Code: code, Message: msg}
		if requeued = retryable && j.Attempt+1 < j.MaxAttempts; requeued {
			j.Status, j.Attempt, j.AgentID, j.Accepted = JobQueued, j.Attempt+1, "", false
			j.EventSeq, j.LeaseUntil, j.Progress, j.StopRequested = 0, 0, 0, false
		} else {
			j.Status, j.FinishedAt = JobFailed, now()
		}
		return true
	})
	if !written {
		return held, false, false, err
	}
	if ss := a.sessions[agentID]; ss != nil {
		delete(ss.pending, job.ID)
	}
	a.emit(ChangeJob, job.ID)
	if !requeued {
		a.log.Info("задача провалена", "job", job.ID, "code", code)
	}
	return true, true, requeued, nil
}

// mergeCapabilities — объединение возможностей (§6.1): новые имена
// добавляются, у домена — большая применённая версия; очереди — из status.
func mergeCapabilities(cur *message.Capabilities, add message.Capabilities) *message.Capabilities {
	out := message.Capabilities{}
	if cur != nil {
		out = *cur
	}
	if add.Jobs != nil {
		out.Jobs = add.Jobs
	}
	if add.Commands != nil {
		var names []string
		if out.Commands != nil {
			names = out.Commands.Names
		}
		out.Commands = &message.CommandsCapability{Names: union(names, add.Commands.Names)}
	}
	if add.Telemetry != nil {
		var channels []string
		if out.Telemetry != nil {
			channels = out.Telemetry.Channels
		}
		out.Telemetry = &message.TelemetryCapability{Channels: union(channels, add.Telemetry.Channels)}
	}
	if add.State != nil {
		domains := map[string]*int64{}
		if out.State != nil {
			maps.Copy(domains, out.State.Domains)
		}
		for d, v := range add.State.Domains {
			if prev, ok := domains[d]; !ok || prev == nil || (v != nil && *v > *prev) {
				domains[d] = v
			}
		}
		out.State = &message.StateCapability{Domains: domains}
	}
	if add.Update != nil {
		out.Update = add.Update
	}
	return &out
}

func union(a, b []string) []string {
	out := slices.Clone(a)
	for _, x := range b {
		if !slices.Contains(out, x) {
			out = append(out, x)
		}
	}
	slices.Sort(out)
	if out == nil {
		out = []string{}
	}
	return out
}
