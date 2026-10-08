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
	agent, err := a.store.GetAgent(ss.agentID)
	if err != nil || agent.Revoked {
		ss.close(message.CloseUnauthorized)
		return
	}
	if a.closed {
		ss.close(message.CloseRestart)
		return
	}
	if prev := a.sessions[agent.ID]; prev != nil && prev != ss {
		prev.close(message.CloseReplaced)
	}
	a.sessions[agent.ID] = ss
	// Переподключился в пределах отсрочки — online не менялся.
	if t := a.offline[agent.ID]; t != nil {
		t.Stop()
		delete(a.offline, agent.ID)
	}
	if st := a.streams[agent.ID]; st == nil || st.bootID != hello.Agent.BootID {
		a.streams[agent.ID] = &stream{bootID: hello.Agent.BootID}
	}
	caps := hello.Capabilities
	ss.greeted, ss.caps = true, &caps
	agent.Hello, agent.Capabilities, agent.Status = &hello, &caps, nil
	agent.Labels = helloLabels(agent, hello.Labels)
	agent.Online, agent.Transport, agent.LastSeenAt = true, ss.mode, now()
	if ss.address != "" {
		agent.Address = ss.address
	}
	a.saveAgent(agent)
	a.setAlert(agent, AlertOffline, "", false, "")
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
	a.deliver(ss)
}

// helloLabels — метки агента после hello: hello.labels, поверх — выданные
// бэкендом при регистрации (GrantedLabels главнее — иначе узел выдал бы себя
// за другой). Изменение меток попадает в уведомление change agent (saveAgent).
func helloLabels(agent *Agent, labels map[string]string) map[string]string {
	out := map[string]string{}
	maps.Copy(out, labels)
	maps.Copy(out, agent.GrantedLabels)
	return out
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
		a.setOffline(agentID)
		return
	}
	if t := a.offline[agentID]; t != nil {
		t.Stop()
	}
	var timer *time.Timer
	timer = time.AfterFunc(a.offlineGrace, func() {
		a.mu.Lock()
		defer a.unlock()
		if a.offline[agentID] == timer && a.sessions[agentID] == nil {
			a.setOffline(agentID)
		}
	})
	a.offline[agentID] = timer
}

// setOffline — агент без связи (отсрочка истекла или не нужна).
func (a *Agents) setOffline(agentID string) {
	if t := a.offline[agentID]; t != nil {
		t.Stop()
		delete(a.offline, agentID)
	}
	agent, err := a.store.GetAgent(agentID)
	if err != nil || !agent.Online {
		return
	}
	agent.Online = false
	a.saveAgent(agent)
	a.log.Info("агент без связи", "agent", agent.Name)
	if !a.closed {
		a.setAlert(agent, AlertOffline, "", true, "Агент без связи")
	}
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
		// Перечитать: другой процесс мог обновить запись после ListAgents.
		cur, err := a.store.GetAgent(agent.ID)
		if err != nil || !stale(cur) {
			continue
		}
		cur.Online = false
		a.saveAgent(cur)
		a.log.Info("агент без вестей — без связи", "agent", cur.Name, "lastSeenAt", cur.LastSeenAt)
		a.setAlert(cur, AlertOffline, "", true, "Агент без связи")
	}
	a.sweepOfflineSubscriptions(list, ts)
}

func (a *Agents) saveAgent(agent *Agent) {
	if a.storeAgent(agent) {
		a.emit(ChangeAgent, agent.ID)
	}
}

// storeAgent — сохранить агента без уведомления (lastSeenAt, неизменный status).
func (a *Agents) storeAgent(agent *Agent) bool {
	if err := a.store.UpdateAgent(agent); err != nil {
		a.log.Error("агент не сохранён", "agent", agent.ID, "err", err)
		return false
	}
	return true
}

func (a *Agents) saveJob(job *Job) bool {
	if err := a.store.UpdateJob(job); err != nil {
		a.log.Error("задача не сохранена", "job", job.ID, "err", err)
		return false
	}
	a.emit(ChangeJob, job.ID)
	return true
}

// handle — сообщение открытой сессии; ack или error по классу доставки (§4).
func (a *Agents) handle(ss *session, env message.Envelope) {
	if ss.closed {
		return
	}
	isStream := streamTypes[env.Type] && env.Seq > 0
	if isStream {
		st := a.streams[ss.agentID]
		if st == nil {
			st = &stream{}
			a.streams[ss.agentID] = st
		}
		if env.Seq <= st.lastSeq {
			ss.send(message.TypeAck, message.Ack{Seq: st.lastSeq}, "")
			return
		}
		st.lastSeq = env.Seq
	}
	err := a.dispatch(ss, env)
	switch {
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
	return &message.Error{Code: "INTERNAL", Message: err.Error(), Retryable: true}
}

func (a *Agents) dispatch(ss *session, env message.Envelope) *message.Error {
	agent, err := a.store.GetAgent(ss.agentID)
	if err != nil {
		return internal(err)
	}
	agent.LastSeenAt = now()
	switch env.Type {
	case message.TypeStatus:
		var st message.Status
		if err := env.Decode(&st); err != nil {
			return invalid(env.Type, err)
		}
		// Тот же status (пульс) — сохранить без уведомления: интерфейсу нечего обновлять.
		same := agent.Status != nil && reflect.DeepEqual(*agent.Status, st)
		agent.Status, ss.status = &st, &st
		if same {
			a.storeAgent(agent)
		} else {
			a.saveAgent(agent)
			a.statusAlerts(agent, &st)
		}
		lease := time.Now()
		for _, ref := range st.Jobs {
			delete(ss.pending, ref.JobID)
			if job := a.held(agent.ID, message.JobRef{JobID: ref.JobID, Attempt: ref.Attempt}); job != nil {
				job.LeaseUntil = lease.Add(time.Duration(job.LeaseSeconds) * time.Second).UnixMilli()
				if err := a.store.UpdateJob(job); err != nil {
					return internal(err)
				}
			}
		}
		a.dispatchJobs()
		return nil
	case message.TypeMetrics:
		var m message.Metrics
		if err := env.Decode(&m); err != nil {
			return invalid(env.Type, err)
		}
		if err := a.addMetrics(agent, m); err != nil {
			return internal(err)
		}
		if m.Backfill {
			a.storeAgent(agent) // досланная точка не меняет текущие метрики агента
		} else {
			a.saveAgent(agent)
		}
		return nil
	case message.TypeLog:
		return a.receiveLog(agent, env)
	case message.TypeInventory:
		var inv message.Inventory
		if err := env.Decode(&inv); err != nil {
			return invalid(env.Type, err)
		}
		agent.Inventory = &inv
		a.saveAgent(agent)
		return nil
	case message.TypeCapabilities:
		var caps message.Capabilities
		if err := env.Decode(&caps); err != nil {
			return invalid(env.Type, err)
		}
		merged := mergeCapabilities(agent.Capabilities, caps)
		agent.Capabilities, ss.caps = merged, merged
		a.saveAgent(agent)
		a.learnDomains(ss, &caps)
		a.deliver(ss)
		return nil
	case message.TypeEvent:
		var e message.Event
		if err := env.Decode(&e); err != nil || e.Type == "" {
			return invalid(env.Type, err)
		}
		a.storeAgent(agent) // lastSeenAt: событие агента не меняет
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
	case message.TypeStateApplied:
		var applied message.StateApplied
		if err := env.Decode(&applied); err != nil || applied.Domain == "" {
			return invalid(env.Type, err)
		}
		if agent.StateApplied == nil {
			agent.StateApplied = map[string]message.StateApplied{}
		}
		agent.StateApplied[applied.Domain] = applied
		a.saveAgent(agent)
		if applied.OK {
			a.setAlert(agent, AlertStateFailed, applied.Domain, false, "")
		} else {
			a.setAlert(agent, AlertStateFailed, applied.Domain, true, applied.Error)
		}
		if applied.OK && applied.Version > ss.known[applied.Domain] {
			ss.known[applied.Domain] = applied.Version
		}
		a.emit(ChangeState, applied.Domain)
		return nil
	}
	if err := a.store.UpdateAgent(agent); err != nil { // lastSeenAt
		return internal(err)
	}
	switch env.Type {
	case message.TypeJobAccept, message.TypeJobProgress, message.TypeJobEvent, message.TypeJobURLs,
		message.TypeJobComplete, message.TypeJobFail, message.TypeJobReject:
		return a.dispatchJob(ss, env)
	case message.TypeCmdAccept, message.TypeCmdOutput, message.TypeCmdDone:
		return a.dispatchCommand(ss, env)
	}
	return &message.Error{Code: "UNKNOWN_TYPE", Message: "Неизвестный тип: " + env.Type}
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
		if job := a.held(agentID, ref); job != nil && !job.Accepted {
			job.Accepted = true
			a.saveJob(job)
		}
	case message.TypeJobProgress:
		var p message.JobProgress
		if err := env.Decode(&p); err != nil {
			return invalid(env.Type, err)
		}
		job := a.held(agentID, p.JobRef)
		if job == nil {
			return nil
		}
		if p.Progress != nil {
			job.Progress = *p.Progress
		}
		if p.Text != nil {
			job.Text = *p.Text
		}
		job.Log = append(job.Log, p.Log...)
		if len(job.Log) > keepJobLog {
			job.Log = slices.Clone(job.Log[len(job.Log)-keepJobLog:])
		}
		a.saveJob(job)
	case message.TypeJobEvent:
		var e message.JobEvent
		if err := env.Decode(&e); err != nil {
			return invalid(env.Type, err)
		}
		job := a.held(agentID, e.JobRef)
		if job == nil {
			return leaseLost()
		}
		if e.Seq > job.EventSeq {
			at := env.TS
			if at == 0 {
				at = now()
			}
			job.EventSeq = e.Seq
			job.Events = append(job.Events, JobEvent{Seq: e.Seq, Type: e.Type, Data: e.Data, At: at})
			a.saveJob(job)
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
		job := a.held(agentID, c.JobRef)
		if job == nil {
			if a.settled(agentID, c.JobRef, JobCompleted) {
				return nil // повтор итога
			}
			return leaseLost()
		}
		delete(ss.pending, job.ID)
		job.Status, job.Result, job.Progress, job.Error, job.FinishedAt = JobCompleted, c.Result, 1, nil, now()
		a.saveJob(job)
		a.log.Info("задача выполнена", "job", job.ID, "queue", job.Queue)
		a.dispatchJobs()
	case message.TypeJobFail:
		var f message.JobFail
		if err := env.Decode(&f); err != nil {
			return invalid(env.Type, err)
		}
		job := a.held(agentID, f.JobRef)
		if job == nil {
			if a.settled(agentID, f.JobRef, JobFailed) {
				return nil
			}
			return leaseLost()
		}
		a.failAttempt(job, f.Code, f.Message, f.Retryable)
	case message.TypeJobReject:
		var r message.JobReject
		if err := env.Decode(&r); err != nil {
			return invalid(env.Type, err)
		}
		delete(ss.pending, r.JobID)
		if job := a.held(agentID, r.JobRef); job != nil {
			job.Status, job.AgentID, job.Accepted = JobQueued, "", false
			if a.saveJob(job) {
				a.dispatchJobs()
			}
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
	switch env.Type {
	case message.TypeCmdAccept:
		var ref message.CommandRef
		if err := env.Decode(&ref); err != nil {
			return invalid(env.Type, err)
		}
		if cmd := a.agentCommand(ss.agentID, ref.CommandID); cmd != nil && cmd.Status == CommandPending {
			cmd.Status = CommandRunning
			a.saveCommand(cmd)
		}
	case message.TypeCmdOutput:
		var out message.CommandOutput
		if err := env.Decode(&out); err != nil {
			return invalid(env.Type, err)
		}
		if cmd := a.agentCommand(ss.agentID, out.CommandID); cmd != nil && !cmd.Finished() {
			cmd.Output += out.Chunk
			if len(cmd.Output) > keepCmdOutput {
				cmd.Output = cmd.Output[len(cmd.Output)-keepCmdOutput:]
			}
			a.saveCommand(cmd)
		}
	case message.TypeCmdDone:
		var done message.CommandDone
		if err := env.Decode(&done); err != nil {
			return invalid(env.Type, err)
		}
		if cmd := a.agentCommand(ss.agentID, done.CommandID); cmd != nil && !cmd.Finished() {
			cmd.Status = CommandFailed
			if done.OK {
				cmd.Status = CommandSucceeded
			}
			cmd.Result, cmd.Error, cmd.ExitCode, cmd.FinishedAt = done.Result, done.Error, done.ExitCode, now()
			a.saveCommand(cmd)
			// Новый секрет — переподключиться с ним сразу (1012) после ack.
			if done.OK && cmd.Name == message.CommandRotateKey && a.rotated(ss.agentID, done.Result) {
				ss.closeAfter = message.CloseRestart
			}
		}
	}
	return nil
}

func (a *Agents) agentCommand(agentID, id string) *Command {
	cmd, err := a.store.GetCommand(id)
	if err != nil || cmd.AgentID != agentID {
		return nil
	}
	return cmd
}

// held — задача за агентом в этой попытке и выполняется.
func (a *Agents) held(agentID string, ref message.JobRef) *Job {
	job, err := a.store.GetJob(ref.JobID)
	if err != nil || job.Status != JobRunning || job.AgentID != agentID || job.Attempt != ref.Attempt {
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
	for _, job := range jobs {
		switch {
		case listed[job.Ref()]:
			if job.StopRequested {
				ss.send(message.TypeJobStop, job.Ref(), "")
			}
		case !job.Accepted:
			// Выдана, но агент её не видел: выдать заново.
			job.LeaseUntil = time.Now().Add(time.Duration(job.LeaseSeconds) * time.Second).UnixMilli()
			a.saveJob(job)
			ss.pending[job.ID] = job.Queue
			ss.send(message.TypeJobAssign, a.assignment(ss, job), "")
		default:
			a.failAttempt(job, "AGENT_LOST", "Агент перезапустился и потерял задачу", true)
		}
	}
	for _, ref := range reported {
		if a.held(ss.agentID, ref) == nil {
			ss.send(message.TypeJobCancel, ref, "")
		}
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
// закреплённую — только своему.
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

func (a *Agents) assign(ss *session, job *Job) {
	job.Status, job.AgentID, job.Accepted = JobRunning, ss.agentID, false
	job.LeaseUntil = time.Now().Add(time.Duration(job.LeaseSeconds) * time.Second).UnixMilli()
	if !a.saveJob(job) {
		return
	}
	ss.pending[job.ID] = job.Queue
	ss.send(message.TypeJobAssign, a.assignment(ss, job), "")
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

// failAttempt — провал попытки: повтор, если попытки остались.
func (a *Agents) failAttempt(job *Job, code, msg string, retryable bool) {
	if ss := a.sessions[job.AgentID]; ss != nil {
		delete(ss.pending, job.ID)
	}
	job.Error = &JobError{Code: code, Message: msg}
	if retryable && job.Attempt+1 < job.MaxAttempts {
		job.Status, job.Attempt, job.AgentID, job.Accepted = JobQueued, job.Attempt+1, "", false
		job.EventSeq, job.LeaseUntil, job.StopRequested = 0, 0, false
		if a.saveJob(job) {
			a.dispatchJobs()
		}
		return
	}
	job.Status, job.FinishedAt = JobFailed, now()
	a.saveJob(job)
	a.log.Info("задача провалена", "job", job.ID, "code", code)
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
