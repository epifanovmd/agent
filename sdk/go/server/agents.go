// Package server — SDK сервера на Go (контракт — sdk/README.md §2): Agents —
// все подключённые агенты и механика связи с ними (sdk/spec §2–§7).
//
// Agents делает всю механику связи: регистрацию агентов, WebSocket и HTTP
// sync, сессии, классы доставки и подтверждения, сверку задач при hello,
// раздачу задач по слотам с арендой, сроки команд, версии желаемого
// состояния, события. Бэкенду остаются его данные (Store, Files) и решения
// (Enqueue, Command, SetState):
//
//	agents := server.New(server.Options{EnrollToken: "…"})
//	defer agents.Close()
//	http.ListenAndServe(":8080", agents.Handler())
//
// Agents работает в одном процессе: сессии агентов живут в нём.
package server

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Options — настройки Agents.
type Options struct {
	// EnrollToken — токен регистрации агентов (если не задан Enroll).
	EnrollToken string
	// Enroll — своя проверка токена регистрации: ok == false — отказ; labels
	// добавляются к меткам агента.
	Enroll func(token string) (labels map[string]string, ok bool)
	// Store — хранилище (по умолчанию MemoryStore).
	Store Store
	// Files — провайдер файлов задач (по умолчанию MemoryFiles).
	Files Files
	// StatusInterval, MetricsInterval — интервалы, которые сервер задаёт
	// агентам в welcome (по умолчанию 5 с и 15 с).
	StatusInterval  time.Duration
	MetricsInterval time.Duration
	// MetricsStoreInterval — прореживание истории метрик: точка агента
	// сохраняется в Store, только если её At не раньше последней сохранённой
	// + интервал (досланные — так же). 0 — по умолчанию 15 с, отрицательное —
	// сохранять каждую точку. OnMetrics и Agent.Metrics получают каждую точку.
	MetricsStoreInterval time.Duration
	// MetricsRetention — срок хранения истории метрик: раз в час (и при
	// запуске) удаляются точки старше (Store.PruneMetrics). 0 — по умолчанию
	// 7 суток, отрицательное — хранить всегда.
	MetricsRetention time.Duration
	// OfflineGrace — сколько агент остаётся online после закрытия сессии
	// (по умолчанию 20 с): переподключился за это время — изменений нет.
	OfflineGrace time.Duration
	// OfflineAfter — бэкенд из нескольких процессов: агент online, но без
	// сессии в этом процессе и без вестей (LastSeenAt) дольше OfflineAfter —
	// процесс с его сессией, видимо, упал: сверка переводит агента в offline
	// (change agent, alert offline). 0 — по умолчанию
	// max(3 × StatusInterval, 30 с) + OfflineGrace.
	OfflineAfter time.Duration
	// ReleasesDir — каталог выпуска агента (make release: manifest.json,
	// сборки agent-<os>-<arch>, install.sh); пусто — раздачи релизов нет.
	ReleasesDir string
	// PublicKey — ключ проверки релизов (base64); подставляется в install.sh.
	PublicKey string
	// PublicURL — адрес сервера для ссылок на файлы и install.sh; пусто — из
	// запроса (Host, X-Forwarded-Proto).
	PublicURL string
	// TrustProxy — сервер за доверенным прокси: адрес агента (Agent.Address) —
	// первый из X-Forwarded-For; иначе заголовок не учитывается.
	TrustProxy bool
	// Log — лог (по умолчанию slog.Default()).
	Log *slog.Logger
	// OnChange — уведомление об изменении; вызывается синхронно после
	// изменения (не блокировать), можно вызывать методы Agents.
	OnChange func(Change)
	// OnMetrics — точка метрик агента (в том числе досланная, Backfill)
	// принята — каждая, в том числе не сохранённая в историю из-за
	// MetricsStoreInterval; вызывается синхронно после изменения, вне
	// блокировки Agents (не блокировать), можно вызывать методы Agents.
	OnMetrics func(agentID string, p MetricsPoint)
	// OnStateDeleted — снимок домена удалён (DeleteState; agentID пустой —
	// общий). Вызывается синхронно после изменения, вне блокировки Agents,
	// раньше OnChange этого изменения (в том числе о переизданном общем);
	// не блокировать, можно вызывать методы Agents.
	OnStateDeleted func(domain, agentID string)
	// OnAudit — изменяющее действие приложения (Enqueue, Command, SetState…,
	// с By и без): запись журнала аудита. Вызывается синхронно после
	// изменения, вне блокировки Agents (не блокировать), можно вызывать
	// методы Agents. Журнал SDK не хранит.
	OnAudit func(AuditEntry)
	// OnLog — пачка записей лога агента (сообщение log: лог агента и вывод
	// воркеров; что слать — log.forward агента и подписки (Subscribe)). SDK логи не
	// хранит. Вызывается вне блокировки Agents (не блокировать), можно
	// вызывать методы Agents.
	OnLog func(agentID string, entries []message.LogEntry)
	// OnAlert — проблема агента началась или закончилась (Alert.Active);
	// каждое начало и конец — одно событие. Вызывается вне блокировки Agents
	// (не блокировать), можно вызывать методы Agents.
	OnAlert func(Alert)
	// EnrollFailureLimit — неудачных регистраций с одного адреса за
	// EnrollFailureWindow, после которых регистрация с него отклоняется (429
	// ENROLL_RATE_LIMITED, Retry-After), пока окно не пройдёт. 0 — по
	// умолчанию 10, отрицательное — без ограничения.
	EnrollFailureLimit int
	// EnrollFailureWindow — окно счёта неудачных регистраций (по умолчанию 1 мин).
	EnrollFailureWindow time.Duration
}

// Сроки транспорта и сессии (§2, §3).
const (
	helloTimeout = 10 * time.Second
	pingInterval = 20 * time.Second
	pongTimeout  = 10 * time.Second
	syncMaxWait  = 25 * time.Second
	syncIdle     = 60 * time.Second
	commandGrace = 15 * time.Second
	// callPollInterval, callSlack — Call проверяет итог в Store не реже раза в
	// секунду и ждёт не дольше срока команды + commandGrace + callSlack.
	callPollInterval = time.Second
	callSlack        = 5 * time.Second
	sweepInterval    = time.Second
	pruneInterval    = time.Hour
	keepJobLog       = 500
	keepCmdOutput    = 256 * 1024
	seenEventsKept   = 4096
)

// Agents — все подключённые агенты: серверная часть связи с ними.
type Agents struct {
	opts  Options
	store Store
	files Files
	log   *slog.Logger

	// Сроки (в тестах — короче).
	helloTimeout, pingInterval, syncIdle, sweepInterval, offlineGrace, pruneInterval time.Duration

	mu        sync.Mutex
	publicURL string
	sessions  map[string]*session // agentId → текущая сессия
	streams   map[string]*stream  // agentId → поток (bootId, последний seq)
	seen      map[string]bool     // id принятых event (повтор — идемпотентно)
	seenOrder []string
	waiters   map[string][]chan struct{} // commandId → ждущие Call
	kick      chan struct{}              // закрывается Refresh: ждущие Call перечитывают Store
	offline   map[string]*time.Timer     // agentId → отложенный переход в offline
	stored    map[string]int64           // agentId → At последней сохранённой точки метрик
	lastPrune time.Time                  // последняя чистка истории метрик
	changes   []Change
	points    []agentPoint       // сохранённые точки метрик для OnMetrics
	deleted   []stateKey         // удалённые снимки для OnStateDeleted
	audits    []AuditEntry       // записи аудита для OnAudit
	alerts    map[alertKey]Alert // активные уведомления о проблемах
	alertQ    []Alert            // начала и концы проблем для OnAlert
	logs      []agentLog         // пачки записей лога для OnLog
	closed    bool

	// enrollFails — адрес клиента → времена неудачных регистраций в окне.
	enrollMu    sync.Mutex
	enrollFails map[string][]time.Time

	stop      chan struct{}
	closeOnce sync.Once
}

// stream — поток агента в пределах запуска (§4): повтор seq ≤ последнего — только ack.
type stream struct {
	bootID  string
	lastSeq int64
}

// New — сервер агентов (Agents); фоновая работа (аренды, сроки команд, HTTP-сессии) — до Close.
func New(opts Options) *Agents {
	if opts.Store == nil {
		opts.Store = NewMemoryStore()
	}
	if opts.Files == nil {
		opts.Files = NewMemoryFiles()
	}
	if opts.StatusInterval <= 0 {
		opts.StatusInterval = 5 * time.Second
	}
	if opts.MetricsInterval <= 0 {
		opts.MetricsInterval = 15 * time.Second
	}
	if opts.MetricsStoreInterval == 0 {
		opts.MetricsStoreInterval = 15 * time.Second
	}
	if opts.MetricsRetention == 0 {
		opts.MetricsRetention = 7 * 24 * time.Hour
	}
	if opts.OfflineGrace <= 0 {
		opts.OfflineGrace = 20 * time.Second
	}
	if opts.OfflineAfter <= 0 {
		opts.OfflineAfter = max(3*opts.StatusInterval, 30*time.Second) + opts.OfflineGrace
	}
	if opts.Log == nil {
		opts.Log = slog.Default()
	}
	if opts.EnrollFailureLimit == 0 {
		opts.EnrollFailureLimit = 10
	}
	if opts.EnrollFailureWindow <= 0 {
		opts.EnrollFailureWindow = time.Minute
	}
	a := &Agents{
		opts: opts, store: opts.Store, files: opts.Files, log: opts.Log,
		helloTimeout: helloTimeout, pingInterval: pingInterval, syncIdle: syncIdle, sweepInterval: sweepInterval,
		offlineGrace: opts.OfflineGrace, pruneInterval: pruneInterval,
		publicURL:   opts.PublicURL,
		sessions:    map[string]*session{},
		streams:     map[string]*stream{},
		seen:        map[string]bool{},
		waiters:     map[string][]chan struct{}{},
		kick:        make(chan struct{}),
		offline:     map[string]*time.Timer{},
		stored:      map[string]int64{},
		alerts:      map[alertKey]Alert{},
		enrollFails: map[string][]time.Time{},
		stop:        make(chan struct{}),
	}
	go a.sweeper()
	return a
}

// SetPublicURL — адрес сервера для ссылок на файлы и install.sh (известен
// после запуска).
func (a *Agents) SetPublicURL(url string) {
	a.mu.Lock()
	a.publicURL = url
	a.mu.Unlock()
}

// serverURL — адрес сервера: PublicURL, иначе адрес из запроса (под a.mu).
func (a *Agents) serverURL(fromRequest string) string {
	if a.publicURL != "" {
		return a.publicURL
	}
	return fromRequest
}

// Close — остановить фоновую работу и закрыть сессии (1012: сервер
// перезапускается).
func (a *Agents) Close() {
	a.closeOnce.Do(func() {
		close(a.stop)
		a.mu.Lock()
		a.closed = true
		for _, ss := range a.sessions {
			ss.close(message.CloseRestart)
		}
		// Отложенные переходы в offline — сразу: Agents больше ничего не ждёт.
		for agentID := range a.offline {
			a.setOffline(agentID)
		}
		a.unlock()
	})
}

// agentPoint — точка метрик агента, ждущая OnMetrics.
type agentPoint struct {
	agentID string
	point   MetricsPoint
}

// unlock — снять блокировку и разослать накопленные уведомления.
func (a *Agents) unlock() {
	changes, points, deleted, audits, alerts, logs := a.changes, a.points, a.deleted, a.audits, a.alertQ, a.logs
	a.changes, a.points, a.deleted, a.audits, a.alertQ, a.logs = nil, nil, nil, nil, nil, nil
	a.mu.Unlock()
	if a.opts.OnStateDeleted != nil {
		for _, k := range deleted {
			a.opts.OnStateDeleted(k.domain, k.agentID)
		}
	}
	if a.opts.OnChange != nil {
		for _, c := range changes {
			a.opts.OnChange(c)
		}
	}
	if a.opts.OnMetrics != nil {
		for _, p := range points {
			a.opts.OnMetrics(p.agentID, p.point)
		}
	}
	if a.opts.OnAudit != nil {
		for _, e := range audits {
			a.opts.OnAudit(e)
		}
	}
	if a.opts.OnAlert != nil {
		for _, al := range alerts {
			a.opts.OnAlert(al)
		}
	}
	if a.opts.OnLog != nil {
		for _, l := range logs {
			a.opts.OnLog(l.agentID, l.entries)
		}
	}
}

// emit — уведомление (под a.mu); подряд одинаковые схлопываются.
func (a *Agents) emit(kind, id string) {
	c := Change{Kind: kind, ID: id}
	if n := len(a.changes); n > 0 && a.changes[n-1] == c {
		return
	}
	a.changes = append(a.changes, c)
}

func now() int64 { return time.Now().UnixMilli() }

func newID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	x := hex.EncodeToString(b[:])
	return x[0:8] + "-" + x[8:12] + "-" + x[12:16] + "-" + x[16:20] + "-" + x[20:32]
}

func hashSecret(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

func protoErr(code, msg string) *message.Error {
	return &message.Error{Code: code, Message: msg}
}

// marshalAny — значение в JSON; json.RawMessage — как есть; nil — пусто.
func marshalAny(v any) (json.RawMessage, error) {
	switch x := v.(type) {
	case nil:
		return nil, nil
	case json.RawMessage:
		if len(x) == 0 {
			return nil, nil
		}
		if !json.Valid(x) {
			return nil, errors.New("некорректный JSON")
		}
		return x, nil
	}
	return json.Marshal(v)
}

// ─── регистрация и учётные данные (§5) ─────────────────────────────────

// Enroll — регистрация агента по токену: учётные данные (в хранилище —
// sha256 секрета).
func (a *Agents) Enroll(token, name string, labels map[string]string) (agentID, secret string, err error) {
	if name == "" {
		return "", "", protoErr("MESSAGE_INVALID", "Нужны token и name")
	}
	var extra map[string]string
	switch {
	case a.opts.Enroll != nil:
		var ok bool
		if extra, ok = a.opts.Enroll(token); !ok {
			return "", "", protoErr("AGENT_ENROLLMENT_TOKEN_INVALID", "Токен регистрации неверен")
		}
	case a.opts.EnrollToken != "" && subtle.ConstantTimeCompare([]byte(token), []byte(a.opts.EnrollToken)) == 1:
	default:
		return "", "", protoErr("AGENT_ENROLLMENT_TOKEN_INVALID", "Токен регистрации неверен")
	}
	merged := maps.Clone(labels)
	if merged == nil {
		merged = map[string]string{}
	}
	maps.Copy(merged, extra)
	granted := maps.Clone(extra)
	if granted == nil {
		granted = map[string]string{}
	}
	var raw [24]byte
	_, _ = rand.Read(raw[:])
	secret = hex.EncodeToString(raw[:])
	agent := &Agent{
		ID: newID(), Name: name, Labels: merged, GrantedLabels: granted, EnrolledAt: now(),
		StateApplied: map[string]message.StateApplied{}, SecretHash: hashSecret(secret),
	}
	a.mu.Lock()
	err = a.store.CreateAgent(agent)
	if err == nil {
		a.emit(ChangeAgent, agent.ID)
	}
	a.unlock()
	if err != nil {
		return "", "", err
	}
	a.log.Info("агент зарегистрирован", "agent", name, "id", agent.ID)
	return agent.ID, secret, nil
}

// authenticate — агент по `Authorization: Agent <id>.<secret>`.
func (a *Agents) authenticate(header string) (string, bool) {
	raw, ok := strings.CutPrefix(header, "Agent ")
	if !ok {
		return "", false
	}
	id, secret, ok := strings.Cut(raw, ".")
	if !ok || id == "" || secret == "" {
		return "", false
	}
	hash := []byte(hashSecret(secret))
	a.mu.Lock()
	defer a.unlock()
	agent, err := a.store.GetAgent(id)
	if err != nil || agent.SecretHash == "" || agent.Revoked {
		return "", false
	}
	if subtle.ConstantTimeCompare(hash, []byte(agent.SecretHash)) == 1 {
		return id, true
	}
	// Новый секрет после RotateKey: агент подключился с ним — он основной.
	if agent.PendingSecretHash == "" || subtle.ConstantTimeCompare(hash, []byte(agent.PendingSecretHash)) != 1 {
		return "", false
	}
	agent.SecretHash, agent.PendingSecretHash = agent.PendingSecretHash, ""
	if err := a.store.UpdateAgent(agent); err != nil {
		a.log.Error("новый секрет агента не сохранён", "agent", id, "err", err)
		return "", false
	}
	a.log.Info("агент перешёл на новый секрет", "agent", agent.Name)
	return id, true
}

// ─── API приложения ────────────────────────────────────────────────────

// Enqueue — поставить задачу; раздаётся агентам по свободным слотам.
func (a *Agents) Enqueue(req JobRequest) (*Job, error) { return a.enqueue("", req) }

func (a *Agents) enqueue(actor string, req JobRequest) (*Job, error) {
	if req.Queue == "" {
		return nil, protoErr("MESSAGE_INVALID", "Нужна queue")
	}
	if !message.ValidName(req.Queue) {
		return nil, invalidName("queue", req.Queue)
	}
	data, err := marshalAny(req.Data)
	if err != nil {
		return nil, protoErr("MESSAGE_INVALID", "data: "+err.Error())
	}
	if data == nil {
		data = json.RawMessage("{}")
	}
	lease := req.LeaseSeconds
	if lease <= 0 {
		lease = 60
	}
	job := &Job{
		ID: newID(), Queue: req.Queue, Data: data, Status: JobQueued,
		MaxAttempts: max(req.MaxAttempts, 1), LeaseSeconds: max(lease, 5), PinnedAgentID: req.AgentID,
		Log: []string{}, Events: []JobEvent{},
		Inputs: slices.Sorted(maps.Keys(req.Inputs)), Outputs: slices.Clone(req.Outputs), CreatedAt: now(),
		Actor: actor,
	}
	if job.Inputs == nil {
		job.Inputs = []string{}
	}
	if job.Outputs == nil {
		job.Outputs = []string{}
	}
	a.mu.Lock()
	defer a.unlock()
	if req.AgentID != "" {
		if _, err := a.store.GetAgent(req.AgentID); err != nil {
			return nil, protoErr("AGENT_NOT_FOUND", "Агент не найден")
		}
	}
	if len(req.Inputs) > 0 {
		if err := a.files.Inputs(job, req.Inputs); err != nil {
			return nil, fmt.Errorf("server: входные файлы: %w", err)
		}
	}
	if err := a.store.CreateJob(job); err != nil {
		return nil, err
	}
	a.emit(ChangeJob, job.ID)
	a.audit(actor, AuditJobEnqueue, req.AgentID, job.ID, map[string]any{"queue": job.Queue})
	a.dispatchJobs()
	return a.store.GetJob(job.ID)
}

// CancelJob — отменить задачу: агенту — job.cancel, итог не нужен.
func (a *Agents) CancelJob(id string) error { return a.signalJob("", id, false) }

// StopJob — досрочная остановка: агенту — job.stop, он доводит шаг и сдаёт
// итог. Задача ещё в очереди — отменяется.
func (a *Agents) StopJob(id string) error { return a.signalJob("", id, true) }

func (a *Agents) signalJob(actor, id string, stop bool) error {
	a.mu.Lock()
	defer a.unlock()
	job, err := a.store.GetJob(id)
	if errors.Is(err, ErrNotFound) {
		return protoErr("JOB_NOT_FOUND", "Задача не найдена")
	}
	if err != nil {
		return err
	}
	if job.Status != JobQueued && job.Status != JobRunning {
		return protoErr("JOB_NOT_ACTIVE", "Задача уже завершена")
	}
	ss := a.sessions[job.AgentID]
	if stop && job.Status == JobRunning {
		job.StopRequested = true
		if ss != nil {
			ss.send(message.TypeJobStop, job.Ref(), "")
		}
	} else {
		if ss != nil && job.Status == JobRunning {
			delete(ss.pending, job.ID)
			ss.send(message.TypeJobCancel, job.Ref(), "")
		}
		job.Status, job.FinishedAt = JobCancelled, now()
	}
	if err := a.store.UpdateJob(job); err != nil {
		return err
	}
	a.emit(ChangeJob, job.ID)
	action := AuditJobCancel
	if stop {
		action = AuditJobStop
	}
	a.audit(actor, action, job.AgentID, job.ID, nil)
	return nil
}

// Job — задача по id (ErrNotFound — нет).
func (a *Agents) Job(id string) (*Job, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.store.GetJob(id)
}

// Jobs — задачи, новые первыми.
func (a *Agents) Jobs() ([]*Job, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.store.ListJobs(JobFilter{})
}

// Command — поручить команду агенту (пусто — агенту на связи, объявившему
// её; нет такого — COMMAND_NOT_SUPPORTED). Доставка — когда агент на связи и
// объявил команду; без итога за timeoutSec + 15 с — TIMEOUT.
func (a *Agents) Command(req CommandRequest) (*Command, error) { return a.command("", req) }

// command — Command от имени actor (аудит command).
func (a *Agents) command(actor string, req CommandRequest) (*Command, error) {
	return a.newCommand(actor, req, func(cmd *Command) {
		a.audit(actor, AuditCommand, cmd.AgentID, cmd.ID, map[string]any{"name": cmd.Name})
	})
}

// newCommand — создать команду; audit — запись аудита (под a.mu, после создания).
func (a *Agents) newCommand(actor string, req CommandRequest, audit func(*Command)) (*Command, error) {
	if req.Name == "" {
		return nil, protoErr("MESSAGE_INVALID", "Нужно name")
	}
	if !message.ValidName(req.Name) {
		return nil, invalidName("name", req.Name)
	}
	args, err := marshalAny(req.Args)
	if err != nil {
		return nil, protoErr("MESSAGE_INVALID", "args: "+err.Error())
	}
	a.mu.Lock()
	defer a.unlock()
	agentID := req.AgentID
	if agentID == "" {
		list, err := a.store.ListAgents()
		if err != nil {
			return nil, err
		}
		var capable []*Agent
		for _, agent := range list {
			if declaresCommand(agent.Capabilities, req.Name) {
				capable = append(capable, agent)
			}
		}
		if len(capable) == 0 {
			return nil, protoErr("COMMAND_NOT_SUPPORTED", "Ни один агент не объявил команду "+req.Name)
		}
		agentID = capable[0].ID
		for _, agent := range capable {
			if agent.Online {
				agentID = agent.ID
				break
			}
		}
	} else if _, err := a.store.GetAgent(agentID); err != nil {
		return nil, protoErr("AGENT_NOT_FOUND", "Агент не найден")
	}
	timeout := req.TimeoutSec
	if timeout <= 0 {
		timeout = 60
	}
	cmd := &Command{
		ID: newID(), AgentID: agentID, Name: req.Name, Args: args, TimeoutSec: timeout,
		Status: CommandPending, CreatedAt: now(), Actor: actor,
	}
	if err := a.store.CreateCommand(cmd); err != nil {
		return nil, err
	}
	a.emit(ChangeCommand, cmd.ID)
	audit(cmd)
	if ss := a.sessions[agentID]; ss != nil {
		a.deliver(ss)
	}
	return a.store.GetCommand(cmd.ID)
}

// Call — Command и ждать итог (или отмену ctx). Неуспех команды — в
// Status/Error результата, не ошибка Call.
func (a *Agents) Call(ctx context.Context, req CommandRequest) (*Command, error) {
	return a.call(ctx, "", req)
}

func (a *Agents) call(ctx context.Context, actor string, req CommandRequest) (*Command, error) {
	cmd, err := a.command(actor, req)
	if err != nil {
		return nil, err
	}
	// Итог ловится тремя путями: событие своего процесса (агент подключён сюда),
	// проверка Store раз в секунду и по Refresh (агент подключён к другому
	// процессу бэкенда — итог приходит туда). Предел — срок команды + отсрочка
	// TIMEOUT + запас: дальше возвращается команда как есть, ожидание не вечное.
	done := make(chan struct{})
	a.mu.Lock()
	a.waiters[cmd.ID] = append(a.waiters[cmd.ID], done)
	a.mu.Unlock()
	defer a.dropWaiter(cmd.ID, done)
	deadline := time.NewTimer(time.Duration(cmd.TimeoutSec)*time.Second + commandGrace + callSlack)
	defer deadline.Stop()
	tick := time.NewTicker(callPollInterval)
	defer tick.Stop()
	for {
		if cur, err := a.CommandByID(cmd.ID); err == nil && cur.Finished() {
			return cur, nil
		}
		select {
		case <-done:
			return a.CommandByID(cmd.ID)
		case <-tick.C:
		case <-a.callKick():
		case <-deadline.C:
			return a.CommandByID(cmd.ID)
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
}

// dropWaiter — снять ждущего Call.
func (a *Agents) dropWaiter(id string, done chan struct{}) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.waiters[id] = slices.DeleteFunc(a.waiters[id], func(c chan struct{}) bool { return c == done })
	if len(a.waiters[id]) == 0 {
		delete(a.waiters, id)
	}
}

// callKick — канал, который закроет ближайший Refresh: ждущие Call сразу
// перечитывают итог из Store.
func (a *Agents) callKick() <-chan struct{} {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.kick
}

// CommandByID — команда по id (ErrNotFound — нет).
func (a *Agents) CommandByID(id string) (*Command, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.store.GetCommand(id)
}

// Commands — команды, новые первыми.
func (a *Agents) Commands() ([]*Command, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.store.ListCommands(CommandFilter{})
}

// SetState — новый снимок домена: общий (agentID == "") или для агента
// (перекрывает общий). Версия монотонна и между перезапусками; доставка —
// агентам на связи, объявившим домен.
func (a *Agents) SetState(domain string, spec any, agentID string) (*DesiredState, error) {
	return a.setState("", domain, spec, agentID)
}

func validDomain(domain string) error {
	if domain == "" {
		return protoErr("MESSAGE_INVALID", "Нужен domain")
	}
	if !message.ValidName(domain) {
		return invalidName("domain", domain)
	}
	return nil
}

func (a *Agents) setState(actor, domain string, spec any, agentID string) (*DesiredState, error) {
	if err := validDomain(domain); err != nil {
		return nil, err
	}
	raw, err := marshalAny(spec)
	if err != nil {
		return nil, protoErr("MESSAGE_INVALID", "spec: "+err.Error())
	}
	if raw == nil {
		raw = json.RawMessage("{}")
	}
	a.mu.Lock()
	defer a.unlock()
	if agentID != "" {
		if _, err := a.store.GetAgent(agentID); err != nil {
			return nil, protoErr("AGENT_NOT_FOUND", "Агент не найден")
		}
	}
	st, err := a.putState(actor, domain, agentID, raw)
	if err != nil {
		return nil, err
	}
	a.audit(actor, AuditStateSet, agentID, domain, map[string]any{"version": st.Version})
	return st, nil
}

// putState — сохранить снимок и доставить агентам. Под a.mu.
func (a *Agents) putState(actor, domain, agentID string, raw json.RawMessage) (*DesiredState, error) {
	st, err := a.store.SetState(domain, agentID, raw, actor)
	if err != nil {
		return nil, err
	}
	if !a.domainDeclared(domain, agentID) {
		if agentID == "" {
			a.log.Warn("раздел состояния ещё никто не объявлял — снимок сохранён и уйдёт агентам, когда воркер его объявит",
				"domain", domain)
		} else {
			a.log.Warn("раздел состояния агент ещё не объявлял — снимок сохранён и уйдёт агенту, когда воркер его объявит",
				"domain", domain, "agentId", agentID)
		}
	}
	a.emit(ChangeState, domain)
	for _, ss := range a.sortedSessions() {
		if agentID == "" || ss.agentID == agentID {
			a.deliver(ss)
		}
	}
	return st, nil
}

// DeleteState — удалить снимок домена: личный снимок агента (agentID != "")
// или общий (agentID == ""). Снимка (или агента) нет — не ошибка; удалённый
// снимок — в Options.OnStateDeleted.
//
// Удалён личный и есть общий — общий сохраняется заново с новой версией и
// доставляется: агент пропускает снимки не новее применённого, а личный мог
// быть новее общего (остальные агенты получат тот же общий повторно —
// безопасно). Возвращает этот общий снимок; общего нет — nil, у агента
// остаётся последнее применённое. Удаление общего агентам ничего не
// отправляет (они держат последнее применённое) и личных снимков не трогает;
// возвращает nil.
func (a *Agents) DeleteState(domain, agentID string) (*DesiredState, error) {
	return a.deleteState("", domain, agentID)
}

func (a *Agents) deleteState(actor, domain, agentID string) (*DesiredState, error) {
	if err := validDomain(domain); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.unlock()
	deleted, err := a.store.DeleteState(domain, agentID)
	if err != nil {
		return nil, err
	}
	if deleted {
		a.deleted = append(a.deleted, stateKey{domain, agentID})
		a.emit(ChangeState, domain)
		a.audit(actor, AuditStateDelete, agentID, domain, nil)
	}
	if agentID == "" {
		return nil, nil
	}
	shared, err := a.store.GetState(domain, "")
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil || !deleted {
		return shared, err
	}
	st, err := a.store.SetState(domain, "", shared.Spec, shared.Actor)
	if err != nil {
		return nil, err
	}
	for _, ss := range a.sortedSessions() {
		a.deliver(ss)
	}
	return st, nil
}

// StateHistory — история снимков домена (общих при agentID == "" или
// личных агента) от новых к старым, не больше limit (≤ 0 — 20).
func (a *Agents) StateHistory(domain, agentID string, limit int) ([]*DesiredState, error) {
	if limit <= 0 {
		limit = 20
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	h, err := a.store.ListStateHistory(domain, agentID, limit)
	if h == nil && err == nil {
		h = []*DesiredState{}
	}
	return h, err
}

// RollbackState — вернуть домен (общий при agentID == "" или личный агента) к
// spec версии version из истории: обычный SetState — новая версия с тем же
// spec. Нет такой версии в истории — STATE_VERSION_NOT_FOUND.
func (a *Agents) RollbackState(domain string, version int64, agentID string) (*DesiredState, error) {
	return a.rollbackState("", domain, version, agentID)
}

func (a *Agents) rollbackState(actor, domain string, version int64, agentID string) (*DesiredState, error) {
	if err := validDomain(domain); err != nil {
		return nil, err
	}
	a.mu.Lock()
	defer a.unlock()
	if agentID != "" {
		if _, err := a.store.GetAgent(agentID); err != nil {
			return nil, protoErr("AGENT_NOT_FOUND", "Агент не найден")
		}
	}
	history, err := a.store.ListStateHistory(domain, agentID, 0)
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(history, func(st *DesiredState) bool { return st.Version == version })
	if i < 0 {
		return nil, protoErr("STATE_VERSION_NOT_FOUND", fmt.Sprintf("Версии %d раздела %s нет в истории", version, domain))
	}
	st, err := a.putState(actor, domain, agentID, history[i].Spec)
	if err != nil {
		return nil, err
	}
	a.audit(actor, AuditStateRollback, agentID, domain, map[string]any{"fromVersion": version, "version": st.Version})
	return st, nil
}

// States — снимки доменов.
func (a *Agents) States() ([]*DesiredState, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.store.ListStates()
}

// List — агенты в порядке регистрации.
func (a *Agents) List() ([]*Agent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.store.ListAgents()
}

// Agent — агент по id (ErrNotFound — нет).
func (a *Agents) Agent(id string) (*Agent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.store.GetAgent(id)
}

// Events — последние n событий агентов и воркеров (n ≤ 0 — все), новые первыми.
func (a *Agents) Events(n int) ([]AgentEvent, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.store.ListEvents(n)
}

// ─── фоновая работа ────────────────────────────────────────────────────

// sweeper — раз в секунду: аренды задач, сроки команд, HTTP-сессии без
// запросов, сроки подписок, агенты без вестей (OfflineAfter); раз в pruneInterval (и при запуске) — срок хранения метрик.
func (a *Agents) sweeper() {
	t := time.NewTicker(a.sweepInterval)
	defer t.Stop()
	for {
		a.mu.Lock()
		if !a.closed {
			a.pruneMetrics()
		}
		a.unlock()
		select {
		case <-a.stop:
			return
		case <-t.C:
		}
		a.mu.Lock()
		a.sweep()
		a.unlock()
		a.pruneEnrollFails()
	}
}

// pruneMetrics — удалить точки метрик старше MetricsRetention, если с
// прошлой чистки прошло pruneInterval. Под a.mu.
func (a *Agents) pruneMetrics() {
	if a.opts.MetricsRetention < 0 || (!a.lastPrune.IsZero() && time.Since(a.lastPrune) < a.pruneInterval) {
		return
	}
	a.lastPrune = time.Now()
	n, err := a.store.PruneMetrics(time.Now().Add(-a.opts.MetricsRetention).UnixMilli())
	if err != nil {
		a.log.Error("старые метрики не удалены", "err", err)
		return
	}
	if n > 0 {
		a.log.Debug("удалены старые точки метрик", "count", n)
	}
}

func (a *Agents) sweep() {
	ts := now()
	jobs, err := a.store.ListJobs(JobFilter{Status: JobRunning})
	if err != nil {
		a.log.Error("задачи не прочитаны", "err", err)
	}
	for _, job := range jobs {
		if ts > job.LeaseUntil {
			// Агент на связи, но задачу не перечисляет: её заберут — прервать.
			if ss := a.sessions[job.AgentID]; ss != nil {
				ss.send(message.TypeJobCancel, job.Ref(), "")
			}
			a.failAttempt(job, "LEASE_EXPIRED", "Агент перестал отвечать: аренда истекла", true)
		}
	}
	for _, status := range []string{CommandPending, CommandRunning} {
		cmds, err := a.store.ListCommands(CommandFilter{Status: status})
		if err != nil {
			a.log.Error("команды не прочитаны", "err", err)
		}
		for _, cmd := range cmds {
			// Запас 15 с: итог агента с TIMEOUT приходит сам, здесь — если агент пропал.
			if ts > cmd.CreatedAt+int64(cmd.TimeoutSec)*1000+commandGrace.Milliseconds() {
				cmd.Status, cmd.FinishedAt = CommandFailed, ts
				cmd.Error = &message.CommandError{Code: "TIMEOUT", Message: "Нет итога от агента"}
				a.saveCommand(cmd)
			}
		}
	}
	for _, ss := range a.sortedSessions() {
		if ss.mode == TransportHTTP && time.Since(ss.lastActive) > a.syncIdle {
			a.closeSession(ss, message.CloseNormal)
			continue
		}
		a.expireSubscriptions(ss, ts)
	}
	a.sweepOffline(ts)
}

// saveCommand — сохранить команду; завершённая будит Call.
func (a *Agents) saveCommand(cmd *Command) {
	if err := a.store.UpdateCommand(cmd); err != nil {
		a.log.Error("команда не сохранена", "command", cmd.ID, "err", err)
		return
	}
	a.emit(ChangeCommand, cmd.ID)
	if cmd.Finished() {
		for _, c := range a.waiters[cmd.ID] {
			close(c)
		}
		delete(a.waiters, cmd.ID)
	}
}

func (a *Agents) sortedSessions() []*session {
	out := make([]*session, 0, len(a.sessions))
	for _, ss := range a.sessions {
		out = append(out, ss)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].agentID < out[j].agentID })
	return out
}

// invalidName — имя не по правилу message.NamePattern.
func invalidName(field, name string) error {
	return protoErr("MESSAGE_INVALID", fmt.Sprintf("%s %q не по правилу %s", field, name, message.NamePattern))
}

// domainDeclared — домен объявлял известный агент (agentID != "" — именно
// он): по сохранённым capabilities и текущим сессиям. Под a.mu.
func (a *Agents) domainDeclared(domain, agentID string) bool {
	if agentID != "" {
		if ss := a.sessions[agentID]; ss != nil && declaresDomain(ss.caps, domain) {
			return true
		}
		agent, err := a.store.GetAgent(agentID)
		return err == nil && declaresDomain(agent.Capabilities, domain)
	}
	for _, ss := range a.sessions {
		if declaresDomain(ss.caps, domain) {
			return true
		}
	}
	list, err := a.store.ListAgents()
	if err != nil {
		return true // не знаем — не предупреждаем
	}
	for _, agent := range list {
		if declaresDomain(agent.Capabilities, domain) {
			return true
		}
	}
	return false
}

func declaresDomain(caps *message.Capabilities, domain string) bool {
	if caps == nil || caps.State == nil {
		return false
	}
	_, ok := caps.State.Domains[domain]
	return ok
}

func declaresCommand(caps *message.Capabilities, name string) bool {
	return caps != nil && caps.Commands != nil && slices.Contains(caps.Commands.Names, name)
}
