package server

import (
	"encoding/json"
	"maps"
	"slices"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Статусы задачи.
const (
	JobQueued    = "queued"
	JobRunning   = "running"
	JobCompleted = "completed"
	JobFailed    = "failed"
	JobCancelled = "cancelled"
)

// Статусы команды.
const (
	CommandPending   = "pending"
	CommandRunning   = "running"
	CommandSucceeded = "succeeded"
	CommandFailed    = "failed"
	// CommandCancelled — команду отменили (Agents.CancelCommand).
	CommandCancelled = "cancelled"
)

// Транспорт сессии агента.
const (
	TransportWS   = "ws"
	TransportHTTP = "http"
)

// Время в модели — миллисекунды UTC (как во всех SDK).

// Agent — зарегистрированный агент. Секрет наружу не отдаётся.
type Agent struct {
	ID     string            `json:"id"`
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels"`
	Online bool              `json:"online"`
	// Revoked — учётные данные отозваны (Revoke): нужна новая регистрация.
	Revoked   bool   `json:"revoked"`
	Transport string `json:"transport,omitempty"`
	// Address — адрес (IP без порта), с которого пришло последнее подключение
	// (WebSocket или HTTP sync с hello); X-Forwarded-For — только при
	// Options.TrustProxy.
	Address      string                `json:"address,omitempty"`
	EnrolledAt   int64                 `json:"enrolledAt"`
	LastSeenAt   int64                 `json:"lastSeenAt,omitempty"`
	Hello        *message.Hello        `json:"hello,omitempty"`
	Capabilities *message.Capabilities `json:"capabilities,omitempty"`
	Status       *message.Status       `json:"status,omitempty"`
	// Metrics — последняя по времени точка без backfill («текущие» метрики).
	Metrics *message.Metrics `json:"metrics,omitempty"`
	// Inventory — последнее inventory агента.
	Inventory    *message.Inventory              `json:"inventory,omitempty"`
	StateApplied map[string]message.StateApplied `json:"stateApplied"`
	// Subscriptions — подписки на агента (Agents.Subscribe); в хранилище —
	// общие для всех процессов бэкенда. Истёкшие удаляет сверка.
	Subscriptions []Subscription `json:"subscriptions,omitempty"`
	// SecretHash — sha256 секрета (hex); MetricsAt — время точки Metrics по
	// часам сервера (мс). Служебные: хранит Store, наружу не отдаются.
	SecretHash string `json:"-"`
	MetricsAt  int64  `json:"-"`
	// PendingSecretHash — sha256 нового секрета после RotateKey, пока агент
	// не подключился с ним (тогда он становится SecretHash). Служебное.
	PendingSecretHash string `json:"-"`
	// GrantedLabels — метки, выданные бэкендом при регистрации (Options.Enroll):
	// при каждом hello Labels = hello.labels + GrantedLabels, выданные узел
	// переписать не может. Служебное.
	GrantedLabels map[string]string `json:"-"`
	// BootID, LastSeq — учёт seq потока агента (§4): запуск агента и
	// последний принятый seq; повтор (seq ≤ LastSeq) не обрабатывается
	// второй раз ни одним процессом. Служебные.
	BootID  string `json:"-"`
	LastSeq int64  `json:"-"`
	// Alerts — активные уведомления о проблемах агента (Agents.Alerts). Служебное.
	Alerts []Alert `json:"-"`
	// Rev — версия записи для условной записи (Store.UpdateAgent). Служебное.
	Rev int64 `json:"-"`
}

// MetricsPoint — точка истории метрик: At — время точки по часам сервера
// (мс): collectedAt + clockOffsetMs, без clockOffsetMs — момент получения;
// Backfill — из сообщения; Metrics — сообщение metrics целиком.
type MetricsPoint struct {
	At       int64           `json:"at"`
	Backfill bool            `json:"backfill"`
	Metrics  message.Metrics `json:"metrics"`
}

// Clone — копия: карты копируются, Hello/Status/Metrics/Inventory/Capabilities Agents
// заменяет целиком и не меняет на месте.
func (a *Agent) Clone() *Agent {
	if a == nil {
		return nil
	}
	c := *a
	c.Labels = maps.Clone(a.Labels)
	c.GrantedLabels = maps.Clone(a.GrantedLabels)
	c.StateApplied = maps.Clone(a.StateApplied)
	c.Alerts = slices.Clone(a.Alerts)
	if a.Subscriptions != nil {
		c.Subscriptions = make([]Subscription, len(a.Subscriptions))
		for i, s := range a.Subscriptions {
			c.Subscriptions[i] = s.clone()
		}
	}
	return &c
}

// JobEvent — событие задачи (job.event).
type JobEvent struct {
	Seq  int64           `json:"seq"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
	At   int64           `json:"at"`
}

// JobError — ошибка попытки.
type JobError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Job — задача очереди.
type Job struct {
	ID            string          `json:"id"`
	Queue         string          `json:"queue"`
	Data          json.RawMessage `json:"data"`
	Status        string          `json:"status"`
	Attempt       int             `json:"attempt"`
	MaxAttempts   int             `json:"maxAttempts"`
	LeaseSeconds  int             `json:"leaseSeconds"`
	AgentID       string          `json:"agentId,omitempty"`
	PinnedAgentID string          `json:"pinnedAgentId,omitempty"`
	Accepted      bool            `json:"accepted"`
	Progress      float64         `json:"progress"`
	Text          string          `json:"text,omitempty"`
	Log           []string        `json:"log"`
	Events        []JobEvent      `json:"events"`
	Result        json.RawMessage `json:"result,omitempty"`
	Error         *JobError       `json:"error,omitempty"`
	StopRequested bool            `json:"stopRequested"`
	Inputs        []string        `json:"inputs"`
	Outputs       []string        `json:"outputs"`
	CreatedAt     int64           `json:"createdAt"`
	FinishedAt    int64           `json:"finishedAt,omitempty"`
	// Actor — кто поставил задачу (Agents.By); пусто — не указан.
	Actor string `json:"actor,omitempty"`

	// LeaseUntil — срок аренды попытки (мс); EventSeq — последний принятый
	// seq событий попытки. Служебные: хранит Store, наружу не отдаются.
	LeaseUntil int64 `json:"-"`
	EventSeq   int64 `json:"-"`
	// Rev — версия записи для условной записи (Store.UpdateJob). Служебное.
	Rev int64 `json:"-"`
}

// Ref — задача и текущая попытка.
func (j *Job) Ref() message.JobRef { return message.JobRef{JobID: j.ID, Attempt: j.Attempt} }

// Clone — копия со своими срезами.
func (j *Job) Clone() *Job {
	if j == nil {
		return nil
	}
	c := *j
	c.Log = slices.Clone(j.Log)
	c.Events = slices.Clone(j.Events)
	c.Inputs = slices.Clone(j.Inputs)
	c.Outputs = slices.Clone(j.Outputs)
	if j.Error != nil {
		e := *j.Error
		c.Error = &e
	}
	return &c
}

// Command — команда агенту.
type Command struct {
	ID         string                `json:"id"`
	AgentID    string                `json:"agentId"`
	Name       string                `json:"name"`
	Args       json.RawMessage       `json:"args,omitempty"`
	TimeoutSec int                   `json:"timeoutSec"`
	Status     string                `json:"status"`
	Output     string                `json:"output"`
	Result     json.RawMessage       `json:"result,omitempty"`
	Error      *message.CommandError `json:"error,omitempty"`
	ExitCode   *int                  `json:"exitCode,omitempty"`
	CreatedAt  int64                 `json:"createdAt"`
	FinishedAt int64                 `json:"finishedAt,omitempty"`
	// Actor — кто поручил команду (Agents.By); пусто — не указан.
	Actor string `json:"actor,omitempty"`
	// Rev — версия записи для условной записи (Store.UpdateCommand). Служебное.
	Rev int64 `json:"-"`
}

// Finished — команда завершена (выполнена, провалена или отменена).
func (c *Command) Finished() bool {
	return c.Status == CommandSucceeded || c.Status == CommandFailed || c.Status == CommandCancelled
}

// Clone — копия.
func (c *Command) Clone() *Command {
	if c == nil {
		return nil
	}
	x := *c
	if c.Error != nil {
		e := *c.Error
		x.Error = &e
	}
	return &x
}

// DesiredState — снимок домена: общий (AgentID пусто) или для агента.
type DesiredState struct {
	Domain    string          `json:"domain"`
	AgentID   string          `json:"agentId,omitempty"`
	Version   int64           `json:"version"`
	Spec      json.RawMessage `json:"spec"`
	UpdatedAt int64           `json:"updatedAt"`
	// Actor — кто задал снимок (Agents.By); пусто — не указан.
	Actor string `json:"actor,omitempty"`
}

// AgentEvent — событие агента или воркера (event).
type AgentEvent struct {
	AgentID   string          `json:"agentId"`
	AgentName string          `json:"agentName"`
	Source    string          `json:"source"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data,omitempty"`
	At        int64           `json:"at"`
}

// JobRequest — постановка задачи.
type JobRequest struct {
	Queue string `json:"queue"`
	// Data — данные задачи (сериализуются в JSON; json.RawMessage — как есть).
	Data any `json:"data"`
	// MaxAttempts — попыток всего (по умолчанию 1).
	MaxAttempts int `json:"maxAttempts"`
	// LeaseSeconds — аренда: допустимое время без связи (по умолчанию 60, не меньше 5).
	LeaseSeconds int `json:"leaseSeconds"`
	// Inputs — входные файлы: имя → содержимое или URL (по провайдеру файлов).
	Inputs map[string]string `json:"inputs"`
	// Outputs — имена выходных файлов.
	Outputs []string `json:"outputs"`
	// AgentID — закрепить задачу за агентом.
	AgentID string `json:"agentId"`
}

// CommandRequest — команда агенту.
type CommandRequest struct {
	// AgentID — агент; пусто — агент на связи, объявивший команду.
	AgentID string `json:"agentId"`
	Name    string `json:"name"`
	// Args — аргументы (JSON; json.RawMessage — как есть).
	Args any `json:"args"`
	// TimeoutSec — срок (по умолчанию 60).
	TimeoutSec int `json:"timeoutSec"`
}

// Виды изменений (Change.Kind).
const (
	ChangeAgent   = "agent"
	ChangeJob     = "job"
	ChangeCommand = "command"
	ChangeState   = "state"
	ChangeEvent   = "event"
)

// Change — что изменилось (подробности — чтением): ID — агента, задачи,
// команды; для state — раздел, для event — id сообщения события.
type Change struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

// Действия журнала аудита (AuditEntry.Action).
const (
	AuditJobEnqueue     = "job.enqueue"
	AuditJobCancel      = "job.cancel"
	AuditJobStop        = "job.stop"
	AuditCommand        = "command"
	AuditCommandCancel  = "command.cancel"
	AuditStateSet       = "state.set"
	AuditStateDelete    = "state.delete"
	AuditStateRollback  = "state.rollback"
	AuditAgentRevoke    = "agent.revoke"
	AuditAgentUpdate    = "agent.update"
	AuditAgentRotateKey = "agent.rotateKey"
	AuditAgentDelete    = "agent.delete"
	AuditWorkerUpdate   = "worker.update"
	AuditWorkerPause    = "worker.pause"
	AuditWorkerResume   = "worker.resume"
)

// AuditEntry — запись журнала аудита (Options.OnAudit): изменяющее действие
// приложения. Target — id задачи или команды, раздел состояния, id агента
// (agent.*); AgentID — агент, к которому относится действие (если есть).
// SDK журнал не хранит.
type AuditEntry struct {
	At      int64          `json:"at"`
	Actor   string         `json:"actor"`
	Action  string         `json:"action"`
	AgentID string         `json:"agentId,omitempty"`
	Target  string         `json:"target"`
	Details map[string]any `json:"details,omitempty"`
}

// Типы уведомлений о проблемах (Alert.Type).
const (
	AlertOffline     = "offline"
	AlertStateFailed = "stateFailed"
	AlertWorkerDown  = "workerDown"
	AlertDegraded    = "degraded"
	// AlertWorkerDegraded — воркер сам сообщил, что не в порядке
	// (status.workers[].health = degraded).
	AlertWorkerDegraded = "workerDegraded"
)

// Alert — уведомление о проблеме агента (Options.OnAlert, Agents.Alerts):
// Active — проблема началась (true) или закончилась (false). Domain — для
// stateFailed, Worker — для workerDown и workerDegraded. Message — текст
// проблемы; в конце — тот же, что в начале.
type Alert struct {
	Type      string `json:"type"`
	AgentID   string `json:"agentId"`
	AgentName string `json:"agentName"`
	Active    bool   `json:"active"`
	Message   string `json:"message"`
	At        int64  `json:"at"`
	Domain    string `json:"domain,omitempty"`
	Worker    string `json:"worker,omitempty"`
}
