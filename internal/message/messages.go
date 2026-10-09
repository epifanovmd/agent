package message

import "encoding/json"

// ErrorInfo — описание ошибки `{ code, message }` (§15).
type ErrorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *ErrorInfo) Error() string { return e.Code + ": " + e.Message }

// NewError — ErrorInfo с кодом и текстом.
func NewError(code, msg string) *ErrorInfo { return &ErrorInfo{Code: code, Message: msg} }

// ─── Регистрация (§2) ──────────────────────────────────────────────────

// Enroll — тело POST /api/v1/agent-link/enroll.
type Enroll struct {
	Token  string            `json:"token"`
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Host   Host              `json:"host"`
}

// EnrollResult — ответ на регистрацию: ключ агента.
type EnrollResult struct {
	AgentID string `json:"agentId"`
	Secret  string `json:"secret"`
}

// ─── Начало разговора (§4) ─────────────────────────────────────────────

// Hello — первое сообщение агента. Configs — версии настроек на диске:
// воркер → ключ → версия.
type Hello struct {
	Agent   HelloAgent                  `json:"agent"`
	Host    Host                        `json:"host"`
	Labels  map[string]string           `json:"labels,omitempty"`
	Workers []HelloWorker               `json:"workers,omitempty"`
	Configs map[string]map[string]int64 `json:"configs,omitempty"`
}

type HelloAgent struct {
	Version   string `json:"version"`
	BootID    string `json:"bootId"`
	StartedAt int64  `json:"startedAt"`
}

// Host — платформа узла.
type Host struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	Hostname string `json:"hostname"`
	Kernel   string `json:"kernel,omitempty"`
}

// HelloWorker — воркер из настроек агента; Version — версия сборки у воркера
// из выпуска, у остальных — из манифеста.
type HelloWorker struct {
	Name     string          `json:"name"`
	Version  string          `json:"version,omitempty"`
	Release  bool            `json:"release,omitempty"`
	Manifest *WorkerManifest `json:"manifest,omitempty"`
}

type Welcome struct {
	ServerTime        int64 `json:"serverTime"`
	MetricsIntervalMs int64 `json:"metricsIntervalMs"`
	StatusIntervalMs  int64 `json:"statusIntervalMs"`
}

// ─── Сервер → агент (§5) ───────────────────────────────────────────────

type ConfigPut struct {
	Worker  string          `json:"worker"`
	Key     string          `json:"key"`
	Version int64           `json:"version"`
	Data    json.RawMessage `json:"data"`
}

type ConfigDelete struct {
	Worker string `json:"worker"`
	Key    string `json:"key"`
}

// Fetch — HTTP-запрос к воркеру (§7); id запроса — в конверте.
type Fetch struct {
	Worker    string            `json:"worker"`
	Method    string            `json:"method"`
	Path      string            `json:"path"`
	Headers   map[string]string `json:"headers,omitempty"`
	Body      string            `json:"body,omitempty"`
	Encoding  string            `json:"encoding,omitempty"`
	TimeoutMs int64             `json:"timeoutMs,omitempty"`
}

// Watch — сводная просьба сервера (§9); пустая — снять.
type Watch struct {
	MetricsIntervalMs int64  `json:"metricsIntervalMs,omitempty"`
	LogLevel          string `json:"logLevel,omitempty"`
	UntilMs           int64  `json:"untilMs,omitempty"`
}

// Empty — watch снят.
func (w Watch) Empty() bool { return w == Watch{} }

// Action — встроенное действие (§10); id — в конверте.
type Action struct {
	Name string          `json:"name"`
	Args json.RawMessage `json:"args,omitempty"`
}

type Ack struct {
	IDs []string `json:"ids,omitempty"`
	Seq int64    `json:"seq,omitempty"`
}

// Error — сообщение error: важное сообщение с id из Re не принято.
type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// ─── Агент → сервер (§6) ───────────────────────────────────────────────

// Status — состояние воркеров; Outbox — сколько важных сообщений ждут ack.
type Status struct {
	Workers []WorkerStatus `json:"workers"`
	Outbox  int            `json:"outbox"`
}

type WorkerStatus struct {
	Name  string `json:"name"`
	State string `json:"state"`
	// Message — причина состояния WorkerInvalid.
	Message  string  `json:"message,omitempty"`
	Version  string  `json:"version,omitempty"`
	Release  bool    `json:"release,omitempty"`
	Builtin  bool    `json:"builtin,omitempty"`
	Restarts int     `json:"restarts"`
	Health   *Health `json:"health,omitempty"`
	// Pending — замена ждёт, пока воркер занят: PendingRestart | PendingUpdate.
	Pending  string                  `json:"pending,omitempty"`
	Configs  map[string]ConfigStatus `json:"configs,omitempty"`
	Manifest *WorkerManifest         `json:"manifest,omitempty"`
}

// ConfigStatus — последняя сохранённая версия ключа и итог её применения;
// OK == nil — ещё применяется.
type ConfigStatus struct {
	Version int64      `json:"version"`
	OK      *bool      `json:"ok,omitempty"`
	Error   *ErrorInfo `json:"error,omitempty"`
}

// Metrics — точка метрик; Workers — ответы GET /metrics воркеров как есть.
type Metrics struct {
	CollectedAt int64                      `json:"collectedAt"`
	Host        *HostMetrics               `json:"host,omitempty"`
	Workers     map[string]json.RawMessage `json:"workers,omitempty"`
}

// Log — пачка записей лога (не больше 500).
type Log struct {
	Entries []LogEntry `json:"entries"`
}

// LogEntry — запись лога: Source — LogSourceAgent или имя воркера.
type LogEntry struct {
	At     int64          `json:"at"`
	Level  string         `json:"level"`
	Source string         `json:"source"`
	Msg    string         `json:"msg"`
	Attrs  map[string]any `json:"attrs,omitempty"`
}

// Event — событие воркера; Worker и At заполняет агент.
type Event struct {
	Worker string          `json:"worker"`
	Type   string          `json:"type"`
	Data   json.RawMessage `json:"data,omitempty"`
	At     int64           `json:"at"`
}

type ConfigApplied struct {
	Worker  string     `json:"worker"`
	Key     string     `json:"key"`
	Version int64      `json:"version"`
	OK      bool       `json:"ok"`
	Error   *ErrorInfo `json:"error,omitempty"`
}

type FetchHead struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers"`
}

type FetchChunk struct {
	Data     string `json:"data"`
	Encoding string `json:"encoding"`
}

// FetchEnd — конец ответа; Error — запрос не удался.
type FetchEnd struct {
	Error *ErrorInfo `json:"error,omitempty"`
}

type ActionResult struct {
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *ErrorInfo      `json:"error,omitempty"`
}

// ActionDone — итог отложенной замены воркера (§10); id действия — Re
// конверта, Result — как у ActionResult без отсрочки.
type ActionDone struct {
	Name   string          `json:"name"`
	Worker string          `json:"worker"`
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *ErrorInfo      `json:"error,omitempty"`
}

// DeferredResult — итог worker.restart и worker.update, когда воркер занят:
// замена отложена (Pending — PendingRestart | PendingUpdate), фактический
// итог придёт в action.done.
type DeferredResult struct {
	Deferred bool   `json:"deferred"`
	Pending  string `json:"pending"`
}

// ─── Аргументы и итоги действий (§10) ──────────────────────────────────

// WorkerRestartArgs — args worker.restart; Force — не ждать, пока воркер занят.
type WorkerRestartArgs struct {
	Name  string `json:"name"`
	Force bool   `json:"force,omitempty"`
}

// WorkerUpdateArgs — args worker.update. URL от корня агент дополняет
// адресом сервера.
type WorkerUpdateArgs struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	URL       string `json:"url"`
	SHA256    string `json:"sha256"`
	Signature string `json:"signature"`
	// Force — не ждать, пока воркер занят.
	Force bool `json:"force,omitempty"`
}

type AgentUpdateArgs struct {
	Version   string `json:"version"`
	URL       string `json:"url"`
	SHA256    string `json:"sha256"`
	Signature string `json:"signature"`
}

// AgentLogsArgs — args agent.logs; Worker пусто — журнал агента.
type AgentLogsArgs struct {
	Worker string `json:"worker,omitempty"`
	Lines  int    `json:"lines,omitempty"`
}

// UpdateResult — итог worker.update и agent.update.
type UpdateResult struct {
	Version  string `json:"version"`
	Previous string `json:"previous"`
}

// RotateKeyResult — итог agent.rotateKey: sha256 (hex) нового секрета.
type RotateKeyResult struct {
	SecretHash string `json:"secretHash"`
}

// LogsResult — итог agent.logs.
type LogsResult struct {
	Entries []LogEntry `json:"entries"`
}

// ─── HTTP между агентом и воркером (§12) ───────────────────────────────

// Переменные окружения воркера.
const (
	EnvWorker       = "AGENT_WORKER"
	EnvWorkerSocket = "AGENT_WORKER_SOCKET"
	EnvSocket       = "AGENT_SOCKET"
	EnvWorkerToken  = "AGENT_WORKER_TOKEN"
	EnvVersion      = "AGENT_VERSION"
)

// Пути воркера, которые вызывает агент; ConfigPathPrefix + ключ.
// JobsPath — задачи воркера (§12): их вызывает сервер через fetch.
const (
	JobsPath           = "/jobs"
	ConfigPathPrefix   = "/config/"
	MetricsPath        = "/metrics"
	HealthPath         = "/health"
	CleanupPath        = "/cleanup"
	WorkerManifestPath = "/manifest"
)

// Пути сокета агента для воркера (GET /config/{key} — ConfigPathPrefix).
const (
	EventsPath  = "/events"
	ContextPath = "/context"
)

// ConfigValue — тело PUT /config/{key} воркеру и ответ GET /config/{key} агента.
type ConfigValue struct {
	Version int64           `json:"version"`
	Data    json.RawMessage `json:"data"`
}

// WorkerError — тело ответа воркера с ошибкой (или просто текст).
type WorkerError struct {
	Message string `json:"message"`
}

// Health — ответ GET /health воркера; Busy — занят долгой работой
// (плановая замена ждёт).
type Health struct {
	OK      bool            `json:"ok"`
	Busy    bool            `json:"busy,omitempty"`
	Message string          `json:"message,omitempty"`
	Info    json.RawMessage `json:"info,omitempty"`
}

// EventPost — тело POST /events.
type EventPost struct {
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

// JobRequest — тело POST /jobs (§12).
type JobRequest struct {
	Type  string          `json:"type"`
	JobID string          `json:"jobId"`
	Data  json.RawMessage `json:"data,omitempty"`
	Files *JobFiles       `json:"files,omitempty"`
}

// JobFiles — файлы задачи по ссылкам: воркер сам скачивает входные и
// загружает выходные.
type JobFiles struct {
	Inputs  map[string]string `json:"inputs,omitempty"`
	Outputs map[string]string `json:"outputs,omitempty"`
}

// JobReply — ответ на POST /jobs: Result — итог быстрой задачи (200), ID —
// долгая задача начата (202).
type JobReply struct {
	ID     string          `json:"id,omitempty"`
	Result json.RawMessage `json:"result,omitempty"`
}

// JobStatus — ответ GET /jobs/{id} и POST /jobs/{id}/cancel; Progress — от
// 0 до 1.
type JobStatus struct {
	ID       string          `json:"id"`
	State    string          `json:"state"`
	Progress *float64        `json:"progress,omitempty"`
	Result   json.RawMessage `json:"result,omitempty"`
	Error    *ErrorInfo      `json:"error,omitempty"`
}

// Context — ответ GET /context; Online — есть ли связь с сервером.
type Context struct {
	Agent  ContextAgent `json:"agent"`
	Online bool         `json:"online"`
}

type ContextAgent struct {
	ID      string            `json:"id"`
	Name    string            `json:"name"`
	Version string            `json:"version"`
	Labels  map[string]string `json:"labels,omitempty"`
}
