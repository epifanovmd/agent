// Package protocol — типы и константы протокола агента 4.0 (docs/PROTOCOL.md): связь бэкенд ↔
// агент (HTTPS, вниз — поток SSE, вверх — POST) и агент ↔ воркер (unix-сокет агента, та же
// модель). Пакет публичный: им пользуются агент, клиент воркера (пакет worker) и бэкенды на Go.
package protocol

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"time"
)

// NewID — случайный id (128 бит, hex) для команд, событий и запросов.
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// ─── Адреса ──────────────────────────────────────────────────────────────

// Маршруты бэкенда для агента — под BackendPrefix (§3).
const (
	BackendPrefix = "/agent/v1"
	PathEnroll    = "/enroll"
	PathStream    = "/stream"
	PathPoll      = "/poll"
	PathHello     = "/hello"
	PathResults   = "/results"
	PathEvents    = "/events"
	PathState     = "/state"
	PathMetrics   = "/metrics"
	PathLogs      = "/logs"
	PathRequests  = "/requests"
)

// Маршруты сокета агента для воркеров (§12).
const (
	WorkerPathRegister = "/register"
	WorkerPathStream   = "/stream"
	WorkerPathApplied  = "/applied"
	WorkerPathResults  = "/results"
	WorkerPathProgress = "/progress"
	WorkerPathEvents   = "/events"
	WorkerPathHealth   = "/health"
	WorkerPathRequests = "/requests"
)

// Типы событий потоков SSE (§6, §14, §15).
const (
	SSEDesired = "desired"
	SSECommand = "command"
	SSECancel  = "cancel"
)

// Заголовок входа: агент — "Agent <agentId>.<secret>" (§5), воркер — "Worker <token>" (§12).
const (
	AuthAgentScheme  = "Agent"
	AuthWorkerScheme = "Worker"
)

// Переменные окружения воркера (§11).
const (
	EnvAgentSocket = "AGENT_SOCKET"
	EnvWorkerToken = "AGENT_WORKER_TOKEN"
	EnvWorkerName  = "AGENT_WORKER_NAME"
	EnvStateDir    = "WORKER_STATE_DIR"
)

// ─── Общее ───────────────────────────────────────────────────────────────

// ErrorInfo — ошибка: тело ответа с ошибкой и поле error итогов.
type ErrorInfo struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

func (e *ErrorInfo) Error() string { return e.Code + ": " + e.Message }

// NewError — ошибка с кодом и текстом.
func NewError(code, msg string) *ErrorInfo { return &ErrorInfo{Code: code, Message: msg} }

// Коды ошибок.
const (
	CodeEnrollDenied      = "ENROLL_DENIED"
	CodeUnauthorized      = "UNAUTHORIZED"
	CodeAgentUnknown      = "AGENT_UNKNOWN"
	CodeAgentRevoked      = "AGENT_REVOKED"
	CodeInvalid           = "MESSAGE_INVALID"
	CodeUnknownCommand    = "UNKNOWN_COMMAND"
	CodeExpired           = "EXPIRED"
	CodeCancelled         = "CANCELLED"
	CodeTimeout           = "TIMEOUT"
	CodeBusy              = "BUSY"
	CodeBodyTooLarge      = "BODY_TOO_LARGE"
	CodeWorkerUnknown     = "WORKER_UNKNOWN"
	CodeWorkerUnavailable = "WORKER_UNAVAILABLE"
	CodeWorkerInvalid     = "WORKER_INVALID"
	CodeWorkerNotReleased = "WORKER_NOT_RELEASED"
	CodeCommandUndeclared = "COMMAND_UNDECLARED"
	CodeEventUndeclared   = "EVENT_UNDECLARED"
	CodeRequestUndeclared = "REQUEST_UNDECLARED"
	CodeConfigRejected    = "CONFIG_REJECTED"
	CodeOutboxFull        = "OUTBOX_FULL"
	CodeAgentOffline      = "AGENT_OFFLINE"
	CodeUpdateFailed      = "UPDATE_FAILED"
	CodeUpdateNotVerified = "UPDATE_NOT_VERIFIED"
	CodeUpdateUnsupported = "UPDATE_NOT_SUPPORTED"
	CodeInternal          = "INTERNAL"
)

// Уровни журнала.
const (
	LogDebug = "debug"
	LogInfo  = "info"
	LogWarn  = "warn"
	LogError = "error"
)

// ─── Бэкенд ↔ агент ──────────────────────────────────────────────────────

// Enroll — тело POST /agent/v1/enroll (§4).
type Enroll struct {
	Token  string            `json:"token"`
	Name   string            `json:"name"`
	Labels map[string]string `json:"labels,omitempty"`
	Host   Host              `json:"host"`
}

// EnrollReply — ответ на регистрацию: ключ агента.
type EnrollReply struct {
	AgentID string `json:"agentId"`
	Secret  string `json:"secret"`
}

// Host — платформа узла.
type Host struct {
	OS       string `json:"os"`
	Arch     string `json:"arch"`
	Hostname string `json:"hostname"`
	Kernel   string `json:"kernel,omitempty"`
}

// Hello — тело POST /agent/v1/hello (§9): кто агент и какие у него воркеры.
type Hello struct {
	Agent   HelloAgent        `json:"agent"`
	Host    Host              `json:"host"`
	Name    string            `json:"name"`
	Labels  map[string]string `json:"labels,omitempty"`
	Workers []HelloWorker     `json:"workers"`
}

type HelloAgent struct {
	Version   string `json:"version"`
	BootID    string `json:"bootId"`
	StartedAt int64  `json:"startedAt"`
	// UpdateMode — self | external | disabled.
	UpdateMode string `json:"updateMode"`
	// Install — как агент поставлен: systemd | user | manual; "" — запущен иначе.
	Install string `json:"install,omitempty"`
}

// HelloWorker — воркер из настроек агента и его последняя регистрация.
type HelloWorker struct {
	Name     string          `json:"name"`
	Version  string          `json:"version,omitempty"`
	Release  bool            `json:"release,omitempty"`
	Manifest *WorkerManifest `json:"manifest,omitempty"`
}

// HelloReply — ответ на hello: частоты отправки состояния и метрик.
type HelloReply struct {
	ServerTime int64 `json:"serverTime"`
	StateMs    int64 `json:"stateMs,omitempty"`
	MetricsMs  int64 `json:"metricsMs,omitempty"`
}

// Desired — желаемое состояние узла (§7).
type Desired struct {
	Rev     int64                    `json:"rev"`
	Workers map[string]WorkerDesired `json:"workers,omitempty"`
	Watch   *Watch                   `json:"watch,omitempty"`
}

// WorkerDesired — желаемое одного воркера: настройки по ключам.
type WorkerDesired struct {
	Configs map[string]ConfigValue `json:"configs"`
}

// ConfigValue — значение ключа настроек и его версия.
type ConfigValue struct {
	Version int64           `json:"version"`
	Data    json.RawMessage `json:"data"`
}

// Watch — наблюдение (§7): метрики чаще, журнал подробнее, до Until.
type Watch struct {
	MetricsIntervalMs int64  `json:"metricsIntervalMs,omitempty"`
	LogLevel          string `json:"logLevel,omitempty"`
	Until             int64  `json:"until,omitempty"`
}

// Command — команда (§8, §15): агенту — с Seq, воркеру — без.
type Command struct {
	Seq      int64           `json:"seq,omitempty"`
	ID       string          `json:"id"`
	Type     string          `json:"type"`
	Args     json.RawMessage `json:"args,omitempty"`
	Deadline int64           `json:"deadline,omitempty"`
}

// Типы команд агенту (§8).
const (
	CmdWorkerCommand  = "worker.command"
	CmdWorkerCancel   = "worker.cancel"
	CmdWorkerRestart  = "worker.restart"
	CmdWorkerUpdate   = "worker.update"
	CmdAgentUpdate    = "agent.update"
	CmdAgentRotateKey = "agent.rotateKey"
	CmdAgentLogs      = "agent.logs"
)

// Result — итог команды (§9, §15).
type Result struct {
	Re     string          `json:"re"`
	OK     bool            `json:"ok"`
	Result json.RawMessage `json:"result,omitempty"`
	Error  *ErrorInfo      `json:"error,omitempty"`
}

// Event — событие воркера, как его получает бэкенд (§9).
type Event struct {
	ID     string          `json:"id"`
	Worker string          `json:"worker"`
	Type   string          `json:"type"`
	Data   json.RawMessage `json:"data,omitempty"`
	At     int64           `json:"at"`
}

// EventCommandProgress — событие хода команды воркера (§15): data — Progress с worker.
const EventCommandProgress = "command.progress"

// Accepted — ответ бэкенда на итоги и события (§9).
type Accepted struct {
	Accepted []string    `json:"accepted"`
	Rejected []Rejection `json:"rejected,omitempty"`
}

// Rejection — запись, которую бэкенд не принял и повторять не нужно.
type Rejection struct {
	ID     string `json:"id"`
	Reason string `json:"reason"`
}

// PollReply — ответ длинного опроса (§6).
type PollReply struct {
	Desired  *Desired  `json:"desired,omitempty"`
	Commands []Command `json:"commands"`
}

// State — тело POST /agent/v1/state (§9).
type State struct {
	DesiredRev int64            `json:"desiredRev"`
	Outbox     int              `json:"outbox"`
	Update     *AgentUpdateInfo `json:"update,omitempty"`
	Workers    []WorkerState    `json:"workers"`
}

// AgentUpdateInfo — новая версия агента в его каталоге сборок (§10).
type AgentUpdateInfo struct {
	Latest    string `json:"latest"`
	CheckedAt int64  `json:"checkedAt"`
}

// WorkerState — состояние воркера для бэкенда.
type WorkerState struct {
	Name    string `json:"name"`
	State   string `json:"state"`
	Message string `json:"message,omitempty"`
	Version string `json:"version,omitempty"`
	Release bool   `json:"release,omitempty"`
	// Restarts — перезапусков после падений с запуска агента.
	Restarts int     `json:"restarts"`
	Health   *Health `json:"health,omitempty"`
	// Pending — плановая замена ждёт, пока воркер занят: restart | update.
	Pending string                   `json:"pending,omitempty"`
	Configs map[string]AppliedStatus `json:"configs,omitempty"`
}

// Состояния воркера.
const (
	WorkerStarting = "starting"
	WorkerRunning  = "running"
	WorkerInvalid  = "invalid"
	WorkerBackoff  = "backoff"
	WorkerStopped  = "stopped"
)

// Отложенная замена воркера.
const (
	PendingRestart = "restart"
	PendingUpdate  = "update"
)

// AppliedStatus — версия ключа настроек у воркера и итог её применения; OK nil — применяется.
type AppliedStatus struct {
	Version int64      `json:"version"`
	OK      *bool      `json:"ok,omitempty"`
	Error   *ErrorInfo `json:"error,omitempty"`
}

// Metrics — точка метрик (§9): узел и воркеры (metrics из их пульса).
type Metrics struct {
	At      int64                      `json:"at"`
	Host    *HostMetrics               `json:"host,omitempty"`
	Workers map[string]json.RawMessage `json:"workers,omitempty"`
}

// LogEntry — запись журнала: Source — "agent" или имя воркера.
type LogEntry struct {
	At     int64          `json:"at"`
	Level  string         `json:"level"`
	Source string         `json:"source"`
	Msg    string         `json:"msg"`
	Attrs  map[string]any `json:"attrs,omitempty"`
}

// LogSourceAgent — Source записей самого агента.
const LogSourceAgent = "agent"

// Request — запрос воркера к бэкенду (§17): воркер → агент без Worker, агент → бэкенд с ним.
type Request struct {
	ID        string          `json:"id,omitempty"`
	Worker    string          `json:"worker,omitempty"`
	Type      string          `json:"type"`
	Data      json.RawMessage `json:"data,omitempty"`
	TimeoutMs int64           `json:"timeoutMs,omitempty"`
}

// RequestReply — ответ на запрос воркера.
type RequestReply struct {
	OK    bool            `json:"ok"`
	Data  json.RawMessage `json:"data,omitempty"`
	Error *ErrorInfo      `json:"error,omitempty"`
}

// ─── Аргументы и итоги команд агенту (§8) ────────────────────────────────

type WorkerCommandArgs struct {
	Worker    string          `json:"worker"`
	Type      string          `json:"type"`
	Args      json.RawMessage `json:"args,omitempty"`
	TimeoutMs int64           `json:"timeoutMs,omitempty"`
}

type WorkerCancelArgs struct {
	Worker string `json:"worker"`
	Re     string `json:"re"`
}

type WorkerRestartArgs struct {
	Worker string `json:"worker"`
	Force  bool   `json:"force,omitempty"`
}

type WorkerUpdateArgs struct {
	Worker    string `json:"worker"`
	Version   string `json:"version"`
	URL       string `json:"url"`
	SHA256    string `json:"sha256"`
	Signature string `json:"signature"`
	Force     bool   `json:"force,omitempty"`
}

type AgentUpdateArgs struct {
	Version string `json:"version"`
}

type AgentLogsArgs struct {
	Worker string `json:"worker,omitempty"`
	Lines  int    `json:"lines,omitempty"`
}

type UpdateResult struct {
	Version  string `json:"version"`
	Previous string `json:"previous,omitempty"`
}

type RotateKeyResult struct {
	SecretHash string `json:"secretHash"`
}

type LogsResult struct {
	Entries []LogEntry `json:"entries"`
}

// ─── Агент ↔ воркер (§11–§17) ────────────────────────────────────────────

// Register — тело POST /register воркера (§13).
type Register struct {
	Version  string         `json:"version,omitempty"`
	Manifest WorkerManifest `json:"manifest"`
}

// WorkerDesiredEvent — data события desired потока воркера: его настройки целиком (§14).
type WorkerDesiredEvent struct {
	Configs map[string]ConfigValue `json:"configs"`
}

// Applied — итог применения ключа настроек воркером (§14).
type Applied struct {
	Key     string     `json:"key"`
	Version int64      `json:"version"`
	OK      bool       `json:"ok"`
	Error   *ErrorInfo `json:"error,omitempty"`
}

// Cancel — data события cancel потока воркера: отменить команду Re (§15).
type Cancel struct {
	Re string `json:"re"`
}

// Progress — ход долгой команды (§15): Progress — от 0 до 1.
type Progress struct {
	Re       string  `json:"re"`
	Worker   string  `json:"worker,omitempty"`
	Progress float64 `json:"progress"`
	Message  string  `json:"message,omitempty"`
}

// WorkerEvent — событие воркера, как он его присылает агенту (§16).
type WorkerEvent struct {
	ID   string          `json:"id"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

// Health — пульс воркера (§16).
type Health struct {
	OK      bool            `json:"ok"`
	Busy    bool            `json:"busy,omitempty"`
	Message string          `json:"message,omitempty"`
	Metrics json.RawMessage `json:"metrics,omitempty"`
}

// ─── Пределы и сроки (§20) ───────────────────────────────────────────────

const (
	MaxBatch          = 500
	MaxBatchBytes     = 1 << 20
	MaxResultBytes    = 4 << 20
	MaxArgsBytes      = 4 << 20
	MaxEventDataBytes = 64 << 10
	MaxRequestBytes   = 64 << 10
	MaxManifestBytes  = 64 << 10
	MaxDesiredBytes   = 16 << 20
	MaxEnrollBytes    = 64 << 10
	MetricsBacklog    = 600
	DefaultLogLines   = 200
	MaxLogLines       = 5000
	CommandJournal    = 10000
	// MaxOutbox — предел очереди событий и итогов агента по умолчанию (outbox.limit).
	MaxOutbox = 10000
)

const (
	StreamPing            = 15 * time.Second
	StreamSilence         = 45 * time.Second
	StreamOpenTimeout     = 10 * time.Second
	PollWait              = 30 * time.Second
	RegisterTimeout       = 60 * time.Second
	HealthInterval        = 10 * time.Second
	HealthSilence         = 30 * time.Second
	HealthRestartAfter    = 60 * time.Second
	BusyTimeout           = 24 * time.Hour
	DefaultStateInterval  = 30 * time.Second
	DefaultMetricsEvery   = 10 * time.Second
	MinWatchInterval      = time.Second
	DefaultRequestTimeout = 30 * time.Second
	MaxRequestTimeout     = 5 * time.Minute
	DefaultCommandTimeout = 30 * time.Second
)
