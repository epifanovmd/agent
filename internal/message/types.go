package message

// Типы сообщений сервер → агент (§5).
const (
	TypeWelcome      = "welcome"
	TypeConfigPut    = "config.put"
	TypeConfigDelete = "config.delete"
	TypeFetch        = "fetch"
	TypeFetchCancel  = "fetch.cancel"
	TypeWatch        = "watch"
	TypeAction       = "action"
	TypeAck          = "ack"
	TypeError        = "error"
)

// Типы сообщений агент → сервер (§6).
const (
	TypeHello         = "hello"
	TypeStatus        = "status"
	TypeMetrics       = "metrics"
	TypeLog           = "log"
	TypeEvent         = "event"
	TypeConfigApplied = "config.applied"
	TypeFetchHead     = "fetch.head"
	TypeFetchChunk    = "fetch.chunk"
	TypeFetchEnd      = "fetch.end"
	TypeActionResult  = "action.result"
)

// Класс доставки сообщения агента (§3).
type Class int

const (
	// ClassControl — действует в текущем соединении.
	ClassControl Class = iota
	// ClassImportant — хранится в outbox до ack {ids}.
	ClassImportant
	// ClassStream — нумеруется seq, без связи держатся последние сообщения.
	ClassStream
	// ClassReply — ответ на запрос в текущем соединении.
	ClassReply
)

// ClassOf — класс доставки сообщения агента по типу.
func ClassOf(typ string) Class {
	switch typ {
	case TypeEvent, TypeConfigApplied, TypeActionResult:
		return ClassImportant
	case TypeStatus, TypeMetrics, TypeLog:
		return ClassStream
	case TypeFetchHead, TypeFetchChunk, TypeFetchEnd:
		return ClassReply
	}
	return ClassControl
}

// Встроенные действия (§10).
const (
	ActionWorkerRestart  = "worker.restart"
	ActionWorkerUpdate   = "worker.update"
	ActionAgentUpdate    = "agent.update"
	ActionAgentRotateKey = "agent.rotateKey"
	ActionAgentLogs      = "agent.logs"
)

// Коды ошибок (§15).
const (
	CodeMessageInvalid     = "MESSAGE_INVALID"
	CodeUnknownType        = "UNKNOWN_TYPE"
	CodeEnrollDenied       = "ENROLL_DENIED"
	CodeRateLimited        = "RATE_LIMITED"
	CodeUnauthorized       = "UNAUTHORIZED"
	CodeNotFound           = "NOT_FOUND"
	CodeOutboxFull         = "OUTBOX_FULL"
	CodeWorkerUnknown      = "WORKER_UNKNOWN"
	CodeWorkerUnavailable  = "WORKER_UNAVAILABLE"
	CodeTimeout            = "TIMEOUT"
	CodeCancelled          = "CANCELLED"
	CodeDisconnected       = "DISCONNECTED"
	CodePathForbidden      = "PATH_FORBIDDEN"
	CodeBodyTooLarge       = "BODY_TOO_LARGE"
	CodeBusy               = "BUSY"
	CodeConfigRejected     = "CONFIG_REJECTED"
	CodeActionUnknown      = "ACTION_UNKNOWN"
	CodeActionFailed       = "ACTION_FAILED"
	CodeWorkerNotReleased  = "WORKER_NOT_RELEASED"
	CodeUpdateNotVerified  = "UPDATE_NOT_VERIFIED"
	CodeUpdateNotSupported = "UPDATE_NOT_SUPPORTED"
	CodeUpdateFailed       = "UPDATE_FAILED"
	// CodeWorkerInvalid — воркер не зарегистрирован (state: invalid, §12).
	CodeWorkerInvalid = "WORKER_INVALID"
	// CodeConfigKeyUnknown — ключа нет в манифесте воркера (configs, §8).
	CodeConfigKeyUnknown = "CONFIG_KEY_UNKNOWN"
	// CodeEventUndeclared — типа события нет в манифесте воркера (events, §12).
	CodeEventUndeclared = "EVENT_UNDECLARED"
	// CodeInternal — сообщение не обработано из-за внутренней ошибки
	// (сервер — `error`, агент — сокет агента, §15).
	CodeInternal = "INTERNAL"
)

// Codes — все коды ошибок.
var Codes = []string{
	CodeMessageInvalid, CodeUnknownType, CodeEnrollDenied, CodeRateLimited, CodeUnauthorized, CodeNotFound,
	CodeOutboxFull, CodeWorkerUnknown, CodeWorkerUnavailable, CodeTimeout, CodeCancelled, CodeDisconnected,
	CodePathForbidden, CodeBodyTooLarge, CodeBusy, CodeConfigRejected, CodeActionUnknown, CodeActionFailed,
	CodeWorkerNotReleased, CodeUpdateNotVerified, CodeUpdateNotSupported, CodeUpdateFailed,
	CodeWorkerInvalid, CodeConfigKeyUnknown, CodeEventUndeclared, CodeInternal,
}

// Состояния воркера (WorkerStatus.State).
const (
	WorkerStarting = "starting"
	WorkerRunning  = "running"
	// WorkerInvalid — процесс работает, но не зарегистрирован: GET /health или
	// GET /manifest ответил не по §12; причина — WorkerStatus.Message.
	WorkerInvalid = "invalid"
	WorkerBackoff = "backoff"
	WorkerStopped = "stopped"
)

// Какая замена ждёт, пока воркер занят (WorkerStatus.Pending).
const (
	PendingRestart = "restart"
	PendingUpdate  = "update"
)

// Уровни лога (LogEntry.Level, Watch.LogLevel).
const (
	LogDebug = "debug"
	LogInfo  = "info"
	LogWarn  = "warn"
	LogError = "error"
)

// LogSourceAgent — источник записей лога самого агента.
const LogSourceAgent = "agent"

// Кодировки тела fetch.
const (
	EncodingUTF8   = "utf8"
	EncodingBase64 = "base64"
)

// BuiltinSysmetrics — имя встроенного воркера метрик узла (§9).
const BuiltinSysmetrics = "sysmetrics"
