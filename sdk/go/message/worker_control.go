package message

// Управление воркером (§10): контекст агента для воркера, самоуправление
// воркера и встроенные команды паузы.

// Обмен воркера с агентом: контекст и самоуправление.
const (
	// TypeWorkerContext — контекст агента (A→W, без ответа): сразу после
	// worker.ready и при каждом изменении.
	TypeWorkerContext = "worker.context"
	// TypeWorkerHealth — воркер сообщает, в порядке ли он (W→A).
	TypeWorkerHealth = "worker.health"
	// TypeWorkerPause — не брать новые задачи очередей (W→A).
	TypeWorkerPause = "worker.pause"
	// TypeWorkerResume — снова брать задачи очередей (W→A).
	TypeWorkerResume = "worker.resume"
	// TypeWorkerRestart — воркер просит заменить себя штатно (W→A).
	TypeWorkerRestart = "worker.restart"
)

// Режимы воркера (WorkerContext.Mode).
const (
	// WorkerModeRun — обычная работа.
	WorkerModeRun = "run"
	// WorkerModeCleanup — запущен командой `agent cleanup` только для уборки.
	WorkerModeCleanup = "cleanup"
)

// Состояние воркера по его собственной оценке (StatusWorker.Health).
const (
	WorkerHealthOK       = "ok"
	WorkerHealthDegraded = "degraded"
)

// Встроенные команды агента: пауза и возобновление очередей воркера.
const (
	// CommandWorkerPause — args WorkerPauseArgs: места воркера по очередям — 0.
	CommandWorkerPause = "worker.pause"
	// CommandWorkerResume — args WorkerPauseArgs: снять паузу, выставленную сервером.
	CommandWorkerResume = "worker.resume"
)

// WorkerContext — контекст агента для воркера (worker.context).
type WorkerContext struct {
	// Mode — run | cleanup.
	Mode  string             `json:"mode"`
	Agent WorkerContextAgent `json:"agent"`
	// Online — есть ли связь с сервером.
	Online bool `json:"online"`
	// MetricsIntervalMs — действующая частота метрик агента (с учётом подписки).
	MetricsIntervalMs int64 `json:"metricsIntervalMs"`
	// Channels — подписки на каналы показателей этого воркера: канал →
	// частота, мс. Нет подписок — поля нет.
	Channels map[string]int64 `json:"channels,omitempty"`
	// LogLevel — с какого уровня агент сейчас отправляет лог серверу.
	LogLevel string `json:"logLevel"`
}

// WorkerContextAgent — сведения об агенте; ID пустой, пока агент не
// зарегистрирован.
type WorkerContextAgent struct {
	ID      string            `json:"id"`
	Name    string            `json:"name"`
	Version string            `json:"version"`
	Labels  map[string]string `json:"labels,omitempty"`
}

// WorkerHealth — worker.health: ok или нет и почему.
type WorkerHealth struct {
	OK      bool   `json:"ok"`
	Message string `json:"message,omitempty"`
}

// WorkerPause — worker.pause и worker.resume от воркера: очереди (пусто —
// все очереди воркера).
type WorkerPause struct {
	Queues []string `json:"queues,omitempty"`
}

// WorkerResume — worker.resume от воркера.
type WorkerResume = WorkerPause

// WorkerRestartRequest — worker.restart от воркера: заменить его штатно.
type WorkerRestartRequest struct {
	Reason string `json:"reason,omitempty"`
}

// WorkerPauseArgs — args команд worker.pause и worker.resume: воркер и
// очереди (пусто — все его очереди).
type WorkerPauseArgs struct {
	Name   string   `json:"name"`
	Queues []string `json:"queues,omitempty"`
}
