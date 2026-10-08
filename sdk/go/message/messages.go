package message

import (
	"encoding/json"
	"regexp"
	"strings"
)

// Типы сообщений агент → сервер.
const (
	TypeHello        = "hello"
	TypeStatus       = "status"
	TypeMetrics      = "metrics"
	TypeJobAccept    = "job.accept"
	TypeJobReject    = "job.reject"
	TypeJobProgress  = "job.progress"
	TypeJobEvent     = "job.event"
	TypeJobURLs      = "job.urls"
	TypeJobComplete  = "job.complete"
	TypeJobFail      = "job.fail"
	TypeCmdAccept    = "cmd.accept"
	TypeCmdOutput    = "cmd.output"
	TypeCmdDone      = "cmd.done"
	TypeStateApplied = "state.applied"
	// TypeCapabilities — возможности, появившиеся после hello (воркеры).
	TypeCapabilities = "capabilities"
	// TypeEvent — событие агента или воркера (надёжно).
	TypeEvent = "event"
	// TypeInventory — сведения об узле, меняющиеся редко (поток).
	TypeInventory = "inventory"
	// TypeLog — пачка записей лога агента и вывода воркеров (поток).
	TypeLog = "log"
)

// Типы сообщений сервер → агент.
const (
	TypeWelcome   = "welcome"
	TypeConfig    = "config"
	TypeAck       = "ack"
	TypeError     = "error"
	TypeJobAssign = "job.assign"
	TypeJobCancel = "job.cancel"
	TypeJobStop   = "job.stop"
	TypeCmdRun    = "cmd.run"
	TypeStatePut  = "state.put"
)

// Обмен воркера с агентом (§10).
const (
	TypeWorkerRegister = "worker.register"
	TypeWorkerReady    = "worker.ready"
	TypeWorkerDrain    = "worker.drain"
	TypeCmdCancel      = "cmd.cancel"
	TypeTelemetry      = "telemetry"
	// TypeWorkerCleanup — уборка перед удалением агента с узла (A→W, с id);
	// шлёт только `agent cleanup`, без связи с сервером.
	TypeWorkerCleanup = "worker.cleanup"
	// TypeWorkerCleaned — итог уборки (W→A, re = id запроса).
	TypeWorkerCleaned = "worker.cleaned"
)

// WorkerRegister — воркер объявляет себя и что обслуживает: очереди,
// команды, домены желаемого состояния, каналы телеметрии.
type WorkerRegister struct {
	Name     string          `json:"name"`
	Version  string          `json:"version,omitempty"`
	SDK      string          `json:"sdk,omitempty"`
	Queues   []QueueCapacity `json:"queues,omitempty"`
	Commands []string        `json:"commands,omitempty"`
	Domains  []string        `json:"domains,omitempty"`
	Channels []string        `json:"channels,omitempty"`
}

// WorkerCleaned — итог worker.cleanup: ok или текст ошибки.
type WorkerCleaned struct {
	OK    bool   `json:"ok"`
	Error string `json:"error,omitempty"`
}

// WorkerReady — регистрация принята; Rejected — отклонённые имена
// (зарезервированные или занятые другим воркером).
type WorkerReady struct {
	AgentVersion string   `json:"agentVersion"`
	Rejected     []string `json:"rejected,omitempty"`
}

// Telemetry — данные канала телеметрии воркера (последние на момент metrics).
type Telemetry struct {
	Channel string          `json:"channel"`
	Data    json.RawMessage `json:"data"`
}

// Event — событие агента или воркера; Source — имя воркера (заполняет агент).
type Event struct {
	Source string          `json:"source,omitempty"`
	Type   string          `json:"type"`
	Data   json.RawMessage `json:"data,omitempty"`
}

// NamePattern — правило имени всего, что объявляет воркер: очереди, команды,
// разделы состояния (domains), каналы показателей. Начинается с латинской
// буквы или цифры, дальше латиница, цифры, «.», «_», «-»; всего не больше 64.
const NamePattern = `^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`

var nameRe = regexp.MustCompile(NamePattern)

// ValidName — имя соответствует NamePattern.
func ValidName(name string) bool { return nameRe.MatchString(name) }

// Reserved — имя команды или домена принадлежит агенту (§6.7).
func Reserved(name string) bool {
	return strings.HasPrefix(name, "agent.") || strings.HasPrefix(name, "worker.")
}

// Встроенные команды агента (§6.7).
const (
	CommandLogs    = "agent.logs"
	CommandDrain   = "agent.drain"
	CommandResume  = "agent.resume"
	CommandRestart = "agent.restart"
	CommandUpdate  = "agent.update"
	// CommandRotateKey — сменить секрет агента (§5): итог `{secretHash}`.
	CommandRotateKey = "agent.rotateKey"
)

// RotateKeyResult — итог agent.rotateKey: sha256 (hex) нового секрета.
type RotateKeyResult struct {
	SecretHash string `json:"secretHash"`
}

// Состояния агента в status.state.
const (
	StateStarting = "starting"
	StateIdle     = "idle"
	StateBusy     = "busy"
	StateDraining = "draining"
	StateUpdating = "updating"
	StateDegraded = "degraded"
)

// Режимы обновления агента.
const (
	UpdateSelf     = "self"
	UpdateExternal = "external"
	UpdateDisabled = "disabled"
)

// ─── Сессия ────────────────────────────────────────────────────────────

type Hello struct {
	Versions     []int             `json:"versions"`
	Agent        HelloAgent        `json:"agent"`
	Host         Host              `json:"host"`
	Labels       map[string]string `json:"labels,omitempty"`
	Capabilities Capabilities      `json:"capabilities"`
	Jobs         []JobRef          `json:"jobs"`
}

type HelloAgent struct {
	Name      string `json:"name"`
	Version   string `json:"version"`
	SDK       string `json:"sdk,omitempty"`
	CodeHash  string `json:"codeHash,omitempty"`
	BootID    string `json:"bootId"`
	StartedAt int64  `json:"startedAt"`
	// EncryptionKey — открытый ключ X25519 агента (base64, 32 байта): им
	// сервер запечатывает секреты в снимках состояния (пакет sdk/go/sealed).
	EncryptionKey string `json:"encryptionKey,omitempty"`
}

type Host struct {
	Hostname    string `json:"hostname"`
	OS          string `json:"os"`
	Arch        string `json:"arch"`
	Platform    string `json:"platform,omitempty"`
	Kernel      string `json:"kernel,omitempty"`
	CPUs        int    `json:"cpus,omitempty"`
	MemoryBytes uint64 `json:"memoryBytes,omitempty"`
}

type Capabilities struct {
	Jobs      *JobsCapability      `json:"jobs,omitempty"`
	Commands  *CommandsCapability  `json:"commands,omitempty"`
	State     *StateCapability     `json:"state,omitempty"`
	Telemetry *TelemetryCapability `json:"telemetry,omitempty"`
	Update    *UpdateCapability    `json:"update,omitempty"`
}

type JobsCapability struct {
	Queues []QueueCapacity `json:"queues"`
}

type QueueCapacity struct {
	Name        string `json:"name"`
	Concurrency int    `json:"concurrency"`
}

type CommandsCapability struct {
	Names []string `json:"names"`
}

// StateCapability — домен → применённая версия (nil — ещё не применялась).
type StateCapability struct {
	Domains map[string]*int64 `json:"domains"`
}

type TelemetryCapability struct {
	Channels []string `json:"channels"`
}

type UpdateCapability struct {
	Mode string `json:"mode"`
}

type Welcome struct {
	Version    int           `json:"version"`
	AgentID    string        `json:"agentId"`
	SessionID  string        `json:"sessionId"`
	ServerTime int64         `json:"serverTime"`
	Config     SessionConfig `json:"config"`
}

// SessionConfig — настройки сессии от сервера; в `config` — частично (нули не меняют).
type SessionConfig struct {
	StatusIntervalMs  int64 `json:"statusIntervalMs,omitempty"`
	MetricsIntervalMs int64 `json:"metricsIntervalMs,omitempty"`
	// Subscription — сводная подписка сервера: в welcome — текущая (nil —
	// подписок нет); в config — заменить целиком (пустая — подписок нет,
	// nil — не менять).
	Subscription *Subscription `json:"subscription,omitempty"`
}

// Subscription — сводная подписка: что сервер сейчас просит присылать чаще и
// подробнее обычного. Агент ужесточает ей свои настройки, не ослабляет их.
// Нулевое поле — подписка его не касается.
type Subscription struct {
	// StatusIntervalMs — частота статуса (минимум по подпискам).
	StatusIntervalMs int64 `json:"statusIntervalMs,omitempty"`
	// MetricsIntervalMs — частота метрик (минимум по подпискам).
	MetricsIntervalMs int64 `json:"metricsIntervalMs,omitempty"`
	// Metrics — группы метрик узла (MetricGroups) сверх telemetry.metrics.
	Metrics []string `json:"metrics,omitempty"`
	// LogLevel — порог отправки лога (LogError…LogDebug), если подробнее log.forward.
	LogLevel string `json:"logLevel,omitempty"`
	// Channels — канал показателей воркера → частота, мс (минимум по подпискам).
	Channels map[string]int64 `json:"channels,omitempty"`
}

// Уровни записей лога (LogEntry.Level) и порогов отправки (Subscription.LogLevel).
const (
	LogOff   = "off" // только порог: не слать ничего
	LogError = "error"
	LogWarn  = "warn"
	LogInfo  = "info"
	LogDebug = "debug"
)

// LogSourceAgent — LogEntry.Source записей самого агента (у воркеров — имя воркера).
const LogSourceAgent = "agent"

// LogBatch — сообщение log: пачка записей (не больше 500).
type LogBatch struct {
	Entries []LogEntry `json:"entries"`
}

// LogEntry — запись лога: время (мс UTC), уровень, источник (LogSourceAgent
// или имя воркера), текст и необязательные атрибуты.
type LogEntry struct {
	At     int64          `json:"at"`
	Level  string         `json:"level"`
	Source string         `json:"source"`
	Msg    string         `json:"msg"`
	Attrs  map[string]any `json:"attrs,omitempty"`
}

type Ack struct {
	IDs []string `json:"ids,omitempty"`
	Seq int64    `json:"seq,omitempty"`
}

type Error struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// ─── Состояние и телеметрия ────────────────────────────────────────────

type Status struct {
	State    string         `json:"state"`
	Message  string         `json:"message,omitempty"`
	Slots    map[string]int `json:"slots"`
	Capacity map[string]int `json:"capacity,omitempty"`
	Jobs     []StatusJob    `json:"jobs"`
	Workers  []StatusWorker `json:"workers"`
	Outbox   int            `json:"outbox"`
}

type StatusJob struct {
	JobID     string `json:"jobId"`
	Attempt   int    `json:"attempt"`
	Queue     string `json:"queue"`
	StartedAt int64  `json:"startedAt,omitempty"`
}

type StatusWorker struct {
	Name      string `json:"name"`
	State     string `json:"state"`
	Instances int    `json:"instances"`
	Version   string `json:"version,omitempty"`
	// Release — воркер из выпуска (release: true): его сборку можно обновить
	// командой worker.update.
	Release bool `json:"release,omitempty"`
	// Health — оценка самого воркера (worker.health): ok | degraded; пусто —
	// воркер её не сообщал. Message — причина degraded.
	Health  string `json:"health,omitempty"`
	Message string `json:"message,omitempty"`
	// Paused — хоть одна очередь воркера на паузе (от воркера или сервера).
	Paused bool `json:"paused,omitempty"`
}

type Metrics struct {
	CollectedAt int64 `json:"collectedAt"`
	// ClockOffsetMs — часы сервера − часы агента (по welcome.serverTime):
	// время точки на сервере — CollectedAt + ClockOffsetMs.
	ClockOffsetMs *int64 `json:"clockOffsetMs,omitempty"`
	// Backfill — точка собрана без связи и дослана после подключения.
	Backfill bool                       `json:"backfill,omitempty"`
	Host     *HostMetrics               `json:"host,omitempty"`
	GPUs     []GPUMetrics               `json:"gpus,omitempty"`
	Channels map[string]json.RawMessage `json:"channels,omitempty"`
}

// Группы метрик узла (telemetry.metrics агента, Subscription.Metrics):
// какие поля HostMetrics собираются. Нет группы — нет её полей.
const (
	MetricsCPU          = "cpu"          // cpuPercent
	MetricsCPUCores     = "cpu.cores"    // cpuCores
	MetricsLoad         = "load"         // load1, load5, load15
	MetricsMemory       = "memory"       // memUsedBytes, memTotalBytes, memAvailableBytes
	MetricsSwap         = "swap"         // swapUsedBytes, swapTotalBytes
	MetricsDisk         = "disk"         // diskUsedBytes, diskTotalBytes (корень /), disks
	MetricsDiskIO       = "diskio"       // diskReadBps, diskWriteBps, diskReadIops, diskWriteIops
	MetricsNetwork      = "network"      // netRxBps, netTxBps, netErrors, netDrops
	MetricsInterfaces   = "interfaces"   // interfaces
	MetricsConntrack    = "conntrack"    // conntrack, conntrackMax (Linux)
	MetricsSockets      = "sockets"      // tcp
	MetricsProcesses    = "processes"    // processes, threads
	MetricsFDs          = "fds"          // fdsOpen, fdsMax (Linux)
	MetricsUptime       = "uptime"       // uptimeSec
	MetricsTemperatures = "temperatures" // temperatures
)

// MetricGroups — все группы метрик узла.
var MetricGroups = []string{
	MetricsCPU, MetricsCPUCores, MetricsLoad, MetricsMemory, MetricsSwap, MetricsDisk, MetricsDiskIO,
	MetricsNetwork, MetricsInterfaces, MetricsConntrack, MetricsSockets, MetricsProcesses, MetricsFDs,
	MetricsUptime, MetricsTemperatures,
}

// HostMetrics — метрики узла; все поля необязательные (нет группы или
// значение недоступно на платформе — нет поля). Скорости — за интервал
// между сборами.
type HostMetrics struct {
	CPUPercent *float64 `json:"cpuPercent,omitempty"`
	// CPUCores — загрузка по ядрам, %.
	CPUCores          []float64 `json:"cpuCores,omitempty"`
	Load1             *float64  `json:"load1,omitempty"`
	Load5             *float64  `json:"load5,omitempty"`
	Load15            *float64  `json:"load15,omitempty"`
	MemUsedBytes      *uint64   `json:"memUsedBytes,omitempty"`
	MemTotalBytes     *uint64   `json:"memTotalBytes,omitempty"`
	MemAvailableBytes *uint64   `json:"memAvailableBytes,omitempty"`
	SwapUsedBytes     *uint64   `json:"swapUsedBytes,omitempty"`
	SwapTotalBytes    *uint64   `json:"swapTotalBytes,omitempty"`
	// DiskUsedBytes, DiskTotalBytes — корневая файловая система.
	DiskUsedBytes  *uint64 `json:"diskUsedBytes,omitempty"`
	DiskTotalBytes *uint64 `json:"diskTotalBytes,omitempty"`
	// Disks — файловые системы по настройке агента telemetry.disks.
	Disks []DiskMetrics `json:"disks,omitempty"`
	// DiskReadBps … DiskWriteIops — сумма по физическим дискам: байт/с и операций/с.
	DiskReadBps   *uint64 `json:"diskReadBps,omitempty"`
	DiskWriteBps  *uint64 `json:"diskWriteBps,omitempty"`
	DiskReadIops  *uint64 `json:"diskReadIops,omitempty"`
	DiskWriteIops *uint64 `json:"diskWriteIops,omitempty"`
	// NetRxBps, NetTxBps — байт/с; NetErrors, NetDrops — ошибок и отброшенных
	// пакетов (приём + передача) за интервал; сумма по показываемым интерфейсам.
	NetRxBps  *uint64 `json:"netRxBps,omitempty"`
	NetTxBps  *uint64 `json:"netTxBps,omitempty"`
	NetErrors *uint64 `json:"netErrors,omitempty"`
	NetDrops  *uint64 `json:"netDrops,omitempty"`
	UptimeSec *uint64 `json:"uptimeSec,omitempty"`
	// Conntrack, ConntrackMax — записей conntrack и предел таблицы (Linux).
	Conntrack    *uint64 `json:"conntrack,omitempty"`
	ConntrackMax *uint64 `json:"conntrackMax,omitempty"`
	// TCP — TCP-соединения по состояниям.
	TCP *TCPMetrics `json:"tcp,omitempty"`
	// Processes, Threads — процессов и потоков на узле.
	Processes *uint64 `json:"processes,omitempty"`
	Threads   *uint64 `json:"threads,omitempty"`
	// FDsOpen, FDsMax — открытых файлов на узле и предел (Linux).
	FDsOpen *uint64 `json:"fdsOpen,omitempty"`
	FDsMax  *uint64 `json:"fdsMax,omitempty"`
	// Temperatures — датчики температуры (где доступны).
	Temperatures *TemperatureMetrics `json:"temperatures,omitempty"`
	// Interfaces — сеть по интерфейсам (без lo, veth, docker, br-…).
	Interfaces []InterfaceMetrics `json:"interfaces,omitempty"`
}

// DiskMetrics — файловая система: занято и всего байт и inode.
type DiskMetrics struct {
	Mount       string `json:"mount"`
	UsedBytes   uint64 `json:"usedBytes"`
	TotalBytes  uint64 `json:"totalBytes"`
	InodesUsed  uint64 `json:"inodesUsed,omitempty"`
	InodesTotal uint64 `json:"inodesTotal,omitempty"`
}

// TCPMetrics — TCP-соединения (IPv4 и IPv6) по состояниям.
type TCPMetrics struct {
	Established uint64 `json:"established"`
	TimeWait    uint64 `json:"timeWait"`
	CloseWait   uint64 `json:"closeWait"`
	Listen      uint64 `json:"listen"`
}

// TemperatureMetrics — самая высокая температура и датчики, °C.
type TemperatureMetrics struct {
	MaxC    float64             `json:"maxC"`
	Sensors []TemperatureSensor `json:"sensors,omitempty"`
}

type TemperatureSensor struct {
	Name string  `json:"name"`
	C    float64 `json:"c"`
}

// InterfaceMetrics — сеть интерфейса: байт/с, ошибок и отброшенных пакетов
// (приём + передача) за интервал.
type InterfaceMetrics struct {
	Name   string `json:"name"`
	RxBps  uint64 `json:"rxBps"`
	TxBps  uint64 `json:"txBps"`
	Errors uint64 `json:"errors,omitempty"`
	Drops  uint64 `json:"drops,omitempty"`
}

// Inventory — сведения об узле, меняющиеся редко (§6.2).
type Inventory struct {
	CollectedAt int64              `json:"collectedAt"`
	OS          *InventoryOS       `json:"os,omitempty"`
	CPU         *InventoryCPU      `json:"cpu,omitempty"`
	MemoryBytes uint64             `json:"memoryBytes,omitempty"`
	Disks       []InventoryDisk    `json:"disks,omitempty"`
	Interfaces  []InventoryNetwork `json:"interfaces,omitempty"`
	GPUs        []InventoryGPU     `json:"gpus,omitempty"`
	Ports       *InventoryPorts    `json:"ports,omitempty"`
}

type InventoryOS struct {
	Hostname       string `json:"hostname,omitempty"`
	Platform       string `json:"platform,omitempty"`
	Kernel         string `json:"kernel,omitempty"`
	Arch           string `json:"arch,omitempty"`
	Virtualization string `json:"virtualization,omitempty"`
}

type InventoryCPU struct {
	Model   string `json:"model,omitempty"`
	Cores   int    `json:"cores,omitempty"`
	Threads int    `json:"threads,omitempty"`
}

type InventoryDisk struct {
	Mount      string `json:"mount"`
	FS         string `json:"fs,omitempty"`
	TotalBytes uint64 `json:"totalBytes"`
}

type InventoryNetwork struct {
	Name      string   `json:"name"`
	MAC       string   `json:"mac,omitempty"`
	Addresses []string `json:"addresses,omitempty"`
}

type InventoryGPU struct {
	Index       int    `json:"index"`
	Name        string `json:"name"`
	MemoryBytes uint64 `json:"memoryBytes,omitempty"`
}

// InventoryPorts — слушающие порты.
type InventoryPorts struct {
	TCP []int `json:"tcp"`
	UDP []int `json:"udp"`
}

type GPUMetrics struct {
	Index         int      `json:"index"`
	Name          string   `json:"name"`
	UtilPercent   *float64 `json:"utilPercent,omitempty"`
	MemUsedBytes  *uint64  `json:"memUsedBytes,omitempty"`
	MemTotalBytes *uint64  `json:"memTotalBytes,omitempty"`
	TemperatureC  *float64 `json:"temperatureC,omitempty"`
}

// ─── Задачи ────────────────────────────────────────────────────────────

// JobRef — задача и попытка (барьер против устаревшего исполнителя).
type JobRef struct {
	JobID   string `json:"jobId"`
	Attempt int    `json:"attempt"`
}

type JobAssign struct {
	JobID        string               `json:"jobId"`
	Attempt      int                  `json:"attempt"`
	Queue        string               `json:"queue"`
	Data         json.RawMessage      `json:"data"`
	LeaseSeconds int                  `json:"leaseSeconds"`
	Inputs       map[string]string    `json:"inputs"`
	Outputs      map[string]OutputURL `json:"outputs"`
	URLsExpireAt int64                `json:"urlsExpireAt,omitempty"`
}

// Ref — ссылка на задачу назначения.
func (a JobAssign) Ref() JobRef { return JobRef{JobID: a.JobID, Attempt: a.Attempt} }

type OutputURL struct {
	URL         string `json:"url"`
	ContentType string `json:"contentType,omitempty"`
}

type JobReject struct {
	JobRef
	Code    string `json:"code"`
	Message string `json:"message"`
}

type JobProgress struct {
	JobRef
	Progress *float64 `json:"progress,omitempty"`
	Text     *string  `json:"text,omitempty"`
	Log      []string `json:"log,omitempty"`
}

type JobEvent struct {
	JobRef
	Seq  int64           `json:"seq"`
	Type string          `json:"type"`
	Data json.RawMessage `json:"data,omitempty"`
}

type JobURLsRequest struct {
	JobRef
	Inputs  []string `json:"inputs,omitempty"`
	Outputs []string `json:"outputs,omitempty"`
}

type JobURLs struct {
	Inputs    map[string]string    `json:"inputs"`
	Outputs   map[string]OutputURL `json:"outputs"`
	ExpiresAt int64                `json:"expiresAt"`
}

type JobComplete struct {
	JobRef
	Result json.RawMessage `json:"result,omitempty"`
}

type JobFail struct {
	JobRef
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable"`
}

// Коды ошибок задачи, которые выставляет агент.
const (
	ErrWorker         = "WORKER_ERROR"
	ErrWorkerCrashed  = "WORKER_CRASHED"
	ErrQueueNotServed = "QUEUE_NOT_SERVED"
	ErrQueueBusy      = "QUEUE_BUSY"
	ErrUploadFailed   = "UPLOAD_FAILED"
)

// ─── Команды ───────────────────────────────────────────────────────────

type CommandRun struct {
	CommandID  string          `json:"commandId"`
	Name       string          `json:"name"`
	Args       json.RawMessage `json:"args,omitempty"`
	TimeoutSec int             `json:"timeoutSec"`
}

type CommandRef struct {
	CommandID string `json:"commandId"`
}

type CommandOutput struct {
	CommandID string `json:"commandId"`
	Chunk     string `json:"chunk"`
}

type CommandDone struct {
	CommandID string          `json:"commandId"`
	OK        bool            `json:"ok"`
	ExitCode  *int            `json:"exitCode,omitempty"`
	Result    json.RawMessage `json:"result,omitempty"`
	Error     *CommandError   `json:"error,omitempty"`
}

type CommandError struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// ─── Желаемое состояние ────────────────────────────────────────────────

type StatePut struct {
	Domain  string          `json:"domain"`
	Version int64           `json:"version"`
	Spec    json.RawMessage `json:"spec"`
}

type StateApplied struct {
	Domain  string          `json:"domain"`
	Version int64           `json:"version"`
	OK      bool            `json:"ok"`
	Error   string          `json:"error,omitempty"`
	Report  json.RawMessage `json:"report,omitempty"`
}
