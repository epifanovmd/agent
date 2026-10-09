package message

// Метрики узла — ответ GET /metrics встроенного воркера sysmetrics и
// metrics.host (§9).

// Группы метрик узла (telemetry.metrics агента, §9):
// какие поля HostMetrics собираются. Нет группы — нет её полей.
const (
	MetricsCPU          = "cpu"          // cpuPercent, cpuCores
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
	MetricsGPU          = "gpu"          // gpus
)

// MetricGroups — все группы метрик узла.
var MetricGroups = []string{
	MetricsCPU, MetricsLoad, MetricsMemory, MetricsSwap, MetricsDisk, MetricsDiskIO,
	MetricsNetwork, MetricsInterfaces, MetricsConntrack, MetricsSockets, MetricsProcesses, MetricsFDs,
	MetricsUptime, MetricsTemperatures, MetricsGPU,
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
	// Interfaces — сеть по интерфейсам (без исключённых настройкой агента).
	Interfaces []InterfaceMetrics `json:"interfaces,omitempty"`
	// GPUs — видеокарты.
	GPUs []GPUMetrics `json:"gpus,omitempty"`
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

// GPUMetrics — видеокарта: загрузка, %; память, байт; температура, °C.
type GPUMetrics struct {
	Index         int      `json:"index"`
	Name          string   `json:"name"`
	UtilPercent   *float64 `json:"utilPercent,omitempty"`
	MemUsedBytes  *uint64  `json:"memUsedBytes,omitempty"`
	MemTotalBytes *uint64  `json:"memTotalBytes,omitempty"`
	TemperatureC  *float64 `json:"temperatureC,omitempty"`
}
