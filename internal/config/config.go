// Package config — настройки агента: значения по умолчанию → YAML-файл
// (с подстановкой ${ENV}) → переменные окружения AGENT_*.
package config

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/epifanovmd/agent/internal/message"
)

// geteuid — подменяется в тестах (проверка workers[].user).
var geteuid = os.Geteuid

// Duration — длительность в YAML строкой: "30s", "10m".
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("строка %d: длительность %q (нужно, например, 30s или 10m): %w", node.Line, node.Value, err)
	}
	*d = Duration(parsed)
	return nil
}

// MarshalYAML — длительность строкой (agent config check).
func (d Duration) MarshalYAML() (any, error) { return dur(d), nil }

// Std — time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// ByteSize — размер в YAML: число байт или строка с единицей: "512KB",
// "10MB", "1GB" (по 1024).
type ByteSize int64

var byteUnits = []struct {
	suffix string
	n      int64
}{{"GB", 1 << 30}, {"MB", 1 << 20}, {"KB", 1 << 10}, {"B", 1}}

// ParseByteSize — размер из строки ("10MB", "65536").
func ParseByteSize(s string) (ByteSize, error) {
	text := strings.ToUpper(strings.TrimSpace(s))
	for _, u := range byteUnits {
		if num, ok := strings.CutSuffix(text, u.suffix); ok {
			n, err := strconv.ParseInt(strings.TrimSpace(num), 10, 64)
			if err != nil || n < 0 {
				break
			}
			return ByteSize(n * u.n), nil
		}
	}
	n, err := strconv.ParseInt(text, 10, 64)
	if err != nil || n < 0 {
		return 0, fmt.Errorf("размер %q (нужно, например, 10MB или 512KB)", s)
	}
	return ByteSize(n), nil
}

func (b *ByteSize) UnmarshalYAML(node *yaml.Node) error {
	v, err := ParseByteSize(node.Value)
	if err != nil {
		return fmt.Errorf("строка %d: %w", node.Line, err)
	}
	*b = v
	return nil
}

// MarshalYAML — размер строкой в самых крупных целых единицах.
func (b ByteSize) MarshalYAML() (any, error) { return b.String(), nil }

func (b ByteSize) String() string {
	for _, u := range byteUnits {
		if int64(b) >= u.n && int64(b)%u.n == 0 {
			return strconv.FormatInt(int64(b)/u.n, 10) + u.suffix
		}
	}
	return strconv.FormatInt(int64(b), 10) + "B"
}

// Config — настройки агента.
type Config struct {
	Server    Server            `yaml:"server"`
	DataDir   string            `yaml:"dataDir"`
	Name      string            `yaml:"name"`
	Labels    map[string]string `yaml:"labels"`
	Enroll    Enroll            `yaml:"enroll"`
	Log       Log               `yaml:"log"`
	Telemetry Telemetry         `yaml:"telemetry"`
	Update    Update            `yaml:"update"`
	Outbox    Outbox            `yaml:"outbox"`
	Workers   []Worker          `yaml:"workers"`

	// envErrs — неверные значения переменных AGENT_* (их сообщает Validate).
	envErrs []string
}

// Outbox — очередь важных сообщений на диске.
type Outbox struct {
	// MaxMessages — сколько сообщений очередь держит; полна — события
	// воркеров отклоняются (503 OUTBOX_FULL).
	MaxMessages int `yaml:"maxMessages"`
}

// Backoff — растущая пауза между попытками: от Min, удваивается до Max.
type Backoff struct {
	Min Duration `yaml:"min"`
	Max Duration `yaml:"max"`
}

type Server struct {
	// URL бэкенда: https://api.example.com.
	URL string `yaml:"url"`
	// URLs — ещё адреса того же бэкенда: агент подключается к первому
	// доступному, при обрыве или отказе — к следующему по кругу.
	URLs []string `yaml:"urls"`
	// CAFile — PEM с корневыми сертификатами в дополнение к системным.
	CAFile string `yaml:"caFile"`
	// CertFile, KeyFile — клиентский сертификат агента (mTLS); оба или ни одного.
	CertFile string `yaml:"certFile"`
	KeyFile  string `yaml:"keyFile"`
	// Reconnect — пауза переподключения (и повтора регистрации).
	Reconnect Backoff `yaml:"reconnect"`
	// StreamBuffer — сколько последних сообщений потока (status, metrics,
	// log) агент держит без связи и досылает.
	StreamBuffer int `yaml:"streamBuffer"`
}

type Enroll struct {
	// Token — токен регистрации; нужен только до первой регистрации.
	Token string `yaml:"token"`
}

type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	// Forward — с какого уровня записи лога агента и вывод воркеров уходят
	// серверу (сообщение log): off | error | warn | info | debug. watch
	// сервера может временно сделать подробнее.
	Forward string `yaml:"forward"`
	// Buffer — сколько последних записей журнала агент держит в памяти для
	// каждого источника (агент и каждый воркер) — их отдаёт agent.logs.
	Buffer int `yaml:"buffer"`
}

type Telemetry struct {
	// Metrics — группы метрик узла (message.MetricGroups); [] — без метрик узла.
	Metrics []string `yaml:"metrics"`
	// Disks — точки монтирования для группы disk; ["all"] — все реальные
	// файловые системы.
	Disks []string `yaml:"disks"`
	// ExcludeInterfaces — префиксы имён сетевых интерфейсов, которых нет в
	// метриках; заданный список заменяет умолчание.
	ExcludeInterfaces []string `yaml:"excludeInterfaces"`
}

// DefaultMetrics — группы метрик узла по умолчанию.
var DefaultMetrics = []string{
	message.MetricsCPU, message.MetricsLoad, message.MetricsMemory, message.MetricsSwap, message.MetricsDisk,
	message.MetricsNetwork, message.MetricsInterfaces, message.MetricsConntrack, message.MetricsUptime,
}

// DisksAll — telemetry.disks: все реальные файловые системы.
const DisksAll = "all"

// DefaultExcludeInterfaces — интерфейсы, которых по умолчанию нет в метриках:
// петля, контейнеры, мосты и служебные интерфейсы macOS.
var DefaultExcludeInterfaces = []string{
	"lo", "veth", "docker", "br-", "virbr", "vnet", "cni", "flannel", "cali", "kube", "tunl",
	"gif", "stf", "awdl", "llw", "anpi", "utun", "bridge", "ap",
}

// Режимы обновления агента.
const (
	UpdateSelf     = "self"
	UpdateExternal = "external"
	UpdateDisabled = "disabled"
)

type Update struct {
	// Mode: self (агент сам заменяет свой файл) | external (контейнер) | disabled.
	Mode string `yaml:"mode"`
	// PublicKey — ключ проверки подписи сборок Ed25519, base64.
	PublicKey string `yaml:"publicKey"`
	// PublicKeys — ещё ключи проверки (base64): подпись сборки принимается,
	// если сходится с любым из ключей — вшитым при сборке, PublicKey и этими.
	PublicKeys []string `yaml:"publicKeys"`
}

// Keys — ключи проверки подписи сборок из настроек: publicKey и publicKeys без
// пустых и повторов (вшитый при сборке ключ сюда не входит).
func (u Update) Keys() []string {
	var out []string
	for _, k := range append([]string{u.PublicKey}, u.PublicKeys...) {
		if k = strings.TrimSpace(k); k != "" && !slices.Contains(out, k) {
			out = append(out, k)
		}
	}
	return out
}

// SysmetricsWorker — имя встроенного воркера метрик узла; в workers это имя занято.
const SysmetricsWorker = message.BuiltinSysmetrics

// Worker — воркер: HTTP-сервис на unix-сокете, который запускает агент.
type Worker struct {
	Name    string   `yaml:"name"`
	Command []string `yaml:"command"`
	// Args — аргументы, дописываемые к command (или к файлу сборки).
	Args []string          `yaml:"args"`
	Dir  string            `yaml:"dir"`
	Env  map[string]string `yaml:"env"`
	// InheritEnv — какие ещё переменные окружения агента передать воркеру
	// сверх обычных (PATH, HOME, LANG и др.); "PREFIX_*" — все с этим
	// началом. Переменные AGENT_* агента воркеру не передаются.
	InheritEnv []string `yaml:"inheritEnv"`
	// User — пользователь, от которого запускается воркер; агенту нужен root.
	User string `yaml:"user"`
	// Release — воркер со сборкой с сервера: сборку ведёт агент
	// (<dataDir>/workers/<name>/current), сервер обновляет её действием
	// worker.update. Сборка — исполняемый файл или каталог из архива
	// .tar.gz; command (если задан) выполняется в каталоге сборки, без
	// command запускается сам файл или ./run архива.
	Release bool `yaml:"release"`
	// Lifecycle — как агент запускает, проверяет, заменяет и останавливает
	// воркер (§13); изменение применяется без перезапуска воркера.
	Lifecycle Lifecycle `yaml:"lifecycle"`
	// Logs — файлы вывода воркера.
	Logs Logs `yaml:"logs"`
	// Routes — какие fetch агент пропускает к воркеру (§7): RoutesStrict (по
	// умолчанию) — только по манифесту, RoutesOpen — любой путь, кроме
	// служебных. Изменение применяется без перезапуска воркера.
	Routes string `yaml:"routes"`
	// ReleaseDir — <dataDir>/workers/<name>; заполняет Validate для release.
	ReleaseDir string `yaml:"-"`
	// Builtin — встроенный воркер агента (sysmetrics), в agent.yaml его нет.
	Builtin bool `yaml:"-"`
}

// Значения Worker.Routes.
const (
	RoutesStrict = "strict"
	RoutesOpen   = "open"
)

// OpenRoutes — агент не сверяет fetch с манифестом воркера (routes: open).
func (w Worker) OpenRoutes() bool { return w.Routes == RoutesOpen }

// Файлы сборки воркера в его каталоге ReleaseDir.
const (
	ReleaseCurrent         = "current"
	ReleaseVersion         = "version"
	ReleasePrevious        = "previous"
	ReleasePreviousVersion = "previous.version"
	// ReleaseRun — что запускать в сборке-архиве, если command не задан.
	ReleaseRun = "run"
)

// Что делать с воркером при перезапуске и остановке агента и после выхода
// его процесса (Lifecycle).
const (
	Keep           = "keep"
	StopWorker     = "stop"
	RestartWorker  = "restart"
	RestartAlways  = "always"
	RestartFailure = "on-failure"
	RestartNever   = "never"
)

// Lifecycle — жизнь воркера (§13). Поля-указатели: nil — значение по
// умолчанию (у них 0 и false — тоже значение); остальные: 0 — по умолчанию.
// Validate (FillDefaults) заполняет всё.
type Lifecycle struct {
	// OnAgentRestart — перезапуск агента (обновление, agent restart): keep —
	// воркер работает дальше, новый агент его подхватывает; restart —
	// остановить, новый агент запустит заново.
	OnAgentRestart string `yaml:"onAgentRestart"`
	// OnAgentStop — остановка агента (SIGTERM, SIGINT): keep | stop.
	OnAgentStop string `yaml:"onAgentStop"`
	// Restart — после выхода процесса: always | on-failure | never.
	Restart string `yaml:"restart"`
	// Backoff — пауза перед перезапуском после падения.
	Backoff Backoff `yaml:"backoff"`
	// MaxRestarts — перезапусков после падения подряд не больше (0 — без
	// предела); превышено — воркер остаётся остановленным до worker.restart
	// или изменения его настроек.
	MaxRestarts int `yaml:"maxRestarts"`
	// StartTimeout — сколько ждать, пока сокет воркера начнёт отвечать.
	StartTimeout Duration `yaml:"startTimeout"`
	// StopTimeout — сколько ждать выхода после SIGTERM, потом SIGKILL; ещё
	// — срок POST /cleanup.
	StopTimeout Duration `yaml:"stopTimeout"`
	// KeepChildren — дочерние процессы воркера переживают его остановку:
	// сигналы получает только сам воркер, а не вся группа процессов.
	KeepChildren bool `yaml:"keepChildren"`
	// Busy — плановая замена занятого воркера (GET /health → busy: true).
	Busy Busy `yaml:"busy"`
	// Health — проверка GET /health.
	Health Health `yaml:"health"`
	// ProbeTimeout — срок GET /manifest, GET /metrics и GET /health при регистрации.
	ProbeTimeout Duration `yaml:"probeTimeout"`
	// ConfigRetry — повтор неприменённых настроек.
	ConfigRetry Duration `yaml:"configRetry"`
	// UpdateHealthyTimeout — сколько новая сборка (worker.update) может не
	// отвечать ok: true; потом — возврат прежней.
	UpdateHealthyTimeout Duration `yaml:"updateHealthyTimeout"`
}

// Busy — ждать ли окончания работы перед плановой заменой и сколько.
type Busy struct {
	// Wait — ждать (по умолчанию true).
	Wait *bool `yaml:"wait"`
	// Timeout — дольше не ждать: замена идёт.
	Timeout Duration `yaml:"timeout"`
}

// Health — проверка GET /health: раз в Interval (0 — не проверять), срок
// ответа Timeout; Failures пропусков подряд — перезапуск (0 — не
// перезапускать).
type Health struct {
	Interval *Duration `yaml:"interval"`
	Timeout  Duration  `yaml:"timeout"`
	Failures *int      `yaml:"failures"`
}

// Logs — файлы вывода воркера: больше MaxSize — копия в .1 (прежние —
// .2 … до MaxFiles; 0 — без копий) и очистка.
type Logs struct {
	MaxSize  ByteSize `yaml:"maxSize"`
	MaxFiles *int     `yaml:"maxFiles"`
}

// Значения по умолчанию жизни воркера.
const (
	DefaultStopTimeout = 30 * time.Second
	DefaultBusyTimeout = 24 * time.Hour
	DefaultLogMaxSize  = 10 << 20
	DefaultLogMaxFiles = 1
)

// DefaultLifecycle — жизнь воркера по умолчанию: долгая работа не
// прерывается перезапуском и остановкой агента и ждёт плановой замены.
func DefaultLifecycle() Lifecycle {
	var l Lifecycle
	l.fill()
	return l
}

func ptr[T any](v T) *T { return &v }

func (l *Lifecycle) fill() {
	def := func(d *Duration, v time.Duration) {
		if *d <= 0 {
			*d = Duration(v)
		}
	}
	str := func(s *string, v string) {
		if *s == "" {
			*s = v
		}
	}
	str(&l.OnAgentRestart, Keep)
	str(&l.OnAgentStop, Keep)
	str(&l.Restart, RestartFailure)
	def(&l.Backoff.Min, time.Second)
	def(&l.Backoff.Max, 30*time.Second)
	def(&l.StartTimeout, message.WorkerStartTimeout)
	def(&l.StopTimeout, DefaultStopTimeout)
	if l.Busy.Wait == nil {
		l.Busy.Wait = ptr(true)
	}
	def(&l.Busy.Timeout, DefaultBusyTimeout)
	if l.Health.Interval == nil {
		l.Health.Interval = ptr(Duration(message.HealthInterval))
	}
	def(&l.Health.Timeout, message.ProbeTimeout)
	if l.Health.Failures == nil {
		l.Health.Failures = ptr(message.HealthMisses)
	}
	def(&l.ProbeTimeout, message.ProbeTimeout)
	def(&l.ConfigRetry, message.ConfigRetry)
	def(&l.UpdateHealthyTimeout, message.UpdateHealthTimeout)
}

// HealthInterval — частота GET /health (0 — не проверять).
func (l Lifecycle) HealthInterval() time.Duration {
	if l.Health.Interval == nil {
		return message.HealthInterval
	}
	return l.Health.Interval.Std()
}

// HealthFailures — пропусков подряд до перезапуска (0 — не перезапускать).
func (l Lifecycle) HealthFailures() int {
	if l.Health.Failures == nil {
		return message.HealthMisses
	}
	return *l.Health.Failures
}

// BusyWait — ждать окончания работы перед плановой заменой.
func (l Lifecycle) BusyWait() bool { return l.Busy.Wait == nil || *l.Busy.Wait }

// DefaultLogs — файлы вывода воркера по умолчанию.
func DefaultLogs() Logs {
	var l Logs
	l.fill()
	return l
}

func (l *Logs) fill() {
	if l.MaxSize <= 0 {
		l.MaxSize = DefaultLogMaxSize
	}
	if l.MaxFiles == nil {
		l.MaxFiles = ptr(DefaultLogMaxFiles)
	}
}

// Files — сколько копий хранить.
func (l Logs) Files() int {
	if l.MaxFiles == nil {
		return DefaultLogMaxFiles
	}
	return *l.MaxFiles
}

// FillDefaults — значения по умолчанию незаданных полей жизни и файлов
// вывода воркера.
func (w *Worker) FillDefaults() {
	w.Lifecycle.fill()
	w.Logs.fill()
}

// check — допустимые значения жизни и файлов вывода воркера i.
func (w *Worker) check(i int) []error {
	var errs []error
	l := w.Lifecycle
	bad := func(field, format string, args ...any) {
		errs = append(errs, fmt.Errorf("workers[%d].%s: "+format, append([]any{i, field}, args...)...))
	}
	if w.Routes != "" && w.Routes != RoutesStrict && w.Routes != RoutesOpen {
		bad("routes", "strict | open, а не %q", w.Routes)
	}
	if l.OnAgentRestart != Keep && l.OnAgentRestart != RestartWorker {
		bad("lifecycle.onAgentRestart", "keep | restart, а не %q", l.OnAgentRestart)
	}
	if l.OnAgentStop != Keep && l.OnAgentStop != StopWorker {
		bad("lifecycle.onAgentStop", "keep | stop, а не %q", l.OnAgentStop)
	}
	if !slices.Contains([]string{RestartAlways, RestartFailure, RestartNever}, l.Restart) {
		bad("lifecycle.restart", "always | on-failure | never, а не %q", l.Restart)
	}
	if l.Backoff.Max < l.Backoff.Min {
		bad("lifecycle.backoff", "max (%s) меньше min (%s)", dur(l.Backoff.Max), dur(l.Backoff.Min))
	}
	if l.MaxRestarts < 0 {
		bad("lifecycle.maxRestarts", "не меньше 0 (0 — без предела)")
	}
	if l.Health.Interval != nil && *l.Health.Interval < 0 {
		bad("lifecycle.health.interval", "не меньше 0 (0s — не проверять)")
	}
	if l.Health.Failures != nil && *l.Health.Failures < 0 {
		bad("lifecycle.health.failures", "не меньше 0 (0 — не перезапускать)")
	}
	if l.Health.Interval != nil && *l.Health.Interval > 0 && l.Health.Timeout > *l.Health.Interval {
		bad("lifecycle.health.timeout", "не больше interval (%s)", dur(*l.Health.Interval))
	}
	if w.Logs.MaxSize < 64<<10 {
		bad("logs.maxSize", "не меньше 64KB, а не %s", w.Logs.MaxSize)
	}
	if n := w.Logs.Files(); n < 0 || n > 100 {
		bad("logs.maxFiles", "от 0 до 100, а не %d", n)
	}
	return errs
}

// Current — текущая сборка воркера (<ReleaseDir>/current).
func (w Worker) Current() string { return filepath.Join(w.ReleaseDir, ReleaseCurrent) }

// ReleasesDir — каталог сборок воркеров в dataDir.
func ReleasesDir(dataDir string) string { return filepath.Join(dataDir, "workers") }

// Defaults — значения по умолчанию.
func Defaults() Config {
	host, _ := os.Hostname()
	return Config{
		DataDir: DefaultDataDir(),
		Name:    host,
		Labels:  map[string]string{},
		Server:  Server{Reconnect: Backoff{Min: Duration(time.Second), Max: Duration(time.Minute)}, StreamBuffer: message.StreamBacklog},
		Log:     Log{Level: "info", Format: "text", Forward: message.LogWarn, Buffer: message.MaxLogLines},
		Outbox:  Outbox{MaxMessages: message.MaxOutbox},
		Telemetry: Telemetry{
			Metrics: slices.Clone(DefaultMetrics), Disks: []string{"/"},
			ExcludeInterfaces: slices.Clone(DefaultExcludeInterfaces),
		},
		Update: Update{Mode: UpdateSelf},
	}
}

// Load — настройки из файла path (пустой — без файла) и окружения.
func Load(path string) (Config, error) {
	cfg := Defaults()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return cfg, fmt.Errorf("config: %w", err)
		}
		if err := yaml.Unmarshal([]byte(os.ExpandEnv(string(raw))), &cfg); err != nil {
			return cfg, fmt.Errorf("config %s: %w", path, err)
		}
	}
	applyEnv(&cfg)
	return cfg, cfg.Validate()
}

func applyEnv(cfg *Config) {
	set := func(target *string, name string) {
		if v, ok := os.LookupEnv(name); ok && v != "" {
			*target = v
		}
	}
	set(&cfg.Server.URL, "AGENT_SERVER_URL")
	set(&cfg.DataDir, "AGENT_DATA_DIR")
	set(&cfg.Name, "AGENT_NAME")
	set(&cfg.Enroll.Token, "AGENT_ENROLL_TOKEN")
	set(&cfg.Log.Level, "AGENT_LOG_LEVEL")
	set(&cfg.Log.Format, "AGENT_LOG_FORMAT")
	set(&cfg.Log.Forward, "AGENT_LOG_FORWARD")
	set(&cfg.Server.CAFile, "AGENT_SERVER_CA_FILE")
	set(&cfg.Server.CertFile, "AGENT_SERVER_CERT_FILE")
	set(&cfg.Server.KeyFile, "AGENT_SERVER_KEY_FILE")
	set(&cfg.Update.Mode, "AGENT_UPDATE_MODE")
	set(&cfg.Update.PublicKey, "AGENT_UPDATE_PUBLIC_KEY")
	if labels, ok := os.LookupEnv("AGENT_LABELS"); ok {
		if cfg.Labels == nil {
			cfg.Labels = map[string]string{}
		}
		for _, pair := range strings.Split(labels, ",") {
			if k, v, found := strings.Cut(strings.TrimSpace(pair), "="); found && k != "" {
				cfg.Labels[k] = v
			}
		}
	}
	// Списки через запятую; пустая переменная — пустой список.
	list := func(target *[]string, name string) {
		if v, ok := os.LookupEnv(name); ok {
			*target = []string{}
			for _, item := range strings.Split(v, ",") {
				if item = strings.TrimSpace(item); item != "" {
					*target = append(*target, item)
				}
			}
		}
	}
	list(&cfg.Server.URLs, "AGENT_SERVER_URLS")
	list(&cfg.Update.PublicKeys, "AGENT_UPDATE_PUBLIC_KEYS")
	list(&cfg.Telemetry.ExcludeInterfaces, "AGENT_TELEMETRY_EXCLUDE_INTERFACES")
	list(&cfg.Telemetry.Metrics, "AGENT_TELEMETRY_METRICS")
	list(&cfg.Telemetry.Disks, "AGENT_TELEMETRY_DISKS")
	number := func(target *int, name string) {
		if v, ok := os.LookupEnv(name); ok && v != "" {
			n, err := strconv.Atoi(strings.TrimSpace(v))
			if err != nil {
				cfg.envErrs = append(cfg.envErrs, fmt.Sprintf("%s: %q — нужно целое число", name, v))
				return
			}
			*target = n
		}
	}
	duration := func(target *Duration, name string) {
		if v, ok := os.LookupEnv(name); ok && v != "" {
			d, err := time.ParseDuration(strings.TrimSpace(v))
			if err != nil {
				cfg.envErrs = append(cfg.envErrs, fmt.Sprintf("%s: %q — нужна длительность (например, 30s)", name, v))
				return
			}
			*target = Duration(d)
		}
	}
	duration(&cfg.Server.Reconnect.Min, "AGENT_SERVER_RECONNECT_MIN")
	duration(&cfg.Server.Reconnect.Max, "AGENT_SERVER_RECONNECT_MAX")
	number(&cfg.Server.StreamBuffer, "AGENT_SERVER_STREAM_BUFFER")
	number(&cfg.Outbox.MaxMessages, "AGENT_OUTBOX_MAX_MESSAGES")
	number(&cfg.Log.Buffer, "AGENT_LOG_BUFFER")
}

// Validate — обязательные поля и допустимые значения; заполняет умолчания воркеров.
func (c *Config) Validate() error {
	var errs []error
	for _, e := range c.envErrs {
		errs = append(errs, errors.New(e))
	}
	addrs := c.Server.Addresses()
	if len(addrs) == 0 {
		errs = append(errs, errors.New("server.url или server.urls (AGENT_SERVER_URL, AGENT_SERVER_URLS): нужен адрес http(s)://"))
	}
	for _, addr := range addrs {
		if u, err := url.Parse(addr); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Errorf("server.url / server.urls: %q — нужен адрес http(s)://", addr))
		}
	}
	if !slices.Contains([]string{"off", message.LogError, message.LogWarn, message.LogInfo, message.LogDebug}, c.Log.Forward) {
		errs = append(errs, fmt.Errorf("log.forward (AGENT_LOG_FORWARD): off | error | warn | info | debug, а не %q", c.Log.Forward))
	}
	if !slices.Contains([]string{UpdateSelf, UpdateExternal, UpdateDisabled}, c.Update.Mode) {
		errs = append(errs, fmt.Errorf("update.mode (AGENT_UPDATE_MODE): self | external | disabled, а не %q", c.Update.Mode))
	}
	for _, k := range c.Update.Keys() {
		if raw, err := base64.StdEncoding.DecodeString(k); err != nil || len(raw) != ed25519.PublicKeySize {
			errs = append(errs, fmt.Errorf("update.publicKey / update.publicKeys (AGENT_UPDATE_PUBLIC_KEY, AGENT_UPDATE_PUBLIC_KEYS): %q — нужен base64 32 байт Ed25519", k))
		}
	}
	for _, g := range c.Telemetry.Metrics {
		if !slices.Contains(message.MetricGroups, g) {
			errs = append(errs, fmt.Errorf("telemetry.metrics (AGENT_TELEMETRY_METRICS): неизвестная группа %q (%s)",
				g, strings.Join(message.MetricGroups, ", ")))
		}
	}
	for _, d := range c.Telemetry.Disks {
		if d == DisksAll && len(c.Telemetry.Disks) > 1 {
			errs = append(errs, errors.New(`telemetry.disks (AGENT_TELEMETRY_DISKS): "all" — только один, без других путей`))
		} else if d != DisksAll && !filepath.IsAbs(d) {
			errs = append(errs, fmt.Errorf(`telemetry.disks (AGENT_TELEMETRY_DISKS): %q — нужен абсолютный путь или "all"`, d))
		}
	}
	if _, err := c.Server.TLSConfig(); err != nil {
		errs = append(errs, err)
	}
	if r := c.Server.Reconnect; r.Min <= 0 || r.Max < r.Min {
		errs = append(errs, fmt.Errorf("server.reconnect (AGENT_SERVER_RECONNECT_MIN, _MAX): min больше 0, max не меньше min, а не %s и %s", dur(r.Min), dur(r.Max)))
	}
	if n := c.Server.StreamBuffer; n < 1 || n > 100_000 {
		errs = append(errs, fmt.Errorf("server.streamBuffer (AGENT_SERVER_STREAM_BUFFER): от 1 до 100000, а не %d", n))
	}
	if n := c.Outbox.MaxMessages; n < 1 || n > 1_000_000 {
		errs = append(errs, fmt.Errorf("outbox.maxMessages (AGENT_OUTBOX_MAX_MESSAGES): от 1 до 1000000, а не %d", n))
	}
	if n := c.Log.Buffer; n < 100 || n > message.MaxLogLines {
		errs = append(errs, fmt.Errorf("log.buffer (AGENT_LOG_BUFFER): от 100 до %d, а не %d", message.MaxLogLines, n))
	}
	if c.DataDir == "" {
		errs = append(errs, errors.New("dataDir (AGENT_DATA_DIR) обязателен"))
	}
	if !message.ValidAgentName(c.Name) {
		errs = append(errs, fmt.Errorf("name (AGENT_NAME): непустое, до %d символов", message.MaxAgentName))
	}
	if err := message.CheckLabels(c.Labels); err != nil {
		errs = append(errs, fmt.Errorf("labels (AGENT_LABELS): %w", err))
	}
	names := map[string]bool{}
	for i := range c.Workers {
		w := &c.Workers[i]
		switch {
		case !message.ValidName(w.Name):
			errs = append(errs, fmt.Errorf("workers[%d]: имя %q — строчная латиница, цифры и «-», начало — буква, до 32 символов", i, w.Name))
		case w.Name == SysmetricsWorker:
			errs = append(errs, fmt.Errorf("workers[%d]: имя %q занято встроенным воркером агента", i, w.Name))
		case names[w.Name]:
			errs = append(errs, fmt.Errorf("workers: имя %q повторяется", w.Name))
		}
		names[w.Name] = true
		if !w.Release && len(w.Command) == 0 {
			errs = append(errs, fmt.Errorf("workers[%d]: нужен command (или release: true)", i))
		}
		w.ReleaseDir = ""
		if w.Release && c.DataDir != "" {
			w.ReleaseDir = filepath.Join(ReleasesDir(c.DataDir), w.Name)
		}
		w.FillDefaults()
		errs = append(errs, w.check(i)...)
		for _, name := range w.InheritEnv {
			if name == "" || strings.HasPrefix(name, "AGENT_") || strings.ContainsAny(name, "= ") {
				errs = append(errs, fmt.Errorf("workers[%d].inheritEnv: %q — имя переменной (AGENT_* воркеру не передаются)", i, name))
			}
		}
		if w.User != "" && geteuid() != 0 {
			errs = append(errs, fmt.Errorf("workers[%d].user: запуск воркера от пользователя %q возможен, только если агент работает от root", i, w.User))
		}
	}
	return errors.Join(errs...)
}

// Addresses — адреса сервера по порядку: url, затем urls (без повторов и пустых).
func (s Server) Addresses() []string {
	var out []string
	for _, u := range append([]string{s.URL}, s.URLs...) {
		if u = strings.TrimSpace(u); u != "" && !slices.Contains(out, u) {
			out = append(out, u)
		}
	}
	return out
}

// TLSConfig — TLS для запросов агента к серверу: системные корни плюс
// caFile, клиентский сертификат (certFile + keyFile). nil — ничего не задано.
func (s Server) TLSConfig() (*tls.Config, error) {
	if s.CAFile == "" && s.CertFile == "" && s.KeyFile == "" {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if s.CAFile != "" {
		pem, err := os.ReadFile(s.CAFile)
		if err != nil {
			return nil, fmt.Errorf("server.caFile (AGENT_SERVER_CA_FILE): %w", err)
		}
		pool, err := x509.SystemCertPool()
		if err != nil || pool == nil {
			pool = x509.NewCertPool()
		}
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("server.caFile (AGENT_SERVER_CA_FILE): в %s нет сертификатов PEM", s.CAFile)
		}
		cfg.RootCAs = pool
	}
	if (s.CertFile == "") != (s.KeyFile == "") {
		return nil, errors.New("server.certFile и server.keyFile (AGENT_SERVER_CERT_FILE, AGENT_SERVER_KEY_FILE): нужны оба или ни одного")
	}
	if s.CertFile != "" {
		cert, err := tls.LoadX509KeyPair(s.CertFile, s.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("server.certFile/keyFile: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

// Insecure — связь без шифрования (http://) не с этой машиной: ключ агента и
// настройки воркеров идут открытым текстом. Локальные — localhost,
// 127.0.0.0/8, ::1.
func (c *Config) Insecure() bool {
	return slices.ContainsFunc(c.Server.Addresses(), Insecure)
}

// Insecure — адрес http:// на нелокальный хост (см. Config.Insecure).
func Insecure(serverURL string) bool {
	u, err := url.Parse(serverURL)
	if err != nil || !strings.EqualFold(u.Scheme, "http") {
		return false
	}
	host := strings.ToLower(u.Hostname())
	if host == "localhost" || strings.HasSuffix(host, ".localhost") {
		return false
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return false
	}
	return true
}
