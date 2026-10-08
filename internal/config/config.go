// Package config — конфигурация агента: значения по умолчанию → YAML-файл
// (с подстановкой ${ENV}) → переменные окружения AGENT_*.
package config

import (
	"crypto/tls"
	"crypto/x509"
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

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Duration — длительность в YAML строкой: "30s", "10m".
type Duration time.Duration

func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	parsed, err := time.ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("длительность %q: %w", node.Value, err)
	}
	*d = Duration(parsed)
	return nil
}

// Std — time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

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
	State     State             `yaml:"state"`
	Commands  Commands          `yaml:"commands"`
	Workers   []Worker          `yaml:"workers"`
}

type Server struct {
	// URL бэкенда: https://api.example.com.
	URL string `yaml:"url"`
	// URLs — адреса одного и того же бэкенда (вместо url или вместе с ним):
	// агент подключается к первому доступному, при обрыве или отказе — к
	// следующему по кругу. Учётные данные одни на все адреса.
	URLs []string `yaml:"urls"`
	// Transport: auto (WebSocket, при недоступности — HTTP sync) | ws | http.
	Transport string `yaml:"transport"`
	// CAFile — PEM с корневыми сертификатами, которым агент доверяет в
	// дополнение к системным (свой CA сервера).
	CAFile string `yaml:"caFile"`
	// CertFile, KeyFile — клиентский сертификат агента (mTLS); оба или ни одного.
	CertFile string `yaml:"certFile"`
	KeyFile  string `yaml:"keyFile"`
}

// Commands — встроенные команды агента.
type Commands struct {
	// Disabled — встроенные команды (BuiltinCommands), которые агент не
	// объявляет и не выполняет.
	Disabled []string `yaml:"disabled"`
}

// BuiltinCommands — встроенные команды агента (их можно выключить в commands.disabled).
var BuiltinCommands = []string{
	"agent.logs", "agent.drain", "agent.resume", "agent.restart", "agent.update", "agent.rotateKey", "worker.restart",
	"worker.update", "worker.pause", "worker.resume",
}

type Enroll struct {
	// Token — токен регистрации; нужен только до первой регистрации
	// (и для эфемерных агентов — при каждом старте).
	Token string `yaml:"token"`
}

type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
	// Forward — с какого уровня записи лога агента и вывод воркеров уходят
	// серверу (сообщение log): off | error | warn | info | debug, по
	// умолчанию warn. Подписка сервера может временно сделать подробнее (subscription.logLevel).
	Forward string `yaml:"forward"`
}

type Telemetry struct {
	// GPU: auto (nvidia-smi, если есть) | off.
	GPU string `yaml:"gpu"`
	// Metrics — группы метрик узла (message.MetricGroups); [] — без метрик
	// узла. По умолчанию DefaultMetrics.
	Metrics []string `yaml:"metrics"`
	// Disks — точки монтирования для metrics.host.disks (группа disk);
	// ["all"] — все реальные файловые системы. По умолчанию ["/"].
	Disks []string `yaml:"disks"`
	// InventoryInterval — как часто проверять сведения об узле; 0 — сведения
	// не отправляются вовсе. По умолчанию 10m.
	InventoryInterval Duration `yaml:"inventoryInterval"`
	// ExcludeInterfaces — префиксы имён сетевых интерфейсов, которые не
	// показываются в метриках и сведениях; заданный список заменяет умолчание.
	ExcludeInterfaces []string `yaml:"excludeInterfaces"`
	// Backlog — точек метрик, копящихся без связи (дошлются после
	// подключения); 0 — не копить. По умолчанию 720.
	Backlog int `yaml:"backlog"`
}

// DefaultMetrics — группы метрик узла по умолчанию.
var DefaultMetrics = []string{
	message.MetricsCPU, message.MetricsLoad, message.MetricsMemory, message.MetricsSwap, message.MetricsDisk,
	message.MetricsNetwork, message.MetricsInterfaces, message.MetricsConntrack, message.MetricsUptime,
}

// DisksAll — telemetry.disks: все реальные файловые системы.
const DisksAll = "all"

// DefaultExcludeInterfaces — интерфейсы, которые по умолчанию не показываются:
// петля, контейнеры, мосты и служебные интерфейсы macOS.
var DefaultExcludeInterfaces = []string{
	"lo", "veth", "docker", "br-", "virbr", "vnet", "cni", "flannel", "cali", "kube", "tunl",
	"gif", "stf", "awdl", "llw", "anpi", "utun", "bridge", "ap",
}

type Update struct {
	// Mode: self (замена исполняемого файла) | external (контейнер) | disabled.
	Mode string `yaml:"mode"`
	// PublicKey — ключ проверки подписи релизов Ed25519, base64.
	PublicKey string `yaml:"publicKey"`
}

type State struct {
	// ResyncInterval — раз в интервал агент заново отдаёт воркеру-владельцу
	// последний снимок каждого раздела (та же версия): воркер исправляет
	// ручные изменения на узле. 0 — выключено. По умолчанию 10m.
	ResyncInterval Duration `yaml:"resyncInterval"`
}

// Стратегии замены экземпляров воркера.
const (
	RestartRolling   = "rolling"
	RestartStopFirst = "stop-first"
)

// Worker — воркер: дочерний процесс агента на любом языке.
type Worker struct {
	Name    string            `yaml:"name"`
	Command []string          `yaml:"command"`
	Dir     string            `yaml:"dir"`
	Env     map[string]string `yaml:"env"`
	// Replicas — экземпляров процесса (по умолчанию 1).
	Replicas int `yaml:"replicas"`
	// Queues — ограничить очереди воркера (по умолчанию — все, что он объявил).
	Queues []string `yaml:"queues"`
	// StopTimeout — сколько ждать доработки задач при остановке (по умолчанию 30s).
	StopTimeout Duration `yaml:"stopTimeout"`
	// Restart — замена экземпляров: rolling (по умолчанию: новый рядом, старый
	// дорабатывает — без простоя) | stop-first (сначала уходит старый — для
	// воркеров, держащих порт, интерфейс или другой единственный ресурс).
	Restart string `yaml:"restart"`
	// Release — воркер из выпуска: исполняемый файл ведёт агент
	// (<dataDir>/workers/<name>/current), сервер может обновить его командой
	// worker.update. command не задаётся.
	Release bool `yaml:"release"`
	// Args — аргументы, дописываемые к command (или к файлу выпуска).
	Args []string `yaml:"args"`
	// Limits — ограничения ресурсов воркера (все его экземпляры вместе; Linux,
	// cgroup v2 с делегированием — иначе предупреждение и запуск без них).
	Limits Limits `yaml:"limits"`
	// User — пользователь, от которого запускается воркер; агенту нужен root.
	User string `yaml:"user"`
	// Dir выпуска — <dataDir>/workers/<name>; заполняет Validate для release.
	ReleaseDir string `yaml:"-"`
}

// Файлы воркера из выпуска в его каталоге ReleaseDir.
const (
	ReleaseCurrent         = "current"
	ReleaseVersion         = "version"
	ReleasePrevious        = "previous"
	ReleasePreviousVersion = "previous.version"
)

// Argv — команда запуска: command + args; для воркера из выпуска —
// <ReleaseDir>/current + args.
func (w Worker) Argv() []string {
	if w.Release {
		return append([]string{filepath.Join(w.ReleaseDir, ReleaseCurrent)}, w.Args...)
	}
	return append(slices.Clone(w.Command), w.Args...)
}

// ReleasesDir — каталог воркеров из выпуска в dataDir.
func ReleasesDir(dataDir string) string { return filepath.Join(dataDir, "workers") }

// Defaults — значения по умолчанию.
func Defaults() Config {
	host, _ := os.Hostname()
	return Config{
		Server:  Server{Transport: "auto"},
		DataDir: "/var/lib/agent",
		Name:    host,
		Labels:  map[string]string{},
		Log:     Log{Level: "info", Format: "text", Forward: "warn"},
		Telemetry: Telemetry{
			GPU: "auto", Metrics: slices.Clone(DefaultMetrics), Disks: []string{"/"}, InventoryInterval: Duration(10 * time.Minute),
			ExcludeInterfaces: slices.Clone(DefaultExcludeInterfaces), Backlog: 720,
		},
		Update: Update{Mode: "self"},
		State:  State{ResyncInterval: Duration(10 * time.Minute)},
	}
}

// Load — конфигурация из файла path (пустой — без файла) и окружения.
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
	if err := applyEnv(&cfg); err != nil {
		return cfg, err
	}
	return cfg, cfg.Validate()
}

func applyEnv(cfg *Config) error {
	set := func(target *string, name string) {
		if v, ok := os.LookupEnv(name); ok && v != "" {
			*target = v
		}
	}
	set(&cfg.Server.URL, "AGENT_SERVER_URL")
	set(&cfg.Server.Transport, "AGENT_TRANSPORT")
	set(&cfg.DataDir, "AGENT_DATA_DIR")
	set(&cfg.Name, "AGENT_NAME")
	set(&cfg.Enroll.Token, "AGENT_ENROLL_TOKEN")
	set(&cfg.Log.Level, "AGENT_LOG_LEVEL")
	set(&cfg.Log.Format, "AGENT_LOG_FORMAT")
	set(&cfg.Log.Forward, "AGENT_LOG_FORWARD")
	set(&cfg.Telemetry.GPU, "AGENT_GPU")
	set(&cfg.Server.CAFile, "AGENT_SERVER_CA_FILE")
	set(&cfg.Server.CertFile, "AGENT_SERVER_CERT_FILE")
	set(&cfg.Server.KeyFile, "AGENT_SERVER_KEY_FILE")
	set(&cfg.Update.Mode, "AGENT_UPDATE_MODE")
	set(&cfg.Update.PublicKey, "AGENT_UPDATE_PUBLIC_KEY")
	if labels, ok := os.LookupEnv("AGENT_LABELS"); ok {
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
	list(&cfg.Commands.Disabled, "AGENT_COMMANDS_DISABLED")
	list(&cfg.Server.URLs, "AGENT_SERVER_URLS")
	list(&cfg.Telemetry.ExcludeInterfaces, "AGENT_TELEMETRY_EXCLUDE_INTERFACES")
	list(&cfg.Telemetry.Metrics, "AGENT_TELEMETRY_METRICS")
	list(&cfg.Telemetry.Disks, "AGENT_TELEMETRY_DISKS")
	duration := func(target *Duration, name string) error {
		if v, ok := os.LookupEnv(name); ok && v != "" {
			d, err := time.ParseDuration(v)
			if err != nil {
				return fmt.Errorf("%s: длительность %q: %w", name, v, err)
			}
			*target = Duration(d)
		}
		return nil
	}
	if err := duration(&cfg.State.ResyncInterval, "AGENT_STATE_RESYNC"); err != nil {
		return err
	}
	if err := duration(&cfg.Telemetry.InventoryInterval, "AGENT_INVENTORY_INTERVAL"); err != nil {
		return err
	}
	if v, ok := os.LookupEnv("AGENT_TELEMETRY_BACKLOG"); ok && v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return fmt.Errorf("AGENT_TELEMETRY_BACKLOG: число %q: %w", v, err)
		}
		cfg.Telemetry.Backlog = n
	}
	return nil
}

// Validate — обязательные поля и допустимые значения.
func (c *Config) Validate() error {
	var errs []error
	addrs := c.Server.Addresses()
	if len(addrs) == 0 {
		errs = append(errs, errors.New("server.url или server.urls (AGENT_SERVER_URL, AGENT_SERVER_URLS): нужен адрес http(s)://"))
	}
	for _, addr := range addrs {
		if u, err := url.Parse(addr); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Errorf("server.url / server.urls: %q — нужен адрес http(s)://", addr))
		}
	}
	if !oneOf(c.Log.Forward, "off", "error", "warn", "info", "debug") {
		errs = append(errs, fmt.Errorf("log.forward (AGENT_LOG_FORWARD): off | error | warn | info | debug, а не %q", c.Log.Forward))
	}
	if !oneOf(c.Server.Transport, "auto", "ws", "http") {
		errs = append(errs, fmt.Errorf("server.transport: auto | ws | http, а не %q", c.Server.Transport))
	}
	if !oneOf(c.Update.Mode, "self", "external", "disabled") {
		errs = append(errs, fmt.Errorf("update.mode: self | external | disabled, а не %q", c.Update.Mode))
	}
	if c.State.ResyncInterval < 0 {
		errs = append(errs, errors.New("state.resyncInterval (AGENT_STATE_RESYNC): не меньше 0 (0 — выключено)"))
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
	if c.Telemetry.InventoryInterval < 0 {
		errs = append(errs, errors.New("telemetry.inventoryInterval (AGENT_INVENTORY_INTERVAL): не меньше 0 (0 — не отправлять)"))
	}
	if c.Telemetry.Backlog < 0 {
		errs = append(errs, errors.New("telemetry.backlog (AGENT_TELEMETRY_BACKLOG): не меньше 0 (0 — не копить)"))
	}
	for _, name := range c.Commands.Disabled {
		if !slices.Contains(BuiltinCommands, name) {
			errs = append(errs, fmt.Errorf("commands.disabled (AGENT_COMMANDS_DISABLED): %q — не встроенная команда (%s)",
				name, strings.Join(BuiltinCommands, ", ")))
		}
	}
	if _, err := c.Server.TLSConfig(); err != nil {
		errs = append(errs, err)
	}
	if c.DataDir == "" {
		errs = append(errs, errors.New("dataDir (AGENT_DATA_DIR) обязателен"))
	}
	if c.Name == "" {
		errs = append(errs, errors.New("name (AGENT_NAME) обязателен"))
	}
	names := map[string]bool{}
	for i := range c.Workers {
		w := &c.Workers[i]
		switch {
		case w.Release && len(w.Command) > 0:
			errs = append(errs, fmt.Errorf("workers[%d] (%s): при release: true command не задаётся — исполняемый файл ведёт агент (args — аргументы)", i, w.Name))
		case w.Release && !message.ValidName(w.Name):
			errs = append(errs, fmt.Errorf("workers[%d]: имя воркера из выпуска %q — латиница, цифры, «.», «_», «-», до 64 символов", i, w.Name))
		case w.Name == "" || (!w.Release && len(w.Command) == 0):
			errs = append(errs, fmt.Errorf("workers[%d]: нужны name и command (или release: true)", i))
		}
		w.ReleaseDir = ""
		if w.Release && c.DataDir != "" {
			w.ReleaseDir = filepath.Join(ReleasesDir(c.DataDir), w.Name)
		}
		if names[w.Name] {
			errs = append(errs, fmt.Errorf("workers: имя %q повторяется", w.Name))
		}
		names[w.Name] = true
		if w.Replicas <= 0 {
			w.Replicas = 1
		}
		if w.StopTimeout <= 0 {
			w.StopTimeout = Duration(30 * time.Second)
		}
		if !oneOf(w.Restart, "", RestartRolling, RestartStopFirst) {
			errs = append(errs, fmt.Errorf("workers[%d].restart: rolling | stop-first, а не %q", i, w.Restart))
		}
		if err := w.Limits.Validate(); err != nil {
			errs = append(errs, fmt.Errorf("workers[%d].limits: %w", i, err))
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

// TLSConfig — настройки TLS для запросов агента к серверу: системные корни
// плюс caFile, клиентский сертификат (certFile + keyFile). nil — ничего не
// задано (умолчания Go).
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
// снимки состояния (в них бывают секреты) идут открытым текстом. Локальные —
// localhost, 127.0.0.0/8, ::1.
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

func oneOf(v string, options ...string) bool {
	for _, o := range options {
		if v == o {
			return true
		}
	}
	return false
}
