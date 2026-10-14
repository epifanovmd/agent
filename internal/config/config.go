// Package config — настройки агента: значения по умолчанию → YAML-файлы
// (цепочка extends, подстановка ${ENV}, файлы переменных envFiles) →
// переменные окружения AGENT_*.
package config

import (
	"crypto/ed25519"
	"crypto/tls"
	"crypto/x509"
	"encoding/base64"
	"errors"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/epifanovmd/agent/protocol"
)

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

// dur — длительность коротко: 10m, а не 10m0s.
func dur(d Duration) string {
	s := d.Std().String()
	for _, suf := range []string{"0s", "0m"} {
		if strings.HasSuffix(s, "m"+suf) || strings.HasSuffix(s, "h"+suf) {
			s = strings.TrimSuffix(s, suf)
		}
	}
	return s
}

// Std — time.Duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// Config — настройки агента.
type Config struct {
	// Extends — файл настроек, поверх которого читается этот (путь — от этого
	// файла); в итоговых настройках пусто.
	Extends string `yaml:"extends,omitempty"`
	// EnvFiles — файлы переменных KEY=VALUE для ${ИМЯ} и AGENT_* (пути — от
	// этого файла); окружение процесса важнее их. В итоговых настройках пусто.
	EnvFiles []string `yaml:"envFiles,omitempty"`

	// Instance — экземпляр агента на узле для agent install (--instance), если флага нет.
	Instance string `yaml:"instance,omitempty"`
	// Install — параметры agent install, если флагов нет (пакеты, права службы, sysctl).
	Install *Install `yaml:"install,omitempty"`
	// Pack — что ещё кладёт в архив agent pack. Агенту на узле не нужно — установка убирает.
	Pack *Pack `yaml:"pack,omitempty"`

	Server  Server            `yaml:"server"`
	DataDir string            `yaml:"dataDir"`
	Name    string            `yaml:"name"`
	Labels  map[string]string `yaml:"labels"`
	Enroll  Enroll            `yaml:"enroll"`
	Log     Log               `yaml:"log"`
	Update  Update            `yaml:"update"`
	Outbox  Outbox            `yaml:"outbox"`
	Workers []Worker          `yaml:"workers"`

	// envErrs — неверные значения переменных AGENT_* (их сообщает Validate).
	envErrs []string
}

// Install — параметры agent install в файле настроек: те же, что флаги
// (флаги важнее). Агенту на узле они не нужны — установка их убирает.
type Install struct {
	Packages []string `yaml:"packages,omitempty"`
	// PackagesByManager — свои имена пакетов для менеджера (apt, dnf, yum, apk, zypper).
	PackagesByManager map[string][]string `yaml:"packagesByManager,omitempty"`
	Privileged        bool                `yaml:"privileged,omitempty"`
	Sysctl            map[string]string   `yaml:"sysctl,omitempty"`
}

// Pack — файлы проекта в архиве agent pack.
type Pack struct {
	// Files — файлы и папки внутри папки агента (пути — от файла настроек): кладутся в архив
	// по тому же пути; папка с «/» в конце — её содержимое в корень архива. Совпасть с тем,
	// что кладёт сам pack (программа, настройки, воркеры, release/, bundle.json), не могут.
	Files []string `yaml:"files,omitempty"`
}

// Outbox — очередь важных сообщений на диске.
type Outbox struct {
	// MaxMessages — сколько сообщений очередь держит; полна — события
	// воркеров отклоняются (503 OUTBOX_FULL).
	MaxMessages int `yaml:"maxMessages"`
}

type Server struct {
	// URL бэкенда: https://api.example.com; пусто — работа без бэкенда (только с Required: false).
	URL string `yaml:"url"`
	// Required — агенту нужен бэкенд (по умолчанию): без адреса, а до регистрации и без токена
	// агент не запускается и не ставится. false — допускается работа без бэкенда.
	Required bool `yaml:"required"`
	// AllowInsecure — разрешить http:// (стенд, локальная сеть): ключ агента и данные идут
	// открытым текстом.
	AllowInsecure bool `yaml:"allowInsecure"`
	// CAFile — PEM с корневыми сертификатами в дополнение к системным.
	CAFile string `yaml:"caFile"`
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
	// Releases — каталог сборок агента, устроенный как релизы на GitHub
	// (<адрес>/latest/download/…, <адрес>/download/v<версия>/…): адрес http(s)://
	// или абсолютный путь к папке; пусто — релизы агента на GitHub.
	Releases string `yaml:"releases,omitempty"`
	// CheckInterval — как часто проверять новую версию в каталоге сборок (§10);
	// nil — DefaultCheckInterval, 0s — не проверять.
	CheckInterval *Duration `yaml:"checkInterval,omitempty"`
}

// DefaultCheckInterval — проверка новой версии агента по умолчанию.
const DefaultCheckInterval = 6 * time.Hour

// CheckEvery — как часто проверять новую версию (0 — не проверять).
func (u Update) CheckEvery() time.Duration {
	if u.CheckInterval == nil {
		return DefaultCheckInterval
	}
	return u.CheckInterval.Std()
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

// Worker — воркер: процесс, который запускает агент; с агентом он говорит через его сокет.
type Worker struct {
	Name    string   `yaml:"name"`
	Command []string `yaml:"command"`
	// Args — аргументы, дописываемые к command (или к файлу сборки).
	Args []string          `yaml:"args"`
	Dir  string            `yaml:"dir"`
	Env  map[string]string `yaml:"env"`
	// Release — воркер со сборкой с сервера: сборку ведёт агент
	// (<dataDir>/workers/<name>/current), сервер обновляет её действием
	// worker.update. Сборка — исполняемый файл или каталог из архива
	// .tar.gz; command (если задан) выполняется в каталоге сборки, без
	// command запускается сам файл или ./run архива.
	Release bool `yaml:"release"`
	// Path — папка воркера (путь — от файла настроек): агент запускает в ней
	// command, по умолчанию ./run; имя по умолчанию — имя папки.
	Path string `yaml:"path,omitempty"`
	// Lifecycle — установка зависимостей и остановка воркера (§11); изменение применяется без
	// перезапуска воркера.
	Lifecycle Lifecycle `yaml:"lifecycle"`
	// ReleaseDir — <dataDir>/workers/<name>; заполняет Validate для release.
	ReleaseDir string `yaml:"-"`
}

// Файлы сборки воркера в его каталоге ReleaseDir.
const (
	ReleaseCurrent         = "current"
	ReleaseVersion         = "version"
	ReleasePrevious        = "previous"
	ReleasePreviousVersion = "previous.version"
	// ReleaseRun — что запускать в сборке-архиве, если command не задан.
	ReleaseRun = "run"
	// Setup — установка зависимостей воркера: исполняемый файл в его рабочем каталоге (каталог
	// сборки-архива или папка воркера); агент выполняет его один раз до запуска воркера.
	Setup = "setup"
	// Cleanup — уборка воркера перед удалением агента: исполняемый файл в рабочем каталоге
	// воркера; его выполняет agent uninstall (agent stop-workers --cleanup).
	Cleanup = "cleanup"
)

// Lifecycle — установка зависимостей и остановка воркера (§11).
type Lifecycle struct {
	// StopTimeout — сколько ждать выхода после SIGTERM, потом SIGKILL. 0 — DefaultStopTimeout.
	StopTimeout Duration `yaml:"stopTimeout"`
	// SetupTimeout — срок ./setup (установка зависимостей). 0 — DefaultSetupTimeout.
	SetupTimeout Duration `yaml:"setupTimeout"`
}

// Сроки lifecycle по умолчанию.
const (
	DefaultStopTimeout  = 30 * time.Second
	DefaultSetupTimeout = 10 * time.Minute
)

// FillDefaults — значения по умолчанию незаданных полей воркера.
func (w *Worker) FillDefaults() {
	if w.Lifecycle.StopTimeout <= 0 {
		w.Lifecycle.StopTimeout = Duration(DefaultStopTimeout)
	}
	if w.Lifecycle.SetupTimeout <= 0 {
		w.Lifecycle.SetupTimeout = Duration(DefaultSetupTimeout)
	}
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
		Log:     Log{Level: "info", Format: "text", Forward: protocol.LogWarn, Buffer: protocol.MaxLogLines},
		Outbox:  Outbox{MaxMessages: protocol.MaxOutbox},
		Update:  Update{Mode: UpdateSelf},
		Server:  Server{Required: true},
	}
}

// RequireServer — запуск или установка агента, которому нужен бэкенд (server.required): адрес
// задан, а до регистрации (enrolled — ключа агента ещё нет) есть и токен. Без server.required
// ошибки нет.
func (c *Config) RequireServer(enrolled bool) error {
	if !c.Server.Required {
		return nil
	}
	if c.Server.URL == "" {
		return errors.New("не задан адрес бэкенда server.url (AGENT_SERVER_URL): задайте его в настройках, " +
			"окружении, файле переменных или флагом --server; работа без бэкенда — server.required: false " +
			"(AGENT_SERVER_REQUIRED=false)")
	}
	if !enrolled && c.Enroll.Token == "" {
		return errors.New("агент не зарегистрирован и нет токена регистрации enroll.token (AGENT_ENROLL_TOKEN): " +
			"задайте его в окружении, файле переменных или флагом --token")
	}
	return nil
}

// Load — настройки из файла path (пустой — без файла; цепочка extends) и окружения.
func Load(path string) (Config, error) {
	cfg := Defaults()
	src := &Source{}
	if path != "" {
		src = ReadSource(path)
		if len(src.Errors) > 0 {
			return cfg, problemsError(src.Errors)
		}
		if err := src.Decode(&cfg); err != nil {
			return cfg, fmt.Errorf("config %s: %w", path, err)
		}
	}
	applyEnv(&cfg, src.Lookup)
	return cfg, cfg.Validate()
}

// LoadFor — настройки из файла path так, как их увидит процесс с окружением env (служба с её
// файлом переменных), без проверки значений.
func LoadFor(path string, env func(string) (string, bool)) (Config, error) {
	cfg := Defaults()
	src := readSource(path, env)
	if len(src.Errors) > 0 {
		return cfg, problemsError(src.Errors)
	}
	if err := src.Decode(&cfg); err != nil {
		return cfg, fmt.Errorf("config %s: %w", path, err)
	}
	applyEnv(&cfg, src.Lookup)
	return cfg, nil
}

// problemsError — ошибки файлов настроек одной ошибкой, по строке на каждую.
func problemsError(problems []Problem) error {
	errs := make([]error, len(problems))
	for i, p := range problems {
		errs[i] = errors.New("config: " + p.String())
	}
	return errors.Join(errs...)
}

// CheckServerURL — адрес бэкенда, если задан: https://, http:// — только с server.allowInsecure.
func (c *Config) CheckServerURL() error {
	addr := c.Server.URL
	if addr == "" {
		return nil
	}
	u, err := url.Parse(addr)
	switch {
	case err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "":
		return fmt.Errorf("server.url (AGENT_SERVER_URL): %q — нужен адрес https://", addr)
	case u.Scheme == "http" && !c.Server.AllowInsecure:
		return fmt.Errorf("server.url (AGENT_SERVER_URL): %q — без шифрования; нужен https:// или server.allowInsecure: true (AGENT_SERVER_ALLOW_INSECURE)", addr)
	}
	return nil
}

// Validate — обязательные поля и допустимые значения; заполняет умолчания воркеров.
func (c *Config) Validate() error {
	var errs []error
	for _, e := range c.envErrs {
		errs = append(errs, errors.New(e))
	}
	if err := c.CheckServerURL(); err != nil {
		errs = append(errs, err)
	}
	if !slices.Contains([]string{"off", protocol.LogError, protocol.LogWarn, protocol.LogInfo, protocol.LogDebug}, c.Log.Forward) {
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
	if _, err := c.Server.TLSConfig(); err != nil {
		errs = append(errs, err)
	}
	if n := c.Outbox.MaxMessages; n < 1 || n > 1_000_000 {
		errs = append(errs, fmt.Errorf("outbox.maxMessages (AGENT_OUTBOX_MAX_MESSAGES): от 1 до 1000000, а не %d", n))
	}
	if n := c.Log.Buffer; n < 100 || n > protocol.MaxLogLines {
		errs = append(errs, fmt.Errorf("log.buffer (AGENT_LOG_BUFFER): от 100 до %d, а не %d", protocol.MaxLogLines, n))
	}
	if c.DataDir == "" {
		errs = append(errs, errors.New("dataDir (AGENT_DATA_DIR) обязателен"))
	}
	if !protocol.ValidAgentName(c.Name) {
		errs = append(errs, fmt.Errorf("name (AGENT_NAME): непустое, до %d символов", protocol.MaxAgentName))
	}
	if err := protocol.CheckLabels(c.Labels); err != nil {
		errs = append(errs, fmt.Errorf("labels (AGENT_LABELS): %w", err))
	}
	if c.Instance != "" && !protocol.ValidName(c.Instance) {
		errs = append(errs, fmt.Errorf("instance: %q — строчная латиница, цифры и «-», первая — буква, до 32 символов", c.Instance))
	}
	if r := c.Update.Releases; r != "" && !filepath.IsAbs(r) {
		if u, err := url.Parse(r); err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
			errs = append(errs, fmt.Errorf("update.releases (AGENT_UPDATE_RELEASES): %q — нужен адрес http(s):// или абсолютный путь к папке", r))
		}
	}
	if d := c.Update.CheckEvery(); d < 0 || (d > 0 && d < time.Minute) {
		errs = append(errs, fmt.Errorf("update.checkInterval (AGENT_UPDATE_CHECK_INTERVAL): 0s (не проверять) или не меньше 1m, а не %s", dur(Duration(d))))
	}
	errs = append(errs, c.checkWorkers()...)
	return errors.Join(errs...)
}

// ValidateWorkers — проверить только воркеры и заполнить их умолчания (имя и
// каталог воркера из папки, сборка, lifecycle): для упаковки, где остальные
// настройки (адрес сервера, токен) могут быть ещё не заданы.
func (c *Config) ValidateWorkers() error { return errors.Join(c.checkWorkers()...) }

// checkWorkers — проверка и умолчания воркеров.
func (c *Config) checkWorkers() []error {
	var errs []error
	names := map[string]bool{}
	for i := range c.Workers {
		w := &c.Workers[i]
		if w.Path != "" {
			if w.Name == "" {
				w.Name = filepath.Base(w.Path)
			}
			if w.Dir != "" && w.Dir != w.Path {
				errs = append(errs, fmt.Errorf("workers[%d]: dir и path — что-то одно (path — папка воркера, command выполняется в ней)", i))
			}
			if w.Release {
				errs = append(errs, fmt.Errorf("workers[%d]: path — воркер из папки, release — воркер со сборкой: что-то одно", i))
			}
			w.Dir = w.Path
			if len(w.Command) == 0 {
				w.Command = []string{"./" + ReleaseRun}
			}
		}
		switch {
		case !protocol.ValidName(w.Name):
			errs = append(errs, fmt.Errorf("workers[%d]: имя %q — строчная латиница, цифры и «-», начало — буква, до 32 символов", i, w.Name))
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
	}
	return errs
}

// TLSConfig — TLS для запросов агента к серверу: системные корни плюс
// caFile. nil — caFile не задан.
func (s Server) TLSConfig() (*tls.Config, error) {
	if s.CAFile == "" {
		return nil, nil
	}
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
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
	return cfg, nil
}
