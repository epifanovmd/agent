package config

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"
)

func TestLoadFileEnvAndDefaults(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	t.Setenv("PY", "/opt/venv/bin/python")
	_ = os.WriteFile(path, []byte(`
server:
  url: https://api.example.com
dataDir: `+dir+`
labels: { zone: eu }
workers:
  - name: echo
    command: ["${PY}", "/opt/example/echo.py"]
    lifecycle:
      stopTimeout: 10m
`), 0o600)
	t.Setenv("AGENT_LABELS", "disk=ssd, pool=a")
	t.Setenv("AGENT_LOG_LEVEL", "debug")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Update.Mode != "self" || cfg.Log.Level != "debug" {
		t.Fatalf("умолчания или env: %+v", cfg)
	}
	if cfg.Labels["zone"] != "eu" || cfg.Labels["disk"] != "ssd" || cfg.Labels["pool"] != "a" {
		t.Fatalf("метки: %v", cfg.Labels)
	}
	w := cfg.Workers[0]
	if w.Command[0] != "/opt/venv/bin/python" || w.Lifecycle.StopTimeout.Std() != 10*time.Minute || w.Lifecycle.KeepChildren {
		t.Fatalf("воркер: %+v", w)
	}
}

func TestValidate(t *testing.T) {
	cfg := Defaults()
	cfg.Server.URL = "ftp://x"
	cfg.Update.Mode = "pigeon"
	cfg.Workers = []Worker{{Name: "a"}, {Name: "a", Command: []string{"x"}}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("ожидались ошибки")
	}
	builtin := Defaults()
	builtin.Server.URL = "https://api.example.com"
	builtin.Workers = []Worker{{Name: SysmetricsWorker, Command: []string{"x"}}}
	if err := builtin.Validate(); err == nil || !strings.Contains(err.Error(), "занято встроенным") {
		t.Fatalf("имя встроенного воркера: %v", err)
	}
	ok := Defaults()
	ok.Server.URL = "http://10.0.0.1:8181"
	if err := ok.Validate(); err != nil || !ok.Insecure() {
		t.Fatalf("валидный http на чужой адрес — небезопасен: %v", err)
	}
	ok.Server.URL = "http://localhost:8181"
	if ok.Insecure() {
		t.Fatal("localhost — не предупреждаем")
	}
}

func TestInsecure(t *testing.T) {
	for url, want := range map[string]bool{
		"http://10.0.0.1:8181":         true,
		"http://api.example.com":       true,
		"HTTP://api.example.com":       true,
		"http://host.docker.internal":  true,
		"http://localhost:8080":        false,
		"http://app.localhost":         false,
		"http://127.0.0.1:8080":        false,
		"http://127.10.20.30":          false,
		"http://[::1]:8080":            false,
		"https://api.example.com":      false,
		"https://10.0.0.1":             false,
		"http://[::ffff:127.0.0.1]:80": false,
	} {
		if got := Insecure(url); got != want {
			t.Errorf("Insecure(%s) = %v, ждали %v", url, got, want)
		}
	}
}

func TestTelemetryAndEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	_ = os.WriteFile(path, []byte(`
server: { url: https://api.example.com }
dataDir: `+dir+`
telemetry:
  metrics: [load, swap, gpu]
  disks: [all]
  excludeInterfaces: [tun]
`), 0o600)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	tl := cfg.Telemetry
	if strings.Join(tl.Metrics, ",") != "load,swap,gpu" || strings.Join(tl.Disks, ",") != "all" ||
		len(tl.ExcludeInterfaces) != 1 || tl.ExcludeInterfaces[0] != "tun" {
		t.Fatalf("из файла: %+v", tl)
	}

	def := Defaults()
	if !slices.Equal(def.Telemetry.Metrics, DefaultMetrics) || strings.Join(def.Telemetry.Disks, ",") != "/" ||
		len(def.Telemetry.ExcludeInterfaces) != len(DefaultExcludeInterfaces) || def.Update.Mode != UpdateSelf {
		t.Fatalf("умолчания: %+v", def.Telemetry)
	}

	// Переменные окружения важнее файла.
	t.Setenv("AGENT_TELEMETRY_METRICS", "sockets, fds")
	t.Setenv("AGENT_TELEMETRY_DISKS", "/,/var/lib/example")
	t.Setenv("AGENT_TELEMETRY_EXCLUDE_INTERFACES", "lo,docker")
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	tl = cfg.Telemetry
	if strings.Join(tl.Metrics, ",") != "sockets,fds" || strings.Join(tl.Disks, ",") != "/,/var/lib/example" ||
		strings.Join(tl.ExcludeInterfaces, ",") != "lo,docker" {
		t.Fatalf("из окружения: %+v", tl)
	}
	// Пустая переменная — без метрик узла.
	t.Setenv("AGENT_TELEMETRY_METRICS", "")
	if cfg, err = Load(path); err != nil || cfg.Telemetry.Metrics == nil || len(cfg.Telemetry.Metrics) != 0 {
		t.Fatalf("пустой список групп: %v %+v", err, cfg.Telemetry.Metrics)
	}
	t.Setenv("AGENT_TELEMETRY_METRICS", "load,nope")
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), `неизвестная группа "nope"`) {
		t.Fatalf("неизвестная группа: %v", err)
	}
	bad := Defaults()
	bad.Server.URL = "https://x"
	bad.Telemetry.Disks = []string{"all", "/"}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), `"all" — только один`) {
		t.Fatalf("all и пути: %v", err)
	}
	bad.Telemetry.Disks = []string{"var/lib"}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "абсолютный путь") {
		t.Fatalf("относительный путь диска: %v", err)
	}
	bad = Defaults()
	bad.Server.URL = "https://x"
	bad.Name = ""
	bad.Labels = map[string]string{"": "x"}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "name") || !strings.Contains(err.Error(), "labels") {
		t.Fatalf("имя и метки: %v", err)
	}
}

func TestTLSFiles(t *testing.T) {
	dir := t.TempDir()
	caPEM, certPEM, keyPEM := testCerts(t)
	ca, cert, key := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	_ = os.WriteFile(ca, caPEM, 0o600)
	_ = os.WriteFile(cert, certPEM, 0o600)
	_ = os.WriteFile(key, keyPEM, 0o600)

	s := Server{}
	if c, err := s.TLSConfig(); c != nil || err != nil {
		t.Fatalf("без файлов — умолчания Go: %v %v", c, err)
	}
	s = Server{CAFile: ca, CertFile: cert, KeyFile: key}
	c, err := s.TLSConfig()
	if err != nil || c.RootCAs == nil || len(c.Certificates) != 1 {
		t.Fatalf("CA и клиентский сертификат: %v", err)
	}
	if _, err := (Server{CertFile: cert}).TLSConfig(); err == nil {
		t.Fatal("certFile без keyFile — ошибка")
	}
	if _, err := (Server{CAFile: key}).TLSConfig(); err == nil {
		t.Fatal("caFile без сертификатов — ошибка")
	}
	if _, err := (Server{CAFile: filepath.Join(dir, "nope.pem")}).TLSConfig(); err == nil {
		t.Fatal("нет файла — ошибка")
	}

	t.Setenv("AGENT_SERVER_URL", "https://api.example.com")
	t.Setenv("AGENT_DATA_DIR", dir)
	t.Setenv("AGENT_SERVER_CA_FILE", ca)
	t.Setenv("AGENT_SERVER_CERT_FILE", cert)
	t.Setenv("AGENT_SERVER_KEY_FILE", key)
	cfg, err := Load("")
	if err != nil || cfg.Server.CAFile != ca || cfg.Server.CertFile != cert || cfg.Server.KeyFile != key {
		t.Fatalf("из окружения: %+v %v", cfg.Server, err)
	}
	t.Setenv("AGENT_SERVER_KEY_FILE", "")
	os.Unsetenv("AGENT_SERVER_KEY_FILE")
	if _, err := Load(""); err == nil || !strings.Contains(err.Error(), "оба или ни одного") {
		t.Fatalf("certFile без keyFile: %v", err)
	}
}

// testCerts — CA и выпущенный им сертификат (PEM).
func testCerts(t *testing.T) (caPEM, certPEM, keyPEM []byte) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "test CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "agent"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, caTpl, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
		pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})
}

func TestCompare(t *testing.T) {
	base := Defaults()
	base.Server.URL = "https://a.example.com"
	base.Labels = map[string]string{"zone": "eu"}
	base.Workers = []Worker{{Name: "w", Command: []string{"x"}}}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	if d := Compare(base, base); d.Changed() {
		t.Fatalf("без изменений: %+v", d)
	}

	next := base
	next.Server.URL = "https://b.example.com"
	next.Server.CAFile = "/etc/agent/ca.pem"
	next.DataDir = "/srv/agent"
	next.Update.Mode = "disabled"
	next.Enroll.Token = "t"
	next.Log.Level = "debug"
	next.Telemetry.Metrics = []string{"load"}
	next.Labels = map[string]string{"zone": "us"}
	next.Workers = []Worker{{Name: "w", Command: []string{"x"}, Lifecycle: Lifecycle{KeepChildren: true, StopTimeout: Duration(30 * time.Second)}}}
	d := Compare(base, next)
	if strings.Join(d.Restart, ",") != "server.url,server.caFile,dataDir,update.mode,enroll.token" {
		t.Fatalf("перезапуск: %v", d.Restart)
	}
	if !d.Log || !d.Telemetry || !d.Hello || !d.Workers {
		t.Fatalf("на ходу: %+v", d)
	}
	kept := KeepRestartOnly(base, next)
	if kept.Server.URL != base.Server.URL || kept.Server.CAFile != "" || kept.DataDir != base.DataDir ||
		kept.Update.Mode != base.Update.Mode || kept.Enroll.Token != "" ||
		kept.Log.Level != "debug" || kept.Labels["zone"] != "us" || !kept.Workers[0].Lifecycle.KeepChildren {
		t.Fatalf("ключи перезапуска — прежние, остальное новое: %+v", kept)
	}
}

// release: true — каталог сборки <dataDir>/workers/<name>; имена воркеров — по
// правилу §1; ошибки настроек воркера.
func TestWorkerRelease(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	_ = os.WriteFile(path, []byte(`
server:
  url: https://api.example.com
dataDir: `+dir+`
workers:
  - name: report
    release: true
    args: ["--verbose"]
  - name: plain
    command: ["/bin/echo", "a"]
    lifecycle:
      keepChildren: true
      maxRestarts: 5
`), 0o600)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	rel, plain := cfg.Workers[0], cfg.Workers[1]
	wantDir := filepath.Join(dir, "workers", "report")
	if !rel.Release || rel.ReleaseDir != wantDir || rel.Current() != filepath.Join(wantDir, "current") {
		t.Fatalf("воркер из выпуска: %+v", rel)
	}
	if l := plain.Lifecycle; !l.KeepChildren || l.MaxRestarts != 5 || l.StopTimeout.Std() != DefaultStopTimeout || plain.ReleaseDir != "" {
		t.Fatalf("воркер: %+v", plain)
	}
	// Повторная проверка не ломает уже проверенные настройки.
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, w := range map[string]Worker{
		"inheritEnv AGENT_": {Name: "x", Command: []string{"/bin/x"}, InheritEnv: []string{"AGENT_ENROLL_TOKEN"}},
		"maxRestarts < 0":   {Name: "x", Command: []string{"/bin/x"}, Lifecycle: Lifecycle{MaxRestarts: -1}},
		"имя с путём":       {Name: "../x", Release: true},
		"заглавные":         {Name: "Report", Command: []string{"/bin/x"}},
		"встроенный":        {Name: SysmetricsWorker, Command: []string{"/bin/x"}},
		"без command":       {Name: "x"},
	} {
		c := Defaults()
		c.Server.URL = "https://api.example.com"
		c.Workers = []Worker{w}
		if err := c.Validate(); err == nil {
			t.Errorf("%s: ожидалась ошибка", name)
		}
	}
}

// Перечитывание: release/args — изменение воркеров; каталог выпуска — в
// прежнем dataDir до перезапуска.
func TestCompareRelease(t *testing.T) {
	base := Defaults()
	base.Server.URL = "https://api.example.com"
	base.DataDir = "/old"
	base.Workers = []Worker{{Name: "w", Command: []string{"x"}}}
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	next := base
	next.DataDir = "/new"
	next.Workers = []Worker{{Name: "w", Release: true, Args: []string{"-v"}}}
	if err := next.Validate(); err != nil {
		t.Fatal(err)
	}
	d := Compare(base, next)
	if !d.Workers || !slices.Contains(d.Restart, "dataDir") {
		t.Fatalf("diff: %+v", d)
	}
	kept := KeepRestartOnly(base, next)
	if kept.Workers[0].ReleaseDir != filepath.Join("/old", "workers", "w") || next.Workers[0].ReleaseDir != filepath.Join("/new", "workers", "w") {
		t.Fatalf("каталог выпуска: %q / %q", kept.Workers[0].ReleaseDir, next.Workers[0].ReleaseDir)
	}
	args := next
	args.Workers = []Worker{{Name: "w", Release: true, Args: []string{"-q"}}}
	_ = args.Validate()
	if !Compare(next, args).Workers {
		t.Fatal("args — изменение воркера")
	}
}

// server.urls (AGENT_SERVER_URLS) — адреса по порядку после server.url, без
// повторов; каждый — http(s); небезопасный — хоть один. log.forward — warn по
// умолчанию, AGENT_LOG_FORWARD, проверка значения.
func TestServerURLsAndLogForward(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	if err := os.WriteFile(path, []byte("server:\n  urls: [https://a.example.com, https://b.example.com]\ndataDir: "+dir+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Server.Addresses(); !slices.Equal(got, []string{"https://a.example.com", "https://b.example.com"}) || cfg.Log.Forward != "warn" {
		t.Fatalf("адреса %v, forward %q", got, cfg.Log.Forward)
	}
	t.Setenv("AGENT_SERVER_URL", "https://b.example.com")
	t.Setenv("AGENT_SERVER_URLS", "https://c.example.com, http://10.0.0.1")
	t.Setenv("AGENT_LOG_FORWARD", "debug")
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Server.Addresses(); !slices.Equal(got, []string{"https://b.example.com", "https://c.example.com", "http://10.0.0.1"}) ||
		cfg.Log.Forward != "debug" || !cfg.Insecure() {
		t.Fatalf("из окружения: %v %q", got, cfg.Log.Forward)
	}
	t.Setenv("AGENT_SERVER_URLS", "https://c.example.com,ftp://x")
	t.Setenv("AGENT_LOG_FORWARD", "громко")
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "ftp://x") || !strings.Contains(err.Error(), "log.forward") {
		t.Fatalf("ошибки: %v", err)
	}
	old, next := Defaults(), Defaults()
	next.Server.URLs = []string{"https://x.example.com"}
	if d := Compare(old, next); !slices.Contains(d.Restart, "server.urls") {
		t.Fatalf("server.urls — после перезапуска: %+v", d)
	}
	next = Defaults()
	next.Log.Forward = "off"
	if d := Compare(old, next); !d.Log {
		t.Fatal("log.forward — на ходу")
	}
}

// Группы метрик и диски меняются на ходу: только Telemetry, без перезапуска.
func TestCompareMetrics(t *testing.T) {
	base := Defaults()
	next := base
	next.Telemetry.Metrics = []string{"load", "sockets"}
	if d := Compare(base, next); !d.Telemetry || len(d.Restart) > 0 || d.Hello || d.Workers {
		t.Fatalf("metrics: %+v", d)
	}
	next = base
	next.Telemetry.Disks = []string{DisksAll}
	if d := Compare(base, next); !d.Telemetry || len(d.Restart) > 0 {
		t.Fatalf("disks: %+v", d)
	}
}

// workers[].user читается из agent.yaml; user без root — ошибка конфигурации.
func TestWorkerUser(t *testing.T) {
	defer func(f func() int) { geteuid = f }(geteuid)
	geteuid = func() int { return 0 }
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	_ = os.WriteFile(path, []byte(`
server:
  url: https://api.example.com
workers:
  - name: report
    command: ["/bin/report"]
    user: nobody
`), 0o600)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	w := cfg.Workers[0]
	if w.User != "nobody" {
		t.Fatalf("воркер: %+v", w)
	}

	geteuid = func() int { return 1000 }
	if err := cfg.Validate(); err == nil || !strings.Contains(err.Error(), "workers[0].user") {
		t.Fatalf("user без root: %v", err)
	}
	cfg.Workers[0].User = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("без user: %v", err)
	}
}

// lifecycle и logs: умолчания (и у воркера, собранного в коде), заданные
// значения — в том числе 0 и false там, где это значение; неверные — ошибка
// с именем поля.
func TestLifecycle(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	_ = os.WriteFile(path, []byte(`
server: { url: https://api.example.com }
dataDir: `+dir+`
workers:
  - name: plain
    command: [x]
  - name: tuned
    command: [x]
    lifecycle:
      onAgentRestart: restart
      onAgentStop: stop
      restart: never
      backoff: {min: 2s, max: 1m}
      startTimeout: 5s
      busy: {wait: false, timeout: 1h}
      health: {interval: 0s, failures: 0}
      probeTimeout: 1s
      configRetry: 5s
      updateHealthyTimeout: 2m
    logs: {maxSize: 1MB, maxFiles: 0}
`), 0o600)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	plain, tuned := cfg.Workers[0], cfg.Workers[1]
	def := DefaultLifecycle()
	if !reflect.DeepEqual(plain.Lifecycle, def) || !reflect.DeepEqual(plain.Logs, DefaultLogs()) {
		t.Fatalf("умолчания: %+v %+v", plain.Lifecycle, plain.Logs)
	}
	if def.OnAgentRestart != Keep || def.OnAgentStop != Keep || def.Restart != RestartFailure || !def.BusyWait() ||
		def.Busy.Timeout.Std() != 24*time.Hour || def.HealthInterval() != 5*time.Second || def.HealthFailures() != 3 ||
		def.StartTimeout.Std() != time.Minute || def.Backoff.Max.Std() != 30*time.Second || DefaultLogs().MaxSize != 10<<20 {
		t.Fatalf("значения по умолчанию: %+v", def)
	}
	l := tuned.Lifecycle
	if l.OnAgentRestart != RestartWorker || l.OnAgentStop != StopWorker || l.Restart != RestartNever || l.BusyWait() ||
		l.Busy.Timeout.Std() != time.Hour || l.HealthInterval() != 0 || l.HealthFailures() != 0 ||
		l.Health.Timeout.Std() != 2*time.Second || l.Backoff.Min.Std() != 2*time.Second || l.StartTimeout.Std() != 5*time.Second ||
		l.ProbeTimeout.Std() != time.Second || l.ConfigRetry.Std() != 5*time.Second || l.UpdateHealthyTimeout.Std() != 2*time.Minute ||
		l.StopTimeout.Std() != DefaultStopTimeout || tuned.Logs.MaxSize != 1<<20 || tuned.Logs.Files() != 0 {
		t.Fatalf("заданные: %+v %+v", l, tuned.Logs)
	}
	var coded Worker
	coded.FillDefaults()
	if !reflect.DeepEqual(coded.Lifecycle, def) {
		t.Fatalf("воркер из кода: %+v", coded.Lifecycle)
	}
	for field, src := range map[string]string{
		"lifecycle.onAgentRestart": "lifecycle: {onAgentRestart: stop}",
		"lifecycle.onAgentStop":    "lifecycle: {onAgentStop: restart}",
		"lifecycle.restart":        "lifecycle: {restart: sometimes}",
		"lifecycle.backoff":        "lifecycle: {backoff: {min: 1m, max: 1s}}",
		"lifecycle.health.timeout": "lifecycle: {health: {interval: 1s, timeout: 5s}}",
		"logs.maxSize":             "logs: {maxSize: 1KB}",
		"logs.maxFiles":            "logs: {maxFiles: -1}",
	} {
		res := Check(writeConfig(t, "server: {url: https://api.example.com}\nworkers:\n  - name: a\n    command: [a]\n    "+src+"\n"))
		if len(res.Errors) != 1 || !strings.Contains(res.Errors[0].Text, "workers[0]."+field) || res.Errors[0].Line != 5 {
			t.Errorf("%s: %+v", field, res.Errors)
		}
	}
	if res := Check(writeConfig(t, "server: {url: https://api.example.com}\nworkers:\n  - name: a\n    command: [a]\n    logs: {maxSize: lots}\n")); len(res.Errors) != 1 || !strings.Contains(res.Errors[0].Text, "размер") {
		t.Errorf("размер: %+v", res.Errors)
	}
}

// Размеры: единицы, запись обратно.
func TestByteSize(t *testing.T) {
	for in, want := range map[string]ByteSize{"10MB": 10 << 20, "512kb": 512 << 10, "1GB": 1 << 30, "65536": 65536, "100B": 100} {
		got, err := ParseByteSize(in)
		if err != nil || got != want {
			t.Errorf("%s: %d %v", in, got, err)
		}
	}
	if _, err := ParseByteSize("-1MB"); err == nil {
		t.Error("отрицательный размер")
	}
	if ByteSize(10<<20).String() != "10MB" || ByteSize(1500).String() != "1500B" {
		t.Error("запись размера")
	}
}

// Настройки агента целиком: переподключение, поток без связи, очередь
// важных сообщений, журнал — умолчания, файл, окружение, проверка; их
// изменение требует перезапуска агента.
func TestAgentTuning(t *testing.T) {
	def := Defaults()
	if def.Server.Reconnect.Min.Std() != time.Second || def.Server.Reconnect.Max.Std() != time.Minute ||
		def.Server.StreamBuffer != 600 || def.Outbox.MaxMessages != 10000 || def.Log.Buffer != 5000 {
		t.Fatalf("умолчания: %+v %+v %+v", def.Server, def.Outbox, def.Log)
	}
	path := writeConfig(t, `server:
  url: https://api.example.com
  reconnect: {min: 2s, max: 5m}
  streamBuffer: 100
outbox: {maxMessages: 500}
log: {buffer: 1000}
`)
	t.Setenv("AGENT_OUTBOX_MAX_MESSAGES", "700")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Reconnect.Max.Std() != 5*time.Minute || cfg.Server.StreamBuffer != 100 || cfg.Outbox.MaxMessages != 700 || cfg.Log.Buffer != 1000 {
		t.Fatalf("файл и окружение: %+v %+v %+v", cfg.Server, cfg.Outbox, cfg.Log)
	}
	t.Setenv("AGENT_OUTBOX_MAX_MESSAGES", "много")
	t.Setenv("AGENT_SERVER_RECONNECT_MAX", "1ms")
	res := Check(path)
	text := ""
	for _, p := range res.Errors {
		text += p.Text + "\n"
	}
	if !strings.Contains(text, "AGENT_OUTBOX_MAX_MESSAGES") || !strings.Contains(text, "server.reconnect") {
		t.Fatalf("ошибки: %s", text)
	}
	next := cfg
	next.Server.StreamBuffer, next.Outbox.MaxMessages, next.Log.Buffer = 50, 10, 200
	d := Compare(cfg, next)
	if strings.Join(d.Restart, ",") != "server.streamBuffer,outbox.maxMessages,log.buffer" || d.Log {
		t.Fatalf("перезапуск: %+v", d)
	}
	if kept := KeepRestartOnly(cfg, next); kept.Outbox != cfg.Outbox || kept.Log.Buffer != cfg.Log.Buffer {
		t.Fatalf("прежние до перезапуска: %+v", kept)
	}
}
