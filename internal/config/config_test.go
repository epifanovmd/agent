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
    command: ["${PY}", "-m", "examples.echo_worker"]
    stopTimeout: 10m
`), 0o600)
	t.Setenv("AGENT_LABELS", "disk=ssd, pool=a")
	t.Setenv("AGENT_LOG_LEVEL", "debug")

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.Transport != "auto" || cfg.Update.Mode != "self" || cfg.Log.Level != "debug" {
		t.Fatalf("умолчания или env: %+v", cfg)
	}
	if cfg.Labels["zone"] != "eu" || cfg.Labels["disk"] != "ssd" || cfg.Labels["pool"] != "a" {
		t.Fatalf("метки: %v", cfg.Labels)
	}
	w := cfg.Workers[0]
	if w.Command[0] != "/opt/venv/bin/python" || w.Replicas != 1 || w.StopTimeout.Std() != 10*time.Minute {
		t.Fatalf("воркер: %+v", w)
	}
}

func TestValidate(t *testing.T) {
	cfg := Defaults()
	cfg.Server.URL = "ftp://x"
	cfg.Server.Transport = "pigeon"
	cfg.Workers = []Worker{{Name: "a"}, {Name: "a", Command: []string{"x"}}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("ожидались ошибки")
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

func TestStateResync(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	_ = os.WriteFile(path, []byte("server: { url: https://api.example.com }\ndataDir: "+dir+"\n"), 0o600)
	cfg, err := Load(path)
	if err != nil || cfg.State.ResyncInterval.Std() != 10*time.Minute {
		t.Fatalf("по умолчанию 10m: %v %v", cfg.State.ResyncInterval.Std(), err)
	}
	_ = os.WriteFile(path, []byte("server: { url: https://api.example.com }\ndataDir: "+dir+"\nstate: { resyncInterval: 0 }\n"), 0o600)
	if cfg, err = Load(path); err != nil || cfg.State.ResyncInterval != 0 {
		t.Fatalf("0 — выключено: %v %v", cfg.State.ResyncInterval.Std(), err)
	}
	t.Setenv("AGENT_STATE_RESYNC", "90s")
	if cfg, err = Load(path); err != nil || cfg.State.ResyncInterval.Std() != 90*time.Second {
		t.Fatalf("AGENT_STATE_RESYNC: %v %v", cfg.State.ResyncInterval.Std(), err)
	}
	t.Setenv("AGENT_STATE_RESYNC", "часто")
	if _, err = Load(path); err == nil {
		t.Fatal("неверная длительность — ошибка")
	}
	t.Setenv("AGENT_STATE_RESYNC", "-1s")
	if _, err = Load(path); err == nil {
		t.Fatal("отрицательная — ошибка")
	}
}

func TestBuiltinsTelemetryAndEnv(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	_ = os.WriteFile(path, []byte(`
server: { url: https://api.example.com }
dataDir: `+dir+`
commands: { disabled: [agent.update, worker.restart] }
telemetry:
  metrics: [load, swap, cpu.cores]
  disks: [all]
  inventoryInterval: 0
  excludeInterfaces: [tun]
  backlog: 0
`), 0o600)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	tl := cfg.Telemetry
	if len(cfg.Commands.Disabled) != 2 || strings.Join(tl.Metrics, ",") != "load,swap,cpu.cores" ||
		strings.Join(tl.Disks, ",") != "all" || tl.InventoryInterval != 0 || tl.Backlog != 0 ||
		len(tl.ExcludeInterfaces) != 1 || tl.ExcludeInterfaces[0] != "tun" {
		t.Fatalf("из файла: %+v %+v", cfg.Commands, tl)
	}

	// Умолчания.
	def := Defaults()
	if !slices.Equal(def.Telemetry.Metrics, DefaultMetrics) || strings.Join(def.Telemetry.Disks, ",") != "/" || def.Telemetry.InventoryInterval.Std() != 10*time.Minute || def.Telemetry.Backlog != 720 ||
		len(def.Telemetry.ExcludeInterfaces) != len(DefaultExcludeInterfaces) || len(def.Commands.Disabled) != 0 {
		t.Fatalf("умолчания: %+v", def.Telemetry)
	}

	// Переменные окружения важнее файла.
	t.Setenv("AGENT_COMMANDS_DISABLED", "agent.logs, agent.restart")
	t.Setenv("AGENT_TELEMETRY_METRICS", "sockets, fds")
	t.Setenv("AGENT_TELEMETRY_DISKS", "/,/var/lib/example")
	t.Setenv("AGENT_INVENTORY_INTERVAL", "1m")
	t.Setenv("AGENT_TELEMETRY_BACKLOG", "10")
	t.Setenv("AGENT_TELEMETRY_EXCLUDE_INTERFACES", "lo,docker")
	cfg, err = Load(path)
	if err != nil {
		t.Fatal(err)
	}
	tl = cfg.Telemetry
	if strings.Join(cfg.Commands.Disabled, ",") != "agent.logs,agent.restart" ||
		strings.Join(tl.Metrics, ",") != "sockets,fds" || strings.Join(tl.Disks, ",") != "/,/var/lib/example" ||
		tl.InventoryInterval.Std() != time.Minute || tl.Backlog != 10 || strings.Join(tl.ExcludeInterfaces, ",") != "lo,docker" {
		t.Fatalf("из окружения: %+v %+v", cfg.Commands, tl)
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
	t.Setenv("AGENT_TELEMETRY_METRICS", "load")

	// Неизвестная команда, неверные host и backlog — ошибки.
	t.Setenv("AGENT_COMMANDS_DISABLED", "agent.nope")
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "agent.nope") {
		t.Fatalf("неизвестная команда: %v", err)
	}
	bad := Defaults()
	bad.Server.URL = "https://x"
	bad.Telemetry.Metrics = []string{"cpu", "gpu"}
	bad.Telemetry.Disks = []string{"all", "/"}
	bad.Telemetry.Backlog = -1
	bad.Telemetry.InventoryInterval = -1
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), `неизвестная группа "gpu"`) ||
		!strings.Contains(err.Error(), `"all" — только один`) ||
		!strings.Contains(err.Error(), "telemetry.backlog") || !strings.Contains(err.Error(), "telemetry.inventoryInterval") {
		t.Fatalf("ошибки телеметрии: %v", err)
	}
	bad = Defaults()
	bad.Server.URL = "https://x"
	bad.Telemetry.Disks = []string{"var/lib"}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "абсолютный путь") {
		t.Fatalf("относительный путь диска: %v", err)
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

// Перечитывание: что применяется на ходу, что требует перезапуска.
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
	next.Telemetry.Backlog = 0
	next.Telemetry.Metrics = []string{"load"}
	next.State.ResyncInterval = 0
	next.Labels = map[string]string{"zone": "us"}
	next.Commands.Disabled = []string{"agent.logs"}
	next.Workers = []Worker{{Name: "w", Command: []string{"x"}, Replicas: 2, StopTimeout: Duration(30 * time.Second)}}
	d := Compare(base, next)
	if strings.Join(d.Restart, ",") != "server.url,server.caFile,dataDir,update.mode,enroll.token" {
		t.Fatalf("перезапуск: %v", d.Restart)
	}
	if !d.Log || !d.Telemetry || !d.Resync || !d.Hello || !d.Workers {
		t.Fatalf("на ходу: %+v", d)
	}

	kept := KeepRestartOnly(base, next)
	if kept.Server.URL != base.Server.URL || kept.Server.CAFile != "" || kept.DataDir != base.DataDir ||
		kept.Update.Mode != base.Update.Mode || kept.Enroll.Token != "" ||
		kept.Log.Level != "debug" || kept.Labels["zone"] != "us" || kept.Workers[0].Replicas != 2 {
		t.Fatalf("ключи перезапуска — прежние, остальное новое: %+v", kept)
	}

	// Порядок commands.disabled не важен.
	a, b := base, base
	a.Commands.Disabled = []string{"agent.logs", "agent.drain"}
	b.Commands.Disabled = []string{"agent.drain", "agent.logs"}
	if Compare(a, b).Changed() {
		t.Fatal("тот же набор выключенных команд")
	}
}

// release: true — команда по умолчанию <dataDir>/workers/<name>/current + args;
// command вместе с release — ошибка; имя воркера из выпуска — по правилу имён.
func TestWorkerRelease(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	_ = os.WriteFile(path, []byte(`
server:
  url: https://api.example.com
dataDir: `+dir+`
workers:
  - name: sysinfo
    release: true
    args: ["--verbose"]
  - name: plain
    command: ["/bin/echo", "a"]
    args: ["b"]
`), 0o600)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	rel, plain := cfg.Workers[0], cfg.Workers[1]
	wantDir := filepath.Join(dir, "workers", "sysinfo")
	if !rel.Release || rel.ReleaseDir != wantDir {
		t.Fatalf("воркер из выпуска: %+v", rel)
	}
	if got := rel.Argv(); !slices.Equal(got, []string{filepath.Join(wantDir, "current"), "--verbose"}) {
		t.Fatalf("argv выпуска: %v", got)
	}
	if got := plain.Argv(); !slices.Equal(got, []string{"/bin/echo", "a", "b"}) {
		t.Fatalf("argv: %v", got)
	}
	// Повторная проверка не ломает уже проверенные настройки.
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}

	for name, w := range map[string]Worker{
		"command и release": {Name: "x", Release: true, Command: []string{"/bin/x"}},
		"имя с путём":       {Name: "../x", Release: true},
		"без command":       {Name: "x"},
	} {
		c := Defaults()
		c.Server.URL = "https://api.example.com"
		c.Workers = []Worker{w}
		if err := c.Validate(); err == nil {
			t.Errorf("%s: ожидалась ошибка", name)
		}
	}
	if !slices.Contains(BuiltinCommands, "worker.update") {
		t.Fatal("worker.update — встроенная команда")
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

// Пауза воркера с сервера — встроенные команды (их можно выключить).
func TestBuiltinWorkerPause(t *testing.T) {
	for _, name := range []string{"worker.pause", "worker.resume"} {
		if !slices.Contains(BuiltinCommands, name) {
			t.Fatalf("%s — встроенная команда", name)
		}
		c := Defaults()
		c.Server.URL = "https://api.example.com"
		c.Commands.Disabled = []string{name}
		c.Name = "node"
		if err := c.Validate(); err != nil && strings.Contains(err.Error(), name) {
			t.Fatalf("%s можно выключить: %v", name, err)
		}
	}
}
