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
	if w.Command[0] != "/opt/venv/bin/python" || w.Lifecycle.StopTimeout.Std() != 10*time.Minute {
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
	ok := Defaults()
	ok.Server.URL = "http://10.0.0.1:8181"
	if err := ok.Validate(); err == nil || !strings.Contains(err.Error(), "allowInsecure") {
		t.Fatalf("http без allowInsecure: %v", err)
	}
	ok.Server.AllowInsecure = true
	if err := ok.Validate(); err != nil {
		t.Fatalf("http с allowInsecure: %v", err)
	}
	ok.Server.URL = ""
	if err := ok.Validate(); err != nil {
		t.Fatalf("без бэкенда: %v", err)
	}
}

func TestNameAndLabels(t *testing.T) {
	bad := Defaults()
	bad.Server.URL = "https://x"
	bad.Name = ""
	bad.Labels = map[string]string{"": "x"}
	if err := bad.Validate(); err == nil || !strings.Contains(err.Error(), "name") || !strings.Contains(err.Error(), "labels") {
		t.Fatalf("имя и метки: %v", err)
	}
}

func TestTLSFiles(t *testing.T) {
	dir := t.TempDir()
	caPEM, keyPEM := testCerts(t)
	ca, key := filepath.Join(dir, "ca.pem"), filepath.Join(dir, "key.pem")
	_ = os.WriteFile(ca, caPEM, 0o600)
	_ = os.WriteFile(key, keyPEM, 0o600)

	s := Server{}
	if c, err := s.TLSConfig(); c != nil || err != nil {
		t.Fatalf("без файлов — умолчания Go: %v %v", c, err)
	}
	s = Server{CAFile: ca}
	c, err := s.TLSConfig()
	if err != nil || c.RootCAs == nil {
		t.Fatalf("CA: %v", err)
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
	cfg, err := Load("")
	if err != nil || cfg.Server.CAFile != ca {
		t.Fatalf("из окружения: %+v %v", cfg.Server, err)
	}
}

// testCerts — сертификат CA и закрытый ключ (PEM).
func testCerts(t *testing.T) (caPEM, keyPEM []byte) {
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
	keyDER, _ := x509.MarshalECPrivateKey(caKey)
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER}),
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
	next.Labels = map[string]string{"zone": "us"}
	next.Workers = []Worker{{Name: "w", Command: []string{"x"}, Lifecycle: Lifecycle{StopTimeout: Duration(time.Minute)}}}
	d := Compare(base, next)
	if strings.Join(d.Restart, ",") != "server.url,server.caFile,dataDir,update.mode,enroll.token" {
		t.Fatalf("перезапуск: %v", d.Restart)
	}
	if !d.Log || !d.Hello || !d.Workers {
		t.Fatalf("на ходу: %+v", d)
	}
	kept := KeepRestartOnly(base, next)
	if kept.Server.URL != base.Server.URL || kept.Server.CAFile != "" || kept.DataDir != base.DataDir ||
		kept.Update.Mode != base.Update.Mode || kept.Enroll.Token != "" ||
		kept.Log.Level != "debug" || kept.Labels["zone"] != "us" || kept.Workers[0].Lifecycle.StopTimeout.Std() != time.Minute {
		t.Fatalf("ключи перезапуска — прежние, остальное новое: %+v", kept)
	}
}

// Всё, что KeepRestartOnly оставляет прежним, Compare называет ключом
// перезапуска: изменение не пропадает молча.
func TestCompareUpdateKeys(t *testing.T) {
	base := Defaults()
	base.Server.URL = "https://a.example.com"
	if err := base.Validate(); err != nil {
		t.Fatal(err)
	}
	next := base
	next.Update.Releases = "https://releases.example.com/agent"
	every := Duration(time.Hour)
	next.Update.CheckInterval = &every
	next.Outbox.MaxMessages = base.Outbox.MaxMessages + 1
	d := Compare(base, next)
	if strings.Join(d.Restart, ",") != "update.releases,update.checkInterval,outbox.maxMessages" {
		t.Fatalf("перезапуск: %v", d.Restart)
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
`), 0o600)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	rel, plain := cfg.Workers[0], cfg.Workers[1]
	wantDir := filepath.Join(dir, "workers", "report")
	if !rel.Release || rel.ReleaseDir != wantDir || rel.Current() != filepath.Join(wantDir, "current") {
		t.Fatalf("воркер со сборкой с сервера: %+v", rel)
	}
	if plain.Lifecycle.StopTimeout.Std() != DefaultStopTimeout || plain.ReleaseDir != "" {
		t.Fatalf("воркер: %+v", plain)
	}
	// Повторная проверка не ломает уже проверенные настройки.
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	for name, w := range map[string]Worker{
		"имя с путём": {Name: "../x", Release: true},
		"заглавные":   {Name: "Report", Command: []string{"/bin/x"}},
		"без command": {Name: "x"},
	} {
		c := Defaults()
		c.Server.URL = "https://api.example.com"
		c.Workers = []Worker{w}
		if err := c.Validate(); err == nil {
			t.Errorf("%s: ожидалась ошибка", name)
		}
	}
}

// Перечитывание: release/args — изменение воркеров; каталог сборок — в
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
		t.Fatalf("каталог сборок: %q / %q", kept.Workers[0].ReleaseDir, next.Workers[0].ReleaseDir)
	}
	args := next
	args.Workers = []Worker{{Name: "w", Release: true, Args: []string{"-q"}}}
	_ = args.Validate()
	if !Compare(next, args).Workers {
		t.Fatal("args — изменение воркера")
	}
}

// server.url — один адрес https (http — с allowInsecure). log.forward — warn по умолчанию,
// AGENT_LOG_FORWARD, проверка значения.
func TestServerURLAndLogForward(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, "server:\n  url: https://a.example.com\ndataDir: "+dir+"\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Server.URL != "https://a.example.com" || cfg.Log.Forward != "warn" {
		t.Fatalf("адрес %q, forward %q", cfg.Server.URL, cfg.Log.Forward)
	}
	t.Setenv("AGENT_SERVER_URL", "http://10.0.0.1")
	t.Setenv("AGENT_SERVER_ALLOW_INSECURE", "true")
	t.Setenv("AGENT_LOG_FORWARD", "debug")
	if cfg, err = Load(path); err != nil || cfg.Server.URL != "http://10.0.0.1" || cfg.Log.Forward != "debug" || !cfg.Server.AllowInsecure {
		t.Fatalf("из окружения: %+v %v", cfg.Server, err)
	}
	t.Setenv("AGENT_SERVER_URL", "ftp://x")
	t.Setenv("AGENT_LOG_FORWARD", "громко")
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "ftp://x") || !strings.Contains(err.Error(), "log.forward") {
		t.Fatalf("ошибки: %v", err)
	}
	old, next := Defaults(), Defaults()
	next.Log.Forward = "off"
	if d := Compare(old, next); !d.Log || len(d.Restart) > 0 {
		t.Fatal("log.forward — на ходу")
	}
}

// lifecycle: stopTimeout — по умолчанию DefaultStopTimeout (и у воркера,
// собранного в коде); прочие ключи lifecycle и воркера — неизвестные поля
// (замечание, не ошибка).
func TestLifecycle(t *testing.T) {
	dir := t.TempDir()
	path := writeConfig(t, `server: { url: https://api.example.com }
dataDir: `+dir+`
workers:
  - name: plain
    command: [x]
  - name: tuned
    command: [x]
    lifecycle:
      stopTimeout: 2m
      restart: never
    routes: open
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Workers[0].Lifecycle.StopTimeout.Std() != DefaultStopTimeout || cfg.Workers[1].Lifecycle.StopTimeout.Std() != 2*time.Minute {
		t.Fatalf("stopTimeout: %+v", cfg.Workers)
	}
	var coded Worker
	coded.FillDefaults()
	if coded.Lifecycle.StopTimeout.Std() != DefaultStopTimeout {
		t.Fatalf("воркер из кода: %+v", coded.Lifecycle)
	}
	res := Check(path)
	if len(res.Errors) != 0 || len(res.Warnings) != 2 {
		t.Fatalf("замечания: %+v %+v", res.Warnings, res.Errors)
	}
}

// Настройки агента целиком: очередь важных сообщений, журнал — умолчания,
// файл, окружение, проверка; их изменение требует перезапуска агента.
func TestAgentTuning(t *testing.T) {
	def := Defaults()
	if def.Outbox.MaxMessages != 10000 || def.Log.Buffer != 5000 {
		t.Fatalf("умолчания: %+v %+v", def.Outbox, def.Log)
	}
	path := writeConfig(t, `server:
  url: https://api.example.com
outbox: {maxMessages: 500}
log: {buffer: 1000}
`)
	t.Setenv("AGENT_OUTBOX_MAX_MESSAGES", "700")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Outbox.MaxMessages != 700 || cfg.Log.Buffer != 1000 {
		t.Fatalf("файл и окружение: %+v %+v", cfg.Outbox, cfg.Log)
	}
	t.Setenv("AGENT_OUTBOX_MAX_MESSAGES", "много")
	t.Setenv("AGENT_LOG_BUFFER", "1")
	res := Check(path)
	text := ""
	for _, p := range res.Errors {
		text += p.Text + "\n"
	}
	if !strings.Contains(text, "AGENT_OUTBOX_MAX_MESSAGES") || !strings.Contains(text, "log.buffer") {
		t.Fatalf("ошибки: %s", text)
	}
	next := cfg
	next.Outbox.MaxMessages, next.Log.Buffer = 10, 200
	d := Compare(cfg, next)
	if strings.Join(d.Restart, ",") != "outbox.maxMessages,log.buffer" || d.Log {
		t.Fatalf("перезапуск: %+v", d)
	}
	if kept := KeepRestartOnly(cfg, next); kept.Outbox != cfg.Outbox || kept.Log.Buffer != cfg.Log.Buffer {
		t.Fatalf("прежние до перезапуска: %+v", kept)
	}
}

// Ключи проверки подписи сборок: publicKey и publicKeys вместе, без повторов;
// AGENT_UPDATE_PUBLIC_KEYS — через запятую; неверный ключ — ошибка настроек.
func TestUpdateKeys(t *testing.T) {
	const a, b = "MCowBQYDK2VwAyEAAAAAAAAAAAAAAAAAAAAAAAAAAAA=", "Q0hBTkdFTUVDSEFOR0VNRUNIQU5HRU1FQ0hBTkdFTUU="
	dir := t.TempDir()
	path := writeConfig(t, "server: {url: https://api.example.com}\ndataDir: "+dir+"\nupdate: {publicKey: "+b+", publicKeys: ["+b+"]}\n")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := cfg.Update.Keys(); !slices.Equal(got, []string{b}) {
		t.Fatalf("ключи: %v", got)
	}
	t.Setenv("AGENT_UPDATE_PUBLIC_KEYS", " "+b+", ,"+b)
	if cfg, err = Load(path); err != nil || !slices.Equal(cfg.Update.PublicKeys, []string{b, b}) {
		t.Fatalf("из окружения: %v %v", cfg.Update.PublicKeys, err)
	}
	t.Setenv("AGENT_UPDATE_PUBLIC_KEYS", a+",a2V5")
	if _, err := Load(path); err == nil || !strings.Contains(err.Error(), "update.publicKeys") {
		t.Fatalf("неверный ключ: %v", err)
	}
}
