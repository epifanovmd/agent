//go:build unix

package integration

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/sdk/go/message"
	"github.com/epifanovmd/agent/test/testserver"
)

// reloadYAML — agent.yaml стенда: Go-воркер go (replicas), при extra — ещё
// воркер extra; extraYAML — дополнительные ключи верхнего уровня.
func reloadYAML(s *stand, dir string, replicas int, extra bool, extraYAML string) string {
	exe, err := os.Executable()
	if err != nil {
		s.t.Fatal(err)
	}
	if !strings.Contains(extraYAML, "telemetry:") {
		extraYAML += "\ntelemetry: { gpu: \"off\" }\n"
	}
	y := fmt.Sprintf(`
server: { url: %q, transport: ws }
dataDir: %q
name: it-agent
enroll: { token: %q }
update: { mode: disabled }
log: { level: error }
%s
workers:
  - name: go
    command: [%q]
    env: { IT_WORKER: "1" }
    replicas: %d
    stopTimeout: 10s
`, s.http.URL, dir, token, extraYAML, exe, replicas)
	if extra {
		y += fmt.Sprintf(`  - name: extra
    command: [%q]
    env: { IT_WORKER: "1", IT_WORKER_KIND: extra }
    stopTimeout: 10s
`, exe)
	}
	return y
}

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
}

func workersOf(a testserver.Agent) map[string]message.StatusWorker {
	out := map[string]message.StatusWorker{}
	if a.Status != nil {
		for _, w := range a.Status.Workers {
			out[w.Name] = w
		}
	}
	return out
}

func commandsOf(a testserver.Agent) []string {
	if a.Capabilities == nil || a.Capabilities.Commands == nil {
		return nil
	}
	return a.Capabilities.Commands.Names
}

// Перечитывание agent.yaml по SIGHUP: replicas и новый воркер — без разрыва
// связи; метки, выключенные команды и показатели — новым hello; задача,
// начатая до перечитывания, доходит до сервера; ошибка в файле — агент
// работает со старыми настройками.
func TestReloadOnSIGHUP(t *testing.T) {
	s := newStand(t)
	dir := t.TempDir()
	path := filepath.Join(dir, "agent.yaml")
	dataDir := filepath.Join(dir, "data")
	writeFile(t, path, reloadYAML(s, dataDir, 1, false, ""))

	cfg, err := config.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	agent, err := app.New(cfg, "it")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	agent.ReloadOnSignal(ctx, path)
	go func() {
		defer close(done)
		_ = agent.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})

	eventually(t, "агент на связи с воркером", func() bool {
		a, ok := s.server.Agent("it-agent")
		return ok && a.Online && slices.Contains(commandsOf(a), "it.ping") && workersOf(a)["go"].Instances == 1 &&
			a.Metrics != nil && a.Metrics.Host != nil
	})
	dials := s.dials.Load()
	long := s.server.Enqueue(testserver.EnqueueRequest{Queue: "go.echo", Data: data(map[string]any{"text": "сквозь reload", "sleep": 3}), LeaseSeconds: 60})
	s.waitJob(long.ID, testserver.JobRunning)

	// replicas 2 и новый воркер — именно сигналом SIGHUP.
	writeFile(t, path, reloadYAML(s, dataDir, 2, true, ""))
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	eventually(t, "второй экземпляр и новый воркер у сервера", func() bool {
		a, _ := s.server.Agent("it-agent")
		w := workersOf(a)
		return slices.Contains(commandsOf(a), "it.extra") && w["go"].Instances == 2 && w["extra"].Instances == 1
	})
	if got := s.dials.Load(); got != dials {
		t.Fatalf("воркеры добавлены без разрыва связи: подключений %d → %d", dials, got)
	}

	// Метки, выключенная команда, без метрик узла (telemetry.metrics: []) — новый hello.
	writeFile(t, path, reloadYAML(s, dataDir, 2, true, `
labels: { zone: eu }
commands: { disabled: [agent.logs] }
telemetry: { gpu: "off", metrics: [] }
`))
	if err := syscall.Kill(os.Getpid(), syscall.SIGHUP); err != nil {
		t.Fatal(err)
	}
	eventually(t, "новый hello: метки и команды", func() bool {
		a, _ := s.server.Agent("it-agent")
		cmds := commandsOf(a)
		// Метки hello (Agent.Labels сервер берёт из регистрации).
		return a.Online && a.Hello != nil && a.Hello.Labels["zone"] == "eu" && !slices.Contains(cmds, message.CommandLogs) &&
			slices.Contains(cmds, "it.extra") && slices.Contains(cmds, "it.ping")
	})
	if got := s.dials.Load(); got != dials+1 {
		t.Fatalf("одно переподключение для нового hello: подключений %d → %d", dials, got)
	}
	eventually(t, "метрики без host", func() bool {
		a, _ := s.server.Agent("it-agent")
		return a.Metrics != nil && a.Metrics.Host == nil
	})
	cmd, cerr := s.server.Command(testserver.CommandRequest{Name: message.CommandLogs})
	if cerr == nil {
		eventually(t, "выключенная команда не выполняется", func() bool {
			c, _ := s.server.CommandSnapshot(cmd.ID)
			return c.Status == testserver.CommandFailed
		})
	}

	// Задача, начатая до перечитывания, завершилась той же попыткой.
	if done := s.waitJob(long.ID, testserver.JobCompleted); done.Attempt != 0 {
		t.Fatalf("задача пережила переподключение: %+v", done)
	}

	// Ошибка в файле — старые настройки; удалённый воркер уходит, его
	// команда снимается.
	writeFile(t, path, "workers: [ {name: \n")
	if err := agent.ReloadFile(path); err == nil {
		t.Fatal("ошибка в файле должна вернуться")
	}
	writeFile(t, path, reloadYAML(s, dataDir, 1, false, `
labels: { zone: eu }
commands: { disabled: [agent.logs] }
`))
	if err := agent.ReloadFile(path); err != nil {
		t.Fatal(err)
	}
	eventually(t, "воркер extra удалён", func() bool {
		a, _ := s.server.Agent("it-agent")
		w := workersOf(a)
		_, extra := w["extra"]
		return a.Online && !extra && w["go"].Instances == 1 && !slices.Contains(commandsOf(a), "it.extra") &&
			a.Metrics != nil && a.Metrics.Host != nil
	})
	job := s.server.Enqueue(testserver.EnqueueRequest{Queue: "go.echo", Data: data(map[string]string{"text": "после"})})
	s.waitJob(job.ID, testserver.JobCompleted)
}

// ─── TLS ────────────────────────────────────────────────────────────────

// testPKI — свой CA и клиентский сертификат агента, выпущенный им (PEM-файлы в dir).
func testPKI(t *testing.T, dir string) (caFile, certFile, keyFile string, pool *x509.CertPool) {
	t.Helper()
	caKey, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	caTpl := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "it CA"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTpl, caTpl, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, _ := x509.ParseCertificate(caDER)
	pool = x509.NewCertPool()
	pool.AddCert(ca)
	key, _ := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	tpl := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "it-agent"},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, caTpl, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	caFile, certFile, keyFile = filepath.Join(dir, "ca.pem"), filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeFile(t, caFile, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})))
	writeFile(t, certFile, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
	writeFile(t, keyFile, string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})))
	return caFile, certFile, keyFile, pool
}

// serverCA — сертификат тестового TLS-сервера в PEM (его «свой CA»).
func serverCA(t *testing.T, s *stand, dir string) string {
	path := filepath.Join(dir, "server-ca.pem")
	writeFile(t, path, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.http.Certificate().Raw})))
	return path
}

// noAgentFor — агент не смог зарегистрироваться за d.
func noAgentFor(t *testing.T, s *stand, d time.Duration) {
	t.Helper()
	time.Sleep(d)
	if list, _ := s.server.Agents().List(); len(list) != 0 {
		t.Fatalf("без доверия к сертификату сервера агент не должен подключиться: %d", len(list))
	}
}

// Сервер со своим CA: без server.caFile агент не доверяет ему; с caFile —
// регистрация, WebSocket и HTTP sync работают.
func TestTLSCustomCA(t *testing.T) {
	for _, transport := range []string{"ws", "http"} {
		t.Run(transport, func(t *testing.T) {
			s := newUnstartedStand(t)
			s.http.StartTLS()
			s.server.SetPublicURL(s.http.URL)
			ca := serverCA(t, s, t.TempDir())

			ctx, cancel := context.WithCancel(context.Background())
			untrusted := s.runRaw(ctx, transport, nil)
			noAgentFor(t, s, time.Second)
			cancel()
			<-untrusted

			s.agent(transport, nil, func(c *config.Config) { c.Server.CAFile = ca })
			job := s.server.Enqueue(testserver.EnqueueRequest{Queue: "go.echo", Data: data(map[string]string{"text": "tls"})})
			s.waitJob(job.ID, testserver.JobCompleted)
		})
	}
}

// mTLS: сервер требует клиентский сертификат от своего CA.
func TestTLSClientCertificate(t *testing.T) {
	s := newUnstartedStand(t)
	_, certFile, keyFile, pool := testPKI(t, t.TempDir())
	s.http.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	s.http.StartTLS()
	s.server.SetPublicURL(s.http.URL)
	serverCAFile := serverCA(t, s, t.TempDir())

	ctx, cancel := context.WithCancel(context.Background())
	noCert := s.runRaw(ctx, "ws", func(c *config.Config) { c.Server.CAFile = serverCAFile })
	noAgentFor(t, s, time.Second)
	cancel()
	<-noCert

	s.agent("ws", nil, func(c *config.Config) {
		c.Server.CAFile, c.Server.CertFile, c.Server.KeyFile = serverCAFile, certFile, keyFile
	})
	eventually(t, "агент с клиентским сертификатом на связи", func() bool {
		a, ok := s.server.Agent("it-agent")
		return ok && a.Online && strings.Contains(strings.Join(commandsOf(a), ","), "it.ping")
	})
}

// runRaw — агент без воркеров до отмены ctx; канал закрывается после выхода.
func (s *stand) runRaw(ctx context.Context, transport string, tune func(*config.Config)) <-chan struct{} {
	cfg := config.Defaults()
	cfg.Server.URL = s.http.URL
	cfg.Server.Transport = transport
	cfg.DataDir = s.t.TempDir()
	cfg.Name = "it-agent"
	cfg.Enroll.Token = token
	cfg.Update.Mode = "disabled"
	cfg.Telemetry.GPU = "off"
	cfg.Log.Level = "error"
	if tune != nil {
		tune(&cfg)
	}
	if err := cfg.Validate(); err != nil {
		s.t.Fatal(err)
	}
	agent, err := app.New(cfg, "it")
	if err != nil {
		s.t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = agent.Run(ctx)
	}()
	return done
}
