//go:build unix

package integration

import (
	"bufio"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/identity"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/test/testserver"
)

// Регистрация токеном, повторный запуск с сохранённым ключом (без новой регистрации),
// неверный токен, второй экземпляр на том же каталоге данных (блокировка).
func TestEnrollAndRestart(t *testing.T) {
	t.Parallel()
	s := newStand(t)

	wrong := s.config()
	wrong.Enroll.Token = "чужой"
	bad := s.start(wrong)
	time.Sleep(time.Second)
	if list := s.server.Agents(); len(list) != 0 {
		t.Fatalf("агент с неверным токеном зарегистрирован: %+v", list)
	}
	bad.stop()

	cfg := s.config()
	n := s.start(cfg)
	first := s.online()
	creds, ok, err := identity.NewStore(cfg.DataDir).Load()
	if err != nil || !ok || creds.AgentID != first.ID {
		t.Fatalf("ключ агента на диске: %+v %v %v", creds, ok, err)
	}
	if st, err := os.Stat(filepath.Join(cfg.DataDir, "credentials.json")); err != nil || st.Mode().Perm() != 0o600 {
		t.Fatalf("credentials.json: %v %v", st.Mode(), err)
	}

	// Каталог данных занят работающим агентом.
	if _, err := app.New(cfg, "it"); err == nil {
		t.Fatal("второй агент на том же каталоге данных запустился")
	}

	n.stop()
	cfg.Enroll.Token = "" // ключ уже есть — токен не нужен
	s.start(cfg)
	eventually(t, "новый запуск на связи", func() bool {
		a, ok := s.server.Agent(agentName)
		return ok && a.Online && a.BootID != "" && a.BootID != first.BootID
	})
	if list := s.server.Agents(); len(list) != 1 || list[0].ID != first.ID {
		t.Fatalf("после перезапуска агентов %d, нужен прежний: %+v", len(list), list)
	}
}

// Отзыв ключа: сервер закрывает соединение (4401), агент регистрируется заново токеном —
// новый id; без токена агент ждёт.
func TestRevoke(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	cfg := s.config()
	s.start(cfg)
	first := s.online()
	s.server.Must("revoke", nil, first.ID)
	var second testserver.Agent
	eventually(t, "агент зарегистрирован заново", func() bool {
		a, ok := s.server.Agent(agentName)
		second = a
		return ok && a.Online && !a.Revoked && a.ID != first.ID
	})
	creds, _, _ := identity.NewStore(cfg.DataDir).Load()
	if creds.AgentID != second.ID {
		t.Fatalf("на диске ключ %s, у сервера %s", creds.AgentID, second.ID)
	}

	// Без токена: после отзыва агент не регистрируется.
	s2 := newStand(t)
	cfg2 := s2.config()
	n := s2.start(cfg2)
	a := s2.online()
	n.stop()
	cfg2.Enroll.Token = ""
	s2.start(cfg2)
	eventually(t, "агент без токена на связи", func() bool {
		b, ok := s2.server.Agent(agentName)
		return ok && b.Online && b.BootID != a.BootID
	})
	s2.server.Must("revoke", nil, a.ID)
	time.Sleep(2 * time.Second)
	if list := s2.server.Agents(); len(list) != 1 || !list[0].Revoked {
		t.Fatalf("агент без токена зарегистрировался заново: %+v", list)
	}
}

// Смена ключа: agent.rotateKey — новый секрет, сервер закрывает соединение (1012), агент
// подключается с новым секретом; старый не принимается, id тот же.
func TestRotateKey(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	cfg := s.config(s.worker("w"))
	s.start(cfg)
	a := s.running("w")
	store := identity.NewStore(cfg.DataDir)
	old, _, _ := store.Load()

	if err := s.server.Action("rotateKey", a.ID, nil); err != nil {
		t.Fatal(err)
	}
	eventually(t, "новый секрет признан", func() bool {
		c, ok, _ := store.Load()
		return ok && c.PendingSecret == "" && c.Secret != old.Secret
	})
	if c, _, _ := store.Load(); c.AgentID != old.AgentID {
		t.Fatalf("id агента сменился: %s → %s", old.AgentID, c.AgentID)
	}
	if code := linkStatus(t, s.http.URL, old.Authorization()); code != http.StatusUnauthorized {
		t.Fatalf("старый секрет: HTTP %d, нужен 401", code)
	}
	a = s.running("w")
	if got := s.fetch(a.ID, "w", "/echo", testserver.FetchInit{Method: "POST", Body: "после смены"}).Body; got != "после смены" {
		t.Fatalf("запрос после смены ключа: %q", got)
	}
	if list := s.server.Agents(); len(list) != 1 {
		t.Fatalf("агентов %d — регистрации заново быть не должно", len(list))
	}
}

// linkStatus — HTTP-код открытия WebSocket с заголовком authorization (только для
// отклонённых ключей: принятый вытеснил бы сессию агента).
func linkStatus(t *testing.T, base, authorization string) int {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, base+message.LinkPath, nil)
	req.Header.Set("Connection", "Upgrade")
	req.Header.Set("Upgrade", "websocket")
	req.Header.Set("Sec-WebSocket-Version", "13")
	req.Header.Set("Sec-WebSocket-Key", "dGhlIHNhbXBsZSBub25jZQ==")
	req.Header.Set("Sec-WebSocket-Protocol", message.Subprotocol)
	req.Header.Set("Authorization", authorization)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	return resp.StatusCode
}

// Несколько адресов сервера: первый недоступен — агент регистрируется и работает через второй.
func TestServerURLs(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	dead := httptest.NewServer(http.NotFoundHandler())
	deadURL := dead.URL
	dead.Close()
	cfg := s.config()
	cfg.Server.URL, cfg.Server.URLs = deadURL, []string{s.http.URL}
	s.start(cfg)
	s.online()
}

// Обрыв связи: события воркера (outbox) и точки метрик (поток) досылаются после
// подключения — события без повторов, метрики с моментами сбора за время без связи.
func TestOutageBackfill(t *testing.T) {
	t.Parallel()
	s := newStand(t, func(c *testserver.Config) { c.Options = map[string]any{"offlineGraceMs": 0} })
	n := s.start(s.config(s.worker("w")))
	a := s.running("w")

	s.setDown(true)
	from := time.Now().UnixMilli()
	n.emit("w", "example.offline", 0, 5)
	time.Sleep(2 * time.Second)
	to := time.Now().UnixMilli()
	s.setDown(false)

	eventually(t, "события после обрыва", func() bool { return countEvents(s, "example.offline") == 5 })
	eventually(t, "точки метрик за время без связи", func() bool {
		n := 0
		for _, p := range s.server.Metrics(a.ID) {
			if p.CollectedAt > from && p.CollectedAt < to && len(p.Workers["w"]) > 0 {
				n++
			}
		}
		return n >= 2
	})
	time.Sleep(time.Second)
	if n := countEvents(s, "example.offline"); n != 5 {
		t.Fatalf("событий %d после досылки, нужно 5 (без повторов)", n)
	}
}

// countEvents — событий типа typ у агента стенда.
func countEvents(s *stand, typ string) int {
	n := 0
	for _, a := range s.server.Agents() {
		for _, e := range a.Events {
			if e.Type == typ {
				n++
			}
		}
	}
	return n
}

// ─── прокси и TLS ──────────────────────────────────────────────────────

// connectProxy — HTTP-прокси, умеющий только CONNECT: имя example.com разрешает сам (в
// 127.0.0.1) — агенту оно недоступно, связь возможна только через прокси. hits — туннели.
func connectProxy(t *testing.T) (proxyURL string, hits *atomic.Int64) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	hits = &atomic.Int64{}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				br := bufio.NewReader(c)
				req, err := http.ReadRequest(br)
				if err != nil {
					return
				}
				host, port, _ := net.SplitHostPort(req.Host)
				if req.Method != http.MethodConnect || host != "example.com" {
					_, _ = io.WriteString(c, "HTTP/1.1 405 Method Not Allowed\r\nContent-Length: 0\r\n\r\n")
					return
				}
				up, err := net.Dial("tcp", net.JoinHostPort("127.0.0.1", port))
				if err != nil {
					_, _ = io.WriteString(c, "HTTP/1.1 502 Bad Gateway\r\nContent-Length: 0\r\n\r\n")
					return
				}
				defer up.Close()
				hits.Add(1)
				_, _ = io.WriteString(c, "HTTP/1.1 200 Connection Established\r\n\r\n")
				done := make(chan struct{}, 2)
				go func() { _, _ = io.Copy(up, br); done <- struct{}{} }()
				go func() { _, _ = io.Copy(c, up); done <- struct{}{} }()
				<-done
			}()
		}
	}()
	return "http://" + ln.Addr().String(), hits
}

// Прокси из окружения (HTTPS_PROXY): регистрация и WebSocket идут через CONNECT-туннель.
// Окружение прокси программа читает один раз, поэтому тест идёт в отдельном процессе
// тестового бинаря.
func TestProxyFromEnvironment(t *testing.T) {
	if os.Getenv("IT_PROXY_TEST") != "1" {
		t.Parallel()
		exe, err := os.Executable()
		if err != nil {
			t.Fatal(err)
		}
		args := []string{"-test.run=^TestProxyFromEnvironment$", "-test.count=1"}
		if testing.Verbose() {
			args = append(args, "-test.v")
		}
		cmd := exec.Command(exe, args...)
		cmd.Env = append(os.Environ(), "IT_PROXY_TEST=1")
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("%v\n%s", err, out)
		}
		if strings.Contains(string(out), "--- SKIP") {
			t.Skipf("%s", out)
		}
		return
	}
	proxy, hits := connectProxy(t)
	for _, n := range []string{"HTTP_PROXY", "http_proxy", "https_proxy", "NO_PROXY", "no_proxy"} {
		t.Setenv(n, "")
	}
	t.Setenv("HTTPS_PROXY", proxy)

	s := newUnstartedStand(t)
	s.http.StartTLS()
	_, port, _ := net.SplitHostPort(strings.TrimPrefix(s.http.URL, "https://"))
	cfg := s.config()
	cfg.Server.URL = "https://example.com:" + port // сертификат httptest выписан и на example.com
	cfg.Server.CAFile = serverCA(t, s, t.TempDir())
	s.start(cfg)
	s.online()
	if hits.Load() == 0 {
		t.Fatal("через прокси ничего не прошло")
	}
}

// testPKI — свой CA и клиентский сертификат агента, выпущенный им (PEM-файлы в dir).
func testPKI(t *testing.T, dir string) (certFile, keyFile string, pool *x509.CertPool) {
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
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: agentName},
		NotBefore: time.Now().Add(-time.Hour), NotAfter: time.Now().Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
	}
	der, err := x509.CreateCertificate(rand.Reader, tpl, caTpl, &key.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, _ := x509.MarshalECPrivateKey(key)
	certFile, keyFile = filepath.Join(dir, "cert.pem"), filepath.Join(dir, "key.pem")
	writeFile(t, certFile, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der})))
	writeFile(t, keyFile, string(pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER})))
	return certFile, keyFile, pool
}

// serverCA — сертификат тестового TLS-сервера в PEM (его «свой CA»).
func serverCA(t *testing.T, s *stand, dir string) string {
	path := filepath.Join(dir, "server-ca.pem")
	writeFile(t, path, string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.http.Certificate().Raw})))
	return path
}

// noAgentFor — за d ни один агент не зарегистрировался.
func noAgentFor(t *testing.T, s *stand, d time.Duration) {
	t.Helper()
	time.Sleep(d)
	if list := s.server.Agents(); len(list) != 0 {
		t.Fatalf("агент не должен был подключиться: %d", len(list))
	}
}

// Сервер со своим CA: без server.caFile агент ему не доверяет; с caFile — работает.
func TestTLSCustomCA(t *testing.T) {
	t.Parallel()
	s := newUnstartedStand(t)
	s.http.StartTLS()
	untrusted := s.start(s.config())
	noAgentFor(t, s, time.Second)
	untrusted.stop()

	cfg := s.config()
	cfg.Server.CAFile = serverCA(t, s, t.TempDir())
	s.start(cfg)
	s.online()
}

// mTLS: сервер требует клиентский сертификат от своего CA.
func TestTLSClientCertificate(t *testing.T) {
	t.Parallel()
	s := newUnstartedStand(t)
	certFile, keyFile, pool := testPKI(t, t.TempDir())
	s.http.TLS = &tls.Config{ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: pool}
	s.http.StartTLS()
	ca := serverCA(t, s, t.TempDir())

	noCert := s.config()
	noCert.Server.CAFile = ca
	n := s.start(noCert)
	noAgentFor(t, s, time.Second)
	n.stop()

	cfg := s.config()
	cfg.Server.CAFile, cfg.Server.CertFile, cfg.Server.KeyFile = ca, certFile, keyFile
	s.start(cfg)
	s.online()
}
