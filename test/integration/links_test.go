//go:build unix

package integration

import (
	"bufio"
	"encoding/json"
	"encoding/pem"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/sdk/go/message"
	"github.com/epifanovmd/agent/sdk/go/sealed"
	"github.com/epifanovmd/agent/sdk/go/server"
	"github.com/epifanovmd/agent/test/testserver"
)

// ping — команда it.ping Go-воркера выполнена: агент на связи и работает.
func (s *stand) ping() {
	s.t.Helper()
	var cmd *testserver.Command
	eventually(s.t, "агент на связи и объявил it.ping", func() bool {
		a, ok := s.server.Agent("it-agent")
		if !ok || !a.Online || a.Capabilities == nil || a.Capabilities.Commands == nil ||
			!slices.Contains(a.Capabilities.Commands.Names, "it.ping") {
			return false
		}
		c, err := s.server.Command(testserver.CommandRequest{Name: "it.ping"})
		cmd = c
		return err == nil
	})
	eventually(s.t, "команда it.ping", func() bool {
		c, _ := s.server.CommandSnapshot(cmd.ID)
		return c.Status == testserver.CommandSucceeded
	})
}

// deadURL — адрес, по которому никто не слушает (соединение отклоняется).
func deadURL(t *testing.T) string {
	dead := httptest.NewServer(http.NotFoundHandler())
	url := dead.URL
	dead.Close()
	return url
}

// Несколько адресов сервера: первый недоступен — агент регистрируется и
// работает через второй (WebSocket и HTTP sync).
func TestServerURLs(t *testing.T) {
	for _, transport := range []string{"ws", "http"} {
		t.Run(transport, func(t *testing.T) {
			s := newStand(t)
			s.agent(transport, nil, func(c *config.Config) {
				c.Server.URL = deadURL(t)
				c.Server.URLs = []string{s.http.URL}
			})
			s.ping()
			job := s.server.Enqueue(testserver.EnqueueRequest{Queue: "go.echo", Data: data(map[string]string{"text": "через второй"})})
			s.waitJob(job.ID, testserver.JobCompleted)
		})
	}
}

// logged — у сервера есть запись лога агента от source с текстом text.
func (s *stand) logged(source, text string) bool {
	return slices.ContainsFunc(s.server.Logs("it-agent"), func(e message.LogEntry) bool {
		return e.Source == source && strings.Contains(e.Msg, text)
	})
}

// Логи в бэкенд: по log.forward (info — записи агента), вывод воркера при
// пороге warn не уходит, после подписки на лог (subscription.logLevel) —
// уходит с источником-именем воркера.
func TestLogForward(t *testing.T) {
	t.Run("log.forward", func(t *testing.T) {
		s := newStand(t)
		s.agent("ws", nil, func(c *config.Config) { c.Log.Forward = "info" })
		eventually(t, "запись агента о сессии", func() bool { return s.logged("agent", "сессия открыта") })
	})

	t.Run("subscription", func(t *testing.T) {
		s := newStand(t)
		s.agent("ws", nil) // log.forward по умолчанию — warn
		s.ping()
		say := func(text string) {
			t.Helper()
			cmd, err := s.server.Command(testserver.CommandRequest{Name: "it.print", Args: data(text)})
			if err != nil {
				t.Fatal(err)
			}
			eventually(t, "it.print", func() bool {
				c, _ := s.server.CommandSnapshot(cmd.ID)
				return c.Status == testserver.CommandSucceeded
			})
		}
		say("до просьбы")
		time.Sleep(2 * time.Second) // пачки — раз в секунду
		if s.logged("go", "до просьбы") {
			t.Fatal("вывод воркера (info) ушёл при пороге warn")
		}
		a, _ := s.server.Agent("it-agent")
		if _, err := s.server.Agents().Subscribe(a.ID, server.SubscribeRequest{
			TTL: time.Minute, Logs: &server.LogsSpec{Level: message.LogInfo},
		}); err != nil {
			t.Fatal(err)
		}
		time.Sleep(300 * time.Millisecond) // config {subscription} доходит до агента
		say("после просьбы")
		eventually(t, "вывод воркера у сервера", func() bool { return s.logged("go", "после просьбы") })
	})
}

// Запечатанное значение в состоянии: на диске агента — запечатанным, воркер
// получает раскрытым; запечатанное чужим ключом — state.applied ok:false.
func TestSealedState(t *testing.T) {
	s := newStand(t)
	dir := s.agent("ws", nil)
	s.ping()
	a, _ := s.server.Agent("it-agent")
	if a.Hello == nil || a.Hello.Agent.EncryptionKey == "" {
		t.Fatalf("hello без encryptionKey: %+v", a.Hello)
	}
	secret, err := s.server.Agents().Seal(a.ID, map[string]string{"password": "очень-секретно"})
	if err != nil {
		t.Fatal(err)
	}
	version := s.server.SetState("it", data(map[string]any{"db": secret, "open": "открыто"}))
	var report struct {
		Spec json.RawMessage `json:"spec"`
	}
	eventually(t, "снимок применён", func() bool {
		a, _ := s.server.Agent("it-agent")
		ap, ok := a.StateApplied["it"]
		return ok && ap.OK && ap.Version == version && json.Unmarshal(ap.Report, &report) == nil
	})
	if want := `{"db":{"password":"очень-секретно"},"open":"открыто"}`; string(report.Spec) != want {
		t.Fatalf("воркер получил %s, ждали %s", report.Spec, want)
	}
	disk, err := os.ReadFile(filepath.Join(dir, "state", "it.json"))
	if err != nil || !strings.Contains(string(disk), sealed.Field) || strings.Contains(string(disk), "очень-секретно") {
		t.Fatalf("на диске агента: %s %v", disk, err)
	}

	other, _ := sealed.GenerateKey()
	foreign, err := sealed.Seal(sealed.EncodeKey(other.PublicKey().Bytes()), "чужое")
	if err != nil {
		t.Fatal(err)
	}
	version = s.server.SetState("it", data(map[string]any{"db": foreign}))
	eventually(t, "ошибка раскрытия", func() bool {
		a, _ := s.server.Agent("it-agent")
		ap := a.StateApplied["it"]
		return ap.Version == version && !ap.OK && strings.Contains(ap.Error, "секрет не расшифрован")
	})
}

// connectProxy — HTTP-прокси, умеющий только CONNECT: имя example.com
// разрешает сам (в 127.0.0.1) — агенту оно недоступно, связь возможна только
// через прокси. hits — туннели к серверу.
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

// Прокси из окружения (HTTPS_PROXY): регистрация, WebSocket и HTTP sync идут
// через CONNECT-туннель.
func TestProxyFromEnvironment(t *testing.T) {
	for _, transport := range []string{"ws", "http"} {
		t.Run(transport, func(t *testing.T) {
			proxy, hits := connectProxy(t)
			for _, n := range []string{"HTTP_PROXY", "http_proxy", "https_proxy", "NO_PROXY", "no_proxy"} {
				t.Setenv(n, "")
			}
			t.Setenv("HTTPS_PROXY", proxy)

			s := newUnstartedStand(t)
			s.http.StartTLS()
			_, port, _ := net.SplitHostPort(strings.TrimPrefix(s.http.URL, "https://"))
			url := "https://example.com:" + port // сертификат httptest выписан и на example.com
			s.server.SetPublicURL(url)
			ca := filepath.Join(t.TempDir(), "ca.pem")
			if err := os.WriteFile(ca, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: s.http.Certificate().Raw}), 0o600); err != nil {
				t.Fatal(err)
			}
			s.agent(transport, nil, func(c *config.Config) {
				c.Server.URL = url
				c.Server.CAFile = ca
			})
			s.ping()
			if hits.Load() == 0 {
				t.Fatal("через прокси ничего не прошло")
			}
		})
	}
}
