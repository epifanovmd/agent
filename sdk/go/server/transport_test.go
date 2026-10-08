package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/coder/websocket"

	"github.com/epifanovmd/agent/sdk/go/message"
)

type stand struct {
	t      *testing.T
	agents *Agents
	srv    *httptest.Server
}

func newStand(t *testing.T) *stand {
	agents := newTestAgents(t, Options{EnrollToken: "tok", StatusInterval: time.Second})
	srv := httptest.NewServer(agents.Handler())
	t.Cleanup(srv.Close)
	return &stand{t: t, agents: agents, srv: srv}
}

func (s *stand) post(path, auth string, body any) (*http.Response, []byte) {
	s.t.Helper()
	raw, _ := json.Marshal(body)
	req, _ := http.NewRequest(http.MethodPost, s.srv.URL+path, bytes.NewReader(raw))
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		s.t.Fatal(err)
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(resp.Body)
	return resp, data
}

func (s *stand) enroll(name string) (id, auth string) {
	s.t.Helper()
	resp, data := s.post(message.EnrollPath, "", map[string]any{"token": "tok", "name": name})
	var creds struct{ AgentID, Secret string }
	if resp.StatusCode != http.StatusCreated || json.Unmarshal(data, &creds) != nil {
		s.t.Fatalf("регистрация: %d %s", resp.StatusCode, data)
	}
	return creds.AgentID, "Agent " + creds.AgentID + "." + creds.Secret
}

// wsAgent — фейковый агент по WebSocket.
type wsAgent struct {
	t    *testing.T
	conn *websocket.Conn
	in   chan message.Envelope
	done chan error
}

func (s *stand) dial(auth string) *wsAgent {
	s.t.Helper()
	url := "ws" + strings.TrimPrefix(s.srv.URL, "http") + message.LinkPath
	conn, _, err := websocket.Dial(context.Background(), url, &websocket.DialOptions{
		Subprotocols: []string{message.WSChannel},
		HTTPHeader:   http.Header{"Authorization": {auth}},
	})
	if err != nil {
		s.t.Fatal(err)
	}
	a := &wsAgent{t: s.t, conn: conn, in: make(chan message.Envelope, 256), done: make(chan error, 1)}
	go func() {
		for {
			_, raw, err := conn.Read(context.Background())
			if err != nil {
				a.done <- err
				close(a.in)
				return
			}
			var env message.Envelope
			_ = json.Unmarshal(raw, &env)
			a.in <- env
		}
	}()
	s.t.Cleanup(func() { _ = conn.CloseNow() })
	return a
}

func (a *wsAgent) send(env message.Envelope) {
	a.t.Helper()
	raw, _ := json.Marshal(env)
	if err := a.conn.Write(context.Background(), websocket.MessageText, raw); err != nil {
		a.t.Fatal(err)
	}
}

// expect — следующее сообщение типа typ (ack пропускаются, если ждём не ack).
func (a *wsAgent) expect(typ string, v any) message.Envelope {
	a.t.Helper()
	timeout := time.After(5 * time.Second)
	for {
		select {
		case env, ok := <-a.in:
			if !ok {
				a.t.Fatalf("соединение закрыто, ждали %s", typ)
			}
			if env.Type == message.TypeAck && typ != message.TypeAck {
				continue
			}
			if env.Type != typ {
				a.t.Fatalf("ждали %s, пришло %s: %s", typ, env.Type, env.Data)
			}
			if v != nil {
				_ = env.Decode(v)
			}
			return env
		case <-timeout:
			a.t.Fatalf("нет %s", typ)
		}
	}
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestTransportRejectsBeforeUpgrade(t *testing.T) {
	s := newStand(t)
	resp, _ := s.post(message.EnrollPath, "", map[string]any{"token": "bad", "name": "a"})
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("неверный токен: %d", resp.StatusCode)
	}
	_, auth := s.enroll("a")
	get := func(header http.Header) int {
		req, _ := http.NewRequest(http.MethodGet, s.srv.URL+message.LinkPath, nil)
		req.Header = header
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := get(http.Header{"Authorization": {auth}}); code != http.StatusUpgradeRequired {
		t.Fatalf("без канала: %d", code)
	}
	if code := get(http.Header{"Sec-Websocket-Protocol": {message.WSChannel}, "Authorization": {auth + "x"}}); code != http.StatusUnauthorized {
		t.Fatalf("неверный секрет: %d", code)
	}
	if resp, _ := s.post(message.SyncPath, "Agent x.y", map[string]any{}); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("sync без учётных данных: %d", resp.StatusCode)
	}
}

func TestWebSocketFlow(t *testing.T) {
	s := newStand(t)
	id, auth := s.enroll("ws-agent")
	if _, err := s.agents.SetState("x.kv", map[string]int{"keys": 1}, ""); err != nil {
		t.Fatal(err)
	}
	a := s.dial(auth)
	a.send(helloEnv("boot", message.Capabilities{
		Commands: &message.CommandsCapability{Names: []string{"x.echo"}},
		State:    &message.StateCapability{Domains: map[string]*int64{"x.kv": nil}},
	}))
	var welcome message.Welcome
	a.expect(message.TypeWelcome, &welcome)
	if welcome.AgentID != id || welcome.Version != 1 || welcome.Config.StatusIntervalMs != 1000 {
		t.Fatalf("welcome: %+v", welcome)
	}
	var put message.StatePut
	a.expect(message.TypeStatePut, &put)
	if put.Domain != "x.kv" || string(put.Spec) != `{"keys":1}` {
		t.Fatalf("state.put: %+v", put)
	}

	// Задача с файлами.
	a.send(status(map[string]int{"q": 1}))
	a.expect(message.TypeAck, nil)
	job, err := s.agents.Enqueue(JobRequest{Queue: "q", Data: map[string]string{"text": "hi"}, Inputs: map[string]string{"src": "вход"}, Outputs: []string{"out"}})
	if err != nil {
		t.Fatal(err)
	}
	var assign message.JobAssign
	a.expect(message.TypeJobAssign, &assign)
	if assign.JobID != job.ID || string(assign.Data) != `{"text":"hi"}` || assign.LeaseSeconds != 60 {
		t.Fatalf("job.assign: %+v", assign)
	}
	resp, err := http.Get(assign.Inputs["src"])
	if err != nil {
		t.Fatal(err)
	}
	in, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(in) != "вход" {
		t.Fatalf("входной файл: %q", in)
	}
	req, _ := http.NewRequest(http.MethodPut, assign.Outputs["out"].URL, strings.NewReader("выход"))
	if resp, err := http.DefaultClient.Do(req); err != nil || resp.StatusCode != http.StatusOK {
		t.Fatalf("загрузка: %v", err)
	}
	a.send(streamEnv(message.TypeJobAccept, assign.Ref()))
	urlsReq := message.MustNew(message.TypeJobURLs, message.JobURLsRequest{JobRef: assign.Ref(), Outputs: []string{"out"}})
	urlsReq.ID = "u1"
	a.send(urlsReq)
	var urls message.JobURLs
	env := a.expect(message.TypeJobURLs, &urls)
	if env.Re != "u1" || len(urls.Inputs) != 0 || urls.Outputs["out"].URL == "" || urls.ExpiresAt == 0 {
		t.Fatalf("job.urls: %s %s", env.Re, env.Data)
	}
	complete := reliableEnv(message.TypeJobComplete, message.JobComplete{JobRef: assign.Ref(), Result: json.RawMessage(`{"ok":true}`)})
	a.send(complete)
	var ack message.Ack
	a.expect(message.TypeAck, &ack)
	if len(ack.IDs) != 1 || ack.IDs[0] != complete.ID {
		t.Fatalf("ack: %+v", ack)
	}
	if j, _ := s.agents.Job(job.ID); j.Status != JobCompleted || string(j.Result) != `{"ok":true}` {
		t.Fatalf("задача: %+v", j)
	}
	if out, _ := s.agents.files.(*MemoryFiles).Get(job.ID + "/out/out"); string(out) != "выход" {
		t.Fatalf("выходной файл: %q", out)
	}

	// Команда и ожидание итога.
	called := make(chan *Command, 1)
	go func() {
		cmd, err := s.agents.Call(context.Background(), CommandRequest{Name: "x.echo", Args: map[string]int{"n": 1}})
		if err != nil {
			t.Error(err)
		}
		called <- cmd
	}()
	var run message.CommandRun
	a.expect(message.TypeCmdRun, &run)
	if run.Name != "x.echo" || string(run.Args) != `{"n":1}` || run.TimeoutSec != 60 {
		t.Fatalf("cmd.run: %+v", run)
	}
	a.send(streamEnv(message.TypeCmdAccept, message.CommandRef{CommandID: run.CommandID}))
	a.send(streamEnv(message.TypeCmdOutput, message.CommandOutput{CommandID: run.CommandID, Chunk: "pong\n"}))
	a.send(reliableEnv(message.TypeCmdDone, message.CommandDone{CommandID: run.CommandID, OK: true, Result: json.RawMessage(`1`)}))
	select {
	case cmd := <-called:
		if cmd.Status != CommandSucceeded || cmd.Output != "pong\n" || string(cmd.Result) != "1" {
			t.Fatalf("Call: %+v", cmd)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Call не вернулся")
	}

	// Возможности после hello: ожидающая команда и новый домен доставляются.
	late, err := s.agents.Command(CommandRequest{Name: "x.late", AgentID: id})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.agents.SetState("x.new", map[string]int{}, id); err != nil {
		t.Fatal(err)
	}
	a.send(streamEnv(message.TypeCapabilities, message.Capabilities{
		Commands: &message.CommandsCapability{Names: []string{"x.late"}},
		State:    &message.StateCapability{Domains: map[string]*int64{"x.new": nil}},
	}))
	a.expect(message.TypeCmdRun, &run)
	if run.CommandID != late.ID {
		t.Fatalf("cmd.run после capabilities: %+v", run)
	}
	a.expect(message.TypeStatePut, &put)
	if put.Domain != "x.new" {
		t.Fatalf("state.put после capabilities: %+v", put)
	}
	agent, _ := s.agents.Agent(id)
	if names := agent.Capabilities.Commands.Names; len(names) != 2 {
		t.Fatalf("объединение возможностей: %v", names)
	}

	a.send(reliableEnv(message.TypeEvent, message.Event{Source: "kv", Type: "kv.applied"}))
	eventually(t, "событие", func() bool {
		events, _ := s.agents.Events(1)
		return len(events) == 1 && events[0].Type == "kv.applied"
	})

	// Вторая сессия вытесняет первую (4410).
	b := s.dial(auth)
	b.send(helloEnv("boot", message.Capabilities{}))
	b.expect(message.TypeWelcome, nil)
	select {
	case err := <-a.done:
		if websocket.CloseStatus(err) != message.CloseReplaced {
			t.Fatalf("закрытие: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("прежняя сессия не закрыта")
	}
}

func TestWebSocketHelloRequired(t *testing.T) {
	s := newStand(t)
	_, auth := s.enroll("a")
	a := s.dial(auth)
	a.send(streamEnv(message.TypeStatus, message.Status{}))
	select {
	case err := <-a.done:
		if websocket.CloseStatus(err) != message.CloseInvalid {
			t.Fatalf("закрытие: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("не закрыто")
	}
	b := s.dial(auth)
	b.send(message.MustNew(message.TypeHello, message.Hello{Versions: []int{7}}))
	select {
	case err := <-b.done:
		if websocket.CloseStatus(err) != message.CloseUnsupported {
			t.Fatalf("закрытие: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("не закрыто")
	}
}

type syncResponse struct {
	SessionID string             `json:"sessionId"`
	Messages  []message.Envelope `json:"messages"`
}

func (s *stand) sync(ctx context.Context, auth string, sessionID *string, wait int, messages ...message.Envelope) (int, syncResponse, error) {
	if messages == nil {
		messages = []message.Envelope{}
	}
	raw, _ := json.Marshal(map[string]any{"sessionId": sessionID, "messages": messages, "waitSeconds": wait})
	req, _ := http.NewRequestWithContext(ctx, http.MethodPost, s.srv.URL+message.SyncPath, bytes.NewReader(raw))
	req.Header.Set("Authorization", auth)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, syncResponse{}, err
	}
	defer resp.Body.Close()
	var out syncResponse
	data, _ := io.ReadAll(resp.Body)
	_ = json.Unmarshal(data, &out)
	return resp.StatusCode, out, nil
}

func TestHTTPSync(t *testing.T) {
	s := newStand(t)
	id, auth := s.enroll("http-agent")
	ctx := context.Background()
	code, resp, err := s.sync(ctx, auth, nil, 0, helloEnv("boot", message.Capabilities{Commands: &message.CommandsCapability{Names: []string{"x.cmd"}}}),
		status(map[string]int{"q": 1}))
	if err != nil || code != http.StatusOK || len(resp.Messages) < 2 || resp.Messages[0].Type != message.TypeWelcome {
		t.Fatalf("hello: %d %+v %v", code, resp, err)
	}
	sid := resp.SessionID
	if a, _ := s.agents.Agent(id); !a.Online || a.Transport != TransportHTTP {
		t.Fatalf("агент: %+v", a)
	}

	// Long-poll: доставка приходит, как только появилась.
	polled := make(chan syncResponse, 1)
	go func() {
		_, r, _ := s.sync(ctx, auth, &sid, 10)
		polled <- r
	}()
	time.Sleep(100 * time.Millisecond)
	job, _ := s.agents.Enqueue(JobRequest{Queue: "q"})
	select {
	case r := <-polled:
		if len(r.Messages) != 1 || r.Messages[0].Type != message.TypeJobAssign {
			t.Fatalf("long-poll: %+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("long-poll не вернулся")
	}

	// Клиент ушёл посреди ожидания — доставка не теряется.
	gone, cancel := context.WithCancel(ctx)
	go func() { _, _, _ = s.sync(gone, auth, &sid, 10) }()
	time.Sleep(100 * time.Millisecond)
	cancel()
	time.Sleep(100 * time.Millisecond)
	cmd, err := s.agents.Command(CommandRequest{Name: "x.cmd"})
	if err != nil {
		t.Fatal(err)
	}
	complete := reliableEnv(message.TypeJobComplete, message.JobComplete{JobRef: message.JobRef{JobID: job.ID}})
	_, r, _ := s.sync(ctx, auth, &sid, 0, complete)
	var run message.CommandRun
	if len(ofType(r.Messages, message.TypeCmdRun)) != 1 || ofType(r.Messages, message.TypeCmdRun)[0].Decode(&run) != nil || run.CommandID != cmd.ID {
		t.Fatalf("доставка после ушедшего клиента: %+v", r.Messages)
	}
	if acks := ofType(r.Messages, message.TypeAck); len(acks) != 1 {
		t.Fatalf("ack итога: %+v", r.Messages)
	}

	// Сессия, которой нет, — 409.
	other := "nope"
	if code, _, _ := s.sync(ctx, auth, &other, 0); code != http.StatusConflict {
		t.Fatalf("чужая сессия: %d", code)
	}
	// Без запросов дольше syncIdle — агент без связи (после отсрочки).
	s.agents.mu.Lock()
	s.agents.syncIdle = 50 * time.Millisecond
	s.agents.offlineGrace = 50 * time.Millisecond
	s.agents.mu.Unlock()
	eventually(t, "агент без связи", func() bool {
		a, _ := s.agents.Agent(id)
		return !a.Online
	})
	if code, _, _ := s.sync(ctx, auth, &sid, 0); code != http.StatusConflict {
		t.Fatalf("забытая сессия: %d", code)
	}
}

// Версия снимка монотонна и после «перезапуска» сервера (новое хранилище в
// памяти): агент получает снимок, хотя помнит версию прошлого запуска.
func TestStateVersionAfterRestart(t *testing.T) {
	first := newTestAgents(t, Options{})
	before, err := first.SetState("x.kv", map[string]int{"v": 1}, "")
	if err != nil {
		t.Fatal(err)
	}
	first.Close()
	// Перезапуск не укладывается в ту же миллисекунду (версия — от времени).
	time.Sleep(2 * time.Millisecond)

	s := newStand(t)
	_, auth := s.enroll("a")
	after, _ := s.agents.SetState("x.kv", map[string]int{"v": 2}, "")
	if after.Version <= before.Version {
		t.Fatalf("версия после перезапуска %d не новее %d", after.Version, before.Version)
	}
	a := s.dial(auth)
	applied := before.Version
	a.send(helloEnv("boot", message.Capabilities{State: &message.StateCapability{Domains: map[string]*int64{"x.kv": &applied}}}))
	a.expect(message.TypeWelcome, nil)
	var put message.StatePut
	a.expect(message.TypeStatePut, &put)
	if put.Version != after.Version {
		t.Fatalf("state.put: %+v", put)
	}
	ap := reliableEnv(message.TypeStateApplied, message.StateApplied{Domain: "x.kv", Version: put.Version, OK: true, Report: json.RawMessage(`{"keys":1}`)})
	a.send(ap)
	a.expect(message.TypeAck, nil)
	eventually(t, "state.applied", func() bool {
		list, _ := s.agents.List()
		return len(list) == 1 && list[0].StateApplied["x.kv"].Version == after.Version
	})
}

func TestCloseSendsRestart(t *testing.T) {
	s := newStand(t)
	_, auth := s.enroll("a")
	a := s.dial(auth)
	a.send(helloEnv("boot", message.Capabilities{}))
	a.expect(message.TypeWelcome, nil)
	s.agents.Close()
	select {
	case err := <-a.done:
		if websocket.CloseStatus(err) != message.CloseRestart {
			t.Fatalf("закрытие: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("не закрыто")
	}
}
