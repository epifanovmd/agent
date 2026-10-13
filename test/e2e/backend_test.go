//go:build unix && e2e

package e2e

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/sse"
	"github.com/epifanovmd/agent/protocol"
)

// backend — бэкенд агентов для тестов (docs/PROTOCOL.md, часть I): всё в памяти.
type backend struct {
	t     *testing.T
	srv   *httptest.Server
	token string

	mu     sync.Mutex
	change chan struct{}
	seq    int64
	agents map[string]*agentRec
	// down — бэкенд «недоступен»: 503 на всё.
	down bool
	// buffered — поток не отдаёт «: ok» (прокси буферизует): агент переходит на длинный опрос.
	buffered bool
	// requests — ответы на запросы воркеров по типу.
	requests map[string]protocol.RequestReply
	// files — сборки воркеров: /agent/v1/files/<имя> → файл.
	files map[string]string
	// helloStatus — состояние агента в ответе на hello ("" — active).
	helloStatus string
}

type agentRec struct {
	id, name    string
	secretHash  string
	pendingHash string
	revoked     bool
	hello       *protocol.Hello
	helloCount  int
	state       *protocol.State
	desired     protocol.Desired
	commands    []protocol.Command
	results     map[string]protocol.Result
	resultCount map[string]int
	events      []protocol.Event
	metrics     []protocol.Metrics
	logs        []protocol.LogEntry
	streams     int
	polls       int
	reqs        []protocol.Request
}

func newBackend(t *testing.T) *backend {
	b := &backend{t: t, token: "e2e-token", change: make(chan struct{}), agents: map[string]*agentRec{},
		requests: map[string]protocol.RequestReply{}, files: map[string]string{}}
	b.srv = httptest.NewServer(http.HandlerFunc(b.serve))
	t.Cleanup(func() {
		b.mu.Lock()
		b.revokeAllStreams()
		b.mu.Unlock()
		b.srv.CloseClientConnections()
		b.srv.Close()
	})
	return b
}

// changed — разбудить ждущих (под b.mu).
func (b *backend) changed() {
	close(b.change)
	b.change = make(chan struct{})
}

// revokeAllStreams — закрыть потоки (под b.mu): они ждут change.
func (b *backend) revokeAllStreams() { b.changed() }

func hash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}

func (b *backend) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func (b *backend) fail(w http.ResponseWriter, status int, code, msg string) {
	b.writeJSON(w, status, protocol.NewError(code, msg))
}

func (b *backend) serve(w http.ResponseWriter, r *http.Request) {
	path, ok := strings.CutPrefix(r.URL.Path, protocol.BackendPrefix)
	if !ok {
		http.NotFound(w, r)
		return
	}
	b.mu.Lock()
	down := b.down
	b.mu.Unlock()
	if down {
		b.fail(w, http.StatusServiceUnavailable, "DOWN", "недоступен")
		return
	}
	if file, ok := strings.CutPrefix(path, "/files/"); ok {
		b.mu.Lock()
		f := b.files[file]
		b.mu.Unlock()
		if f == "" {
			http.NotFound(w, r)
			return
		}
		http.ServeFile(w, r, f)
		return
	}
	if path == protocol.PathEnroll {
		var req protocol.Enroll
		_ = json.NewDecoder(r.Body).Decode(&req)
		if req.Token != b.token {
			b.fail(w, http.StatusUnauthorized, protocol.CodeEnrollDenied, "токен не принят")
			return
		}
		id, secret := protocol.NewID(), protocol.NewID()
		b.mu.Lock()
		b.agents[id] = &agentRec{id: id, name: req.Name, secretHash: hash(secret), desired: protocol.Desired{Rev: 1},
			results: map[string]protocol.Result{}, resultCount: map[string]int{}}
		b.changed()
		b.mu.Unlock()
		b.writeJSON(w, http.StatusOK, protocol.EnrollReply{AgentID: id, Secret: secret})
		return
	}
	a, status, code := b.authorize(r)
	if a == nil {
		b.fail(w, status, code, "вход не принят")
		return
	}
	switch path {
	case protocol.PathStream:
		b.stream(w, r, a)
		return
	case protocol.PathPoll:
		b.poll(w, r, a)
		return
	}
	raw, _ := io.ReadAll(r.Body)
	b.mu.Lock()
	defer b.mu.Unlock()
	defer b.changed()
	switch path {
	case protocol.PathHello:
		var h protocol.Hello
		_ = json.Unmarshal(raw, &h)
		a.hello = &h
		a.helloCount++
		b.writeJSON(w, http.StatusOK, protocol.HelloReply{Status: b.helloStatus, ServerTime: time.Now().UnixMilli(), StateMs: 1000, MetricsMs: 1000})
	case protocol.PathResults:
		var items []protocol.Result
		_ = json.Unmarshal(raw, &items)
		var acc protocol.Accepted
		for _, r := range items {
			a.results[r.Re] = r
			a.resultCount[r.Re]++
			if r.OK && b.commandType(a, r.Re) == protocol.CmdAgentRotateKey {
				var rk protocol.RotateKeyResult
				_ = json.Unmarshal(r.Result, &rk)
				a.pendingHash = rk.SecretHash
			}
			acc.Accepted = append(acc.Accepted, r.Re)
		}
		b.writeJSON(w, http.StatusOK, acc)
	case protocol.PathEvents:
		var items []protocol.Event
		_ = json.Unmarshal(raw, &items)
		var acc protocol.Accepted
		for _, e := range items {
			if !b.hasEvent(a, e.ID) {
				a.events = append(a.events, e)
			}
			acc.Accepted = append(acc.Accepted, e.ID)
		}
		b.writeJSON(w, http.StatusOK, acc)
	case protocol.PathState:
		var st protocol.State
		_ = json.Unmarshal(raw, &st)
		a.state = &st
		b.writeJSON(w, http.StatusOK, struct{}{})
	case protocol.PathMetrics:
		var ms []protocol.Metrics
		_ = json.Unmarshal(raw, &ms)
		a.metrics = append(a.metrics, ms...)
		b.writeJSON(w, http.StatusOK, struct{}{})
	case protocol.PathLogs:
		var ls []protocol.LogEntry
		_ = json.Unmarshal(raw, &ls)
		a.logs = append(a.logs, ls...)
		b.writeJSON(w, http.StatusOK, struct{}{})
	case protocol.PathRequests:
		var req protocol.Request
		_ = json.Unmarshal(raw, &req)
		a.reqs = append(a.reqs, req)
		reply, ok := b.requests[req.Type]
		if !ok {
			reply = protocol.RequestReply{Error: protocol.NewError("UNKNOWN_REQUEST", req.Type)}
		}
		b.writeJSON(w, http.StatusOK, reply)
	default:
		http.NotFound(w, r)
	}
}

func (b *backend) commandType(a *agentRec, id string) string {
	for _, c := range a.commands {
		if c.ID == id {
			return c.Type
		}
	}
	return ""
}

func (b *backend) hasEvent(a *agentRec, id string) bool {
	for _, e := range a.events {
		if e.ID == id {
			return true
		}
	}
	return false
}

// authorize — агент по Authorization: Agent <id>.<secret>; ожидающий секрет становится
// основным при первом запросе с ним.
func (b *backend) authorize(r *http.Request) (*agentRec, int, string) {
	cred, ok := strings.CutPrefix(r.Header.Get("Authorization"), protocol.AuthAgentScheme+" ")
	id, secret, _ := strings.Cut(cred, ".")
	b.mu.Lock()
	defer b.mu.Unlock()
	a := b.agents[id]
	if !ok || a == nil {
		return nil, http.StatusUnauthorized, protocol.CodeAgentUnknown
	}
	switch h := hash(secret); {
	case a.pendingHash != "" && h == a.pendingHash:
		a.secretHash, a.pendingHash = h, ""
	case h != a.secretHash:
		return nil, http.StatusUnauthorized, protocol.CodeAgentUnknown
	}
	if a.revoked {
		return nil, http.StatusForbidden, protocol.CodeAgentRevoked
	}
	return a, 0, ""
}

func (b *backend) pending(a *agentRec, after int64) []protocol.Command {
	var out []protocol.Command
	for _, c := range a.commands {
		if _, done := a.results[c.ID]; !done && c.Seq > after {
			out = append(out, c)
		}
	}
	return out
}

func (b *backend) stream(w http.ResponseWriter, r *http.Request, a *agentRec) {
	after, _ := strconv.ParseInt(r.URL.Query().Get("after"), 10, 64)
	b.mu.Lock()
	a.streams++
	buffered := b.buffered
	b.mu.Unlock()
	if buffered {
		<-r.Context().Done()
		return
	}
	out, err := sse.NewWriter(w)
	if err != nil {
		return
	}
	var rev int64 = -1
	for {
		b.mu.Lock()
		if a.revoked || b.down {
			b.mu.Unlock()
			return
		}
		var events []sse.Event
		if a.desired.Rev != rev {
			rev = a.desired.Rev
			raw, _ := json.Marshal(a.desired)
			events = append(events, sse.Event{ID: strconv.FormatInt(rev, 10), Type: protocol.SSEDesired, Data: string(raw)})
		}
		for _, c := range b.pending(a, after) {
			raw, _ := json.Marshal(c)
			events = append(events, sse.Event{ID: fmt.Sprint("c-", c.Seq), Type: protocol.SSECommand, Data: string(raw)})
			after = c.Seq
		}
		change := b.change
		b.mu.Unlock()
		for _, e := range events {
			if out.Send(e) != nil {
				return
			}
		}
		select {
		case <-r.Context().Done():
			return
		case <-change:
		case <-time.After(protocol.StreamPing):
			if out.Comment("ping") != nil {
				return
			}
		}
	}
}

func (b *backend) poll(w http.ResponseWriter, r *http.Request, a *agentRec) {
	q := r.URL.Query()
	after, _ := strconv.ParseInt(q.Get("after"), 10, 64)
	rev, _ := strconv.ParseInt(q.Get("rev"), 10, 64)
	wait, _ := strconv.Atoi(q.Get("wait"))
	deadline := time.After(time.Duration(wait) * time.Second)
	for {
		b.mu.Lock()
		a.polls++
		cmds := b.pending(a, after)
		reply := protocol.PollReply{Commands: cmds}
		if a.desired.Rev != rev {
			d := a.desired
			reply.Desired = &d
		}
		change := b.change
		b.mu.Unlock()
		if len(cmds) > 0 || reply.Desired != nil {
			b.writeJSON(w, http.StatusOK, reply)
			return
		}
		select {
		case <-r.Context().Done():
			return
		case <-deadline:
			b.writeJSON(w, http.StatusOK, protocol.PollReply{Commands: []protocol.Command{}})
			return
		case <-change:
		}
	}
}

// ─── для тестов ────────────────────────────────────────────────────────

// agent — единственный зарегистрированный агент (ждёт регистрации).
func (b *backend) agent() *agentRec {
	b.t.Helper()
	var a *agentRec
	b.wait("регистрация агента", func() bool {
		for _, rec := range b.agents {
			a = rec
		}
		return a != nil
	})
	return a
}

// wait — дождаться условия под b.mu (не дольше 60 с).
func (b *backend) wait(what string, cond func() bool) {
	b.t.Helper()
	deadline := time.After(60 * time.Second)
	check := func() (bool, chan struct{}) {
		b.mu.Lock()
		defer b.mu.Unlock()
		return cond(), b.change
	}
	for {
		ok, change := check()
		if ok {
			return
		}
		select {
		case <-deadline:
			b.t.Fatalf("не дождались: %s", what)
		case <-change:
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// command — команда агенту a; id — новый.
func (b *backend) command(a *agentRec, typ string, args any) string {
	return b.commandID(a, protocol.NewID(), typ, args)
}

func (b *backend) commandID(a *agentRec, id, typ string, args any) string {
	raw, _ := json.Marshal(args)
	b.mu.Lock()
	defer b.mu.Unlock()
	b.seq++
	a.commands = append(a.commands, protocol.Command{Seq: b.seq, ID: id, Type: typ, Args: raw})
	b.changed()
	return id
}

// result — итог команды id (ждёт).
func (b *backend) result(a *agentRec, id string) protocol.Result {
	b.t.Helper()
	var r protocol.Result
	b.wait("итог команды "+id, func() bool {
		var ok bool
		r, ok = a.results[id]
		return ok
	})
	return r
}

// workerCommand — worker.command и его итог.
func (b *backend) workerCommand(a *agentRec, worker, typ string, args any) protocol.Result {
	b.t.Helper()
	raw, _ := json.Marshal(args)
	return b.result(a, b.command(a, protocol.CmdWorkerCommand, protocol.WorkerCommandArgs{Worker: worker, Type: typ, Args: raw}))
}

// setConfig — ключ настроек воркера в желаемом: новая версия и rev.
func (b *backend) setConfig(a *agentRec, worker, key string, data any) int64 {
	raw, _ := json.Marshal(data)
	b.mu.Lock()
	defer b.mu.Unlock()
	if a.desired.Workers == nil {
		a.desired.Workers = map[string]protocol.WorkerDesired{}
	}
	wd := a.desired.Workers[worker]
	if wd.Configs == nil {
		wd.Configs = map[string]protocol.ConfigValue{}
	}
	v := wd.Configs[key].Version + 1
	wd.Configs[key] = protocol.ConfigValue{Version: v, Data: raw}
	a.desired.Workers[worker] = wd
	a.desired.Rev++
	b.changed()
	return v
}

func (b *backend) set(f func()) {
	b.mu.Lock()
	defer b.mu.Unlock()
	f()
	b.changed()
}

// workerState — состояние воркера из последнего state (под b.mu).
func workerState(a *agentRec, name string) *protocol.WorkerState {
	if a.state == nil {
		return nil
	}
	for i := range a.state.Workers {
		if a.state.Workers[i].Name == name {
			return &a.state.Workers[i]
		}
	}
	return nil
}

// eventsOf — события типа typ (под b.mu).
func eventsOf(a *agentRec, typ string) []protocol.Event {
	var out []protocol.Event
	for _, e := range a.events {
		if e.Type == typ {
			out = append(out, e)
		}
	}
	return out
}
