package server

import (
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"maps"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/epifanovmd/agent/sdk/go/internal/examples"
	"github.com/epifanovmd/agent/sdk/go/message"
)

func quietLog() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newTestAgents(t *testing.T, opts Options) *Agents {
	t.Helper()
	if opts.EnrollToken == "" && opts.Enroll == nil {
		opts.EnrollToken = "t"
	}
	opts.Log = quietLog()
	agents := New(opts)
	t.Cleanup(agents.Close)
	return agents
}

// exampleEnv — конверт образца sdk/spec/examples по имени.
func exampleEnv(t *testing.T, name string) message.Envelope {
	t.Helper()
	var env message.Envelope
	if err := json.Unmarshal(examples.Message(t, name), &env); err != nil {
		t.Fatal(err)
	}
	return env
}

func enroll(t *testing.T, agents *Agents, name string) (id, secret string) {
	t.Helper()
	id, secret, err := agents.Enroll("t", name, nil)
	if err != nil {
		t.Fatal(err)
	}
	return id, secret
}

func helloEnv(bootID string, caps message.Capabilities, jobs ...message.JobRef) message.Envelope {
	if jobs == nil {
		jobs = []message.JobRef{}
	}
	return message.MustNew(message.TypeHello, message.Hello{
		Versions: []int{1}, Agent: message.HelloAgent{Name: "a", Version: "1", BootID: bootID},
		Capabilities: caps, Jobs: jobs,
	})
}

// open — сессия в обход транспорта (под блокировкой, как транспорт).
func open(t *testing.T, agents *Agents, agentID string, hello message.Envelope) *session {
	t.Helper()
	agents.mu.Lock()
	ss := agents.newSession(agentID, TransportWS, "http://server.example")
	agents.open(ss, hello)
	agents.unlock()
	out := take(agents, ss)
	if len(out) == 0 || out[0].Type != message.TypeWelcome {
		t.Fatalf("нет welcome: %+v", out)
	}
	return ss
}

func handle(agents *Agents, ss *session, env message.Envelope) []message.Envelope {
	agents.mu.Lock()
	agents.handle(ss, env)
	agents.unlock()
	return take(agents, ss)
}

func take(agents *Agents, ss *session) []message.Envelope {
	agents.mu.Lock()
	defer agents.mu.Unlock()
	return ss.take()
}

func ofType(out []message.Envelope, typ string) []message.Envelope {
	var res []message.Envelope
	for _, env := range out {
		if env.Type == typ {
			res = append(res, env)
		}
	}
	return res
}

var seqN int64

// streamEnv — сообщение потока со следующим seq.
func streamEnv(typ string, data any) message.Envelope {
	seqN++
	env := message.MustNew(typ, data)
	env.Seq = seqN
	return env
}

func reliableEnv(typ string, data any) message.Envelope {
	env := message.MustNew(typ, data)
	env.ID = message.NewID()
	return env
}

func status(slots map[string]int, jobs ...message.StatusJob) message.Envelope {
	if jobs == nil {
		jobs = []message.StatusJob{}
	}
	return streamEnv(message.TypeStatus, message.Status{State: message.StateIdle, Slots: slots, Jobs: jobs, Workers: []message.StatusWorker{}})
}

// Сервер принимает каждый образец агент → сервер (sdk/spec/examples):
// ни один не отклоняется как неизвестный или некорректный (§9).
func TestExamplesAccepted(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "examples")
	ss := open(t, agents, id, exampleEnv(t, "hello"))
	n := 0
	for _, ex := range examples.Between(t, examples.Agent, examples.Server) {
		if strings.HasPrefix(ex.Name, "hello") {
			continue
		}
		n++
		agents.mu.Lock()
		agents.streams[id].lastSeq = 0 // каждый образец — в обработку, не как повтор
		agents.unlock()
		for _, out := range handle(agents, ss, exampleEnv(t, ex.Name)) {
			if out.Type != message.TypeError {
				continue
			}
			var pe message.Error
			_ = out.Decode(&pe)
			// Итог задачи, которой у агента нет, — семантический отказ, а не ошибка схемы.
			if pe.Code != "JOB_LEASE_LOST" {
				t.Errorf("%s: %+v", ex.Name, pe)
			}
		}
	}
	if n < 10 {
		t.Fatalf("образцов: %d", n)
	}
	// hello.minimal тоже принимается.
	open(t, agents, id, exampleEnv(t, "hello.minimal"))
}

func TestUnknownTypeAndStreamDedup(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b1", message.Capabilities{}))
	out := handle(agents, ss, reliableEnv("example.nope", map[string]int{}))
	if len(out) != 1 || out[0].Type != message.TypeError {
		t.Fatalf("неизвестный тип: %+v", out)
	}
	var pe message.Error
	_ = out[0].Decode(&pe)
	if pe.Code != "UNKNOWN_TYPE" || ss.closed {
		t.Fatalf("UNKNOWN_TYPE без разрыва: %+v", pe)
	}
	m := streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: 1})
	handle(agents, ss, m)
	out = handle(agents, ss, m) // повтор
	var ack message.Ack
	if len(out) != 1 || out[0].Decode(&ack) != nil || ack.Seq != m.Seq {
		t.Fatalf("повтор потока — только ack: %+v", out)
	}
	ev := reliableEnv(message.TypeEvent, message.Event{Source: "kv", Type: "kv.applied"})
	handle(agents, ss, ev)
	out = handle(agents, ss, ev) // повтор надёжного — ack, без второй записи
	if len(out) != 1 || out[0].Decode(&ack) != nil || len(ack.IDs) != 1 || ack.IDs[0] != ev.ID {
		t.Fatalf("ack{ids}: %+v", out)
	}
	if events, _ := agents.Events(0); len(events) != 1 || events[0].Source != "kv" || events[0].AgentName != "a" {
		t.Fatalf("события: %+v", events)
	}
}

func TestDispatchLeastLoadedAndPinned(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id1, _ := enroll(t, agents, "a1")
	id2, _ := enroll(t, agents, "a2")
	s1 := open(t, agents, id1, helloEnv("b1", message.Capabilities{}))
	s2 := open(t, agents, id2, helloEnv("b2", message.Capabilities{}))
	handle(agents, s1, status(map[string]int{"q": 2}, message.StatusJob{JobID: "busy", Queue: "q"}))
	handle(agents, s2, status(map[string]int{"q": 2}))

	first, _ := agents.Enqueue(JobRequest{Queue: "q"})
	if first.AgentID != id2 {
		t.Fatalf("первая — менее загруженному: %s", first.AgentID)
	}
	pinned, _ := agents.Enqueue(JobRequest{Queue: "q", AgentID: id2})
	if pinned.AgentID != id2 || pinned.PinnedAgentID != id2 {
		t.Fatalf("закреплённая: %+v", pinned)
	}
	// Слоты a2 заняты выданными: закреплённая ждёт, незакреплённая — к a1.
	waiting, _ := agents.Enqueue(JobRequest{Queue: "q", AgentID: id2})
	free, _ := agents.Enqueue(JobRequest{Queue: "q"})
	if waiting.Status != JobQueued || free.AgentID != id1 {
		t.Fatalf("закреплённая ждёт своего агента: %s/%s, свободная: %s", waiting.Status, waiting.AgentID, free.AgentID)
	}
	if n := len(ofType(take(agents, s2), message.TypeJobAssign)); n != 2 {
		t.Fatalf("a2 получил %d", n)
	}
	// a2 завершил первую — закреплённая уходит ему.
	handle(agents, s2, reliableEnv(message.TypeJobComplete, message.JobComplete{JobRef: first.Ref()}))
	if j, _ := agents.Job(waiting.ID); j.AgentID != id2 {
		t.Fatalf("закреплённая после освобождения: %+v", j)
	}
}

func TestLeaseExpiredAndCommandTimeout(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{Commands: &message.CommandsCapability{Names: []string{"x.run"}}}))
	handle(agents, ss, status(map[string]int{"q": 1}))
	job, _ := agents.Enqueue(JobRequest{Queue: "q", MaxAttempts: 2})
	cmd, err := agents.Command(CommandRequest{Name: "x.run", TimeoutSec: 1})
	if err != nil {
		t.Fatal(err)
	}

	agents.mu.Lock()
	j, _ := agents.store.GetJob(job.ID)
	j.LeaseUntil = now() - 1
	_ = agents.store.UpdateJob(j)
	c, _ := agents.store.GetCommand(cmd.ID)
	c.CreatedAt -= 20_000
	_ = agents.store.UpdateCommand(c)
	agents.sweep()
	agents.unlock()

	j, _ = agents.Job(job.ID)
	if j.Attempt != 1 || j.Error == nil || j.Error.Code != "LEASE_EXPIRED" {
		t.Fatalf("аренда: %+v", j)
	}
	if len(ofType(take(agents, ss), message.TypeJobCancel)) != 1 {
		t.Fatal("агенту на связи — job.cancel истёкшей попытки")
	}
	c, _ = agents.CommandByID(cmd.ID)
	if c.Status != CommandFailed || c.Error == nil || c.Error.Code != "TIMEOUT" {
		t.Fatalf("срок команды: %+v", c)
	}
}

func TestReconcileOnHello(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b1", message.Capabilities{}))
	handle(agents, ss, status(map[string]int{"q": 3}))
	kept, _ := agents.Enqueue(JobRequest{Queue: "q"})
	unseen, _ := agents.Enqueue(JobRequest{Queue: "q"})
	lost, _ := agents.Enqueue(JobRequest{Queue: "q", MaxAttempts: 2})
	for _, j := range []*Job{kept, lost} {
		handle(agents, ss, streamEnv(message.TypeJobAccept, j.Ref()))
	}

	// Перезапуск агента: держит kept и чужую задачу.
	stray := message.JobRef{JobID: "stray", Attempt: 0}
	ss2 := agents.newSession(id, TransportWS, "http://server.example")
	agents.mu.Lock()
	agents.open(ss2, helloEnv("b2", message.Capabilities{}, kept.Ref(), stray))
	agents.unlock()
	out := take(agents, ss2)
	if !ss.closed || ss.code != message.CloseReplaced {
		t.Fatalf("прежняя сессия — 4410: %v %d", ss.closed, ss.code)
	}
	assigns := ofType(out, message.TypeJobAssign)
	var a message.JobAssign
	if len(assigns) != 1 || assigns[0].Decode(&a) != nil || a.JobID != unseen.ID {
		t.Fatalf("неподтверждённая — заново: %+v", out)
	}
	cancels := ofType(out, message.TypeJobCancel)
	var ref message.JobRef
	if len(cancels) != 1 || cancels[0].Decode(&ref) != nil || ref != stray {
		t.Fatalf("чужая — job.cancel: %+v", out)
	}
	if j, _ := agents.Job(lost.ID); j.Attempt != 1 || j.Error.Code != "AGENT_LOST" {
		t.Fatalf("потерянная: %+v", j)
	}
	if j, _ := agents.Job(kept.ID); j.Status != JobRunning || j.AgentID != id {
		t.Fatalf("перечисленная продолжается: %+v", j)
	}
}

func TestStatePerAgentAndMonotonic(t *testing.T) {
	store := NewMemoryStore()
	agents := newTestAgents(t, Options{Store: store})
	id1, _ := enroll(t, agents, "a1")
	id2, _ := enroll(t, agents, "a2")
	caps := message.Capabilities{State: &message.StateCapability{Domains: map[string]*int64{"d": nil}}}
	s1 := open(t, agents, id1, helloEnv("b1", caps))
	s2 := open(t, agents, id2, helloEnv("b2", caps))

	general, _ := agents.SetState("d", map[string]int{"v": 1}, "")
	own, _ := agents.SetState("d", map[string]int{"v": 2}, id2)
	if own.Version <= general.Version {
		t.Fatalf("версия не растёт: %d → %d", general.Version, own.Version)
	}
	puts := func(ss *session) []message.StatePut {
		var out []message.StatePut
		for _, env := range ofType(take(agents, ss), message.TypeStatePut) {
			var p message.StatePut
			_ = env.Decode(&p)
			out = append(out, p)
		}
		return out
	}
	if p := puts(s1); len(p) != 1 || string(p[0].Spec) != `{"v":1}` {
		t.Fatalf("a1 — общий снимок: %+v", p)
	}
	if p := puts(s2); len(p) != 2 || string(p[1].Spec) != `{"v":2}` {
		t.Fatalf("a2 — свой снимок: %+v", p)
	}
	// Общий не перекрывает свой у a2.
	agents.SetState("d", map[string]int{"v": 3}, "")
	if p := puts(s2); len(p) != 0 {
		t.Fatalf("a2 получил общий поверх своего: %+v", p)
	}
	// Версия уже применена агентом (hello) — повторно не шлётся.
	v := own.Version + 1_000_000
	agents.mu.Lock()
	ss := agents.newSession(id2, TransportWS, "")
	agents.open(ss, helloEnv("b3", message.Capabilities{State: &message.StateCapability{Domains: map[string]*int64{"d": &v}}}))
	agents.unlock()
	if len(ofType(take(agents, ss), message.TypeStatePut)) != 0 {
		t.Fatal("снимок не новее применённого")
	}
}

// Удаление личного снимка возвращает агенту общий с версией новее личной;
// удаление общего ничего не шлёт и личных не трогает.
func TestDeleteState(t *testing.T) {
	var mu sync.Mutex
	var changes []Change
	var deleted []string
	agents := newTestAgents(t, Options{OnChange: func(c Change) {
		mu.Lock()
		changes = append(changes, c)
		mu.Unlock()
	}, OnStateDeleted: func(domain, agentID string) {
		mu.Lock()
		deleted = append(deleted, domain+"/"+agentID)
		changes = append(changes, Change{Kind: "deleted", ID: domain})
		mu.Unlock()
	}})
	id1, _ := enroll(t, agents, "a1")
	id2, _ := enroll(t, agents, "a2")
	caps := message.Capabilities{State: &message.StateCapability{Domains: map[string]*int64{"d": nil, "e": nil}}}
	s1 := open(t, agents, id1, helloEnv("b1", caps))
	s2 := open(t, agents, id2, helloEnv("b2", caps))
	puts := func(ss *session) []message.StatePut {
		var out []message.StatePut
		for _, env := range ofType(take(agents, ss), message.TypeStatePut) {
			var p message.StatePut
			_ = env.Decode(&p)
			out = append(out, p)
		}
		return out
	}

	if _, err := agents.DeleteState("", id2); err == nil {
		t.Fatal("пустой domain принят")
	}
	general, _ := agents.SetState("d", map[string]int{"v": 1}, "")
	own, _ := agents.SetState("d", map[string]int{"v": 2}, id2)
	puts(s1)
	if p := puts(s2); len(p) != 2 || p[1].Version != own.Version {
		t.Fatalf("a2 — свой снимок: %+v", p)
	}
	mu.Lock()
	changes = nil
	mu.Unlock()

	st, err := agents.DeleteState("d", id2)
	if err != nil || st == nil || st.AgentID != "" || string(st.Spec) != `{"v":1}` || st.Version <= own.Version {
		t.Fatalf("общий после удаления личного: %+v, %v (личный %d, общий был %d)", st, err, own.Version, general.Version)
	}
	if p := puts(s2); len(p) != 1 || p[0].Version != st.Version || string(p[0].Spec) != `{"v":1}` {
		t.Fatalf("a2 — общий новее личного: %+v", p)
	}
	if p := puts(s1); len(p) != 1 || p[0].Version != st.Version {
		t.Fatalf("a1 — тот же общий повторно: %+v", p)
	}
	mu.Lock()
	// Удаление — раньше уведомления о переизданном общем.
	if len(changes) != 2 || changes[0] != (Change{Kind: "deleted", ID: "d"}) || changes[1] != (Change{Kind: ChangeState, ID: "d"}) {
		t.Fatalf("уведомления: %+v", changes)
	}
	mu.Unlock()
	// Агента нет — не ошибка.
	if _, err := agents.DeleteState("d", "missing"); err != nil {
		t.Fatalf("несуществующий агент: %v", err)
	}
	// Личного уже нет — не ошибка, общий не переиздаётся.
	if again, err := agents.DeleteState("d", id2); err != nil || again == nil || again.Version != st.Version {
		t.Fatalf("повторное удаление: %+v, %v", again, err)
	}
	if p := puts(s2); len(p) != 0 {
		t.Fatalf("повторное удаление разослало: %+v", p)
	}

	// Без общего — агенту ничего не отправляется.
	agents.SetState("e", map[string]int{"v": 1}, id1)
	puts(s1)
	if st, err := agents.DeleteState("e", id1); err != nil || st != nil {
		t.Fatalf("без общего: %+v, %v", st, err)
	}
	if p := puts(s1); len(p) != 0 {
		t.Fatalf("без общего разослано: %+v", p)
	}

	// Удаление общего: ничего не шлётся, личные остаются.
	mine, _ := agents.SetState("d", map[string]int{"v": 5}, id1)
	puts(s1)
	puts(s2)
	if st, err := agents.DeleteState("d", ""); err != nil || st != nil {
		t.Fatalf("удаление общего: %+v, %v", st, err)
	}
	if p1, p2 := puts(s1), puts(s2); len(p1) != 0 || len(p2) != 0 {
		t.Fatalf("удаление общего разослало: %+v %+v", p1, p2)
	}
	list, _ := agents.States()
	if len(list) != 1 || list[0].AgentID != id1 || list[0].Version != mine.Version {
		t.Fatalf("снимки после удаления общего: %+v", list)
	}
	mu.Lock()
	if want := []string{"d/" + id2, "e/" + id1, "d/"}; !slices.Equal(deleted, want) {
		t.Fatalf("OnStateDeleted: %v, нужно %v", deleted, want)
	}
	mu.Unlock()
	// Счётчик версий после удаления не откатывается.
	agents.mu.Lock()
	agents.store.(*MemoryStore).versions["d"] += 1_000_000 // версия заметно впереди часов
	agents.mu.Unlock()
	last, _ := agents.States()
	agents.DeleteState("d", id1)
	next, _ := agents.SetState("d", map[string]int{"v": 6}, "")
	if next.Version <= last[0].Version+1_000_000 {
		t.Fatalf("версия откатилась после удаления: %d ≤ %d", next.Version, last[0].Version+1_000_000)
	}
}

func TestEnrollOptions(t *testing.T) {
	agents := newTestAgents(t, Options{Enroll: func(token string) (map[string]string, bool) {
		return map[string]string{"pool": token}, token == "ok"
	}})
	if _, _, err := agents.Enroll("bad", "a", nil); err == nil {
		t.Fatal("неверный токен принят")
	}
	id, secret, err := agents.Enroll("ok", "a", map[string]string{"zone": "eu"})
	if err != nil {
		t.Fatal(err)
	}
	a, _ := agents.Agent(id)
	if a.Labels["pool"] != "ok" || a.Labels["zone"] != "eu" || a.SecretHash == secret || a.SecretHash != hashSecret(secret) {
		t.Fatalf("агент: %+v", a)
	}
	raw, _ := json.Marshal(a)
	if strings.Contains(string(raw), a.SecretHash) || strings.Contains(string(raw), secret) {
		t.Fatal("секрет наружу")
	}
	if _, ok := agents.authenticate("Agent " + id + "." + secret); !ok {
		t.Fatal("учётные данные не приняты")
	}
	if _, ok := agents.authenticate("Agent " + id + ".nope"); ok {
		t.Fatal("неверный секрет принят")
	}
}

func TestOnChange(t *testing.T) {
	var mu sync.Mutex
	var changes []Change
	var agents *Agents
	agents = newTestAgents(t, Options{OnChange: func(c Change) {
		mu.Lock()
		changes = append(changes, c)
		mu.Unlock()
		if c.Kind == ChangeJob {
			_, _ = agents.Job(c.ID) // из уведомления можно читать Agents
		}
	}})
	job, _ := agents.Enqueue(JobRequest{Queue: "q"})
	mu.Lock()
	defer mu.Unlock()
	if len(changes) == 0 || changes[0] != (Change{Kind: ChangeJob, ID: job.ID}) {
		t.Fatalf("уведомления: %+v", changes)
	}
}

// Событие: change {event, id сообщения} без change agent; Events — новые первыми.
// state.applied: change agent и change state (раздел).
func TestEventAndStateAppliedChanges(t *testing.T) {
	var mu sync.Mutex
	var changes []Change
	agents := newTestAgents(t, Options{OnChange: func(c Change) {
		mu.Lock()
		changes = append(changes, c)
		mu.Unlock()
	}})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b1", message.Capabilities{}))
	drain := func() []Change {
		mu.Lock()
		defer mu.Unlock()
		out := changes
		changes = nil
		return out
	}
	drain()
	first := reliableEnv(message.TypeEvent, message.Event{Source: "kv", Type: "kv.first"})
	handle(agents, ss, first)
	if got := drain(); len(got) != 1 || got[0] != (Change{Kind: ChangeEvent, ID: first.ID}) {
		t.Fatalf("событие — change {event, id сообщения}: %+v", got)
	}
	second := reliableEnv(message.TypeEvent, message.Event{Source: "kv", Type: "kv.second"})
	handle(agents, ss, second)
	drain()
	events, _ := agents.Events(0)
	if len(events) != 2 || events[0].Type != "kv.second" || events[1].Type != "kv.first" {
		t.Fatalf("Events — новые первыми: %+v", events)
	}
	if last, _ := agents.Events(1); len(last) != 1 || last[0].Type != "kv.second" {
		t.Fatalf("Events(1) — последнее: %+v", last)
	}

	handle(agents, ss, reliableEnv(message.TypeStateApplied, message.StateApplied{Domain: "d", Version: 1, OK: true}))
	got := drain()
	want := map[Change]bool{{Kind: ChangeAgent, ID: id}: true, {Kind: ChangeState, ID: "d"}: true}
	if len(got) != 2 || !want[got[0]] || !want[got[1]] || got[0] == got[1] {
		t.Fatalf("state.applied — change agent и state: %+v", got)
	}
}

func TestCommandNotSupported(t *testing.T) {
	agents := newTestAgents(t, Options{})
	_, err := agents.Command(CommandRequest{Name: "x.nope"})
	var pe *message.Error
	if err == nil || !errors.As(err, &pe) || pe.Code != "COMMAND_NOT_SUPPORTED" {
		t.Fatalf("ошибка: %v", err)
	}
	if _, err := agents.Command(CommandRequest{Name: "x", AgentID: "missing"}); err == nil {
		t.Fatal("несуществующий агент")
	}
}

func TestInvalidNames(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	bad := []string{"-x", "a b", "пример", strings.Repeat("a", 65)}
	for _, name := range bad {
		checks := map[string]error{}
		_, checks["SetState"] = agents.SetState(name, map[string]any{}, "")
		_, checks["DeleteState"] = agents.DeleteState(name, "")
		_, checks["Enqueue"] = agents.Enqueue(JobRequest{Queue: name})
		_, checks["Command"] = agents.Command(CommandRequest{Name: name, AgentID: id})
		_, checks["Call"] = agents.Call(t.Context(), CommandRequest{Name: name, AgentID: id})
		for fn, err := range checks {
			var e *message.Error
			if !errors.As(err, &e) || e.Code != "MESSAGE_INVALID" {
				t.Errorf("%s(%q): ждали MESSAGE_INVALID, получили %v", fn, name, err)
			}
		}
	}
	if _, err := agents.Enqueue(JobRequest{Queue: "example.ok_1-2"}); err != nil {
		t.Fatalf("верное имя: %v", err)
	}
}

func TestSetStateWarnsUndeclaredDomain(t *testing.T) {
	var mu sync.Mutex
	var buf strings.Builder
	logs := func() string {
		mu.Lock()
		defer mu.Unlock()
		s := buf.String()
		buf.Reset()
		return s
	}
	agents := New(Options{EnrollToken: "t", Log: slog.New(slog.NewTextHandler(lockedWriter{&mu, &buf}, nil))})
	t.Cleanup(agents.Close)
	a, _ := enroll(t, agents, "a")
	b, _ := enroll(t, agents, "b")
	open(t, agents, a, helloEnv("boot", message.Capabilities{
		State: &message.StateCapability{Domains: map[string]*int64{"example.app": nil}},
	}))
	logs()

	const warn = "раздел состояния"
	cases := []struct {
		domain, agentID string
		warn            bool
	}{
		{"example.app", "", false},
		{"example.app", a, false},
		{"example.other", "", true},
		{"example.app", b, true},
	}
	for _, c := range cases {
		if _, err := agents.SetState(c.domain, map[string]any{}, c.agentID); err != nil {
			t.Fatal(err)
		}
		got := logs()
		if strings.Contains(got, warn) != c.warn {
			t.Errorf("SetState(%s, %q): предупреждение %v, лог:\n%s", c.domain, c.agentID, c.warn, got)
		}
		if c.warn && (!strings.Contains(got, "domain="+c.domain) || (c.agentID != "" && !strings.Contains(got, "agentId="+c.agentID))) {
			t.Errorf("в предупреждении нет domain/agentId: %s", got)
		}
	}

	// Агент без связи: объявление помнится по сохранённым capabilities.
	agents.mu.Lock()
	agents.sessions = map[string]*session{}
	agents.mu.Unlock()
	if _, err := agents.SetState("example.app", map[string]any{}, ""); err != nil {
		t.Fatal(err)
	}
	if got := logs(); strings.Contains(got, warn) {
		t.Errorf("объявление из Store не учтено: %s", got)
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// Метки следуют за hello, выданные при регистрации — главнее.
func TestHelloLabels(t *testing.T) {
	var mu sync.Mutex
	var changes []Change
	agents := newTestAgents(t, Options{
		Enroll: func(string) (map[string]string, bool) { return map[string]string{"nodeId": "n1"}, true },
		OnChange: func(c Change) {
			mu.Lock()
			changes = append(changes, c)
			mu.Unlock()
		},
	})
	id, _, err := agents.Enroll("t", "a", map[string]string{"zone": "eu", "nodeId": "x"})
	if err != nil {
		t.Fatal(err)
	}
	hello := func(labels map[string]string) map[string]string {
		t.Helper()
		env := helloEnv("b1", message.Capabilities{})
		var h message.Hello
		_ = env.Decode(&h)
		h.Labels = labels
		open(t, agents, id, message.MustNew(message.TypeHello, h))
		a, _ := agents.Agent(id)
		return a.Labels
	}
	got := hello(map[string]string{"zone": "us", "disk": "ssd", "nodeId": "n2"})
	if want := map[string]string{"zone": "us", "disk": "ssd", "nodeId": "n1"}; !maps.Equal(got, want) {
		t.Fatalf("метки: %v, нужно %v", got, want)
	}
	// Метки, которых нет в hello, уходят; выданная остаётся.
	got = hello(map[string]string{"zone": "us"})
	if want := map[string]string{"zone": "us", "nodeId": "n1"}; !maps.Equal(got, want) {
		t.Fatalf("метки: %v, нужно %v", got, want)
	}
	mu.Lock()
	if !slices.Contains(changes, Change{Kind: ChangeAgent, ID: id}) {
		t.Fatalf("нет уведомления: %v", changes)
	}
	mu.Unlock()
	a, _ := agents.Agent(id)
	if raw, _ := json.Marshal(a); strings.Contains(string(raw), "grantedLabels") || strings.Contains(string(raw), "GrantedLabels") {
		t.Fatalf("служебное поле наружу: %s", raw)
	}
}

// Jobs, Commands и ListJobs, ListCommands хранилища — новые первыми; раздача
// задач и доставка команд — старые первыми.
func TestListOrderNewestFirst(t *testing.T) {
	agents := newTestAgents(t, Options{})
	caps := message.Capabilities{Commands: &message.CommandsCapability{Names: []string{"x.run"}}}
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", caps))
	agents.mu.Lock()
	agents.closeSession(ss, message.CloseNormal)
	agents.unlock()

	var jobIDs, cmdIDs []string
	for range 3 {
		j, err := agents.Enqueue(JobRequest{Queue: "q"})
		if err != nil {
			t.Fatal(err)
		}
		jobIDs = append(jobIDs, j.ID)
		c, err := agents.Command(CommandRequest{Name: "x.run", AgentID: id})
		if err != nil {
			t.Fatal(err)
		}
		cmdIDs = append(cmdIDs, c.ID)
	}
	newest := func(ids []string) []string { r := slices.Clone(ids); slices.Reverse(r); return r }
	jobs, _ := agents.Jobs()
	cmds, _ := agents.Commands()
	stJobs, _ := agents.store.ListJobs(JobFilter{})
	stCmds, _ := agents.store.ListCommands(CommandFilter{})
	for name, got := range map[string][]string{
		"Jobs":         mapIDs(jobs, func(j *Job) string { return j.ID }),
		"ListJobs":     mapIDs(stJobs, func(j *Job) string { return j.ID }),
		"Commands":     mapIDs(cmds, func(c *Command) string { return c.ID }),
		"ListCommands": mapIDs(stCmds, func(c *Command) string { return c.ID }),
	} {
		want := newest(jobIDs)
		if name == "Commands" || name == "ListCommands" {
			want = newest(cmdIDs)
		}
		if !slices.Equal(got, want) {
			t.Fatalf("%s: %v, ждали %v", name, got, want)
		}
	}

	// Агент вернулся со слотом на одну задачу: команды и задача — старые первыми.
	agents.mu.Lock()
	ss = agents.newSession(id, TransportWS, "http://server.example")
	agents.open(ss, helloEnv("b", caps))
	agents.unlock()
	var runs []string
	for _, env := range ofType(take(agents, ss), message.TypeCmdRun) {
		var run message.CommandRun
		if err := env.Decode(&run); err != nil {
			t.Fatal(err)
		}
		runs = append(runs, run.CommandID)
	}
	if !slices.Equal(runs, cmdIDs) {
		t.Fatalf("cmd.run: %v, ждали %v", runs, cmdIDs)
	}
	handle(agents, ss, status(map[string]int{"q": 1}))
	if j, _ := agents.Job(jobIDs[0]); j.Status != JobRunning {
		t.Fatalf("первой выдана не старшая задача: %+v", j)
	}
}

func mapIDs[T any](items []T, id func(T) string) []string {
	out := make([]string, 0, len(items))
	for _, it := range items {
		out = append(out, id(it))
	}
	return out
}

// Пределы MemoryStore по умолчанию — одинаковые во всех SDK.
func TestMemoryStoreDefaults(t *testing.T) {
	s := NewMemoryStore()
	if s.KeepJobs != 1000 || s.KeepCommands != 500 || s.KeepEvents != 1000 || s.KeepMetrics != 4320 || s.KeepStateHistory != 50 {
		t.Fatalf("пределы: %+v", s)
	}
}
