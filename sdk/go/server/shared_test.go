package server

import (
	"sync"
	"testing"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Несколько процессов бэкенда с общим Store — несколько Agents на одном
// MemoryStore.

func sharedPair(t *testing.T, opts Options) (*MemoryStore, *Agents, *Agents) {
	t.Helper()
	store := NewMemoryStore()
	opts.Store = store
	return store, newTestAgents(t, opts), newTestAgents(t, opts)
}

// Одна ждущая задача выдаётся только одному агенту, даже если оба процесса
// раздают её одновременно или по устаревшему списку.
func TestSharedQueuedJobAssignedOnce(t *testing.T) {
	store, a1, a2 := sharedPair(t, Options{})
	idA, _ := enroll(t, a1, "a")
	idB, _ := enroll(t, a2, "b")
	ssA := open(t, a1, idA, helloEnv("ba", message.Capabilities{}))
	ssB := open(t, a2, idB, helloEnv("bb", message.Capabilities{}))
	handle(a1, ssA, status(map[string]int{"q": 100}))
	handle(a2, ssB, status(map[string]int{"q": 100}))

	// Одновременная раздача.
	for i := range 20 {
		job := &Job{ID: newID(), Queue: "q", Data: []byte("{}"), Status: JobQueued, MaxAttempts: 1, LeaseSeconds: 60,
			Log: []string{}, Events: []JobEvent{}, Inputs: []string{}, Outputs: []string{}, CreatedAt: now()}
		if err := store.CreateJob(job); err != nil {
			t.Fatal(err)
		}
		var wg sync.WaitGroup
		for _, a := range []*Agents{a1, a2} {
			wg.Add(1)
			go func() {
				defer wg.Done()
				a.Refresh("")
			}()
		}
		wg.Wait()
		assigned := 0
		for _, c := range []struct {
			a  *Agents
			ss *session
		}{{a1, ssA}, {a2, ssB}} {
			for _, env := range ofType(take(c.a, c.ss), message.TypeJobAssign) {
				var as message.JobAssign
				_ = env.Decode(&as)
				if as.JobID == job.ID {
					assigned++
				}
			}
		}
		if assigned != 1 {
			t.Fatalf("прогон %d: задача выдана %d раз", i, assigned)
		}
	}

	// Раздача по устаревшему списку: задачу уже взял другой процесс.
	job, _ := a1.Enqueue(JobRequest{Queue: "q", AgentID: idA})
	stale := *job
	stale.Status, stale.AgentID = JobQueued, ""
	a2.mu.Lock()
	a2.assign(ssB, &stale)
	a2.unlock()
	if out := ofType(take(a2, ssB), message.TypeJobAssign); len(out) != 0 {
		t.Fatalf("выдана по устаревшему списку: %+v", out)
	}
	if cur, _ := store.GetJob(job.ID); cur.AgentID != idA || cur.Status != JobRunning {
		t.Fatalf("задача: %+v", cur)
	}
}

// Отзыв в одном процессе не затирается записью сообщения агента в другом:
// сессия там закрывается кодом 4401.
func TestSharedRevokeNotOverwritten(t *testing.T) {
	store, a1, a2 := sharedPair(t, Options{})
	id, _ := enroll(t, a1, "a")
	ss := open(t, a1, id, helloEnv("b", message.Capabilities{}))
	if _, err := a1.Subscribe(id, SubscribeRequest{Status: &IntervalSpec{IntervalMs: 1000}}); err != nil {
		t.Fatal(err)
	}
	if err := a2.Revoke(id); err != nil {
		t.Fatal(err)
	}
	handle(a1, ss, status(map[string]int{"q": 1}))
	handle(a1, ss, streamEnv(message.TypeInventory, message.Inventory{}))
	cur, _ := store.GetAgent(id)
	if !cur.Revoked || cur.Online || cur.Subscriptions != nil {
		t.Fatalf("отзыв затёрт: %+v", cur)
	}
	a1.mu.Lock()
	closed, code := ss.closed, ss.code
	a1.unlock()
	if !closed || code != message.CloseUnauthorized {
		t.Fatalf("сессия: closed=%v code=%d", closed, code)
	}
	if _, err := a1.Subscribe(id, SubscribeRequest{}); protoCode(err) != "AGENT_REVOKED" {
		t.Fatalf("подписка на отозванного: %v", err)
	}
}

// Повтор сообщения потока после переподключения агента к другому процессу
// (тот же запуск) не обрабатывается второй раз: только ack {seq}.
func TestSharedStreamSeqAfterReconnect(t *testing.T) {
	store, a1, a2 := sharedPair(t, Options{})
	id, _ := enroll(t, a1, "a")
	ss := open(t, a1, id, helloEnv("boot", message.Capabilities{}))
	handle(a1, ss, status(map[string]int{"q": 1}))
	job, _ := a1.Enqueue(JobRequest{Queue: "q", AgentID: id})
	take(a1, ss) // job.assign
	progress := streamEnv(message.TypeJobProgress, message.JobProgress{JobRef: job.Ref(), Log: []string{"шаг"}})
	if out := handle(a1, ss, progress); len(out) != 1 || out[0].Type != message.TypeAck {
		t.Fatalf("первый раз: %+v", out)
	}

	a1.mu.Lock()
	a1.closeSession(ss, message.CloseNormal)
	a1.unlock()
	ss2 := open(t, a2, id, helloEnv("boot", message.Capabilities{}, job.Ref()))
	out := handle(a2, ss2, progress)
	var ack message.Ack
	if len(out) != 1 || out[0].Type != message.TypeAck || out[0].Decode(&ack) != nil || ack.Seq != progress.Seq {
		t.Fatalf("повтор: %+v", out)
	}
	if cur, _ := store.GetJob(job.ID); len(cur.Log) != 1 {
		t.Fatalf("повтор обработан второй раз: %v", cur.Log)
	}

	// Новый запуск агента — seq с начала: то же сообщение — новое.
	ss3 := open(t, a2, id, helloEnv("boot2", message.Capabilities{}, job.Ref()))
	handle(a2, ss3, progress)
	if cur, _ := store.GetJob(job.ID); len(cur.Log) != 2 {
		t.Fatalf("после нового запуска: %v", cur.Log)
	}
}

// listHookStore — MemoryStore, у которого после чтения списка выполняющихся
// задач один раз срабатывает hook (так в тесте другой процесс «успевает»
// изменить запись между чтением списка и записью).
type listHookStore struct {
	*MemoryStore
	mu   sync.Mutex
	hook func()
}

func (s *listHookStore) ListJobs(f JobFilter) ([]*Job, error) {
	list, err := s.MemoryStore.ListJobs(f)
	if f.Status == JobRunning {
		s.mu.Lock()
		hook := s.hook
		s.hook = nil
		s.mu.Unlock()
		if hook != nil {
			hook()
		}
	}
	return list, err
}

// Сверка истёкшей аренды не трогает задачу, если аренду тем временем продлил
// другой процесс.
func TestSharedLeaseSweepRespectsExtension(t *testing.T) {
	mem := NewMemoryStore()
	hooked := &listHookStore{MemoryStore: mem}
	a1 := newTestAgents(t, Options{Store: mem})
	a2 := newTestAgents(t, Options{Store: hooked})
	id, _ := enroll(t, a1, "a")
	ss := open(t, a1, id, helloEnv("b", message.Capabilities{}))
	handle(a1, ss, status(map[string]int{"q": 1}))
	job, _ := a1.Enqueue(JobRequest{Queue: "q", AgentID: id, MaxAttempts: 2})
	setLease := func(until int64) {
		t.Helper()
		if _, _, err := a1.mutateJob(job.ID, func(j *Job) bool { j.LeaseUntil = until; return true }); err != nil {
			t.Fatal(err)
		}
	}
	sweep2 := func() {
		a2.mu.Lock()
		a2.sweep()
		a2.unlock()
	}
	// Сверка процесса a1 не мешает: он заблокирован.
	a1.mu.Lock()
	defer a1.unlock()
	// Аренду продлевают сразу после того, как сверка прочитала список.
	hooked.mu.Lock()
	hooked.hook = func() { setLease(now() + 60_000) }
	hooked.mu.Unlock()
	setLease(now() - 1)
	sweep2()
	cur, _ := mem.GetJob(job.ID)
	if cur.Status != JobRunning || cur.AgentID != id || cur.Attempt != job.Attempt || cur.Error != nil {
		t.Fatalf("задача тронута: %+v", cur)
	}

	// Аренда истекла и по свежей записи — попытка проваливается.
	setLease(now() - 1)
	sweep2()
	if cur, _ := mem.GetJob(job.ID); cur.Attempt != job.Attempt+1 || cur.Error == nil || cur.Error.Code != "LEASE_EXPIRED" {
		t.Fatalf("истёкшая аренда: %+v", cur)
	}
}

// Уведомления о проблемах — в записи агента: их видит и второй процесс;
// отзыв в нём заканчивает все проблемы агента.
func TestSharedAlerts(t *testing.T) {
	_, a1, a2 := sharedPair(t, Options{})
	id, _ := enroll(t, a1, "node")
	ss := open(t, a1, id, helloEnv("b", message.Capabilities{}))
	handle(a1, ss, statusWith(message.StateDegraded, "плохо"))
	list := mustAlerts(t, a2)
	expectAlerts(t, list, id, "node", alertWant{AlertDegraded, true, ""})
	if list[0].Message != "плохо" {
		t.Fatalf("текст: %q", list[0].Message)
	}
	var ended collector[Alert]
	a3 := newTestAgents(t, Options{Store: a1.store, OnAlert: ended.add})
	if err := a3.Revoke(id); err != nil {
		t.Fatal(err)
	}
	if got := ended.take(); len(got) != 1 || got[0].Active || got[0].Message != "плохо" {
		t.Fatalf("конец проблемы: %+v", got)
	}
	if list := mustAlerts(t, a1); len(list) != 0 {
		t.Fatalf("после отзыва: %+v", list)
	}
}

// Переход в offline по отсрочке после закрытия сессии не трогает агента,
// который тем временем подключился к другому процессу.
func TestSharedOfflineAfterReconnectElsewhere(t *testing.T) {
	store, a1, a2 := sharedPair(t, Options{})
	id, _ := enroll(t, a1, "a")
	ss := open(t, a1, id, helloEnv("b", message.Capabilities{}))
	a1.mu.Lock()
	a1.closeSession(ss, message.CloseNormal)
	since := a1.offline[id].since
	a1.unlock()
	time.Sleep(2 * time.Millisecond)
	open(t, a2, id, helloEnv("b", message.Capabilities{}))
	a1.mu.Lock()
	a1.setOffline(id, since)
	a1.unlock()
	if cur, _ := store.GetAgent(id); !cur.Online {
		t.Fatal("агент на связи с другим процессом снят с связи")
	}
}
