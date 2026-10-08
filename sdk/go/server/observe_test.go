package server

import (
	"context"
	"encoding/json"
	"errors"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Образцы inventory и metrics.backfill: inventory — в модели агента,
// досланная точка — в истории, но «текущие» метрики — из обычной точки.
func TestInventoryAndBackfillExamples(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, exampleEnv(t, "hello"))
	resetSeq(t, agents, id)
	for _, name := range []string{"inventory", "metrics", "metrics.backfill"} {
		env := exampleEnv(t, name)
		out := handle(agents, ss, env)
		var ack message.Ack
		if len(out) != 1 || out[0].Type != message.TypeAck || out[0].Decode(&ack) != nil || ack.Seq != env.Seq {
			t.Fatalf("%s: ждали ack{seq: %d}: %+v", name, env.Seq, out)
		}
	}
	a, _ := agents.Agent(id)
	if a.Inventory == nil || a.Inventory.CPU == nil || a.Inventory.CPU.Model != "AMD EPYC 7543" || a.Inventory.Ports == nil || len(a.Inventory.Ports.UDP) != 1 {
		t.Fatalf("inventory: %+v", a.Inventory)
	}
	if a.Metrics == nil || a.Metrics.Backfill || a.Metrics.Host.Conntrack == nil || *a.Metrics.Host.Conntrack != 1234 || len(a.Metrics.Host.Interfaces) != 2 {
		t.Fatalf("текущие метрики — обычная точка: %+v", a.Metrics)
	}
	points, _ := agents.Metrics(id, 0)
	if len(points) != 2 || points[0].Backfill || !points[1].Backfill {
		t.Fatalf("история: %+v", points)
	}
	// Время точек — collectedAt + clockOffsetMs (по часам сервера).
	if points[0].At != 1791380000000-120 || points[1].At != 1791380030000 {
		t.Fatalf("время точек: %d, %d", points[0].At, points[1].At)
	}
}

// Время точки — collectedAt + clockOffsetMs: переотправленная после обрыва
// точка получает верное время.
func TestMetricsClockOffset(t *testing.T) {
	agents := newTestAgents(t, Options{MetricsStoreInterval: -1})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))
	off := func(v int64) *int64 { return &v }

	// Часы агента отстают на час; точка собрана 10 с назад по часам сервера.
	srv := now()
	collected := srv - 3_600_000 - 10_000
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{
		CollectedAt: collected, ClockOffsetMs: off(3_600_000),
	}))
	// Переотправленная: смещение даёт время сбора, а не отправки.
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{
		CollectedAt: collected - 60_000, ClockOffsetMs: off(3_600_000), Backfill: true,
	}))
	// Отрицательное и нулевое смещение.
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: srv + 500, ClockOffsetMs: off(-500)}))
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: srv - 1, ClockOffsetMs: off(0)}))

	points, _ := agents.Metrics(id, 0)
	want := []int64{srv - 70_000, srv - 10_000, srv - 1, srv}
	if len(points) != len(want) {
		t.Fatalf("точек: %d", len(points))
	}
	for i, p := range points {
		if p.At != want[i] {
			t.Fatalf("точка %d: at %d, ждали %d", i, p.At, want[i])
		}
	}
	if a, _ := agents.Agent(id); a.MetricsAt != srv {
		t.Fatalf("текущие — самая поздняя обычная точка: %d", a.MetricsAt)
	}

	// Формула со смещением и без него.
	for _, c := range []struct {
		m    message.Metrics
		want int64
	}{
		{message.Metrics{CollectedAt: 1_000}, 100_000},
		{message.Metrics{}, 100_000},
		{message.Metrics{CollectedAt: 2_000, ClockOffsetMs: off(7)}, 2_007},
	} {
		if got := metricsAt(c.m, 100_000); got != c.want {
			t.Fatalf("metricsAt(%+v) = %d, ждали %d", c.m, got, c.want)
		}
	}
}

// OnMetrics — каждая сохранённая точка (и досланная), вне блокировки Agents:
// из обработчика можно вызывать методы Agents.
func TestOnMetrics(t *testing.T) {
	type got struct {
		agentID string
		p       MetricsPoint
		stored  int
	}
	var (
		mu     sync.Mutex
		calls  []got
		agents *Agents
	)
	agents = newTestAgents(t, Options{MetricsStoreInterval: -1, OnMetrics: func(agentID string, p MetricsPoint) {
		points, err := agents.Metrics(agentID, 0) // точка уже сохранена; блокировки нет
		if err != nil {
			t.Error(err)
		}
		_, _ = agents.Agent(agentID)
		mu.Lock()
		calls = append(calls, got{agentID, p, len(points)})
		mu.Unlock()
	}})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))
	off := int64(0)
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: 1_000, ClockOffsetMs: &off, Backfill: true}))
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: 2_000, ClockOffsetMs: &off}))

	mu.Lock()
	defer mu.Unlock()
	if len(calls) != 2 {
		t.Fatalf("вызовов: %d", len(calls))
	}
	if c := calls[0]; c.agentID != id || c.p.At != 1_000 || !c.p.Backfill || c.stored != 1 || c.p.Metrics.CollectedAt != 1_000 {
		t.Fatalf("досланная: %+v", c)
	}
	if c := calls[1]; c.agentID != id || c.p.At != 2_000 || c.p.Backfill || c.stored != 2 {
		t.Fatalf("обычная: %+v", c)
	}
}

func TestMetricsHistory(t *testing.T) {
	agents := newTestAgents(t, Options{MetricsStoreInterval: -1})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))
	cpu := func(v float64) *message.HostMetrics { return &message.HostMetrics{CPUPercent: &v} }

	off := func(v int64) *int64 { return &v }

	// Часы агента сдвинуты на годы: время точки — collectedAt + clockOffsetMs.
	before := now()
	const skew = 50 * 365 * 24 * 3_600_000
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: before - 5_000 - skew, ClockOffsetMs: off(skew), Host: cpu(1)}))
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: 5, Host: cpu(2)})) // без clockOffsetMs — момент получения
	after := now()
	points, _ := agents.Metrics(id, 0)
	if len(points) != 2 {
		t.Fatalf("точек: %d", len(points))
	}
	if at := points[0].At; at != before-5_000 {
		t.Fatalf("время с поправкой: %d, ждали %d", at, before-5_000)
	}
	if at := points[1].At; at < before || at > after {
		t.Fatalf("время без clockOffsetMs: %d", at)
	}
	a, _ := agents.Agent(id)
	if *a.Metrics.Host.CPUPercent != 2 {
		t.Fatalf("текущие: %+v", a.Metrics.Host)
	}

	// Досланная точка: в истории по своему времени, текущие не меняет — даже
	// если у агента ещё не было текущих.
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: before - 30_000, ClockOffsetMs: off(0), Backfill: true, Host: cpu(3)}))
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: before - 2_000, ClockOffsetMs: off(0), Backfill: true, Host: cpu(4)}))
	points, _ = agents.Metrics(id, 0)
	if len(points) != 4 {
		t.Fatalf("точек: %d", len(points))
	}
	for i := 1; i < len(points); i++ {
		if points[i].At < points[i-1].At {
			t.Fatalf("история не по возрастанию at: %+v", points)
		}
	}
	if *points[0].Metrics.Host.CPUPercent != 3 || !points[0].Backfill {
		t.Fatalf("самая старая — досланная: %+v", points[0])
	}
	if a, _ := agents.Agent(id); *a.Metrics.Host.CPUPercent != 2 {
		t.Fatalf("backfill перезаписал текущие: %+v", a.Metrics.Host)
	}
	// since — строго позже.
	later, _ := agents.Metrics(id, points[1].At)
	if len(later) != 2 || later[0].At <= points[1].At {
		t.Fatalf("since: %+v", later)
	}
	if none, err := agents.Metrics("nobody", 0); err != nil || none == nil || len(none) != 0 {
		t.Fatalf("пустая история: %v %v", none, err)
	}
}

func TestMemoryStoreKeepsLastMetrics(t *testing.T) {
	s := NewMemoryStore()
	if s.KeepMetrics != 4320 {
		t.Fatalf("KeepMetrics: %d", s.KeepMetrics)
	}
	s.KeepMetrics = 3
	for _, at := range []int64{5, 1, 4, 2, 3} {
		_ = s.AddMetrics("a", MetricsPoint{At: at})
	}
	points, _ := s.ListMetrics("a", 0)
	if len(points) != 3 || points[0].At != 3 || points[2].At != 5 {
		t.Fatalf("последние 3 по at: %+v", points)
	}
}

func TestOfflineGrace(t *testing.T) {
	agents := newTestAgents(t, Options{OfflineGrace: 300 * time.Millisecond})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))
	online := func() bool {
		a, _ := agents.Agent(id)
		return a.Online
	}

	// Обрыв и переподключение в пределах отсрочки — online не менялся.
	agents.mu.Lock()
	agents.closeSession(ss, message.CloseNormal)
	agents.unlock()
	if !online() {
		t.Fatal("offline сразу после закрытия сессии")
	}
	time.Sleep(100 * time.Millisecond)
	ss = open(t, agents, id, helloEnv("b", message.Capabilities{}))
	time.Sleep(400 * time.Millisecond)
	if !online() {
		t.Fatal("offline после переподключения в пределах отсрочки")
	}

	// Не вернулся — offline по истечении отсрочки.
	agents.mu.Lock()
	agents.closeSession(ss, message.CloseNormal)
	agents.unlock()
	if !online() {
		t.Fatal("offline до истечения отсрочки")
	}
	eventually(t, "offline", func() bool { return !online() })
}

func TestUpdateNotAvailableWithoutRelease(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	open(t, agents, id, helloEnv("b", message.Capabilities{Update: &message.UpdateCapability{Mode: "self"}}))
	if agents.Release() != nil {
		t.Fatal("выпуска нет")
	}
	if c, err := agents.UpdateCandidates(); err != nil || c == nil || len(c) != 0 {
		t.Fatalf("кандидаты без выпуска: %v %v", c, err)
	}
	var pe *message.Error
	if _, err := agents.UpdateAgent(id); !errors.As(err, &pe) || pe.Code != "UPDATE_NOT_AVAILABLE" {
		t.Fatalf("UpdateAgent: %v", err)
	}
}

// «Агент изменился» — только когда есть что обновить: повторный status (пульс)
// и досланная точка метрик уведомлений не дают.
func TestAgentChangeOnlyOnMeaningfulUpdates(t *testing.T) {
	var mu sync.Mutex
	changes := 0
	agents := newTestAgents(t, Options{OnChange: func(c Change) {
		if c.Kind == ChangeAgent {
			mu.Lock()
			changes++
			mu.Unlock()
		}
	}})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))
	count := func() int { mu.Lock(); defer mu.Unlock(); return changes }
	base := count()

	idle := message.Status{State: message.StateIdle, Slots: map[string]int{"q": 1}, Jobs: []message.StatusJob{}, Workers: []message.StatusWorker{}}
	handle(agents, ss, streamEnv(message.TypeStatus, idle))
	handle(agents, ss, streamEnv(message.TypeStatus, idle))
	if n := count() - base; n != 1 {
		t.Fatalf("два одинаковых status — одно уведомление, а не %d", n)
	}
	off := int64(0)
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: 1_000, ClockOffsetMs: &off, Backfill: true}))
	if n := count() - base; n != 1 {
		t.Fatalf("досланная точка не меняет агента: %d", n)
	}
	busy := idle
	busy.State = message.StateBusy
	handle(agents, ss, streamEnv(message.TypeStatus, busy))
	handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: 2_000, ClockOffsetMs: &off}))
	if n := count() - base; n != 3 {
		t.Fatalf("изменившийся status и обычная точка — по уведомлению: %d", n)
	}
	if a, _ := agents.Agent(id); a.LastSeenAt == 0 {
		t.Fatal("lastSeenAt сохраняется и без уведомления")
	}
}

// Несколько процессов с общим Store: команда и снимок, созданные другим процессом,
// доставляются агенту после Refresh того процесса, где его сессия.
func TestRefreshDeliversChangesFromAnotherProcess(t *testing.T) {
	shared := NewMemoryStore()
	local := newTestAgents(t, Options{Store: shared})
	remote := newTestAgents(t, Options{Store: shared})
	id, _ := enroll(t, local, "a")
	caps := message.Capabilities{
		Commands: &message.CommandsCapability{Names: []string{"x.do"}},
		State:    &message.StateCapability{Domains: map[string]*int64{"x": nil}},
	}
	ss := open(t, local, id, helloEnv("b", caps))
	if _, err := remote.Command(CommandRequest{Name: "x.do", AgentID: id}); err != nil {
		t.Fatal(err)
	}
	if _, err := remote.SetState("x", map[string]int{"n": 1}, ""); err != nil {
		t.Fatal(err)
	}
	if out := take(local, ss); len(out) != 0 {
		t.Fatalf("без Refresh процесс не знает об изменениях: %v", out)
	}
	local.Refresh(id)
	types := map[string]int{}
	for _, env := range take(local, ss) {
		types[env.Type]++
	}
	if types[message.TypeCmdRun] != 1 || types[message.TypeStatePut] != 1 {
		t.Fatalf("после Refresh: %v", types)
	}
	local.Refresh("")
	if out := take(local, ss); len(out) != 0 {
		t.Fatalf("повторный Refresh — без повторной доставки: %v", out)
	}
}

// Revoke в другом процессе: Refresh процесса, где сессия агента, закрывает
// её кодом 4401 (Refresh("") — тоже).
func TestRefreshClosesRevokedFromAnotherProcess(t *testing.T) {
	shared := NewMemoryStore()
	local := newTestAgents(t, Options{Store: shared})
	remote := newTestAgents(t, Options{Store: shared})
	id1, _ := enroll(t, local, "a")
	id2, _ := enroll(t, local, "b")
	ss1 := open(t, local, id1, helloEnv("b1", message.Capabilities{}))
	ss2 := open(t, local, id2, helloEnv("b2", message.Capabilities{}))
	if err := remote.Revoke(id1); err != nil {
		t.Fatal(err)
	}
	closed := func(ss *session) (bool, int) {
		local.mu.Lock()
		defer local.mu.Unlock()
		return ss.closed, ss.code
	}
	if c, _ := closed(ss1); c {
		t.Fatal("сессия закрыта без Refresh")
	}
	local.Refresh(id2) // другой агент — сессия отозванного не трогается
	if c, _ := closed(ss1); c {
		t.Fatal("Refresh другого агента закрыл сессию")
	}
	local.Refresh(id1)
	if c, code := closed(ss1); !c || code != message.CloseUnauthorized {
		t.Fatalf("после Refresh: closed=%v code=%d", c, code)
	}
	local.mu.Lock()
	cur := local.sessions[id1]
	local.mu.Unlock()
	if cur != nil {
		t.Fatal("сессия отозванного осталась текущей")
	}

	if err := remote.Revoke(id2); err != nil {
		t.Fatal(err)
	}
	local.Refresh("")
	if c, code := closed(ss2); !c || code != message.CloseUnauthorized {
		t.Fatalf("после Refresh(\"\"): closed=%v code=%d", c, code)
	}
	if a, _ := local.Agent(id2); !a.Revoked || a.Online {
		t.Fatalf("запись: %+v", a)
	}
}

// Прореживание: в Store — точка не раньше последней сохранённой + интервал
// (досланные — так же); OnMetrics и текущие метрики — каждая точка.
func TestMetricsStoreInterval(t *testing.T) {
	var (
		mu    sync.Mutex
		calls int
	)
	// Срок хранения выключен: точки с давним временем не должна удалить чистка при запуске.
	agents := newTestAgents(t, Options{MetricsStoreInterval: 10 * time.Second, MetricsRetention: -1, OnMetrics: func(string, MetricsPoint) {
		mu.Lock()
		calls++
		mu.Unlock()
	}})
	id, _ := enroll(t, agents, "a")
	ss := open(t, agents, id, helloEnv("b", message.Capabilities{}))
	off := int64(0)
	send := func(at int64, backfill bool) {
		handle(agents, ss, streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: at, ClockOffsetMs: &off, Backfill: backfill}))
	}
	send(1_000, false)  // первая — всегда
	send(5_000, false)  // раньше 11 000 — нет
	send(11_000, false) // ровно +10 с — да
	send(12_000, false)
	send(21_000, false)
	// Досланные (время без связи — раньше уже сохранённых живых) прореживаются
	// отдельно: иначе история без связи пропала бы целиком.
	send(2_000, true)   // первая досланная — да
	send(6_000, true)   // раньше 12 000 — нет
	send(12_000, true)  // да
	send(13_000, true)  // нет
	send(32_000, false) // живая: ≥ 21 000 + 10 с — да
	points, _ := agents.Metrics(id, 0)
	var ats []int64
	for _, p := range points {
		ats = append(ats, p.At)
	}
	if want := []int64{1_000, 2_000, 11_000, 12_000, 21_000, 32_000}; !slices.Equal(ats, want) {
		t.Fatalf("сохранены %v, ждали %v", ats, want)
	}
	mu.Lock()
	n := calls
	mu.Unlock()
	if n != 10 {
		t.Fatalf("OnMetrics — каждая точка: %d", n)
	}
	if a, _ := agents.Agent(id); a.MetricsAt != 32_000 {
		t.Fatalf("текущие — последняя точка: %d", a.MetricsAt)
	}

	// По умолчанию 15 с; отрицательное — каждая точка.
	if def := newTestAgents(t, Options{}); def.opts.MetricsStoreInterval != 15*time.Second || def.opts.MetricsRetention != 7*24*time.Hour {
		t.Fatalf("умолчания: %v %v", def.opts.MetricsStoreInterval, def.opts.MetricsRetention)
	}
	all := newTestAgents(t, Options{MetricsStoreInterval: -1, MetricsRetention: -1})
	id2, _ := enroll(t, all, "a")
	ss2 := open(t, all, id2, helloEnv("b", message.Capabilities{}))
	for _, at := range []int64{1_000, 1_001, 1_002} {
		handle(all, ss2, streamEnv(message.TypeMetrics, message.Metrics{CollectedAt: at, ClockOffsetMs: &off}))
	}
	if points, _ := all.Metrics(id2, 0); len(points) != 3 {
		t.Fatalf("каждая точка: %d", len(points))
	}
}

func TestMemoryStorePruneMetrics(t *testing.T) {
	s := NewMemoryStore()
	for _, at := range []int64{1, 2, 3} {
		_ = s.AddMetrics("a", MetricsPoint{At: at})
	}
	_ = s.AddMetrics("b", MetricsPoint{At: 2})
	_ = s.AddMetrics("c", MetricsPoint{At: 9})
	n, err := s.PruneMetrics(3)
	if err != nil || n != 3 {
		t.Fatalf("удалено %d: %v", n, err)
	}
	a, _ := s.ListMetrics("a", 0)
	b, _ := s.ListMetrics("b", 0)
	c, _ := s.ListMetrics("c", 0)
	if len(a) != 1 || a[0].At != 3 || len(b) != 0 || len(c) != 1 {
		t.Fatalf("после чистки: %v %v %v", a, b, c)
	}
}

// Срок хранения: чистка при запуске и раз в pruneInterval; отрицательный —
// хранить всегда.
func TestMetricsRetention(t *testing.T) {
	store := NewMemoryStore()
	ts := now()
	_ = store.AddMetrics("a", MetricsPoint{At: ts - 2*time.Hour.Milliseconds()})
	_ = store.AddMetrics("a", MetricsPoint{At: ts - 10*time.Minute.Milliseconds()})
	count := func() int {
		points, _ := store.ListMetrics("a", 0)
		return len(points)
	}
	agents := newTestAgents(t, Options{Store: store, MetricsRetention: time.Hour})
	eventually(t, "чистка при запуске", func() bool { return count() == 1 })

	// Следующая — не раньше pruneInterval.
	_ = store.AddMetrics("a", MetricsPoint{At: ts - 3*time.Hour.Milliseconds()})
	agents.mu.Lock()
	agents.pruneMetrics()
	agents.unlock()
	if count() != 2 {
		t.Fatalf("чистка раньше интервала: %d", count())
	}
	agents.mu.Lock()
	agents.pruneInterval = 0
	agents.pruneMetrics()
	agents.unlock()
	if count() != 1 {
		t.Fatalf("чистка по интервалу: %d", count())
	}

	keep := NewMemoryStore()
	_ = keep.AddMetrics("a", MetricsPoint{At: 1})
	forever := newTestAgents(t, Options{Store: keep, MetricsRetention: -1})
	forever.mu.Lock()
	forever.pruneInterval = 0
	forever.pruneMetrics()
	forever.unlock()
	if points, _ := keep.ListMetrics("a", 0); len(points) != 1 {
		t.Fatal("отрицательный срок — хранить всегда")
	}
}

// Call в процессе, к которому агент не подключён: итог сохраняет другой процесс
// (cmd.done приходит туда) — Call находит его в Store, а не ждёт вечно.
func TestCallAcrossProcesses(t *testing.T) {
	shared := NewMemoryStore()
	local := newTestAgents(t, Options{Store: shared})
	remote := newTestAgents(t, Options{Store: shared})
	id, _ := enroll(t, local, "a")
	caps := message.Capabilities{Commands: &message.CommandsCapability{Names: []string{"x.do"}}}
	ss := open(t, local, id, helloEnv("b", caps))

	type result struct {
		cmd *Command
		err error
	}
	got := make(chan result, 1)
	go func() {
		cmd, err := remote.Call(context.Background(), CommandRequest{Name: "x.do", AgentID: id, TimeoutSec: 30})
		got <- result{cmd, err}
	}()

	// Процесс с сессией узнаёт о команде (NOTIFY → Refresh) и доставляет её.
	var run message.CommandRun
	deadline := time.Now().Add(3 * time.Second)
	for run.CommandID == "" && time.Now().Before(deadline) {
		local.Refresh(id)
		for _, env := range take(local, ss) {
			if env.Type == message.TypeCmdRun {
				_ = env.Decode(&run)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}
	if run.CommandID == "" {
		t.Fatal("команда не доставлена")
	}
	handle(local, ss, reliableEnv(message.TypeCmdDone, message.CommandDone{CommandID: run.CommandID, OK: true, Result: json.RawMessage(`{"n":1}`)}))

	select {
	case r := <-got:
		if r.err != nil || r.cmd.Status != CommandSucceeded {
			t.Fatalf("Call: %+v %v", r.cmd, r.err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Call не увидел итог, сохранённый другим процессом")
	}
}
