package server

import (
	"errors"
	"slices"
	"testing"
	"time"
)

// Условная запись MemoryStore: пишет, только если Rev не менялся с чтения;
// успех увеличивает Rev и в хранилище, и в переданной записи; записи нет —
// ErrNotFound.
func TestMemoryStoreConditionalUpdate(t *testing.T) {
	s := NewMemoryStore()
	if err := s.CreateAgent(&Agent{ID: "a", Name: "a"}); err != nil {
		t.Fatal(err)
	}
	x, _ := s.GetAgent("a")
	y, _ := s.GetAgent("a")
	x.Name = "x"
	if ok, err := s.UpdateAgent(x); !ok || err != nil || x.Rev != 1 {
		t.Fatalf("первая запись: %v %v rev=%d", ok, err, x.Rev)
	}
	y.Name = "y"
	if ok, err := s.UpdateAgent(y); ok || err != nil || y.Rev != 0 {
		t.Fatalf("запись по устаревшей копии: %v %v rev=%d", ok, err, y.Rev)
	}
	if cur, _ := s.GetAgent("a"); cur.Name != "x" || cur.Rev != 1 {
		t.Fatalf("в хранилище: %+v", cur)
	}
	if ok, err := s.UpdateAgent(&Agent{ID: "нет"}); ok || !errors.Is(err, ErrNotFound) {
		t.Fatalf("нет записи: %v %v", ok, err)
	}

	_ = s.CreateJob(&Job{ID: "j", Status: JobQueued})
	j1, _ := s.GetJob("j")
	j2, _ := s.GetJob("j")
	j1.Status = JobRunning
	if ok, _ := s.UpdateJob(j1); !ok {
		t.Fatal("задача не записана")
	}
	if ok, err := s.UpdateJob(j2); ok || err != nil {
		t.Fatalf("задача по устаревшей копии: %v %v", ok, err)
	}
	if ok, err := s.UpdateJob(&Job{ID: "нет"}); ok || !errors.Is(err, ErrNotFound) {
		t.Fatalf("нет задачи: %v %v", ok, err)
	}

	_ = s.CreateCommand(&Command{ID: "c", Status: CommandPending})
	c1, _ := s.GetCommand("c")
	c2, _ := s.GetCommand("c")
	c1.Status = CommandRunning
	if ok, _ := s.UpdateCommand(c1); !ok {
		t.Fatal("команда не записана")
	}
	if ok, err := s.UpdateCommand(c2); ok || err != nil {
		t.Fatalf("команда по устаревшей копии: %v %v", ok, err)
	}
	if ok, err := s.UpdateCommand(&Command{ID: "нет"}); ok || !errors.Is(err, ErrNotFound) {
		t.Fatalf("нет команды: %v %v", ok, err)
	}
}

// mutate повторяет при конфликте: изменение другого процесса между чтением и
// записью не теряется.
func TestMutateRetriesOnConflict(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	calls := 0
	agent, written, err := agents.mutateAgent(id, func(ag *Agent) bool {
		calls++
		if calls == 1 { // другой процесс успел записать
			other, _ := agents.store.GetAgent(id)
			other.Revoked = true
			if ok, _ := agents.store.UpdateAgent(other); !ok {
				t.Fatal("другая запись не прошла")
			}
		}
		ag.Name = "b"
		return true
	})
	if err != nil || !written || calls != 2 || !agent.Revoked || agent.Name != "b" {
		t.Fatalf("calls=%d written=%v err=%v agent=%+v", calls, written, err, agent)
	}
}

// Постраничное чтение задач и команд: новые первыми, After — id последней
// записи прошлой страницы, Limit — размер страницы.
func TestListPages(t *testing.T) {
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	var jobIDs, cmdIDs []string
	for range 5 {
		j, err := agents.Enqueue(JobRequest{Queue: "q"})
		if err != nil {
			t.Fatal(err)
		}
		jobIDs = append(jobIDs, j.ID)
		c, err := agents.Command(CommandRequest{AgentID: id, Name: "x.run"})
		if err != nil {
			t.Fatal(err)
		}
		cmdIDs = append(cmdIDs, c.ID)
	}
	slices.Reverse(jobIDs)
	slices.Reverse(cmdIDs)
	jobs := func(f JobFilter) []string {
		list, err := agents.Jobs(f)
		if err != nil {
			t.Fatal(err)
		}
		return mapIDs(list, func(j *Job) string { return j.ID })
	}
	cmds := func(f CommandFilter) []string {
		list, err := agents.Commands(f)
		if err != nil {
			t.Fatal(err)
		}
		return mapIDs(list, func(c *Command) string { return c.ID })
	}
	for name, c := range map[string]struct{ got, want []string }{
		"jobs все":             {jobs(JobFilter{}), jobIDs},
		"jobs первая":          {jobs(JobFilter{Limit: 2}), jobIDs[:2]},
		"jobs вторая":          {jobs(JobFilter{Limit: 2, After: jobIDs[1]}), jobIDs[2:4]},
		"jobs последняя":       {jobs(JobFilter{Limit: 2, After: jobIDs[3]}), jobIDs[4:]},
		"jobs после всех":      {jobs(JobFilter{After: jobIDs[4]}), nil},
		"jobs неизвестный":     {jobs(JobFilter{After: "нет"}), nil},
		"jobs с отбором":       {jobs(JobFilter{Queue: "q", Limit: 3, After: jobIDs[0]}), jobIDs[1:4]},
		"commands первая":      {cmds(CommandFilter{Limit: 3}), cmdIDs[:3]},
		"commands вторая":      {cmds(CommandFilter{Limit: 3, After: cmdIDs[2]}), cmdIDs[3:]},
		"commands неизвестный": {cmds(CommandFilter{After: "нет"}), nil},
	} {
		if !slices.Equal(c.got, c.want) {
			t.Fatalf("%s: %v, ждали %v", name, c.got, c.want)
		}
	}
}

// Уборка: завершённые задачи и команды старше срока, события старше срока;
// незавершённые и свежие остаются; 0 — вид записей не трогать.
func TestPrune(t *testing.T) {
	store := NewMemoryStore()
	agents := newTestAgents(t, Options{Store: store})
	old := now() - 2*time.Hour.Milliseconds()
	fresh := now()
	for _, j := range []*Job{
		{ID: "old-done", Status: JobCompleted, FinishedAt: old},
		{ID: "old-failed", Status: JobFailed, FinishedAt: old},
		{ID: "fresh-done", Status: JobCompleted, FinishedAt: fresh},
		{ID: "queued", Status: JobQueued},
		{ID: "running", Status: JobRunning},
	} {
		_ = store.CreateJob(j)
	}
	for _, c := range []*Command{
		{ID: "old-ok", Status: CommandSucceeded, FinishedAt: old},
		{ID: "old-cancelled", Status: CommandCancelled, FinishedAt: old},
		{ID: "fresh-ok", Status: CommandSucceeded, FinishedAt: fresh},
		{ID: "pending", Status: CommandPending},
	} {
		_ = store.CreateCommand(c)
	}
	_ = store.AddEvent(AgentEvent{Type: "old", At: old})
	_ = store.AddEvent(AgentEvent{Type: "fresh", At: fresh})

	// Только задачи.
	if n, err := agents.Prune(PruneOptions{JobsOlderThan: time.Hour}); err != nil || n != 2 {
		t.Fatalf("задачи: %d %v", n, err)
	}
	jobs, _ := agents.Jobs(JobFilter{})
	if got := mapIDs(jobs, func(j *Job) string { return j.ID }); !slices.Equal(got, []string{"running", "queued", "fresh-done"}) {
		t.Fatalf("задачи после уборки: %v", got)
	}
	if cmds, _ := agents.Commands(CommandFilter{}); len(cmds) != 4 {
		t.Fatalf("команды тронуты: %d", len(cmds))
	}

	// Команды и события.
	if n, err := agents.Prune(PruneOptions{CommandsOlderThan: time.Hour, EventsOlderThan: time.Hour}); err != nil || n != 3 {
		t.Fatalf("команды и события: %d %v", n, err)
	}
	cmds, _ := agents.Commands(CommandFilter{})
	if got := mapIDs(cmds, func(c *Command) string { return c.ID }); !slices.Equal(got, []string{"pending", "fresh-ok"}) {
		t.Fatalf("команды после уборки: %v", got)
	}
	if events, _ := agents.Events(0); len(events) != 1 || events[0].Type != "fresh" {
		t.Fatalf("события после уборки: %+v", events)
	}
	if n, _ := agents.Prune(PruneOptions{}); n != 0 {
		t.Fatalf("без сроков удалено %d", n)
	}
}
