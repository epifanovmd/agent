package server

import (
	"encoding/json"
	"errors"
	"slices"
	"sort"
	"sync"
	"time"
)

// ErrNotFound — записи нет.
var ErrNotFound = errors.New("server: не найдено")

// JobFilter — отбор задач; пустые поля не фильтруют.
type JobFilter struct {
	Status  string
	Queue   string
	AgentID string
}

// CommandFilter — отбор команд; пустые поля не фильтруют.
type CommandFilter struct {
	Status  string
	AgentID string
}

// Store — хранение агентов, задач, команд, состояния и событий. Agents вызывает
// его из одного процесса последовательно; объекты передаются копиями (Agents
// меняет копию и сохраняет Update*). Нет записи — ErrNotFound. Задачи и
// команды (ListJobs, ListCommands) — новые первыми; агенты и снимки — в
// порядке создания.
type Store interface {
	CreateAgent(a *Agent) error
	GetAgent(id string) (*Agent, error)
	UpdateAgent(a *Agent) error
	ListAgents() ([]*Agent, error)

	CreateJob(j *Job) error
	GetJob(id string) (*Job, error)
	UpdateJob(j *Job) error
	ListJobs(f JobFilter) ([]*Job, error)

	CreateCommand(c *Command) error
	GetCommand(id string) (*Command, error)
	UpdateCommand(c *Command) error
	ListCommands(f CommandFilter) ([]*Command, error)

	// SetState — новый снимок домена (общий при agentID == ""); actor — кто
	// задал (DesiredState.Actor, может быть пустым). Версия монотонна и между
	// перезапусками: max(прежняя + 1, now_ms). Снимок попадает и в историю
	// (ListStateHistory).
	SetState(domain, agentID string, spec json.RawMessage, actor string) (*DesiredState, error)
	GetState(domain, agentID string) (*DesiredState, error)
	// DeleteState — удалить снимок (общий при agentID == ""); true — был и
	// удалён. Счётчик версий домена сохраняется.
	DeleteState(domain, agentID string) (bool, error)
	ListStates() ([]*DesiredState, error)
	// ListStateHistory — снимки (domain, agentID) от новых к старым, не
	// больше limit (limit ≤ 0 — все хранимые). DeleteState историю не удаляет.
	ListStateHistory(domain, agentID string, limit int) ([]*DesiredState, error)

	AddEvent(e AgentEvent) error
	// ListEvents — последние limit событий (limit ≤ 0 — все), новые первыми.
	ListEvents(limit int) ([]AgentEvent, error)

	// AddMetrics — точка истории метрик агента.
	AddMetrics(agentID string, p MetricsPoint) error
	// ListMetrics — точки агента с At строго позже since, по возрастанию At.
	ListMetrics(agentID string, since int64) ([]MetricsPoint, error)
	// PruneMetrics — удалить точки всех агентов с At < before (срок хранения,
	// Options.MetricsRetention); возвращает число удалённых.
	PruneMetrics(before int64) (int, error)
}

// MemoryStore — Store в памяти процесса (разработка, один процесс). Хранит
// не больше Keep* завершённых задач и команд, KeepEvents событий,
// KeepMetrics точек метрик на агента и KeepStateHistory снимков истории на
// (domain, agentID).
type MemoryStore struct {
	KeepJobs         int
	KeepCommands     int
	KeepEvents       int
	KeepMetrics      int
	KeepStateHistory int

	mu        sync.Mutex
	agents    map[string]*Agent
	agentList []string
	jobs      map[string]*Job
	jobList   []string
	cmds      map[string]*Command
	cmdList   []string
	states    map[stateKey]*DesiredState
	stateList []stateKey
	versions  map[string]int64             // домен → наибольшая выданная версия
	history   map[stateKey][]*DesiredState // снимки по возрастанию версии
	events    []AgentEvent
	metrics   map[string][]MetricsPoint // агент → точки по возрастанию At
}

type stateKey struct{ domain, agentID string }

// NewMemoryStore — хранилище в памяти: 1000 задач, 500 команд, 1000 событий,
// 4320 точек метрик на агента, 50 снимков истории состояния на (domain, agentID).
func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		KeepJobs: 1000, KeepCommands: 500, KeepEvents: 1000, KeepMetrics: 4320, KeepStateHistory: 50,
		agents: map[string]*Agent{}, jobs: map[string]*Job{}, cmds: map[string]*Command{},
		states: map[stateKey]*DesiredState{}, versions: map[string]int64{}, history: map[stateKey][]*DesiredState{},
		metrics: map[string][]MetricsPoint{},
	}
}

func (s *MemoryStore) CreateAgent(a *Agent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.agents[a.ID]; !ok {
		s.agentList = append(s.agentList, a.ID)
	}
	s.agents[a.ID] = a.Clone()
	return nil
}

func (s *MemoryStore) GetAgent(id string) (*Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, ok := s.agents[id]
	if !ok {
		return nil, ErrNotFound
	}
	return a.Clone(), nil
}

func (s *MemoryStore) UpdateAgent(a *Agent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.agents[a.ID]; !ok {
		return ErrNotFound
	}
	s.agents[a.ID] = a.Clone()
	return nil
}

func (s *MemoryStore) ListAgents() ([]*Agent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Agent, 0, len(s.agentList))
	for _, id := range s.agentList {
		out = append(out, s.agents[id].Clone())
	}
	return out, nil
}

func (s *MemoryStore) CreateJob(j *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[j.ID]; !ok {
		s.jobList = append(s.jobList, j.ID)
	}
	s.jobs[j.ID] = j.Clone()
	s.jobList = trim(s.jobList, s.KeepJobs, func(id string) bool {
		st := s.jobs[id].Status
		if st == JobQueued || st == JobRunning {
			return false
		}
		delete(s.jobs, id)
		return true
	})
	return nil
}

func (s *MemoryStore) GetJob(id string) (*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	j, ok := s.jobs[id]
	if !ok {
		return nil, ErrNotFound
	}
	return j.Clone(), nil
}

func (s *MemoryStore) UpdateJob(j *Job) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.jobs[j.ID]; !ok {
		return ErrNotFound
	}
	s.jobs[j.ID] = j.Clone()
	return nil
}

func (s *MemoryStore) ListJobs(f JobFilter) ([]*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Job
	for _, id := range slices.Backward(s.jobList) {
		j := s.jobs[id]
		if (f.Status == "" || j.Status == f.Status) && (f.Queue == "" || j.Queue == f.Queue) &&
			(f.AgentID == "" || j.AgentID == f.AgentID) {
			out = append(out, j.Clone())
		}
	}
	return out, nil
}

func (s *MemoryStore) CreateCommand(c *Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cmds[c.ID]; !ok {
		s.cmdList = append(s.cmdList, c.ID)
	}
	s.cmds[c.ID] = c.Clone()
	s.cmdList = trim(s.cmdList, s.KeepCommands, func(id string) bool {
		if !s.cmds[id].Finished() {
			return false
		}
		delete(s.cmds, id)
		return true
	})
	return nil
}

func (s *MemoryStore) GetCommand(id string) (*Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.cmds[id]
	if !ok {
		return nil, ErrNotFound
	}
	return c.Clone(), nil
}

func (s *MemoryStore) UpdateCommand(c *Command) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cmds[c.ID]; !ok {
		return ErrNotFound
	}
	s.cmds[c.ID] = c.Clone()
	return nil
}

func (s *MemoryStore) ListCommands(f CommandFilter) ([]*Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Command
	for _, id := range slices.Backward(s.cmdList) {
		c := s.cmds[id]
		if (f.Status == "" || c.Status == f.Status) && (f.AgentID == "" || c.AgentID == f.AgentID) {
			out = append(out, c.Clone())
		}
	}
	return out, nil
}

func (s *MemoryStore) SetState(domain, agentID string, spec json.RawMessage, actor string) (*DesiredState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now().UnixMilli()
	// Версия — новее любой выданной в домене (общей и для агентов) и не меньше
	// текущего времени: состояние в памяти, агент помнит версию на диске (§6.5).
	version := max(s.versions[domain]+1, now)
	s.versions[domain] = version
	key := stateKey{domain, agentID}
	if _, ok := s.states[key]; !ok {
		s.stateList = append(s.stateList, key)
	}
	st := &DesiredState{Domain: domain, AgentID: agentID, Version: version, Spec: spec, UpdatedAt: now, Actor: actor}
	s.states[key] = st
	h := append(s.history[key], st)
	if s.KeepStateHistory > 0 && len(h) > s.KeepStateHistory {
		h = slices.Clone(h[len(h)-s.KeepStateHistory:])
	}
	s.history[key] = h
	c := *st
	return &c, nil
}

func (s *MemoryStore) ListStateHistory(domain, agentID string, limit int) ([]*DesiredState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.history[stateKey{domain, agentID}]
	out := make([]*DesiredState, 0, len(h))
	for i := len(h) - 1; i >= 0 && (limit <= 0 || len(out) < limit); i-- {
		c := *h[i]
		out = append(out, &c)
	}
	return out, nil
}

func (s *MemoryStore) GetState(domain, agentID string) (*DesiredState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	st, ok := s.states[stateKey{domain, agentID}]
	if !ok {
		return nil, ErrNotFound
	}
	c := *st
	return &c, nil
}

func (s *MemoryStore) DeleteState(domain, agentID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := stateKey{domain, agentID}
	if _, ok := s.states[key]; !ok {
		return false, nil
	}
	delete(s.states, key)
	s.stateList = slices.DeleteFunc(s.stateList, func(k stateKey) bool { return k == key })
	return true, nil
}

func (s *MemoryStore) ListStates() ([]*DesiredState, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*DesiredState, 0, len(s.stateList))
	for _, key := range s.stateList {
		c := *s.states[key]
		out = append(out, &c)
	}
	return out, nil
}

func (s *MemoryStore) AddEvent(e AgentEvent) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.events = append(s.events, e)
	if s.KeepEvents > 0 && len(s.events) > s.KeepEvents {
		s.events = slices.Clone(s.events[len(s.events)-s.KeepEvents:])
	}
	return nil
}

func (s *MemoryStore) ListEvents(limit int) ([]AgentEvent, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	events := s.events
	if limit > 0 && len(events) > limit {
		events = events[len(events)-limit:]
	}
	out := slices.Clone(events)
	slices.Reverse(out)
	return out, nil
}

func (s *MemoryStore) AddMetrics(agentID string, p MetricsPoint) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	points := s.metrics[agentID]
	// Досланные (backfill) точки старше текущих: вставка по At.
	i := sort.Search(len(points), func(i int) bool { return points[i].At > p.At })
	points = slices.Insert(points, i, p)
	if s.KeepMetrics > 0 && len(points) > s.KeepMetrics {
		points = slices.Clone(points[len(points)-s.KeepMetrics:])
	}
	s.metrics[agentID] = points
	return nil
}

func (s *MemoryStore) ListMetrics(agentID string, since int64) ([]MetricsPoint, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	points := s.metrics[agentID]
	i := sort.Search(len(points), func(i int) bool { return points[i].At > since })
	return slices.Clone(points[i:]), nil
}

func (s *MemoryStore) PruneMetrics(before int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	for agentID, points := range s.metrics {
		i := sort.Search(len(points), func(i int) bool { return points[i].At >= before })
		if i == 0 {
			continue
		}
		n += i
		if i == len(points) {
			delete(s.metrics, agentID)
		} else {
			s.metrics[agentID] = slices.Clone(points[i:])
		}
	}
	return n, nil
}

// trim — убрать из начала списка удаляемые записи, пока их больше keep.
func trim(list []string, keep int, remove func(id string) bool) []string {
	if keep <= 0 || len(list) <= keep {
		return list
	}
	excess := len(list) - keep
	return slices.DeleteFunc(list, func(id string) bool {
		if excess > 0 && remove(id) {
			excess--
			return true
		}
		return false
	})
}

var _ Store = (*MemoryStore)(nil)
