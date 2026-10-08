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

// JobFilter — отбор задач; пустые поля не фильтруют. Limit, After —
// постраничное чтение: After — id последней записи прошлой страницы
// (страница — записи после неё в том же порядке; такой записи нет — пусто),
// Limit — не больше стольких записей (≤ 0 — все).
type JobFilter struct {
	Status  string
	Queue   string
	AgentID string
	Limit   int
	After   string
}

// CommandFilter — отбор команд; пустые поля не фильтруют. Limit, After — как
// в JobFilter.
type CommandFilter struct {
	Status  string
	AgentID string
	Limit   int
	After   string
}

// PruneBefore — уборка (Store.Prune), время в мс: завершённые задачи и
// команды с FinishedAt раньше Jobs и Commands, события с At раньше Events;
// 0 — этот вид записей не трогать.
type PruneBefore struct {
	Jobs     int64
	Commands int64
	Events   int64
}

// Store — хранение агентов, задач, команд, состояния и событий. Его вызывают
// одновременно горутины Agents и другие процессы бэкенда с тем же
// хранилищем; объекты передаются копиями (Agents меняет копию и сохраняет
// Update*). Нет записи — ErrNotFound. Задачи и команды (ListJobs,
// ListCommands) — новые первыми; агенты и снимки — в порядке создания.
//
// Update* — условная запись по версии Rev: запись сохраняется, только если Rev
// в хранилище равен Rev переданной записи (её не меняли с чтения), и хранится
// с Rev + 1; при успехе Rev переданной записи тоже увеличивается на 1, ответ —
// true. Запись изменилась — false, nil (Agents перечитает и повторит); записи
// нет — false, ErrNotFound. Проверка и запись — атомарно (в SQL: UPDATE …
// SET …, rev = rev + 1 WHERE id = $1 AND rev = $2). Create* сохраняет запись
// как есть.
type Store interface {
	CreateAgent(a *Agent) error
	GetAgent(id string) (*Agent, error)
	UpdateAgent(a *Agent) (bool, error)
	ListAgents() ([]*Agent, error)
	// DeleteAgent — удалить запись агента и его историю метрик; true — была.
	DeleteAgent(id string) (bool, error)

	CreateJob(j *Job) error
	GetJob(id string) (*Job, error)
	UpdateJob(j *Job) (bool, error)
	ListJobs(f JobFilter) ([]*Job, error)

	CreateCommand(c *Command) error
	GetCommand(id string) (*Command, error)
	UpdateCommand(c *Command) (bool, error)
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
	// Prune — удалить завершённые задачи и команды и события старше сроков
	// PruneBefore; возвращает число удалённых записей.
	Prune(p PruneBefore) (int, error)
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

func (s *MemoryStore) UpdateAgent(a *Agent) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.agents[a.ID]
	if !ok {
		return false, ErrNotFound
	}
	if cur.Rev != a.Rev {
		return false, nil
	}
	a.Rev++
	s.agents[a.ID] = a.Clone()
	return true, nil
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

func (s *MemoryStore) DeleteAgent(id string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.agents[id]; !ok {
		return false, nil
	}
	delete(s.agents, id)
	delete(s.metrics, id)
	s.agentList = slices.DeleteFunc(s.agentList, func(x string) bool { return x == id })
	return true, nil
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

func (s *MemoryStore) UpdateJob(j *Job) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.jobs[j.ID]
	if !ok {
		return false, ErrNotFound
	}
	if cur.Rev != j.Rev {
		return false, nil
	}
	j.Rev++
	s.jobs[j.ID] = j.Clone()
	return true, nil
}

func (s *MemoryStore) ListJobs(f JobFilter) ([]*Job, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Job
	for _, id := range page(s.jobList, f.After) {
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
		j := s.jobs[id]
		if (f.Status == "" || j.Status == f.Status) && (f.Queue == "" || j.Queue == f.Queue) &&
			(f.AgentID == "" || j.AgentID == f.AgentID) {
			out = append(out, j.Clone())
		}
	}
	return out, nil
}

// page — id списка (он в порядке создания) от новых к старым после after:
// after пусто — все; такого id нет — ни одного.
func page(list []string, after string) []string {
	ids := slices.Clone(list)
	slices.Reverse(ids)
	if after == "" {
		return ids
	}
	i := slices.Index(ids, after)
	if i < 0 {
		return nil
	}
	return ids[i+1:]
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

func (s *MemoryStore) UpdateCommand(c *Command) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	cur, ok := s.cmds[c.ID]
	if !ok {
		return false, ErrNotFound
	}
	if cur.Rev != c.Rev {
		return false, nil
	}
	c.Rev++
	s.cmds[c.ID] = c.Clone()
	return true, nil
}

func (s *MemoryStore) ListCommands(f CommandFilter) ([]*Command, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Command
	for _, id := range page(s.cmdList, f.After) {
		if f.Limit > 0 && len(out) >= f.Limit {
			break
		}
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

func (s *MemoryStore) Prune(p PruneBefore) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n := 0
	if p.Jobs > 0 {
		s.jobList = slices.DeleteFunc(s.jobList, func(id string) bool {
			j := s.jobs[id]
			if j.Status == JobQueued || j.Status == JobRunning || j.FinishedAt >= p.Jobs {
				return false
			}
			delete(s.jobs, id)
			n++
			return true
		})
	}
	if p.Commands > 0 {
		s.cmdList = slices.DeleteFunc(s.cmdList, func(id string) bool {
			c := s.cmds[id]
			if !c.Finished() || c.FinishedAt >= p.Commands {
				return false
			}
			delete(s.cmds, id)
			n++
			return true
		})
	}
	if p.Events > 0 {
		was := len(s.events)
		s.events = slices.DeleteFunc(s.events, func(e AgentEvent) bool { return e.At < p.Events })
		n += was - len(s.events)
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
