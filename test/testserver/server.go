package testserver

import (
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
	"github.com/epifanovmd/agent/sdk/go/server"
)

// Config — настройки сервера.
type Config struct {
	// EnrollToken — токен регистрации агентов.
	EnrollToken string
	// PublicURL — адрес, от которого строятся ссылки на файлы задач (пусто —
	// из запроса агента).
	PublicURL string
	// StatusInterval, MetricsInterval — интервалы, которые сервер задаёт агентам.
	StatusInterval  time.Duration
	MetricsInterval time.Duration
	// MetricsStoreInterval — прореживание истории метрик (server.Options);
	// 0 — сохранять каждую точку (тестам нужна вся история).
	MetricsStoreInterval time.Duration
	// OfflineGrace — отсрочка перехода агента в offline (по умолчанию 20 с).
	OfflineGrace time.Duration
	// OfflineAfter — агент без сессии и без вестей дольше — offline
	// (server.Options; 0 — по умолчанию).
	OfflineAfter time.Duration
	// ReleasesDir, PublicKey — раздача выпуска агента и install.sh.
	ReleasesDir string
	PublicKey   string
	// TrustProxy — адрес агента из X-Forwarded-For (server.Options).
	TrustProxy bool
	Log        *slog.Logger
}

// Server — эталонный сервер в памяти: Agents из sdk/go/server и dev-API.
type Server struct {
	agents *server.Agents
	files  *server.MemoryFiles
	log    *slog.Logger

	logMu sync.Mutex
	logs  map[string][]message.LogEntry // agentId → принятые записи лога
}

// keepLogEntries — сколько записей лога агента держит сервер (последние).
const keepLogEntries = 10_000

// New — сервер; Handler — его HTTP-обработчик, Close — остановка.
func New(cfg Config) *Server {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	log := cfg.Log.With("component", "testserver")
	files := server.NewMemoryFiles()
	if cfg.MetricsStoreInterval == 0 {
		cfg.MetricsStoreInterval = -1
	}
	s := &Server{files: files, log: log, logs: map[string][]message.LogEntry{}}
	s.agents = server.New(server.Options{
		EnrollToken:          cfg.EnrollToken,
		Store:                server.NewMemoryStore(),
		Files:                files,
		StatusInterval:       cfg.StatusInterval,
		MetricsInterval:      cfg.MetricsInterval,
		MetricsStoreInterval: cfg.MetricsStoreInterval,
		OfflineGrace:         cfg.OfflineGrace,
		OfflineAfter:         cfg.OfflineAfter,
		ReleasesDir:          cfg.ReleasesDir,
		PublicKey:            cfg.PublicKey,
		PublicURL:            cfg.PublicURL,
		TrustProxy:           cfg.TrustProxy,
		Log:                  log,
		OnLog:                s.addLogs,
	})
	return s
}

// addLogs — записи сообщения log агента (Options.OnLog).
func (s *Server) addLogs(agentID string, entries []message.LogEntry) {
	s.logMu.Lock()
	defer s.logMu.Unlock()
	l := append(s.logs[agentID], entries...)
	if over := len(l) - keepLogEntries; over > 0 {
		l = slices.Clone(l[over:])
	}
	s.logs[agentID] = l
}

// Logs — принятые записи лога агента по имени (последние keepLogEntries), по порядку.
func (s *Server) Logs(name string) []message.LogEntry {
	a, ok := s.Agent(name)
	if !ok {
		return nil
	}
	s.logMu.Lock()
	defer s.logMu.Unlock()
	return slices.Clone(s.logs[a.ID])
}

// Agents — все подключённые агенты: сервер агентов из sdk/go/server.
func (s *Server) Agents() *server.Agents { return s.agents }

// SetPublicURL — адрес сервера (известен после запуска, например в тестах).
func (s *Server) SetPublicURL(url string) { s.agents.SetPublicURL(url) }

// Close — остановить фоновую работу и закрыть сессии.
func (s *Server) Close() { s.agents.Close() }

// Handler — HTTP: канал агентов, регистрация, sync, файлы (/files/), dev-API.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	s.agents.Mount(mux)
	return mux
}

// Enqueue — поставить задачу (dev-API и тесты); ошибка — nil и запись в лог.
func (s *Server) Enqueue(req EnqueueRequest) *Job {
	job, err := s.agents.Enqueue(req)
	if err != nil {
		s.log.Error("задача не поставлена", "queue", req.Queue, "err", err)
		return nil
	}
	return job
}

// Job — снимок задачи.
func (s *Server) Job(id string) (Job, bool) {
	job, err := s.agents.Job(id)
	if err != nil {
		return Job{}, false
	}
	return *job, true
}

// File — содержимое файла задачи по ключу <jobId>/(in|out)/<имя>.
func (s *Server) File(key string) ([]byte, bool) { return s.files.Get(key) }

// Command — поручить команду агенту (пусто — агенту на связи, объявившему её).
func (s *Server) Command(req CommandRequest) (*Command, *message.Error) {
	cmd, err := s.agents.Command(req)
	if err != nil {
		return nil, asMessageError(err)
	}
	return cmd, nil
}

// CommandSnapshot — снимок команды.
func (s *Server) CommandSnapshot(id string) (Command, bool) {
	cmd, err := s.agents.CommandByID(id)
	if err != nil {
		return Command{}, false
	}
	return *cmd, true
}

// SetState — новый общий снимок домена; возвращает версию (0 — ошибка).
func (s *Server) SetState(domain string, spec json.RawMessage) int64 {
	st, err := s.agents.SetState(domain, spec, "")
	if err != nil {
		s.log.Error("снимок не сохранён", "domain", domain, "err", err)
		return 0
	}
	return st.Version
}

// Agent — снимок агента по имени, с его событиями.
func (s *Server) Agent(name string) (Agent, bool) {
	list, err := s.listAgents()
	if err != nil {
		return Agent{}, false
	}
	for _, a := range list {
		if a.Name == name {
			return a, true
		}
	}
	return Agent{}, false
}

// listAgents — агенты с событиями (по порядку).
func (s *Server) listAgents() ([]Agent, error) {
	list, err := s.agents.List()
	if err != nil {
		return nil, err
	}
	events, err := s.agents.Events(0)
	if err != nil {
		return nil, err
	}
	slices.Reverse(events)
	out := make([]Agent, 0, len(list))
	for _, a := range list {
		agent := Agent{Agent: *a}
		for _, e := range events {
			if e.AgentID == a.ID {
				agent.Events = append(agent.Events, e)
			}
		}
		out = append(out, agent)
	}
	return out, nil
}

func asMessageError(err error) *message.Error {
	var pe *message.Error
	if errors.As(err, &pe) {
		return pe
	}
	return &message.Error{Code: "INTERNAL", Message: err.Error()}
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
