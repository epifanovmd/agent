// Package workerapi — сокет агента для воркеров (§12): HTTP по unix-сокету, вход по токену
// воркера (Authorization: Worker <token>). Вниз — поток SSE (desired, command, cancel), вверх —
// POST (регистрация, итоги, ход, события, пульс, запросы). Что делать с присланным, решает
// агент (Agent); пакет — только транспорт.
package workerapi

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/epifanovmd/agent/internal/sse"
	"github.com/epifanovmd/agent/protocol"
)

// Agent — что сокет передаёт агенту. Ошибка *protocol.ErrorInfo уходит воркеру с кодом,
// другая — как INTERNAL.
type Agent interface {
	// ByToken — имя воркера по токену.
	ByToken(token string) (string, bool)
	Register(worker string, reg protocol.Register) error
	Heartbeat(worker string, h protocol.Health) error
	Applied(worker string, items []protocol.Applied) error
	Results(worker string, items []protocol.Result) error
	Progress(worker string, p protocol.Progress) error
	Events(worker string, items []protocol.WorkerEvent) error
	Request(ctx context.Context, worker string, r protocol.Request) protocol.RequestReply
	// StreamOpened — поток воркера открыт: агент отправляет в него желаемое и незавершённые
	// команды (Server.Send). Ошибка — поток не открывается.
	StreamOpened(worker string) error
}

// Сроки; переменные — для тестов.
var (
	// pingEvery — комментарий в потоке воркера.
	pingEvery = protocol.StreamPing
	// streamBuffer — сколько событий ждут отправки в поток; переполнен — поток закрывается,
	// воркер переподключается и получает всё заново.
	streamBuffer = 256
)

// Server — сокет агента.
type Server struct {
	agent Agent
	log   *slog.Logger
	srv   *http.Server
	ln    net.Listener

	mu      sync.Mutex
	streams map[string]*stream
}

// stream — открытый поток воркера.
type stream struct {
	out  chan sse.Event
	done chan struct{}
	once sync.Once
}

func (s *stream) close() { s.once.Do(func() { close(s.done) }) }

// Listen — сокет path (только для владельца) и обработка запросов в фоне.
func Listen(path string, agent Agent, log *slog.Logger) (*Server, error) {
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, err
	}
	s := &Server{agent: agent, log: log, ln: ln, streams: map[string]*stream{}}
	s.srv = &http.Server{Handler: s.Handler(), ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = s.srv.Serve(ln) }()
	return s, nil
}

// Close — закрыть сокет и потоки.
func (s *Server) Close() error {
	s.mu.Lock()
	for name, st := range s.streams {
		st.close()
		delete(s.streams, name)
	}
	s.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	return s.srv.Shutdown(ctx)
}

// Send — событие в поток воркера. false — потока нет (воркер получит всё при подключении).
func (s *Server) Send(worker string, e sse.Event) bool {
	s.mu.Lock()
	st := s.streams[worker]
	s.mu.Unlock()
	if st == nil {
		return false
	}
	select {
	case st.out <- e:
		return true
	case <-st.done:
		return false
	default:
		s.log.Warn("поток воркера не успевает — переподключение", "worker", worker)
		s.Drop(worker)
		return false
	}
}

// SendJSON — событие типа typ с данными v.
func (s *Server) SendJSON(worker, typ string, v any) bool {
	raw, err := json.Marshal(v)
	if err != nil {
		return false
	}
	return s.Send(worker, sse.Event{Type: typ, Data: string(raw)})
}

// Connected — поток воркера открыт.
func (s *Server) Connected(worker string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.streams[worker] != nil
}

// Drop — закрыть поток воркера (воркер переподключится).
func (s *Server) Drop(worker string) {
	s.mu.Lock()
	st := s.streams[worker]
	delete(s.streams, worker)
	s.mu.Unlock()
	if st != nil {
		st.close()
	}
}

// Handler — маршруты сокета.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("POST "+protocol.WorkerPathRegister, s.auth(func(w http.ResponseWriter, r *http.Request, name string) {
		var reg protocol.Register
		if !decode(w, r, protocol.MaxManifestBytes+4096, &reg) {
			return
		}
		if err := reg.Check(); err != nil {
			fail(w, protocol.NewError(protocol.CodeInvalid, err.Error()))
			return
		}
		reply(w, http.StatusOK, s.agent.Register(name, reg))
	}))
	mux.HandleFunc("GET "+protocol.WorkerPathStream, s.auth(s.stream))
	mux.HandleFunc("POST "+protocol.WorkerPathApplied, s.auth(func(w http.ResponseWriter, r *http.Request, name string) {
		var items []protocol.Applied
		if decode(w, r, protocol.MaxBatchBytes, &items) {
			reply(w, http.StatusOK, s.agent.Applied(name, items))
		}
	}))
	mux.HandleFunc("POST "+protocol.WorkerPathResults, s.auth(func(w http.ResponseWriter, r *http.Request, name string) {
		var items []protocol.Result
		if decode(w, r, protocol.MaxResultBytes+protocol.MaxBatchBytes, &items) {
			reply(w, http.StatusOK, s.agent.Results(name, items))
		}
	}))
	mux.HandleFunc("POST "+protocol.WorkerPathProgress, s.auth(func(w http.ResponseWriter, r *http.Request, name string) {
		var p protocol.Progress
		if decode(w, r, protocol.MaxEventDataBytes, &p) {
			reply(w, http.StatusOK, s.agent.Progress(name, p))
		}
	}))
	mux.HandleFunc("POST "+protocol.WorkerPathEvents, s.auth(func(w http.ResponseWriter, r *http.Request, name string) {
		var items []protocol.WorkerEvent
		if decode(w, r, protocol.MaxBatchBytes, &items) {
			reply(w, http.StatusAccepted, s.agent.Events(name, items))
		}
	}))
	mux.HandleFunc("POST "+protocol.WorkerPathHealth, s.auth(func(w http.ResponseWriter, r *http.Request, name string) {
		var h protocol.Health
		if decode(w, r, protocol.MaxBatchBytes, &h) {
			reply(w, http.StatusOK, s.agent.Heartbeat(name, h))
		}
	}))
	mux.HandleFunc("POST "+protocol.WorkerPathRequests, s.auth(func(w http.ResponseWriter, r *http.Request, name string) {
		var req protocol.Request
		if decode(w, r, protocol.MaxRequestBytes, &req) {
			writeJSON(w, http.StatusOK, s.agent.Request(r.Context(), name, req))
		}
	}))
	return mux
}

// auth — запрос с токеном воркера.
func (s *Server) auth(h func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		token, ok := strings.CutPrefix(r.Header.Get("Authorization"), protocol.AuthWorkerScheme+" ")
		name, known := s.agent.ByToken(strings.TrimSpace(token))
		if !ok || !known {
			writeJSON(w, http.StatusUnauthorized, protocol.NewError(protocol.CodeUnauthorized,
				"нужен заголовок Authorization: Worker <AGENT_WORKER_TOKEN>"))
			return
		}
		h(w, r, name)
	}
}

// stream — GET /stream: поток воркера; новый поток того же воркера закрывает прежний.
func (s *Server) stream(w http.ResponseWriter, r *http.Request, name string) {
	st := &stream{out: make(chan sse.Event, streamBuffer), done: make(chan struct{})}
	s.mu.Lock()
	prev := s.streams[name]
	s.streams[name] = st
	s.mu.Unlock()
	if prev != nil {
		prev.close()
	}
	defer func() {
		s.mu.Lock()
		if s.streams[name] == st {
			delete(s.streams, name)
		}
		s.mu.Unlock()
		st.close()
	}()
	if err := s.agent.StreamOpened(name); err != nil {
		fail(w, err)
		return
	}
	out, err := sse.NewWriter(w)
	if err != nil {
		return
	}
	ping := time.NewTicker(pingEvery)
	defer ping.Stop()
	for {
		select {
		case <-r.Context().Done():
			return
		case <-st.done:
			return
		case e := <-st.out:
			if out.Send(e) != nil {
				return
			}
		case <-ping.C:
			if out.Comment("ping") != nil {
				return
			}
		}
	}
}

// decode — тело JSON не больше limit; иначе ответ с ошибкой и false.
func decode(w http.ResponseWriter, r *http.Request, limit int, v any) bool {
	raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, int64(limit)))
	var tooLarge *http.MaxBytesError
	switch {
	case errors.As(err, &tooLarge):
		fail(w, protocol.NewError(protocol.CodeBodyTooLarge, fmt.Sprintf("тело больше %d байт", limit)))
		return false
	case err != nil:
		fail(w, protocol.NewError(protocol.CodeInvalid, err.Error()))
		return false
	}
	if err := json.Unmarshal(raw, v); err != nil {
		fail(w, protocol.NewError(protocol.CodeInvalid, "тело: "+err.Error()))
		return false
	}
	return true
}

// reply — {} с кодом status или ошибка.
func reply(w http.ResponseWriter, status int, err error) {
	if err != nil {
		fail(w, err)
		return
	}
	writeJSON(w, status, struct{}{})
}

// statuses — HTTP-код ошибки по её коду.
var statuses = map[string]int{
	protocol.CodeInvalid:           http.StatusBadRequest,
	protocol.CodeBodyTooLarge:      http.StatusRequestEntityTooLarge,
	protocol.CodeUnauthorized:      http.StatusUnauthorized,
	protocol.CodeWorkerUnknown:     http.StatusNotFound,
	protocol.CodeWorkerUnavailable: http.StatusConflict,
	protocol.CodeWorkerInvalid:     http.StatusConflict,
	protocol.CodeCommandUndeclared: http.StatusUnprocessableEntity,
	protocol.CodeEventUndeclared:   http.StatusUnprocessableEntity,
	protocol.CodeRequestUndeclared: http.StatusUnprocessableEntity,
	protocol.CodeOutboxFull:        http.StatusServiceUnavailable,
}

func fail(w http.ResponseWriter, err error) {
	var ei *protocol.ErrorInfo
	if !errors.As(err, &ei) {
		ei = protocol.NewError(protocol.CodeInternal, err.Error())
	}
	status, ok := statuses[ei.Code]
	if !ok {
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, ei)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
