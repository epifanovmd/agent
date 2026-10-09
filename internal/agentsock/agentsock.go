// Package agentsock — сокет агента для воркеров (§12): POST /events
// (только типы из манифеста воркера), POST /requests (запрос к серверу,
// только типы из манифеста), GET /config/{key}, GET /context. Запрос — с токеном воркера
// (Authorization: Bearer); по токену агент узнаёт, какой это воркер.
package agentsock

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/outbox"
	"github.com/epifanovmd/agent/internal/requests"
)

// Agent — что сокет берёт у агента.
type Agent interface {
	// ByToken — имя воркера по токену.
	ByToken(token string) (string, bool)
	// Declared — тип события объявлен в манифесте воркера (§12).
	Declared(ctx context.Context, worker, eventType string) bool
	// Event — записать событие воркера в outbox; полон — outbox.ErrFull,
	// другая ошибка — 500 INTERNAL.
	Event(e message.Event) error
	// Config — сохранённые настройки воркера.
	Config(worker, key string) (message.ConfigValue, bool)
	// Context — ответ GET /context.
	Context() message.Context
	// DeclaredRequest — тип запроса к серверу объявлен в манифесте воркера.
	DeclaredRequest(ctx context.Context, worker, typ string) bool
	// Request — запрос воркера к серверу: data ответа; ошибка —
	// *requests.Error или ошибка ctx.
	Request(ctx context.Context, worker string, p message.RequestPost) (json.RawMessage, error)
}

// maxBody — тело запроса к сокету агента: data события и немного на конверт.
const maxBody = message.MaxEventDataBytes + 4<<10

// declaredWait — сколько событие ждёт итога регистрации только что
// запущенного воркера.
const declaredWait = 10 * time.Second

// Server — HTTP-сервер на unix-сокете агента.
type Server struct {
	agent Agent
	log   *slog.Logger
	srv   *http.Server
	ln    net.Listener
}

// Listen — сокет path (права 0666: доступ — по токену воркера).
func Listen(path string, agent Agent, log *slog.Logger) (*Server, error) {
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o666); err != nil {
		ln.Close()
		return nil, err
	}
	s := &Server{agent: agent, log: log, ln: ln}
	s.srv = &http.Server{Handler: s, ReadHeaderTimeout: 10 * time.Second, ReadTimeout: 30 * time.Second}
	go func() { _ = s.srv.Serve(ln) }()
	return s, nil
}

// Close — остановить сокет.
func (s *Server) Close() {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_ = s.srv.Shutdown(ctx)
	_ = os.Remove(s.ln.Addr().String())
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func fail(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, message.ErrorInfo{Code: code, Message: msg})
}

func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	token, _ := strings.CutPrefix(r.Header.Get("Authorization"), "Bearer ")
	name, ok := s.agent.ByToken(strings.TrimSpace(token))
	if !ok {
		fail(w, http.StatusUnauthorized, message.CodeUnauthorized, "нужен токен воркера (Authorization: Bearer $AGENT_WORKER_TOKEN)")
		return
	}
	switch {
	case r.URL.Path == message.EventsPath && r.Method == http.MethodPost:
		s.event(w, r, name)
	case strings.HasPrefix(r.URL.Path, message.ConfigPathPrefix) && r.Method == http.MethodGet:
		key := strings.TrimPrefix(r.URL.Path, message.ConfigPathPrefix)
		v, ok := s.agent.Config(name, key)
		if !ok {
			fail(w, http.StatusNotFound, message.CodeNotFound, "нет ключа "+key)
			return
		}
		writeJSON(w, http.StatusOK, v)
	case r.URL.Path == message.RequestsPath && r.Method == http.MethodPost:
		s.request(w, r, name)
	case r.URL.Path == message.ContextPath && r.Method == http.MethodGet:
		writeJSON(w, http.StatusOK, s.agent.Context())
	default:
		fail(w, http.StatusNotFound, message.CodeNotFound, "нет пути "+r.Method+" "+r.URL.Path)
	}
}

func (s *Server) event(w http.ResponseWriter, r *http.Request, name string) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxBody+1))
	if err != nil {
		fail(w, http.StatusBadRequest, message.CodeMessageInvalid, "тело не прочитано: "+err.Error())
		return
	}
	if len(raw) > maxBody {
		fail(w, http.StatusRequestEntityTooLarge, message.CodeBodyTooLarge, "тело события больше 64 КБ")
		return
	}
	var post message.EventPost
	if err := json.Unmarshal(raw, &post); err != nil {
		fail(w, http.StatusBadRequest, message.CodeMessageInvalid, "тело — JSON {type, data?}: "+err.Error())
		return
	}
	if !message.ValidEventType(post.Type) {
		fail(w, http.StatusBadRequest, message.CodeMessageInvalid, "type: "+message.EventTypePattern)
		return
	}
	if len(post.Data) > message.MaxEventDataBytes {
		fail(w, http.StatusRequestEntityTooLarge, message.CodeBodyTooLarge, "data события больше 64 КБ")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), declaredWait)
	defer cancel()
	if !s.agent.Declared(ctx, name, post.Type) {
		text := "события " + post.Type + " нет в манифесте воркера (events)"
		if strings.HasPrefix(post.Type, message.JobEventPrefix) {
			text = "событие " + post.Type + ": события задач принимаются, только если манифест воркера объявляет jobs" +
				" (типы — job.progress, job.done, job.failed, job.cancelled)"
		}
		fail(w, http.StatusBadRequest, message.CodeEventUndeclared, text)
		return
	}
	err = s.agent.Event(message.Event{Worker: name, Type: post.Type, Data: post.Data, At: time.Now().UnixMilli()})
	switch {
	case errors.Is(err, outbox.ErrFull):
		fail(w, http.StatusServiceUnavailable, message.CodeOutboxFull, "outbox агента полон")
	case err != nil:
		s.log.Error("событие воркера не записано в outbox", "worker", name, "err", err)
		fail(w, http.StatusInternalServerError, message.CodeInternal, "outbox не записан: "+err.Error())
	default:
		w.WriteHeader(http.StatusAccepted)
	}
}

// maxRequestBody — тело POST /requests.
const maxRequestBody = message.MaxRequestBytes

// request — POST /requests: запрос к серверу, ответ — когда он придёт.
func (s *Server) request(w http.ResponseWriter, r *http.Request, name string) {
	raw, err := io.ReadAll(io.LimitReader(r.Body, maxRequestBody+1))
	if err != nil {
		fail(w, http.StatusBadRequest, message.CodeMessageInvalid, "тело не прочитано: "+err.Error())
		return
	}
	if len(raw) > maxRequestBody {
		fail(w, http.StatusRequestEntityTooLarge, message.CodeBodyTooLarge, "тело запроса больше 1 МБ")
		return
	}
	var post message.RequestPost
	if err := json.Unmarshal(raw, &post); err != nil {
		fail(w, http.StatusBadRequest, message.CodeMessageInvalid, "тело — JSON {type, data?, timeoutMs?}: "+err.Error())
		return
	}
	if !message.ValidEventType(post.Type) {
		fail(w, http.StatusBadRequest, message.CodeMessageInvalid, "type: "+message.EventTypePattern)
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), declaredWait)
	declared := s.agent.DeclaredRequest(ctx, name, post.Type)
	cancel()
	if !declared {
		fail(w, http.StatusBadRequest, message.CodeRequestUndeclared, "запроса "+post.Type+" нет в манифесте воркера (requests)")
		return
	}
	data, err := s.agent.Request(r.Context(), name, post)
	var re *requests.Error
	switch {
	case errors.As(err, &re):
		fail(w, re.Status, re.Info.Code, re.Info.Message)
	case err != nil:
		// Воркер закрыл соединение: отвечать некому.
	default:
		writeJSON(w, http.StatusOK, message.RequestReply{Data: data})
	}
}
