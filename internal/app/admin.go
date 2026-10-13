//go:build unix

package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/epifanovmd/agent/protocol"
)

// AgentStatus — GET /status локального API (§18): агент, связь, воркеры.
type AgentStatus struct {
	PID       int    `json:"pid"`
	Online    bool   `json:"online"`
	StartedAt int64  `json:"startedAt"`
	Version   string `json:"version"`
	Name      string `json:"name"`
	// AgentID — id агента у бэкенда ("" — ещё не зарегистрирован).
	AgentID string `json:"agentId,omitempty"`
	// Server — адрес бэкенда ("" — работа без бэкенда).
	Server string `json:"server,omitempty"`
	// Status — состояние агента (pending, active, revoked), StatusSince — с какого момента (мс
	// UTC), StatusReason — почему.
	Status       string `json:"status"`
	StatusSince  int64  `json:"statusSince,omitempty"`
	StatusReason string `json:"statusReason,omitempty"`
	protocol.State
	// LastError, LastErrorAt — последняя ошибка связи (мс UTC).
	LastError   string `json:"lastError,omitempty"`
	LastErrorAt int64  `json:"lastErrorAt,omitempty"`
}

// CommandRequest — POST /workers/{name}/command локального API.
type CommandRequest struct {
	Type      string          `json:"type"`
	Args      json.RawMessage `json:"args,omitempty"`
	TimeoutMs int64           `json:"timeoutMs,omitempty"`
}

// status — состояние для локального API.
func (a *App) status() AgentStatus {
	cfg := a.config()
	a.errMu.Lock()
	errText, errAt := a.lastErr, a.lastErrAt
	a.errMu.Unlock()
	s := AgentStatus{
		PID: os.Getpid(), Online: a.online(), StartedAt: a.started.UnixMilli(), Version: a.version, Name: cfg.Name,
		AgentID: a.auth.agentID(), Server: cfg.Server.URL, State: a.state(), LastError: errText,
	}
	life := a.lifeState()
	s.Status, s.StatusSince, s.StatusReason = life.Status, life.Since, life.Reason
	if !errAt.IsZero() {
		s.LastErrorAt = errAt.UnixMilli()
	}
	return s
}

// listenAdmin — локальный API на сокете path (только владелец агента): состояние, воркеры,
// перезапуск и команды воркерам, журнал.
func listenAdmin(path string, a *App) (*http.Server, error) {
	_ = os.Remove(path)
	ln, err := net.Listen("unix", path)
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(path, 0o600); err != nil {
		ln.Close()
		return nil, err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /status", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, a.status()) })
	mux.HandleFunc("GET /workers", func(w http.ResponseWriter, _ *http.Request) { writeJSON(w, http.StatusOK, a.state().Workers) })
	mux.HandleFunc("POST /workers/{name}/restart", func(w http.ResponseWriter, r *http.Request) {
		name := r.PathValue("name")
		if !a.workers.Has(name) {
			writeError(w, unknownWorker(name))
			return
		}
		force, _ := strconv.ParseBool(r.URL.Query().Get("force"))
		if err := a.workers.Restart(r.Context(), name, force); err != nil {
			writeError(w, protocol.NewError(protocol.CodeWorkerUnavailable, err.Error()))
			return
		}
		writeJSON(w, http.StatusOK, struct{}{})
	})
	mux.HandleFunc("POST /workers/{name}/command", func(w http.ResponseWriter, r *http.Request) {
		var req CommandRequest
		raw, _ := io.ReadAll(io.LimitReader(r.Body, protocol.MaxResultBytes))
		if err := json.Unmarshal(raw, &req); err != nil || req.Type == "" {
			writeError(w, invalid("тело: нужен {type, args?, timeoutMs?}"))
			return
		}
		if req.TimeoutMs <= 0 {
			req.TimeoutMs = protocol.DefaultRequestTimeout.Milliseconds()
		}
		done := make(chan protocol.Result, 1)
		id := protocol.NewID()
		a.dispatch(id, 0, protocol.WorkerCommandArgs{Worker: r.PathValue("name"), Type: req.Type, Args: req.Args, TimeoutMs: req.TimeoutMs}, done)
		select {
		case res := <-done:
			writeJSON(w, http.StatusOK, res)
		case <-r.Context().Done():
			if c := a.takeCall(id, ""); c != nil {
				a.sock.SendJSON(c.worker, protocol.SSECancel, protocol.Cancel{Re: id})
			}
		}
	})
	mux.HandleFunc("GET /logs", func(w http.ResponseWriter, r *http.Request) {
		lines, _ := strconv.Atoi(r.URL.Query().Get("lines"))
		res, err := a.logs(protocol.AgentLogsArgs{Worker: r.URL.Query().Get("worker"), Lines: lines})
		if err != nil {
			writeError(w, err)
			return
		}
		writeJSON(w, http.StatusOK, res)
	})
	srv := &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	go func() { _ = srv.Serve(ln) }()
	return srv, nil
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, err error) {
	var ei *protocol.ErrorInfo
	if !errors.As(err, &ei) {
		ei = protocol.NewError(protocol.CodeInternal, err.Error())
	}
	status := http.StatusBadRequest
	switch ei.Code {
	case protocol.CodeWorkerUnknown:
		status = http.StatusNotFound
	case protocol.CodeWorkerUnavailable:
		status = http.StatusConflict
	case protocol.CodeInternal:
		status = http.StatusInternalServerError
	}
	writeJSON(w, status, ei)
}

// ─── клиент локального API ─────────────────────────────────────────────

// ErrNotRunning — агент с этим dataDir не запущен.
var ErrNotRunning = errors.New("агент с этим dataDir не запущен")

// Admin — клиент локального API агента с каталогом данных dataDir.
type Admin struct{ client *http.Client }

// NewAdmin — клиент; агент не запущен — ErrNotRunning.
func NewAdmin(dataDir string) (*Admin, error) {
	if lock, err := LockDataDir(dataDir); err == nil {
		lock.Unlock()
		return nil, ErrNotRunning
	} else if !errors.Is(err, ErrLocked) {
		return nil, err
	}
	sock := filepath.Join(RunDir(dataDir), AdminSocketName)
	return &Admin{client: &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}}, nil
}

// Do — запрос к локальному API: ответ не 2xx — *protocol.ErrorInfo.
func (c *Admin) Do(ctx context.Context, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, "http://agent"+path, rd)
	if err != nil {
		return err
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("агент запущен, но не отвечает (запускается или завис): %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		var ei protocol.ErrorInfo
		if json.NewDecoder(resp.Body).Decode(&ei) == nil && ei.Code != "" {
			return &ei
		}
		return fmt.Errorf("HTTP %d", resp.StatusCode)
	}
	if out == nil {
		return nil
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// Status — `agent status` (проверка живости, например HEALTHCHECK Docker): агент с каталогом
// данных dir запущен и отвечает. Ошибка — не запущен или завис.
func Status(dir string) (AgentStatus, error) {
	var st AgentStatus
	c, err := NewAdmin(dir)
	if err != nil {
		return st, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = c.Do(ctx, http.MethodGet, "/status", nil, &st)
	return st, err
}
