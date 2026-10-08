package server

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Handler — HTTP-транспорт Agents: регистрация (POST enroll), WebSocket и HTTP
// sync канала агентов, файлы задач (/files/, если провайдер — http.Handler),
// раздача релизов и install.sh (если задан ReleasesDir).
func (a *Agents) Handler() http.Handler {
	mux := http.NewServeMux()
	a.Mount(mux)
	return mux
}

// Mount — те же маршруты в чужом ServeMux (рядом с API бэкенда).
func (a *Agents) Mount(mux *http.ServeMux) {
	mux.HandleFunc("POST "+message.EnrollPath, a.handleEnroll)
	mux.HandleFunc("GET "+message.LinkPath, a.handleLink)
	mux.HandleFunc("POST "+message.SyncPath, a.handleSync)
	mux.HandleFunc("GET "+ReleasesPath+"{file}", a.handleRelease)
	mux.HandleFunc("GET "+InstallPath, a.handleInstall)
	if fh, ok := a.files.(http.Handler); ok {
		mux.Handle(FilesPrefix, fh)
	}
}

// requestBase — адрес сервера, по которому агент до него дошёл: Host и TLS
// запроса; при TrustProxy — X-Forwarded-Host и X-Forwarded-Proto (первые
// значения), если они есть.
func (a *Agents) requestBase(r *http.Request) string {
	scheme, host := "http", r.Host
	if r.TLS != nil {
		scheme = "https"
	}
	if a.opts.TrustProxy {
		if p := firstValue(r.Header.Get("X-Forwarded-Proto")); p != "" {
			scheme = p
		}
		if h := firstValue(r.Header.Get("X-Forwarded-Host")); h != "" {
			host = h
		}
	}
	return scheme + "://" + host
}

// firstValue — первое значение заголовка-списка через запятую.
func firstValue(header string) string {
	return strings.TrimSpace(strings.Split(header, ",")[0])
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, msg string) {
	writeJSON(w, status, message.Error{Code: code, Message: msg})
}

// handleEnroll — POST /api/v1/agent-link/enroll: токен → учётные данные.
// Тело — не больше 64 КБ (больше — 413), читается до проверки токена;
// неверный токен и запрос не по правилам — в счёт неудач адреса клиента.
func (a *Agents) handleEnroll(w http.ResponseWriter, r *http.Request) {
	client := a.agentAddress(r)
	if client == "" {
		client = "*"
	}
	if wait := a.enrollBlocked(client); wait > 0 {
		w.Header().Set("Retry-After", strconv.Itoa(int((wait+time.Second-1)/time.Second)))
		writeError(w, http.StatusTooManyRequests, "ENROLL_RATE_LIMITED", "Слишком много неудачных регистраций — повторите позже")
		return
	}
	var req struct {
		Token  string            `json:"token"`
		Name   string            `json:"name"`
		Labels map[string]string `json:"labels"`
		Host   json.RawMessage   `json:"host"`
	}
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, enrollBodyLimit)).Decode(&req); err != nil {
		a.enrollFailed(client)
		if tooLarge := (*http.MaxBytesError)(nil); errors.As(err, &tooLarge) {
			writeError(w, http.StatusRequestEntityTooLarge, "MESSAGE_INVALID", "Запрос регистрации больше 64 КБ")
			return
		}
		writeError(w, http.StatusBadRequest, "MESSAGE_INVALID", "Нужны token и name")
		return
	}
	id, secret, err := a.Enroll(req.Token, EnrollInfo{Name: req.Name, Labels: req.Labels, Host: req.Host})
	if err != nil {
		var pe *message.Error
		switch {
		case errors.As(err, &pe) && pe.Code == "AGENT_ENROLLMENT_TOKEN_INVALID":
			a.enrollFailed(client)
			writeJSON(w, http.StatusUnauthorized, pe)
		case errors.As(err, &pe):
			a.enrollFailed(client)
			writeJSON(w, http.StatusBadRequest, pe)
		default:
			a.log.Error("регистрация агента", "err", err)
			writeError(w, http.StatusInternalServerError, "INTERNAL", "Регистрация не удалась")
		}
		return
	}
	writeJSON(w, http.StatusCreated, map[string]string{"agentId": id, "secret": secret})
}

// agentAddress — адрес клиента (Agent.Address, счёт неудачных регистраций): при TrustProxy —
// первый адрес X-Forwarded-For, иначе хост из RemoteAddr; IP без порта.
func (a *Agents) agentAddress(r *http.Request) string {
	if a.opts.TrustProxy {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			if first := strings.TrimSpace(strings.Split(xff, ",")[0]); first != "" {
				return stripPort(first)
			}
		}
	}
	return stripPort(r.RemoteAddr)
}

// stripPort — адрес без порта ("1.2.3.4:5" → "1.2.3.4", "[::1]:5" → "::1").
func stripPort(addr string) string {
	if host, _, err := net.SplitHostPort(addr); err == nil {
		return host
	}
	return strings.Trim(addr, "[]")
}

// enrollBlocked — сколько ещё ждать клиенту: неудачных регистраций в окне
// EnrollFailureWindow не меньше EnrollFailureLimit; 0 — можно.
func (a *Agents) enrollBlocked(client string) time.Duration {
	limit := a.opts.EnrollFailureLimit
	if limit < 0 {
		return 0
	}
	a.enrollMu.Lock()
	defer a.enrollMu.Unlock()
	fails := a.recentFails(client, time.Now())
	if len(fails) < limit {
		return 0
	}
	// Окно отсчитывается от самой старой из последних limit неудач.
	return max(time.Until(fails[len(fails)-limit].Add(a.opts.EnrollFailureWindow)), time.Millisecond)
}

// enrollFailed — неудачная регистрация клиента (неверный токен или запрос).
func (a *Agents) enrollFailed(client string) {
	if a.opts.EnrollFailureLimit < 0 {
		return
	}
	a.enrollMu.Lock()
	defer a.enrollMu.Unlock()
	t := time.Now()
	a.enrollFails[client] = append(a.recentFails(client, t), t)
}

// recentFails — неудачи клиента в окне (старые убираются). Под enrollMu.
func (a *Agents) recentFails(client string, t time.Time) []time.Time {
	fails := a.enrollFails[client]
	i := 0
	for i < len(fails) && t.Sub(fails[i]) >= a.opts.EnrollFailureWindow {
		i++
	}
	fails = fails[i:]
	if len(fails) == 0 {
		delete(a.enrollFails, client)
		return nil
	}
	a.enrollFails[client] = fails
	return fails
}

// pruneEnrollFails — убрать клиентов без неудач в окне (sweep).
func (a *Agents) pruneEnrollFails() {
	a.enrollMu.Lock()
	defer a.enrollMu.Unlock()
	t := time.Now()
	for client := range a.enrollFails {
		a.recentFails(client, t)
	}
}

// ─── WebSocket (§2.1) ──────────────────────────────────────────────────

// handleLink — GET /api/v1/agent-link: канал и авторизация — до
// upgrade (426, 401), затем сессия.
func (a *Agents) handleLink(w http.ResponseWriter, r *http.Request) {
	if !hasWSChannel(r.Header.Get("Sec-WebSocket-Protocol")) {
		w.WriteHeader(http.StatusUpgradeRequired)
		return
	}
	agentID, ok := a.authenticate(r.Header.Get("Authorization"))
	if !ok {
		w.WriteHeader(http.StatusUnauthorized)
		return
	}
	conn, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{message.WSChannel}})
	if err != nil {
		return
	}
	conn.SetReadLimit(16 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a.mu.Lock()
	ss := a.newSession(agentID, TransportWS, a.requestBase(r))
	ss.address = a.agentAddress(r)
	a.mu.Unlock()
	written := make(chan struct{})
	go func() {
		defer close(written)
		a.writeLoop(ctx, conn, ss)
	}()
	go a.pingLoop(ctx, conn)
	// Без hello за 10 с — 4400.
	helloTimer := time.AfterFunc(a.helloTimeout, func() {
		a.mu.Lock()
		if !ss.greeted {
			ss.close(message.CloseInvalid)
		}
		a.unlock()
	})
	defer helloTimer.Stop()

	for {
		_, raw, err := conn.Read(ctx)
		if err != nil {
			break
		}
		var env message.Envelope
		a.mu.Lock()
		switch {
		case ss.closed:
		case json.Unmarshal(raw, &env) != nil:
			ss.close(message.CloseInvalid)
		case !ss.greeted && env.Type != message.TypeHello:
			ss.close(message.CloseInvalid)
		case !ss.greeted:
			a.open(ss, env)
		default:
			a.handle(ss, env)
		}
		closed := ss.closed
		a.unlock()
		if closed {
			break
		}
	}
	a.mu.Lock()
	a.closeSession(ss, message.CloseNormal)
	a.unlock()
	// Хвост очереди и код закрытия (4409, 4410…) уходят до выхода.
	select {
	case <-written:
	case <-time.After(3 * time.Second):
		_ = conn.CloseNow()
	}
}

func hasWSChannel(header string) bool {
	for _, p := range strings.Split(header, ",") {
		if strings.TrimSpace(p) == message.WSChannel {
			return true
		}
	}
	return false
}

// writeLoop — исходящие сессии в WebSocket; закрытие сессии — код закрытия.
func (a *Agents) writeLoop(ctx context.Context, conn *websocket.Conn, ss *session) {
	for {
		a.mu.Lock()
		out, closed, code, notify := ss.take(), ss.closed, ss.code, ss.notify
		a.mu.Unlock()
		for _, env := range out {
			raw, _ := json.Marshal(env)
			wctx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := conn.Write(wctx, websocket.MessageText, raw)
			cancel()
			if err != nil {
				_ = conn.CloseNow()
				return
			}
		}
		if closed {
			_ = conn.Close(websocket.StatusCode(code), "")
			return
		}
		if len(out) > 0 {
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-notify:
		}
	}
}

// pingLoop — ping раз в 20 с; без pong за 10 с соединение закрывается.
func (a *Agents) pingLoop(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(a.pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		pctx, cancel := context.WithTimeout(ctx, pongTimeout)
		err := conn.Ping(pctx)
		cancel()
		if err != nil && ctx.Err() == nil {
			_ = conn.CloseNow()
			return
		}
	}
}

// ─── HTTP sync (§2.2) ──────────────────────────────────────────────────

type syncRequest struct {
	SessionID   *string            `json:"sessionId"`
	Messages    []message.Envelope `json:"messages"`
	WaitSeconds int                `json:"waitSeconds"`
}

// handleSync — POST /api/v1/agent-link/sync: пачка сообщений агента, ответ —
// доставки; доставлять нечего — ждать до waitSeconds (не больше 25 с).
// Клиент ушёл — доставки остаются в сессии до следующего запроса.
func (a *Agents) handleSync(w http.ResponseWriter, r *http.Request) {
	agentID, ok := a.authenticate(r.Header.Get("Authorization"))
	if !ok {
		writeError(w, http.StatusUnauthorized, "AGENT_CREDENTIALS_INVALID", "Неверные учётные данные")
		return
	}
	var req syncRequest
	if err := json.NewDecoder(http.MaxBytesReader(w, r.Body, 32<<20)).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "MESSAGE_INVALID", err.Error())
		return
	}

	a.mu.Lock()
	var ss *session
	messages := req.Messages
	if req.SessionID == nil {
		if len(messages) == 0 || messages[0].Type != message.TypeHello {
			a.unlock()
			writeError(w, http.StatusBadRequest, "AGENT_HELLO_REQUIRED", "Первое сообщение — hello")
			return
		}
		ss = a.newSession(agentID, TransportHTTP, a.requestBase(r))
		ss.address = a.agentAddress(r)
		a.open(ss, messages[0])
		messages = messages[1:]
		if ss.closed {
			code := ss.code
			a.unlock()
			switch code {
			case message.CloseUnsupported:
				writeError(w, http.StatusConflict, "AGENT_VERSION_UNSUPPORTED", "Нет общей версии формата сообщений")
			case message.CloseUnauthorized:
				writeError(w, http.StatusUnauthorized, "AGENT_CREDENTIALS_INVALID", "Неверные учётные данные")
			case message.CloseRestart:
				writeError(w, http.StatusServiceUnavailable, "SERVER_RESTARTING", "Сервер останавливается")
			default:
				writeError(w, http.StatusBadRequest, "MESSAGE_INVALID", "Некорректное hello")
			}
			return
		}
	} else {
		cur := a.sessions[agentID]
		if cur == nil || cur.id != *req.SessionID || cur.closed {
			code := "AGENT_SESSION_EXPIRED"
			if cur != nil && cur.id != *req.SessionID {
				code = "AGENT_SESSION_REPLACED"
			}
			a.unlock()
			writeError(w, http.StatusConflict, code, "Сессия недействительна")
			return
		}
		ss = cur
	}
	ss.lastActive = time.Now()
	for _, env := range messages {
		a.handle(ss, env)
	}

	if wait := min(time.Duration(req.WaitSeconds)*time.Second, syncMaxWait); wait > 0 {
		timer := time.NewTimer(wait)
		defer timer.Stop()
	waiting:
		for len(ss.outq) == 0 && !ss.closed {
			notify := ss.notify
			a.unlock()
			select {
			case <-notify:
				a.mu.Lock()
			case <-timer.C:
				a.mu.Lock()
				break waiting
			case <-r.Context().Done():
				a.mu.Lock()
				break waiting
			}
		}
	}
	// Клиент ушёл — доставки не забирать: ответ до него не дойдёт.
	if r.Context().Err() != nil {
		a.unlock()
		return
	}
	out, closed, code := ss.take(), ss.closed, ss.code
	ss.lastActive = time.Now()
	a.unlock()
	switch {
	case closed && code == message.CloseReplaced:
		writeError(w, http.StatusConflict, "AGENT_SESSION_REPLACED", "Сессию вытеснила другая")
		return
	case closed && code == message.CloseUnauthorized:
		writeError(w, http.StatusUnauthorized, "AGENT_CREDENTIALS_INVALID", "Учётные данные отозваны")
		return
	}
	if out == nil {
		out = []message.Envelope{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"sessionId": ss.id, "messages": out})
}
