// Package link — связь агента с бэкендом: сессия поверх WebSocket или
// HTTP sync, переподключение с backoff, классы доставки (поток — seq и ack,
// надёжные — outbox до ack, запросы — ответ по re), обработка кодов закрытия.
package link

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/internal/backoff"
	"github.com/epifanovmd/agent/internal/outbox"
	"github.com/epifanovmd/agent/internal/stream"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// Тайминги сессии (§2.1, §3).
const (
	welcomeTimeout = 15 * time.Second
	pingInterval   = 20 * time.Second
	pingTimeout    = 10 * time.Second
	requestTimeout = 30 * time.Second
	// fallbackFor — сколько работать по HTTP, прежде чем снова пробовать WebSocket.
	fallbackFor = 10 * time.Minute
	// replacedPause — пауза после вытеснения другой сессией того же агента.
	replacedPause = 30 * time.Second
	// outboxRetry — повтор надёжного сообщения, отклонённого с retryable.
	outboxRetry = 30 * time.Second
)

// Handler — что сессия сообщает агенту.
type Handler interface {
	// Hello — приветствие новой сессии (свежее: возможности и задачи на сейчас).
	Hello() message.Hello
	// OnWelcome — сессия открыта.
	OnWelcome(message.Welcome)
	// OnMessage — сообщение сервера (кроме ack, error по надёжным и ответов на запросы).
	OnMessage(message.Envelope)
	// OnDisconnect — сессия потеряна.
	OnDisconnect(err error)
}

// Auth — учётные данные: текущие, откат на прежний секрет и обновление
// после отзыва.
type Auth interface {
	// Authorization — заголовок для следующего подключения.
	Authorization() string
	// Fallback — сервер отклонил заголовок (401/4401): true — есть другой
	// секрет (прежний после agent.rotateKey), переподключиться с ним сразу;
	// false — отклонены все, нужна Renew.
	Fallback() bool
	// Accepted — сессия открыта (welcome) с заголовком authorization.
	Accepted(authorization string)
	// Renew — учётные данные отозваны (4401): получить новые (повторная
	// регистрация); ошибка — ждать и пробовать прежние.
	Renew(ctx context.Context) error
}

// Options — настройки связи.
type Options struct {
	// ServerURL — адрес сервера; ServerURLs — ещё адреса того же бэкенда:
	// связь идёт с первым доступным, при обрыве или отказе — со следующим по кругу.
	ServerURL  string
	ServerURLs []string
	Transport  string // auto | ws | http
	Auth       Auth
	Outbox     *outbox.Outbox
	Stream     *stream.Buffer
	HTTPClient *http.Client
	Log        *slog.Logger
	Backoff    backoff.Policy
}

// Link — связь с бэкендом. Методы безопасны для вызова из любых горутин.
type Link struct {
	opts    Options
	handler Handler

	mu sync.Mutex
	// streamMu — номер seq выдаётся и сообщение уходит в одном порядке:
	// сервер отбрасывает номер меньше принятого как повтор.
	streamMu sync.Mutex
	conn     conn
	ready    bool
	inflight map[string]time.Time // надёжные, отправленные в этой сессии
	requests map[string]chan message.Envelope

	// urls — адреса сервера; cur — индекс текущего.
	urls []string
	cur  atomic.Int64

	mode          atomic.Value // "ws" | "http" | ""
	fallbackUntil time.Time
	kick          chan struct{}
	// renew — открыть сессию заново (изменилось приветствие); renewGen —
	// число таких запросов (запрос во время рукопожатия не теряется).
	renew    chan struct{}
	renewGen atomic.Int64
}

// errRenew — сессия закрыта агентом для нового hello.
var errRenew = errors.New("link: сессия открывается заново — изменились возможности агента")

// New — связь; запускается Run.
func New(opts Options, handler Handler) *Link {
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{}
	}
	if opts.Backoff == (backoff.Policy{}) {
		opts.Backoff = backoff.Default
	}
	l := &Link{opts: opts, handler: handler, kick: make(chan struct{}, 1), renew: make(chan struct{}, 1)}
	for _, u := range append([]string{opts.ServerURL}, opts.ServerURLs...) {
		if u != "" && !slices.Contains(l.urls, u) {
			l.urls = append(l.urls, u)
		}
	}
	if len(l.urls) == 0 {
		l.urls = []string{""}
	}
	l.mode.Store("")
	return l
}

// ServerURL — адрес сервера, с которым связь сейчас (или будет следующей).
func (l *Link) ServerURL() string { return l.urls[int(l.cur.Load())%len(l.urls)] }

// nextURL — перейти к следующему адресу; true — начат новый круг.
func (l *Link) nextURL() bool {
	next := (int(l.cur.Load()) + 1) % len(l.urls)
	l.cur.Store(int64(next))
	return next == 0
}

// switchable — после такой ошибки есть смысл пробовать другой адрес: сервер
// недоступен (сеть, нет welcome, 5xx). Закрытие кодом и отказ 4xx — сервер
// жив и ответил, адрес не меняется.
func switchable(err error) bool {
	var ce *CloseError
	var de *DialError
	switch {
	case err == nil, errors.Is(err, errRenew), errors.As(err, &ce):
		return false
	case errors.As(err, &de):
		return de.Status >= 500
	}
	return true
}

// Mode — транспорт текущей сессии ("" — нет связи).
func (l *Link) Mode() string { return l.mode.Load().(string) }

// Connected — сессия открыта (после welcome).
func (l *Link) Connected() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.ready
}

// Stream — потоковое сообщение: seq, буфер до подтверждения, отправка при связи.
func (l *Link) Stream(typ string, data any) {
	env, err := message.New(typ, data)
	if err != nil {
		l.opts.Log.Error("link: сообщение не сериализуется", "type", typ, "err", err)
		return
	}
	l.streamMu.Lock()
	defer l.streamMu.Unlock()
	l.sendIfReady(l.opts.Stream.Add(env))
}

// Reliable — надёжное сообщение: в outbox до подтверждения, переживает рестарт.
func (l *Link) Reliable(typ string, data any) error {
	env, err := message.New(typ, data)
	if err != nil {
		return err
	}
	env.ID = message.NewID()
	if err := l.opts.Outbox.Append(env); err != nil {
		return err
	}
	l.flushOutbox()
	return nil
}

// Reconnect — закрыть текущую сессию и сразу открыть новую со свежим hello
// (изменились метки или набор возможностей). Очередь важных сообщений и
// неподтверждённый поток сохраняются и досылаются в новой сессии. Без
// связи — ничего: следующая сессия и так начнётся с нового hello.
func (l *Link) Reconnect() {
	l.renewGen.Add(1)
	select {
	case l.renew <- struct{}{}:
	default:
	}
}

// ErrOffline — запрос невозможен: связи нет.
var ErrOffline = errors.New("link: нет связи с сервером")

// Request — запрос с ответом (job.urls): ответ раскладывается в out.
func (l *Link) Request(ctx context.Context, typ string, data any, out any) error {
	env, err := message.New(typ, data)
	if err != nil {
		return err
	}
	env.ID = message.NewID()
	reply := make(chan message.Envelope, 1)

	l.mu.Lock()
	if !l.ready {
		l.mu.Unlock()
		return ErrOffline
	}
	c := l.conn
	l.requests[env.ID] = reply
	l.mu.Unlock()
	defer func() {
		l.mu.Lock()
		delete(l.requests, env.ID)
		l.mu.Unlock()
	}()

	if err := c.Send(ctx, env); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, requestTimeout)
	defer cancel()
	select {
	case res, ok := <-reply:
		if !ok {
			return ErrOffline
		}
		if res.Type == message.TypeError {
			var e message.Error
			_ = res.Decode(&e)
			return &e
		}
		return res.Decode(out)
	case <-ctx.Done():
		return fmt.Errorf("link: %s: %w", typ, ctx.Err())
	}
}

// Run — держать связь до отмены ctx.
func (l *Link) Run(ctx context.Context) error {
	failures := 0
	for {
		greeted, err := l.session(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		l.handler.OnDisconnect(err)
		if greeted {
			failures = 0
		}

		delay := l.opts.Backoff.Delay(failures)
		if errors.Is(err, errRenew) {
			l.opts.Log.Info("link: переподключение с новым hello")
			continue
		}
		var ce *CloseError
		var de *DialError
		switch {
		case errors.As(err, &ce) && ce.Code == message.CloseRestart:
			delay = time.Duration(time.Now().UnixNano()%1000) * time.Millisecond
		case errors.As(err, &ce) && ce.Code == message.CloseReplaced:
			delay = replacedPause
		case errors.As(err, &ce) && ce.Code == message.CloseUnauthorized,
			errors.As(err, &de) && de.Status == http.StatusUnauthorized:
			if l.opts.Auth.Fallback() {
				l.opts.Log.Warn("link: новый секрет не принят — подключение с прежним")
				delay = 0
				break
			}
			l.opts.Log.Warn("link: учётные данные отозваны или неверны — повторная регистрация")
			if renewErr := l.opts.Auth.Renew(ctx); renewErr == nil {
				delay = 0
			} else {
				l.opts.Log.Error("link: повторная регистрация не удалась", "err", renewErr)
				delay = l.opts.Backoff.Max
			}
		case errors.As(err, &ce) && ce.Code == message.CloseUnsupported:
			l.opts.Log.Error("link: сервер не поддерживает версии формата сообщений агента — нужно обновление")
			delay = l.opts.Backoff.Max
		case errors.As(err, &de) && l.opts.Transport == "auto" && fallbackStatus(de.Status):
			l.opts.Log.Warn("link: WebSocket недоступен — переход на HTTP sync", "status", de.Status)
			l.fallbackUntil = time.Now().Add(fallbackFor)
			delay = 0
		default:
			if len(l.urls) > 1 && switchable(err) {
				from := l.ServerURL()
				if !l.nextURL() {
					delay = 0 // следующий адрес — сразу, пауза — после круга
				}
				l.opts.Log.Warn("link: сервер недоступен — следующий адрес", "err", err, "from", from, "to", l.ServerURL(),
					"retryIn", delay.Round(time.Millisecond))
				break
			}
			if err != nil {
				l.opts.Log.Warn("link: связь потеряна", "err", err, "retryIn", delay.Round(time.Millisecond))
			}
		}
		failures++
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// fallbackStatus — отказ upgrade, после которого WebSocket нет смысла повторять:
// прокси не пропускает upgrade. Авторизация, перегрузка и 5xx — повтор с backoff.
func fallbackStatus(status int) bool {
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests:
		return false
	}
	return status < 500 && status != http.StatusSwitchingProtocols
}

func (l *Link) useHTTP() bool {
	switch l.opts.Transport {
	case "http":
		return true
	case "ws":
		return false
	}
	return time.Now().Before(l.fallbackUntil)
}

// session — одна сессия: подключение, рукопожатие, обмен до разрыва.
// greeted — сессия дошла до welcome.
func (l *Link) session(ctx context.Context) (greeted bool, err error) {
	dialCtx, cancelDial := context.WithTimeout(ctx, 15*time.Second)
	authz := l.opts.Auth.Authorization()
	serverURL := l.ServerURL()
	var c conn
	if l.useHTTP() {
		c, err = dialSync(dialCtx, l.opts.HTTPClient, serverURL, authz)
	} else {
		c, err = dialWS(dialCtx, l.opts.HTTPClient, serverURL, authz)
	}
	cancelDial()
	if err != nil {
		return false, err
	}

	sessCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	closeReason := "agent stopping"
	defer func() { c.Close(message.CloseGoingAway, closeReason) }()
	// Запрос, пришедший до этой сессии, к ней не относится: её hello свежий.
	select {
	case <-l.renew:
	default:
	}
	renewGen := l.renewGen.Load()

	if err := c.Send(sessCtx, message.MustNew(message.TypeHello, l.handler.Hello())); err != nil {
		return false, err
	}

	welcome, err := l.awaitWelcome(sessCtx, c)
	if err != nil {
		return false, err
	}
	l.opts.Auth.Accepted(authz)

	// Досылка неподтверждённого потока прошлой сессии — по порядку seq и до
	// новых потоковых сообщений.
	l.streamMu.Lock()
	for _, env := range l.opts.Stream.Unacked() {
		if err := c.Send(sessCtx, env); err != nil {
			l.streamMu.Unlock()
			return true, err
		}
	}
	l.mu.Lock()
	l.conn = c
	l.ready = true
	l.inflight = map[string]time.Time{}
	l.requests = map[string]chan message.Envelope{}
	l.mu.Unlock()
	l.streamMu.Unlock()
	l.mode.Store(c.Mode())
	defer func() {
		l.mu.Lock()
		l.ready = false
		l.conn = nil
		for _, ch := range l.requests {
			close(ch)
		}
		l.requests = nil
		l.mu.Unlock()
		l.mode.Store("")
	}()

	l.opts.Log.Info("link: сессия открыта", "server", serverURL, "transport", c.Mode(), "sessionId", welcome.SessionID, "version", welcome.Version)
	l.handler.OnWelcome(welcome)

	errs := make(chan error, 3)
	go func() { errs <- l.readLoop(sessCtx, c) }()
	go func() { errs <- l.pingLoop(sessCtx, c) }()
	go func() { errs <- l.outboxLoop(sessCtx, c) }()
	if l.renewGen.Load() != renewGen {
		// Настройки изменились во время рукопожатия.
		closeReason = "agent reconfigured"
		return true, errRenew
	}
	select {
	case err := <-errs:
		return true, err
	case <-l.renew:
		closeReason = "agent reconfigured"
		return true, errRenew
	}
}

func (l *Link) awaitWelcome(ctx context.Context, c conn) (message.Welcome, error) {
	ctx, cancel := context.WithTimeout(ctx, welcomeTimeout)
	defer cancel()
	for {
		env, err := c.Recv(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				c.Close(message.CloseInvalid, "no welcome")
				return message.Welcome{}, errors.New("link: сервер не прислал welcome")
			}
			return message.Welcome{}, err
		}
		switch env.Type {
		case message.TypeWelcome:
			var w message.Welcome
			if err := env.Decode(&w); err != nil {
				return message.Welcome{}, err
			}
			return w, nil
		case message.TypeError:
			var e message.Error
			_ = env.Decode(&e)
			l.opts.Log.Warn("link: сервер отклонил hello", "code", e.Code, "message", e.Message)
		}
	}
}

func (l *Link) readLoop(ctx context.Context, c conn) error {
	for {
		env, err := c.Recv(ctx)
		if err != nil {
			return err
		}
		switch env.Type {
		case message.TypeAck:
			var ack message.Ack
			if env.Decode(&ack) == nil {
				l.onAck(ack)
			}
		case message.TypeError:
			l.onError(env)
		default:
			if env.Re != "" && l.deliverReply(env) {
				continue
			}
			l.handler.OnMessage(env)
		}
	}
}

func (l *Link) onAck(ack message.Ack) {
	if ack.Seq > 0 {
		l.opts.Stream.Ack(ack.Seq)
	}
	if len(ack.IDs) > 0 {
		l.opts.Outbox.Remove(ack.IDs...)
		l.mu.Lock()
		for _, id := range ack.IDs {
			delete(l.inflight, id)
		}
		l.mu.Unlock()
	}
}

// onError — ошибка по сообщению агента: надёжное без повтора — из outbox,
// с повтором — отправить позже; запрос — ответ ошибкой.
func (l *Link) onError(env message.Envelope) {
	var e message.Error
	_ = env.Decode(&e)
	if env.Re != "" && l.deliverReply(env) {
		return
	}
	l.mu.Lock()
	_, reliable := l.inflight[env.Re]
	if reliable && e.Retryable {
		l.inflight[env.Re] = time.Now().Add(outboxRetry)
	} else {
		delete(l.inflight, env.Re)
	}
	l.mu.Unlock()
	if reliable && !e.Retryable {
		l.opts.Outbox.Remove(env.Re)
	}
	l.opts.Log.Warn("link: сервер отклонил сообщение", "re", env.Re, "code", e.Code, "message", e.Message, "retryable", e.Retryable)
}

func (l *Link) deliverReply(env message.Envelope) bool {
	l.mu.Lock()
	ch, ok := l.requests[env.Re]
	l.mu.Unlock()
	if ok {
		ch <- env
	}
	return ok
}

func (l *Link) pingLoop(ctx context.Context, c conn) error {
	t := time.NewTicker(pingInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			pctx, cancel := context.WithTimeout(ctx, pingTimeout)
			err := c.Ping(pctx)
			cancel()
			if err != nil {
				return fmt.Errorf("link: нет pong: %w", err)
			}
		}
	}
}

// outboxLoop — досылка журнала: при открытии сессии, при новом сообщении и
// по таймеру повтора отклонённых с retryable.
func (l *Link) outboxLoop(ctx context.Context, c conn) error {
	t := time.NewTicker(outboxRetry)
	defer t.Stop()
	for {
		if err := l.sendPending(ctx, c); err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-l.opts.Outbox.Notify():
		case <-l.kick:
		case <-t.C:
		}
	}
}

func (l *Link) sendPending(ctx context.Context, c conn) error {
	pending, err := l.opts.Outbox.Pending()
	if err != nil {
		l.opts.Log.Error("link: outbox не читается", "err", err)
		return nil
	}
	now := time.Now()
	for _, env := range pending {
		l.mu.Lock()
		retryAt, sent := l.inflight[env.ID]
		due := !sent || (!retryAt.IsZero() && now.After(retryAt))
		if due {
			l.inflight[env.ID] = time.Time{}
		}
		l.mu.Unlock()
		if !due {
			continue
		}
		if err := c.Send(ctx, env); err != nil {
			return err
		}
	}
	return nil
}

func (l *Link) flushOutbox() {
	select {
	case l.kick <- struct{}{}:
	default:
	}
}

func (l *Link) sendIfReady(env message.Envelope) {
	l.mu.Lock()
	c, ready := l.conn, l.ready
	l.mu.Unlock()
	if !ready {
		return
	}
	// Неудача — разрыв: сообщение останется в буфере и уйдёт после переподключения.
	_ = c.Send(context.Background(), env)
}
