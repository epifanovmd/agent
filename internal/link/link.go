// Package link — связь агента с сервером по WebSocket (§2–§4): подключение
// с ключом агента, переподключение с растущей паузой и по кодам закрытия,
// классы доставки (важные — outbox до ack {ids}, поток — seq и досылка,
// ответы и управление — в текущем соединении).
package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/internal/backoff"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/outbox"
	"github.com/epifanovmd/agent/internal/stream"
)

// Сроки связи; переменные — для тестов.
var (
	welcomeTimeout = message.WelcomeTimeout
	pingInterval   = message.PingInterval
	pongTimeout    = message.PongTimeout
	// replacedPause — пауза после 4409 (подключился другой экземпляр агента).
	replacedPause = 30 * time.Second
	// retryImportant — повтор важного сообщения, отклонённого с retryable.
	retryImportant = 30 * time.Second
	// maxRetryAfter — верхняя граница паузы, которую просит сервер.
	maxRetryAfter = 10 * time.Minute
)

// Handler — что связь сообщает агенту.
type Handler interface {
	// Hello — первое сообщение нового соединения (свежее на каждое).
	Hello() message.Hello
	// OnWelcome — соединение открыто: outbox и поток уже досланы.
	OnWelcome(s *Session, w message.Welcome)
	// OnMessage — сообщение сервера (кроме ack и error по важным). Не должен
	// надолго блокировать: следующее сообщение ждёт возврата.
	OnMessage(s *Session, env message.Envelope)
	// OnDisconnect — соединение потеряно.
	OnDisconnect(err error)
}

// Auth — ключ агента: текущий, откат на прежний секрет, повторная регистрация.
type Auth interface {
	// Authorization — заголовок для следующего подключения.
	Authorization() string
	// Fallback — сервер отклонил заголовок (401/4401): true — есть другой
	// секрет (прежний после agent.rotateKey), подключиться с ним сразу.
	Fallback() bool
	// Accepted — соединение открыто (welcome) с заголовком authorization.
	Accepted(authorization string)
	// Renew — ключ отозван: регистрация заново (нужен токен).
	Renew(ctx context.Context) error
}

// Options — настройки связи.
type Options struct {
	// ServerURLs — адреса сервера: связь с первым доступным, при обрыве или
	// отказе — со следующим по кругу.
	ServerURLs []string
	Auth       Auth
	Outbox     *outbox.Outbox
	Stream     *stream.Buffer
	HTTPClient *http.Client
	Log        *slog.Logger
	Backoff    backoff.Policy
}

// Session — одно открытое соединение: ответы и сообщения управления уходят
// только в него; Context завершается с разрывом.
type Session struct {
	c   conn
	ctx context.Context
}

// Send — сообщение в это соединение; соединение закрыто — ошибка.
func (s *Session) Send(env message.Envelope) error { return s.c.Send(s.ctx, env) }

// Context — действует, пока соединение открыто.
func (s *Session) Context() context.Context { return s.ctx }

// Link — связь с сервером. Методы безопасны для вызова из любых горутин.
type Link struct {
	opts    Options
	handler Handler

	mu sync.Mutex
	// streamMu — seq выдаётся и сообщение уходит в одном порядке.
	streamMu sync.Mutex
	session  *Session
	// inflight — важные, отправленные в этом соединении: время повтора
	// (нулевое — ждут ack).
	inflight map[string]time.Time

	urls []string
	cur  atomic.Int64

	kick     chan struct{}
	renew    chan struct{}
	renewGen atomic.Int64

	// Сроки на момент создания (переменные пакета — для тестов).
	welcomeTimeout, pingInterval, pongTimeout, retryImportant time.Duration
}

// errRenew — соединение закрыто агентом для нового hello.
var errRenew = errors.New("link: соединение открывается заново — изменились имя или метки агента")

// New — связь; запускается Run.
func New(opts Options, handler Handler) *Link {
	if opts.HTTPClient == nil {
		opts.HTTPClient = &http.Client{}
	}
	if opts.Backoff == (backoff.Policy{}) {
		opts.Backoff = backoff.Default
	}
	l := &Link{opts: opts, handler: handler, kick: make(chan struct{}, 1), renew: make(chan struct{}, 1),
		welcomeTimeout: welcomeTimeout, pingInterval: pingInterval, pongTimeout: pongTimeout, retryImportant: retryImportant}
	for _, u := range opts.ServerURLs {
		if u != "" && !slices.Contains(l.urls, u) {
			l.urls = append(l.urls, u)
		}
	}
	if len(l.urls) == 0 {
		l.urls = []string{""}
	}
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

// Connected — соединение открыто (после welcome).
func (l *Link) Connected() bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.session != nil
}

// Stream — сообщение потока: seq, память до ack, отправка, если есть связь.
func (l *Link) Stream(typ string, data any) {
	env, err := message.New(typ, data)
	if err != nil {
		l.opts.Log.Error("link: сообщение не сериализуется", "type", typ, "err", err)
		return
	}
	l.streamMu.Lock()
	defer l.streamMu.Unlock()
	env = l.opts.Stream.Add(env)
	l.mu.Lock()
	s := l.session
	l.mu.Unlock()
	if s != nil {
		// Неудача — разрыв: сообщение уйдёт после переподключения.
		_ = s.Send(env)
	}
}

// Important — важное сообщение: в outbox до ack, переживает перезапуск. Нет
// id — назначается.
func (l *Link) Important(env message.Envelope) error {
	if env.ID == "" {
		env.ID = message.NewID()
	}
	if err := l.opts.Outbox.Append(env); err != nil {
		return err
	}
	select {
	case l.kick <- struct{}{}:
	default:
	}
	return nil
}

// Reconnect — закрыть соединение и сразу открыть новое со свежим hello.
// Outbox и поток сохраняются и досылаются в новом соединении.
func (l *Link) Reconnect() {
	l.renewGen.Add(1)
	select {
	case l.renew <- struct{}{}:
	default:
	}
}

// Run — держать связь до отмены ctx.
func (l *Link) Run(ctx context.Context) error {
	failures := 0
	for {
		greeted, err := l.connect(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if errors.Is(err, errRenew) {
			l.handler.OnDisconnect(nil)
			l.opts.Log.Info("link: переподключение с новым hello")
			continue
		}
		l.handler.OnDisconnect(err)
		if greeted {
			failures = 0
		}
		delay := l.delay(ctx, err, failures)
		failures++
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
}

// delay — пауза перед следующим подключением по причине разрыва (§2).
func (l *Link) delay(ctx context.Context, err error, failures int) time.Duration {
	delay := l.opts.Backoff.Delay(failures)
	var ce *CloseError
	var de *DialError
	closed := errors.As(err, &ce)
	dialed := errors.As(err, &de)
	switch {
	case closed && ce.Code == message.CloseRestart:
		return time.Duration(rand.Int64N(int64(time.Second)))
	case closed && ce.Code == message.CloseReplaced:
		l.opts.Log.Warn("link: подключился другой экземпляр этого агента — пауза", "retryIn", replacedPause+delay)
		return replacedPause + delay
	case retryAfter(err) > 0:
		d := retryAfter(err)
		l.opts.Log.Warn("link: сервер перегружен — подключение позже", "err", err, "retryIn", d)
		return d
	case closed && ce.Code == message.CloseUnauthorized, dialed && de.Status == http.StatusUnauthorized:
		if l.opts.Auth.Fallback() {
			l.opts.Log.Warn("link: новый секрет не принят — подключение с прежним")
			return 0
		}
		l.opts.Log.Warn("link: ключ агента отозван или неверен — регистрация заново")
		if renewErr := l.opts.Auth.Renew(ctx); renewErr != nil {
			l.opts.Log.Error("link: регистрация заново не удалась", "err", renewErr)
			return l.opts.Backoff.Max
		}
		return 0
	case closed && ce.Code == message.CloseInvalid:
		l.opts.Log.Error("link: сервер закрыл соединение: неверное сообщение агента", "reason", ce.Reason, "retryIn", delay)
		return delay
	case dialed && de.Status == http.StatusUpgradeRequired:
		l.opts.Log.Error("link: сервер не поддерживает формат " + message.Subprotocol + " — нужно обновление агента или сервера")
		return l.opts.Backoff.Max
	case len(l.urls) > 1 && switchable(err):
		from := l.ServerURL()
		if !l.nextURL() {
			delay = 0 // следующий адрес — сразу, пауза — после круга
		}
		l.opts.Log.Warn("link: сервер недоступен — следующий адрес", "err", err, "from", from, "to", l.ServerURL(),
			"retryIn", delay.Round(time.Millisecond))
		return delay
	}
	if err != nil {
		l.opts.Log.Warn("link: связь потеряна", "err", err, "retryIn", delay.Round(time.Millisecond))
	}
	return delay
}

// switchable — после такой ошибки есть смысл пробовать другой адрес: сервер
// недоступен (сеть, нет welcome, 5xx). Закрытие кодом и отказ 4xx — сервер
// жив и ответил.
func switchable(err error) bool {
	var ce *CloseError
	var de *DialError
	switch {
	case err == nil, errors.As(err, &ce):
		return false
	case errors.As(err, &de):
		return de.Status >= 500
	}
	return true
}

// retryAfter — через сколько сервер просит подключиться: 4429 (секунды в
// reason) или HTTP 429/503 с Retry-After; не больше maxRetryAfter.
func retryAfter(err error) time.Duration {
	var ce *CloseError
	var de *DialError
	var d time.Duration
	switch {
	case errors.As(err, &ce) && ce.Code == message.CloseOverloaded:
		d = parseRetryAfter(ce.Reason)
	case errors.As(err, &de) && (de.Status == http.StatusTooManyRequests || de.Status == http.StatusServiceUnavailable):
		d = de.RetryAfter
	}
	return min(d, maxRetryAfter)
}

// connect — одно соединение: подключение, hello/welcome, обмен до разрыва.
// greeted — дошло до welcome.
func (l *Link) connect(ctx context.Context) (greeted bool, err error) {
	dialCtx, cancelDial := context.WithTimeout(ctx, 15*time.Second)
	authz := l.opts.Auth.Authorization()
	serverURL := l.ServerURL()
	c, err := dialWS(dialCtx, l.opts.HTTPClient, serverURL, authz)
	cancelDial()
	if err != nil {
		return false, err
	}
	// Соединение живёт своим контекстом: при остановке агента сначала уходит
	// закрытие с кодом 1001, потом обрываются чтение и запись.
	sctx, cancel := context.WithCancel(context.WithoutCancel(ctx))
	defer cancel()
	closeReason := "agent stopping"
	defer func() { c.Close(message.CloseGoingAway, closeReason) }()
	defer context.AfterFunc(ctx, func() {
		c.Close(message.CloseGoingAway, "agent stopping")
		cancel()
	})()
	// Запрос нового hello, пришедший до этого соединения, к нему не относится.
	select {
	case <-l.renew:
	default:
	}
	renewGen := l.renewGen.Load()

	if err := c.Send(sctx, message.MustNew(message.TypeHello, l.handler.Hello())); err != nil {
		return false, err
	}
	welcome, err := l.awaitWelcome(sctx, c)
	if err != nil {
		return false, err
	}
	l.opts.Auth.Accepted(authz)
	s := &Session{c: c, ctx: sctx}

	// После welcome: outbox по порядку, затем поток с первого
	// неподтверждённого seq (§4); новые сообщения потока — после них.
	l.streamMu.Lock()
	l.mu.Lock()
	l.inflight = map[string]time.Time{}
	l.mu.Unlock()
	if err := l.sendImportant(s); err != nil {
		l.streamMu.Unlock()
		return true, err
	}
	for _, env := range l.opts.Stream.Unacked() {
		if !l.sendable(env) {
			continue
		}
		if err := s.Send(env); err != nil {
			l.streamMu.Unlock()
			return true, err
		}
	}
	l.mu.Lock()
	l.session = s
	l.mu.Unlock()
	l.streamMu.Unlock()
	defer func() {
		l.mu.Lock()
		l.session = nil
		l.mu.Unlock()
	}()

	l.opts.Log.Info("link: соединение открыто", "server", serverURL)
	l.handler.OnWelcome(s, welcome)

	errs := make(chan error, 3)
	go func() { errs <- l.readLoop(s) }()
	go func() { errs <- l.pingLoop(s) }()
	go func() { errs <- l.outboxLoop(s) }()
	if l.renewGen.Load() != renewGen {
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
	ctx, cancel := context.WithTimeout(ctx, l.welcomeTimeout)
	defer cancel()
	for {
		env, err := c.Recv(ctx)
		if err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
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

func (l *Link) readLoop(s *Session) error {
	for {
		env, err := s.c.Recv(s.ctx)
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
			l.handler.OnMessage(s, env)
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

// onError — сервер не принял сообщение агента: важное с retryable —
// повтор позже, без — удаляется из outbox (§3).
func (l *Link) onError(env message.Envelope) {
	var e message.Error
	_ = env.Decode(&e)
	l.mu.Lock()
	_, important := l.inflight[env.Re]
	if important && e.Retryable {
		l.inflight[env.Re] = time.Now().Add(l.retryImportant)
	} else {
		delete(l.inflight, env.Re)
	}
	l.mu.Unlock()
	if important && !e.Retryable {
		l.opts.Outbox.Remove(env.Re)
	}
	l.opts.Log.Warn("link: сервер не принял сообщение", "re", env.Re, "code", e.Code, "message", e.Message, "retryable", e.Retryable)
}

func (l *Link) pingLoop(s *Session) error {
	t := time.NewTicker(l.pingInterval)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-t.C:
			pctx, cancel := context.WithTimeout(s.ctx, l.pongTimeout)
			err := s.c.Ping(pctx)
			cancel()
			if err != nil {
				return fmt.Errorf("link: нет pong: %w", err)
			}
		}
	}
}

// outboxLoop — новые важные сообщения и повтор отклонённых с retryable.
func (l *Link) outboxLoop(s *Session) error {
	t := time.NewTicker(l.retryImportant / 3)
	defer t.Stop()
	for {
		select {
		case <-s.ctx.Done():
			return s.ctx.Err()
		case <-l.opts.Outbox.Notify():
		case <-l.kick:
		case <-t.C:
		}
		if err := l.sendImportant(s); err != nil {
			return err
		}
	}
}

// sendImportant — важные, которые пора отправить: ещё не отправленные в
// этом соединении и отклонённые с retryable, срок повтора которых наступил.
func (l *Link) sendImportant(s *Session) error {
	now := time.Now()
	for _, id := range l.opts.Outbox.IDs() {
		l.mu.Lock()
		retryAt, sent := l.inflight[id]
		due := !sent || (!retryAt.IsZero() && now.After(retryAt))
		if due {
			l.inflight[id] = time.Time{}
		}
		l.mu.Unlock()
		if !due {
			continue
		}
		env, ok := l.opts.Outbox.Read(id)
		if !ok {
			continue
		}
		if err := s.Send(env); err != nil {
			return err
		}
	}
	return nil
}

// sendable — данные сообщения потока — корректный JSON; испорченное
// убирается из памяти с ошибкой в логе.
func (l *Link) sendable(env message.Envelope) bool {
	if len(env.Data) == 0 || json.Valid(env.Data) {
		return true
	}
	l.opts.Stream.Remove(env.Seq)
	l.opts.Log.Error("link: сообщение потока испорчено и не отправляется", "type", env.Type, "seq", env.Seq)
	return false
}
