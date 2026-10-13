// Package backend — связь агента с бэкендом (§3–§9): hello, поток вниз (SSE, запасной путь —
// длинный опрос), отправка вверх: итоги и события из очереди на диске, состояние, метрики,
// журнал, запросы воркеров. Повторы с паузой, вход ключом агента.
package backend

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/internal/backoff"
	"github.com/epifanovmd/agent/internal/sse"
	"github.com/epifanovmd/agent/internal/store"
	"github.com/epifanovmd/agent/protocol"
)

// Auth — ключ агента.
type Auth interface {
	// Authorization — заголовок для следующего запроса.
	Authorization() string
	// Accepted — бэкенд принял заголовок (hello прошёл).
	Accepted(authorization string)
	// Fallback — заголовок отклонён (401): true — есть другой секрет, повторить сразу.
	Fallback() bool
	// Renew — ключ не известен бэкенду: регистрация заново (если есть токен).
	Renew(ctx context.Context) error
}

// Handler — что приходит от бэкенда.
type Handler interface {
	Hello() protocol.Hello
	OnHello(protocol.HelloReply)
	OnDesired(protocol.Desired)
	// OnCommand — команда; к возврату она записана в журнал команд (повтор безопасен).
	OnCommand(protocol.Command)
	// Rev — rev последнего желаемого (для длинного опроса).
	Rev() int64
	// OnOnline — связь появилась или пропала.
	OnOnline(bool)
}

// Сроки; переменные — для тестов.
var (
	streamOpenTimeout = protocol.StreamOpenTimeout
	streamSilence     = protocol.StreamSilence
	pollWait          = protocol.PollWait
	retry             = backoff.Default
	postTimeout       = 30 * time.Second
	// stableSession — связь продержалась столько — пауза повтора снова с начала.
	stableSession = time.Minute
	// logsKept — сколько записей журнала ждут отправки.
	logsKept = protocol.MaxLogLines
)

// Options — связь.
type Options struct {
	URL   string
	HTTP  *http.Client
	Auth  Auth
	Store *store.Store
	Log   *slog.Logger
}

// Client — связь с бэкендом.
type Client struct {
	opts Options
	h    Handler
	base string

	online  atomic.Bool
	revoked atomic.Bool
	kick    chan struct{}

	mu sync.Mutex
	// reconnect — закрыть текущую сессию (hello заново); reconnecting — её закрыл Reconnect.
	reconnect    context.CancelFunc
	reconnecting bool
	// auth — заголовок, принятый в текущей сессии: с ним идут запросы вверх.
	auth      string
	state     *protocol.State
	stateSent bool
	metrics   []protocol.Metrics
	logs      []protocol.LogEntry
	hello     bool
}

// New — связь с бэкендом opts.URL.
func New(opts Options, h Handler) *Client {
	return &Client{opts: opts, h: h, base: strings.TrimRight(opts.URL, "/") + protocol.BackendPrefix, kick: make(chan struct{}, 1)}
}

// Online — поток открыт.
func (c *Client) Online() bool { return c.online.Load() }

// Revoked — бэкенд отозвал агента (403 AGENT_REVOKED): связь прекращена.
func (c *Client) Revoked() bool { return c.revoked.Load() }

func (c *Client) wake() {
	select {
	case c.kick <- struct{}{}:
	default:
	}
}

// SetState — состояние агента: уходит последнее, как только есть связь.
func (c *Client) SetState(st protocol.State) {
	c.mu.Lock()
	c.state, c.stateSent = &st, false
	c.mu.Unlock()
	c.wake()
}

// AddMetrics — точка метрик; хранятся последние MetricsBacklog.
func (c *Client) AddMetrics(m protocol.Metrics) {
	c.mu.Lock()
	c.metrics = append(c.metrics, m)
	if over := len(c.metrics) - protocol.MetricsBacklog; over > 0 {
		c.metrics = c.metrics[over:]
	}
	c.mu.Unlock()
	c.wake()
}

// AddLogs — записи журнала для бэкенда; хранятся последние logsKept.
func (c *Client) AddLogs(entries []protocol.LogEntry) {
	if len(entries) == 0 {
		return
	}
	c.mu.Lock()
	c.logs = append(c.logs, entries...)
	if over := len(c.logs) - logsKept; over > 0 {
		c.logs = c.logs[over:]
	}
	c.mu.Unlock()
	c.wake()
}

// Rehello — hello изменился (настройки, воркеры): отправить заново.
func (c *Client) Rehello() {
	c.mu.Lock()
	c.hello = true
	c.mu.Unlock()
	c.wake()
}

// Reconnect — закрыть сессию и подключиться заново.
func (c *Client) Reconnect() {
	c.mu.Lock()
	cancel := c.reconnect
	c.reconnecting = cancel != nil
	c.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// StatusError — ответ бэкенда не 2xx.
type StatusError struct {
	Status     int
	Code       string
	Message    string
	RetryAfter time.Duration
}

func (e *StatusError) Error() string {
	if e.Code != "" {
		return fmt.Sprintf("HTTP %d %s: %s", e.Status, e.Code, e.Message)
	}
	return fmt.Sprintf("HTTP %d", e.Status)
}

// protocolError — ошибка протокола (§3): другой 4xx — данные не повторяются.
func protocolError(err error) bool {
	var se *StatusError
	if !errors.As(err, &se) || se.Status/100 != 4 {
		return false
	}
	switch se.Status {
	case http.StatusUnauthorized, http.StatusForbidden, http.StatusRequestTimeout, http.StatusTooManyRequests:
		return false
	}
	return true
}

// Run — связь до отмены ctx или отзыва агента.
func (c *Client) Run(ctx context.Context) {
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg.Add(1)
	go func() {
		defer wg.Done()
		c.uplink(ctx)
	}()
	log := c.opts.Log
	attempt := 0
	for ctx.Err() == nil {
		began := time.Now()
		err := c.session(ctx)
		if ctx.Err() != nil {
			return
		}
		c.mu.Lock()
		if c.reconnecting {
			// Переподключение по просьбе агента — сразу и без записи в журнал.
			c.reconnecting, err = false, nil
		}
		c.mu.Unlock()
		if time.Since(began) > stableSession {
			attempt = 0
		}
		var se *StatusError
		delay := retry.Delay(attempt)
		attempt++
		switch {
		case errors.As(err, &se) && se.Status == http.StatusForbidden && se.Code == protocol.CodeAgentRevoked:
			c.revoked.Store(true)
			log.Error("агент отозван бэкендом — связь прекращена, воркеры работают по последнему желаемому", "err", err)
			return
		case errors.As(err, &se) && se.Status == http.StatusUnauthorized:
			if c.opts.Auth.Fallback() {
				log.Warn("ключ агента не принят — повтор с прежним секретом")
				continue
			}
			if rerr := c.opts.Auth.Renew(ctx); rerr == nil {
				attempt = 0
				continue
			} else {
				log.Error("ключ агента не известен бэкенду", "err", rerr)
			}
		case errors.As(err, &se) && se.RetryAfter > 0:
			delay = max(delay, se.RetryAfter)
			log.Warn("связь с бэкендом: повтор позже", "err", err, "retryIn", delay.Round(time.Millisecond))
		case err != nil:
			log.Warn("связь с бэкендом прервана — повтор", "err", err, "retryIn", delay.Round(time.Millisecond))
		default:
			delay = 0
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// session — hello и поток вниз до обрыва.
func (c *Client) session(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	c.mu.Lock()
	c.reconnect = cancel
	c.hello = false
	c.mu.Unlock()
	auth := c.opts.Auth.Authorization()
	if err := c.sendHello(ctx, auth); err != nil {
		return err
	}
	c.opts.Auth.Accepted(auth)
	c.mu.Lock()
	c.auth = auth
	c.mu.Unlock()
	defer func() {
		if c.online.Swap(false) {
			c.h.OnOnline(false)
		}
	}()
	err := c.stream(ctx, auth)
	if errors.Is(err, errBuffered) {
		c.opts.Log.Warn("поток SSE не открылся вовремя (прокси буферизует ответ?) — длинный опрос")
		return c.poll(ctx, auth)
	}
	if ctx.Err() != nil && err == nil {
		return nil
	}
	return err
}

func (c *Client) sendHello(ctx context.Context, auth string) error {
	var reply protocol.HelloReply
	if err := c.post(ctx, auth, protocol.PathHello, c.h.Hello(), &reply); err != nil {
		return fmt.Errorf("hello: %w", err)
	}
	c.h.OnHello(reply)
	return nil
}

func (c *Client) setOnline() {
	if !c.online.Swap(true) {
		c.opts.Log.Info("связь с бэкендом есть", "server", c.opts.URL)
		c.h.OnOnline(true)
		c.mu.Lock()
		c.stateSent = false
		c.mu.Unlock()
		c.wake()
	}
}

// errBuffered — поток не отдал «: ok» за streamOpenTimeout.
var errBuffered = errors.New("поток не открылся вовремя")

// stream — GET /stream?after=<seq>: желаемое и команды до обрыва.
func (c *Client) stream(ctx context.Context, auth string) error {
	sctx, stop := context.WithCancel(ctx)
	defer stop()
	q := url.Values{"after": {strconv.FormatInt(c.opts.Store.LastSeq(), 10)}}
	req, err := http.NewRequestWithContext(sctx, http.MethodGet, c.base+protocol.PathStream+"?"+q.Encode(), nil)
	if err != nil {
		return err
	}
	req.Header.Set("Authorization", auth)
	req.Header.Set("Accept", sse.ContentType)
	// Ответ и «: ok» — не позже streamOpenTimeout; дальше — тишина не дольше streamSilence.
	alive := make(chan struct{}, 1)
	opened := make(chan struct{})
	var once sync.Once
	var buffered atomic.Bool
	var body io.Closer
	var bodyMu sync.Mutex
	go func() {
		t := time.NewTimer(streamOpenTimeout)
		defer t.Stop()
		select {
		case <-sctx.Done():
			return
		case <-opened:
		case <-t.C:
			buffered.Store(true)
			stop()
			return
		}
		t.Reset(streamSilence)
		for {
			select {
			case <-sctx.Done():
				return
			case <-alive:
				t.Reset(streamSilence)
			case <-t.C:
				c.opts.Log.Warn("поток бэкенда молчит — переподключение", "silence", streamSilence)
				bodyMu.Lock()
				if body != nil {
					body.Close()
				}
				bodyMu.Unlock()
				return
			}
		}
	}()
	touch := func() {
		once.Do(func() {
			close(opened)
			c.setOnline()
		})
		select {
		case alive <- struct{}{}:
		default:
		}
	}
	resp, err := c.opts.HTTP.Do(req)
	if err != nil {
		if buffered.Load() {
			return errBuffered
		}
		return err
	}
	defer resp.Body.Close()
	bodyMu.Lock()
	body = resp.Body
	bodyMu.Unlock()
	if resp.StatusCode != http.StatusOK {
		return statusError(resp)
	}
	r := sse.NewReader(resp.Body)
	r.OnComment = func(string) { touch() }
	for {
		e, err := r.Next()
		if err != nil {
			switch {
			case buffered.Load():
				return errBuffered
			case err == io.EOF:
				return errors.New("бэкенд закрыл поток")
			}
			return err
		}
		touch()
		c.handle(e.Type, []byte(e.Data))
	}
}

// handle — событие потока.
func (c *Client) handle(typ string, data []byte) {
	switch typ {
	case protocol.SSEDesired:
		var d protocol.Desired
		if err := json.Unmarshal(data, &d); err != nil {
			c.opts.Log.Warn("desired: неверное событие", "err", err)
			return
		}
		c.h.OnDesired(d)
	case protocol.SSECommand:
		var cmd protocol.Command
		if err := json.Unmarshal(data, &cmd); err != nil || cmd.ID == "" {
			c.opts.Log.Warn("command: неверное событие", "err", err)
			return
		}
		c.command(cmd)
	default:
		c.opts.Log.Debug("незнакомое событие потока — пропущено", "type", typ)
	}
}

func (c *Client) command(cmd protocol.Command) {
	c.h.OnCommand(cmd)
	if cmd.Seq > 0 {
		if err := c.opts.Store.SetLastSeq(cmd.Seq); err != nil {
			c.opts.Log.Error("seq команды не сохранён", "err", err)
		}
	}
}

// poll — длинный опрос до ошибки.
func (c *Client) poll(ctx context.Context, auth string) error {
	for ctx.Err() == nil {
		q := url.Values{
			"after": {strconv.FormatInt(c.opts.Store.LastSeq(), 10)},
			"rev":   {strconv.FormatInt(c.h.Rev(), 10)},
			"wait":  {strconv.Itoa(int(pollWait / time.Second))},
		}
		rctx, cancel := context.WithTimeout(ctx, pollWait+postTimeout)
		var reply protocol.PollReply
		err := c.do(rctx, auth, http.MethodGet, protocol.PathPoll+"?"+q.Encode(), nil, &reply)
		cancel()
		if err != nil {
			return err
		}
		c.setOnline()
		if reply.Desired != nil {
			c.h.OnDesired(*reply.Desired)
		}
		for _, cmd := range reply.Commands {
			c.command(cmd)
		}
	}
	return nil
}

// uplink — отправка вверх, пока есть связь: hello заново, итоги и события, состояние,
// метрики, журнал.
func (c *Client) uplink(ctx context.Context) {
	attempt := 0
	for {
		var err error
		if c.online.Load() {
			err = c.flush(ctx)
		}
		wait := time.Duration(-1)
		switch {
		case err != nil && ctx.Err() == nil:
			var se *StatusError
			if errors.As(err, &se) && (se.Status == http.StatusUnauthorized || se.Status == http.StatusForbidden) {
				c.Reconnect()
			}
			wait = retry.Delay(attempt)
			attempt++
			c.opts.Log.Warn("отправка бэкенду не удалась — повтор", "err", err, "retryIn", wait.Round(time.Millisecond))
		case err == nil:
			attempt = 0
		}
		var timer <-chan time.Time
		if wait >= 0 {
			timer = time.After(wait)
		}
		select {
		case <-ctx.Done():
			return
		case <-c.kick:
		case <-c.opts.Store.Notify():
		case <-timer:
		}
	}
}

// sessionAuth — заголовок текущей сессии.
func (c *Client) sessionAuth() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.auth
}

// flush — всё, что ждёт отправки; первая ошибка прерывает. Запросы — с заголовком текущей
// сессии: новый секрет (agent.rotateKey) бэкенд узнаёт из итога, и только после его доставки
// агент переподключается с новым секретом.
func (c *Client) flush(ctx context.Context) error {
	auth := c.sessionAuth()
	c.mu.Lock()
	hello := c.hello
	c.hello = false
	c.mu.Unlock()
	if hello {
		if err := c.sendHello(ctx, auth); err != nil {
			c.Rehello()
			return err
		}
	}
	results, err := c.drain(ctx, auth, store.KindResult, protocol.PathResults)
	if err != nil {
		return err
	}
	if results > 0 && c.opts.Auth.Authorization() != auth {
		c.Reconnect()
		return nil
	}
	if _, err := c.drain(ctx, auth, store.KindEvent, protocol.PathEvents); err != nil {
		return err
	}
	c.mu.Lock()
	st, sent := c.state, c.stateSent
	c.mu.Unlock()
	if st != nil && !sent {
		if err := c.post(ctx, auth, protocol.PathState, st, nil); c.failed("state", err) {
			return fmt.Errorf("state: %w", err)
		}
		c.mu.Lock()
		if c.state == st {
			c.stateSent = true
		}
		c.mu.Unlock()
	}
	for {
		c.mu.Lock()
		points := c.metrics[:min(len(c.metrics), protocol.MaxBatch)]
		c.mu.Unlock()
		if len(points) == 0 {
			break
		}
		if err := c.post(ctx, auth, protocol.PathMetrics, points, nil); c.failed("metrics", err) {
			return fmt.Errorf("metrics: %w", err)
		}
		c.mu.Lock()
		c.metrics = c.metrics[len(points):]
		c.mu.Unlock()
	}
	for {
		c.mu.Lock()
		entries := c.logs[:min(len(c.logs), protocol.MaxBatch)]
		c.mu.Unlock()
		if len(entries) == 0 {
			return nil
		}
		if err := c.post(ctx, auth, protocol.PathLogs, entries, nil); c.failed("logs", err) {
			return fmt.Errorf("logs: %w", err)
		}
		c.mu.Lock()
		c.logs = c.logs[min(len(entries), len(c.logs)):]
		c.mu.Unlock()
	}
}

// failed — отправку нужно повторить; ошибка протокола — в журнал, данные не повторяются.
func (c *Client) failed(what string, err error) bool {
	if protocolError(err) {
		c.opts.Log.Error("бэкенд не принял "+what+" — не повторяется", "err", err)
		return false
	}
	return err != nil
}

// drain — записи очереди kind пачками; подтверждённые и отклонённые удаляются. Итог —
// сколько записей бэкенд подтвердил.
func (c *Client) drain(ctx context.Context, auth, kind, path string) (int, error) {
	sent := 0
	for {
		items, err := c.opts.Store.Pending(kind, protocol.MaxBatch, protocol.MaxBatchBytes)
		if err != nil || len(items) == 0 {
			return sent, err
		}
		var body bytes.Buffer
		body.WriteByte('[')
		for i, it := range items {
			if i > 0 {
				body.WriteByte(',')
			}
			body.Write(it.Raw)
		}
		body.WriteByte(']')
		var reply protocol.Accepted
		err = c.post(ctx, auth, path, json.RawMessage(body.Bytes()), &reply)
		if protocolError(err) {
			ids := make([]string, len(items))
			for i, it := range items {
				ids[i] = it.ID
			}
			c.opts.Log.Error("бэкенд не принял пачку — записи не повторяются", "kind", kind, "count", len(ids), "err", err)
			if err := c.opts.Store.Remove(kind, ids); err != nil {
				return sent, err
			}
			continue
		}
		if err != nil {
			return sent, fmt.Errorf("%s: %w", strings.TrimPrefix(path, "/"), err)
		}
		done := reply.Accepted
		for _, r := range reply.Rejected {
			c.opts.Log.Error("бэкенд отклонил запись — не повторяется", "kind", kind, "id", r.ID, "reason", r.Reason)
			done = append(done, r.ID)
		}
		if len(done) == 0 {
			return sent, fmt.Errorf("%s: бэкенд не подтвердил ни одной записи", strings.TrimPrefix(path, "/"))
		}
		if err := c.opts.Store.Remove(kind, done); err != nil {
			return sent, err
		}
		sent += len(reply.Accepted)
	}
}

// Request — запрос воркера к бэкенду (§17): без повторов; нет связи — AGENT_OFFLINE.
func (c *Client) Request(ctx context.Context, r protocol.Request) protocol.RequestReply {
	if !c.online.Load() {
		return protocol.RequestReply{Error: protocol.NewError(protocol.CodeAgentOffline, "нет связи с бэкендом")}
	}
	timeout := protocol.DefaultRequestTimeout
	if r.TimeoutMs > 0 {
		timeout = min(time.Duration(r.TimeoutMs)*time.Millisecond, protocol.MaxRequestTimeout)
	}
	r.TimeoutMs = timeout.Milliseconds()
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var reply protocol.RequestReply
	err := c.do(ctx, c.sessionAuth(), http.MethodPost, protocol.PathRequests, r, &reply)
	var se *StatusError
	switch {
	case err == nil:
		return reply
	case ctx.Err() == context.DeadlineExceeded:
		return protocol.RequestReply{Error: protocol.NewError(protocol.CodeTimeout, fmt.Sprintf("бэкенд не ответил за %s", timeout))}
	case errors.As(err, &se) && se.Code != "":
		return protocol.RequestReply{Error: protocol.NewError(se.Code, se.Message)}
	default:
		return protocol.RequestReply{Error: protocol.NewError(protocol.CodeAgentOffline, err.Error())}
	}
}

func (c *Client) post(ctx context.Context, auth, path string, body, out any) error {
	ctx, cancel := context.WithTimeout(ctx, postTimeout)
	defer cancel()
	return c.do(ctx, auth, http.MethodPost, path, body, out)
}

// do — запрос к бэкенду; ответ не 2xx — *StatusError.
func (c *Client) do(ctx context.Context, auth, method, path string, body, out any) error {
	var rd io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(raw)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.base+path, rd)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Authorization", auth)
	resp, err := c.opts.HTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return statusError(resp)
	}
	if out == nil {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<20))
		return nil
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, protocol.MaxDesiredBytes))
	if err != nil {
		return err
	}
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil
	}
	return json.Unmarshal(raw, out)
}

func statusError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	se := &StatusError{Status: resp.StatusCode}
	var ei protocol.ErrorInfo
	if json.Unmarshal(raw, &ei) == nil {
		se.Code, se.Message = ei.Code, ei.Message
	}
	if n, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && n > 0 {
		se.RetryAfter = min(time.Duration(n)*time.Second, 10*time.Minute)
	}
	return se
}
