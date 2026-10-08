package link

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// syncWaitSeconds — long-poll сервера, когда отправлять нечего (прокси рвут дольше 30 с).
const syncWaitSeconds = 25

type syncRequest struct {
	SessionID   *string            `json:"sessionId"`
	Messages    []message.Envelope `json:"messages"`
	WaitSeconds int                `json:"waitSeconds"`
}

type syncResponse struct {
	SessionID string             `json:"sessionId"`
	Messages  []message.Envelope `json:"messages"`
}

type syncError struct {
	Code string `json:"code"`
}

// syncConn — запасной транспорт: пачки конвертов через POST. Два потока
// запросов: отправка (исходящее по порядку, без ожидания) и long-poll
// (только приём). Запрос в полёте не отменяется никогда: доставку, которую
// сервер уже отдал ответом, нельзя потерять. Любой сбой обмена завершает
// сессию: link переподключится и дошлёт неподтверждённое.
type syncConn struct {
	client *http.Client
	url    string
	auth   string

	mu        sync.Mutex
	sessionID *string
	opened    chan struct{}
	queue     []message.Envelope
	kick      chan struct{}

	in      chan message.Envelope
	done    chan struct{}
	errOnce sync.Once
	err     error
	cancel  context.CancelFunc
}

func dialSync(ctx context.Context, client *http.Client, serverURL, auth string) (conn, error) {
	loopCtx, cancel := context.WithCancel(context.Background())
	c := &syncConn{
		client: client,
		url:    strings.TrimRight(serverURL, "/") + message.SyncPath,
		auth:   auth,
		opened: make(chan struct{}),
		kick:   make(chan struct{}, 1),
		in:     make(chan message.Envelope, 1024),
		done:   make(chan struct{}),
		cancel: cancel,
	}
	go c.pushLoop(loopCtx)
	go c.pollLoop(loopCtx)
	return c, nil
}

func (c *syncConn) Mode() string { return "http" }

func (c *syncConn) Send(_ context.Context, env message.Envelope) error {
	c.mu.Lock()
	c.queue = append(c.queue, env)
	c.mu.Unlock()
	select {
	case c.kick <- struct{}{}:
	default:
	}
	return nil
}

func (c *syncConn) Recv(ctx context.Context) (message.Envelope, error) {
	select {
	case env := <-c.in:
		return env, nil
	case <-c.done:
		select {
		case env := <-c.in:
			return env, nil
		default:
		}
		return message.Envelope{}, c.err
	case <-ctx.Done():
		return message.Envelope{}, ctx.Err()
	}
}

// Ping — обмен идёт запросами: их неудача и есть потеря связи.
func (c *syncConn) Ping(context.Context) error { return nil }

func (c *syncConn) Close(int, string) { c.fail(&CloseError{Code: message.CloseNormal}) }

// fail — сессия окончена (первая причина побеждает).
func (c *syncConn) fail(err error) {
	c.errOnce.Do(func() {
		c.err = err
		c.cancel()
		close(c.done)
	})
}

// pushLoop — исходящее по порядку: пачка → ответ (подтверждения, доставки).
// Первая пачка несёт hello и открывает сессию.
func (c *syncConn) pushLoop(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-c.kick:
		}
		c.mu.Lock()
		messages := c.queue
		c.queue = nil
		session := c.sessionID
		c.mu.Unlock()
		if len(messages) == 0 {
			continue
		}
		resp, err := c.exchange(ctx, syncRequest{SessionID: session, Messages: messages})
		if err != nil {
			c.fail(err)
			return
		}
		c.mu.Lock()
		first := c.sessionID == nil
		c.sessionID = &resp.SessionID
		c.mu.Unlock()
		if first {
			close(c.opened)
		}
		if !c.deliver(ctx, resp.Messages) {
			return
		}
		// Пока шёл запрос, могло накопиться ещё.
		c.mu.Lock()
		more := len(c.queue) > 0
		c.mu.Unlock()
		if more {
			select {
			case c.kick <- struct{}{}:
			default:
			}
		}
	}
}

// pollLoop — приём доставок сервера long-poll (после открытия сессии).
func (c *syncConn) pollLoop(ctx context.Context) {
	select {
	case <-ctx.Done():
		return
	case <-c.opened:
	}
	for {
		c.mu.Lock()
		session := c.sessionID
		c.mu.Unlock()
		resp, err := c.exchange(ctx, syncRequest{SessionID: session, Messages: []message.Envelope{}, WaitSeconds: syncWaitSeconds})
		if err != nil {
			c.fail(err)
			return
		}
		if !c.deliver(ctx, resp.Messages) {
			return
		}
	}
}

func (c *syncConn) deliver(ctx context.Context, messages []message.Envelope) bool {
	for _, env := range messages {
		select {
		case c.in <- env:
		case <-ctx.Done():
			return false
		}
	}
	return true
}

func (c *syncConn) exchange(ctx context.Context, body syncRequest) (syncResponse, error) {
	if body.Messages == nil {
		body.Messages = []message.Envelope{}
	}
	raw, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewReader(raw))
	if err != nil {
		return syncResponse{}, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", c.auth)
	resp, err := c.client.Do(req)
	if err != nil {
		return syncResponse{}, err
	}
	defer resp.Body.Close()
	data, _ := io.ReadAll(io.LimitReader(resp.Body, readLimit))
	switch {
	case resp.StatusCode == http.StatusOK:
		var out syncResponse
		if err := json.Unmarshal(data, &out); err != nil {
			return syncResponse{}, err
		}
		return out, nil
	case resp.StatusCode == http.StatusUnauthorized:
		return syncResponse{}, &CloseError{Code: message.CloseUnauthorized, Reason: "unauthorized"}
	case resp.StatusCode == http.StatusConflict:
		var e syncError
		_ = json.Unmarshal(data, &e)
		switch e.Code {
		case "AGENT_VERSION_UNSUPPORTED":
			return syncResponse{}, &CloseError{Code: message.CloseUnsupported, Reason: e.Code}
		case "AGENT_SESSION_REPLACED":
			return syncResponse{}, &CloseError{Code: message.CloseReplaced, Reason: e.Code}
		}
		// Сессия истекла (сервер её забыл) — сразу новая с hello.
		return syncResponse{}, &CloseError{Code: message.CloseRestart, Reason: e.Code}
	default:
		return syncResponse{}, &DialError{Status: resp.StatusCode, Err: errors.New(strings.TrimSpace(string(data)))}
	}
}
