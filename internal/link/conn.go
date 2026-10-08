package link

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/coder/websocket"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// conn — транспорт сессии: поток конвертов в обе стороны.
type conn interface {
	Send(ctx context.Context, env message.Envelope) error
	Recv(ctx context.Context) (message.Envelope, error)
	Ping(ctx context.Context) error
	Close(code int, reason string)
	Mode() string
}

// DialError — сервер отказал в соединении HTTP-кодом.
type DialError struct {
	Status int
	Err    error
	// RetryAfter — из заголовка Retry-After (0 — нет).
	RetryAfter time.Duration
}

// parseRetryAfter — секунды (Retry-After или reason кода 4429) или дата HTTP;
// 0 — не разобрать.
func parseRetryAfter(v string) time.Duration {
	v = strings.TrimSpace(v)
	if v == "" {
		return 0
	}
	if n, err := strconv.ParseFloat(v, 64); err == nil {
		if n <= 0 {
			return 0
		}
		return time.Duration(n * float64(time.Second))
	}
	if t, err := http.ParseTime(v); err == nil {
		return max(time.Until(t), 0)
	}
	return 0
}

func (e *DialError) Error() string {
	return fmt.Sprintf("link: отказ HTTP %d: %v", e.Status, e.Err)
}

func (e *DialError) Unwrap() error { return e.Err }

// CloseError — сервер закрыл сессию кодом закрытия.
type CloseError struct {
	Code   int
	Reason string
}

func (e *CloseError) Error() string {
	return fmt.Sprintf("link: закрыто %d %s", e.Code, e.Reason)
}

// readLimit — предел входящего сообщения (снимок состояния может быть большим).
const readLimit = 16 << 20

// writeTimeout — запись одного сообщения.
const writeTimeout = 10 * time.Second

// wsConn — WebSocket-транспорт.
type wsConn struct{ c *websocket.Conn }

func dialWS(ctx context.Context, client *http.Client, serverURL, auth string) (conn, error) {
	url := "ws" + strings.TrimPrefix(strings.TrimRight(serverURL, "/"), "http") + message.LinkPath
	header := http.Header{}
	header.Set("Authorization", auth)
	c, resp, err := websocket.Dial(ctx, url, &websocket.DialOptions{
		HTTPClient:   client,
		HTTPHeader:   header,
		Subprotocols: []string{message.WSChannel},
	})
	if err != nil {
		if resp != nil {
			return nil, &DialError{Status: resp.StatusCode, Err: err, RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"))}
		}
		return nil, err
	}
	c.SetReadLimit(readLimit)
	return &wsConn{c: c}, nil
}

func (w *wsConn) Mode() string { return "ws" }

func (w *wsConn) Send(ctx context.Context, env message.Envelope) error {
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, writeTimeout)
	defer cancel()
	return w.c.Write(ctx, websocket.MessageText, raw)
}

func (w *wsConn) Recv(ctx context.Context) (message.Envelope, error) {
	_, raw, err := w.c.Read(ctx)
	if err != nil {
		if code := websocket.CloseStatus(err); code != -1 {
			var ce websocket.CloseError
			reason := ""
			if errors.As(err, &ce) {
				reason = ce.Reason
			}
			return message.Envelope{}, &CloseError{Code: int(code), Reason: reason}
		}
		return message.Envelope{}, err
	}
	var env message.Envelope
	if err := json.Unmarshal(raw, &env); err != nil {
		return message.Envelope{}, fmt.Errorf("link: сообщение не JSON: %w", err)
	}
	return env, nil
}

func (w *wsConn) Ping(ctx context.Context) error { return w.c.Ping(ctx) }

func (w *wsConn) Close(code int, reason string) {
	_ = w.c.Close(websocket.StatusCode(code), reason)
}
