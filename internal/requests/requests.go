// Package requests — запросы воркеров к серверу (§12): POST /requests на
// сокете агента превращается в сообщение request в текущем соединении, ответ
// сервера request.result возвращается воркеру. Запрос не хранится в outbox и
// не повторяется: нет связи — сразу AGENT_OFFLINE.
package requests

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

// Session — открытое соединение с сервером.
type Session interface {
	Send(env message.Envelope) error
	Context() context.Context
}

// Error — запрос не выполнен: HTTP-статус ответа воркеру и ошибка.
type Error struct {
	Status int
	Info   message.ErrorInfo
}

func (e *Error) Error() string { return e.Info.Code + ": " + e.Info.Message }

func fail(status int, code, msg string) *Error {
	return &Error{Status: status, Info: message.ErrorInfo{Code: code, Message: msg}}
}

// Broker — запросы, ждущие ответа сервера.
type Broker struct {
	// current — соединение сейчас; nil — связи нет.
	current func() Session

	mu      sync.Mutex
	pending map[string]chan message.RequestResult
}

// New — запросы через соединение current().
func New(current func() Session) *Broker {
	return &Broker{current: current, pending: map[string]chan message.RequestResult{}}
}

// Timeout — срок запроса: 0 — по умолчанию, больше предела — предел.
func Timeout(ms int64) time.Duration {
	if ms <= 0 {
		return message.DefaultRequestTimeout
	}
	return min(time.Duration(ms)*time.Millisecond, message.MaxRequestTimeout)
}

// Do — запрос воркера worker серверу и его ответ (data); ошибка — *Error или
// ошибка ctx (воркер закрыл соединение).
func (b *Broker) Do(ctx context.Context, worker string, p message.RequestPost) (json.RawMessage, error) {
	s := b.current()
	if s == nil {
		return nil, fail(http.StatusServiceUnavailable, message.CodeAgentOffline, "нет связи с сервером")
	}
	timeout := Timeout(p.TimeoutMs)
	id := message.NewID()
	ch := make(chan message.RequestResult, 1)
	b.mu.Lock()
	if len(b.pending) >= message.MaxRequests {
		b.mu.Unlock()
		return nil, fail(http.StatusServiceUnavailable, message.CodeBusy, fmt.Sprintf("больше %d запросов к серверу сразу", message.MaxRequests))
	}
	b.pending[id] = ch
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		delete(b.pending, id)
		b.mu.Unlock()
	}()
	env, err := message.New(message.TypeRequest, message.Request{
		Worker: worker, Type: p.Type, Data: p.Data, TimeoutMs: timeout.Milliseconds(),
	})
	if err != nil {
		return nil, fail(http.StatusBadRequest, message.CodeMessageInvalid, err.Error())
	}
	env.ID = id
	if err := s.Send(env); err != nil {
		return nil, fail(http.StatusServiceUnavailable, message.CodeDisconnected, "связь с сервером оборвалась")
	}
	timer := time.NewTimer(timeout)
	defer timer.Stop()
	select {
	case r := <-ch:
		if r.OK {
			return r.Data, nil
		}
		info := message.ErrorInfo{Code: message.CodeRequestFailed, Message: "сервер отклонил запрос"}
		if r.Error != nil && r.Error.Code != "" {
			info = *r.Error
		}
		return nil, &Error{Status: http.StatusUnprocessableEntity, Info: info}
	case <-timer.C:
		return nil, fail(http.StatusGatewayTimeout, message.CodeTimeout, fmt.Sprintf("сервер не ответил за %d мс", timeout.Milliseconds()))
	case <-s.Context().Done():
		return nil, fail(http.StatusServiceUnavailable, message.CodeDisconnected, "связь с сервером оборвалась до ответа")
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// ErrUnexpected — ответ на запрос, которого нет (опоздал или чужой).
var ErrUnexpected = errors.New("requests: ответ на неизвестный запрос")

// Result — сообщение request.result: ответ ждущему запросу.
func (b *Broker) Result(env message.Envelope) error {
	var r message.RequestResult
	if err := env.Decode(&r); err != nil {
		return err
	}
	b.mu.Lock()
	ch := b.pending[env.Re]
	delete(b.pending, env.Re)
	b.mu.Unlock()
	if ch == nil {
		return ErrUnexpected
	}
	ch <- r
	return nil
}
