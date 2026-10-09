// Package fetch — запрос сервера к воркеру (§7): HTTP по unix-сокету
// воркера, ответ кусками fetch.head → fetch.chunk… → fetch.end в том же
// соединении, сроки, отмена, пределы.
package fetch

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"path"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/worker"
)

// Workers — воркеры агента.
type Workers interface {
	Has(name string) bool
	Client(name string) (*http.Client, error)
}

// Session — соединение, в котором пришёл запрос: ответ уходит только в него.
type Session interface {
	Send(env message.Envelope) error
	Context() context.Context
}

// Tunnel — запросы к воркерам, идущие сейчас.
type Tunnel struct {
	workers Workers
	log     *slog.Logger

	mu     sync.Mutex
	active map[key]context.CancelCauseFunc
}

type key struct {
	s  Session
	id string
}

var (
	errCancelled = errors.New("запрос отменён сервером")
	errTimeout   = errors.New("срок запроса истёк")
)

// New — туннель к воркерам.
func New(workers Workers, log *slog.Logger) *Tunnel {
	return &Tunnel{workers: workers, log: log, active: map[key]context.CancelCauseFunc{}}
}

// Forbidden — служебный путь воркера, который сервер вызывать не может.
func Forbidden(p string) bool {
	u, err := url.Parse(p)
	if err != nil {
		return true
	}
	clean := path.Clean("/" + u.Path)
	return clean == message.MetricsPath || clean == message.HealthPath || clean == message.CleanupPath ||
		clean == strings.TrimSuffix(message.ConfigPathPrefix, "/") || strings.HasPrefix(clean, message.ConfigPathPrefix)
}

// Handle — сообщение fetch: запрос к воркеру в отдельной горутине.
func (t *Tunnel) Handle(s Session, env message.Envelope) {
	if env.ID == "" {
		t.log.Warn("fetch без id — пропущен")
		return
	}
	end := func(code, msg string) {
		reply(s, env.ID, message.TypeFetchEnd, message.FetchEnd{Error: message.NewError(code, msg)})
	}
	var f message.Fetch
	if err := env.Decode(&f); err != nil || f.Method == "" || !strings.HasPrefix(f.Path, "/") {
		end(message.CodeMessageInvalid, "нужны worker, method и path от «/»")
		return
	}
	if !t.workers.Has(f.Worker) {
		end(message.CodeWorkerUnknown, fmt.Sprintf("воркера %q нет в настройках агента", f.Worker))
		return
	}
	if Forbidden(f.Path) {
		end(message.CodePathForbidden, "служебный путь воркера: "+f.Path)
		return
	}
	body := []byte(f.Body)
	if f.Encoding == message.EncodingBase64 {
		raw, err := base64.StdEncoding.DecodeString(f.Body)
		if err != nil {
			end(message.CodeMessageInvalid, "body: не base64")
			return
		}
		body = raw
	}
	if len(body) > message.MaxFetchRequestBytes {
		end(message.CodeBodyTooLarge, "тело запроса больше 4 МБ")
		return
	}
	timeout := message.DefaultFetchTimeout
	if f.TimeoutMs > 0 {
		timeout = min(time.Duration(f.TimeoutMs)*time.Millisecond, message.MaxFetchTimeout)
	}
	k := key{s: s, id: env.ID}
	ctx, cancel := context.WithCancelCause(s.Context())
	t.mu.Lock()
	if _, dup := t.active[k]; dup {
		t.mu.Unlock()
		cancel(nil)
		return // повтор того же запроса, пока он идёт
	}
	if len(t.active) >= message.MaxFetches {
		t.mu.Unlock()
		cancel(nil)
		end(message.CodeBusy, fmt.Sprintf("больше %d запросов сразу", message.MaxFetches))
		return
	}
	t.active[k] = cancel
	t.mu.Unlock()
	go func() {
		defer func() {
			t.mu.Lock()
			delete(t.active, k)
			t.mu.Unlock()
			cancel(nil)
		}()
		tctx, stop := context.WithTimeoutCause(ctx, timeout, errTimeout)
		defer stop()
		t.run(tctx, s, env.ID, f, body)
	}()
}

// Cancel — fetch.cancel: прервать запрос re этого соединения.
func (t *Tunnel) Cancel(s Session, re string) {
	t.mu.Lock()
	cancel := t.active[key{s: s, id: re}]
	t.mu.Unlock()
	if cancel != nil {
		cancel(errCancelled)
	}
}

func reply(s Session, re, typ string, data any) bool {
	env := message.MustNew(typ, data)
	env.Re = re
	return s.Send(env) == nil
}

// failure — код ошибки по причине прерывания; "" — соединение закрыто,
// отвечать некому.
func failure(ctx context.Context, s Session, err error) (string, string) {
	switch cause := context.Cause(ctx); {
	case s.Context().Err() != nil:
		return "", ""
	case errors.Is(cause, errCancelled):
		return message.CodeCancelled, "запрос отменён"
	case errors.Is(cause, errTimeout):
		return message.CodeTimeout, "срок запроса истёк"
	}
	return message.CodeWorkerUnavailable, err.Error()
}

func (t *Tunnel) run(ctx context.Context, s Session, id string, f message.Fetch, body []byte) {
	end := func(code, msg string) {
		if code != "" {
			reply(s, id, message.TypeFetchEnd, message.FetchEnd{Error: message.NewError(code, msg)})
		}
	}
	client, err := t.workers.Client(f.Worker)
	switch {
	case errors.Is(err, worker.ErrInvalid):
		end(message.CodeWorkerInvalid, fmt.Sprintf("воркер %s: %v", f.Worker, err))
		return
	case err != nil:
		end(message.CodeWorkerUnavailable, fmt.Sprintf("воркер %s не запущен", f.Worker))
		return
	}
	req, err := http.NewRequestWithContext(ctx, f.Method, "http://worker"+f.Path, bytes.NewReader(body))
	if err != nil {
		end(message.CodeMessageInvalid, err.Error())
		return
	}
	for name, value := range f.Headers {
		req.Header.Set(name, value)
	}
	resp, err := client.Do(req)
	if err != nil {
		end(failure(ctx, s, err))
		return
	}
	defer resp.Body.Close()
	head := message.FetchHead{Status: resp.StatusCode, Headers: map[string]string{}}
	for name, values := range resp.Header {
		head.Headers[strings.ToLower(name)] = strings.Join(values, ", ")
	}
	if !reply(s, id, message.TypeFetchHead, head) {
		return
	}
	buf := make([]byte, message.MaxChunkBytes)
	carry, total := 0, 0
	for {
		n, err := resp.Body.Read(buf[carry:])
		n += carry
		carry = 0
		data := buf[:n]
		if err == nil {
			// Неполный символ UTF-8 в конце куска — в следующий кусок.
			if cut := partialRune(data); cut > 0 && utf8.Valid(data[:n-cut]) {
				data = data[:n-cut]
				carry = cut
			}
		}
		if len(data) > 0 {
			total += len(data)
			if total > message.MaxFetchBodyBytes {
				end(message.CodeBodyTooLarge, "ответ воркера больше 32 МБ")
				return
			}
			chunk := message.FetchChunk{Data: string(data), Encoding: message.EncodingUTF8}
			if !utf8.Valid(data) {
				chunk = message.FetchChunk{Data: base64.StdEncoding.EncodeToString(data), Encoding: message.EncodingBase64}
			}
			if !reply(s, id, message.TypeFetchChunk, chunk) {
				return
			}
		}
		if carry > 0 {
			copy(buf, buf[n-carry:n])
		}
		switch {
		case errors.Is(err, io.EOF):
			reply(s, id, message.TypeFetchEnd, message.FetchEnd{})
			return
		case err != nil:
			end(failure(ctx, s, err))
			return
		}
	}
}

// partialRune — сколько байт в конце b — начало символа UTF-8, которому не
// хватает продолжения (0 — нет такого).
func partialRune(b []byte) int {
	for i := 1; i <= utf8.UTFMax-1 && i <= len(b); i++ {
		c := b[len(b)-i]
		if c < 0x80 {
			return 0
		}
		if c >= 0xC0 { // первый байт символа
			need := 2
			switch {
			case c >= 0xF0:
				need = 4
			case c >= 0xE0:
				need = 3
			}
			if need > i {
				return i
			}
			return 0
		}
	}
	return 0
}
