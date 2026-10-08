// Package commands — возможность `commands`: белый список команд агента.
// Команда принимается (cmd.accept), её вывод уходит потоком (cmd.output),
// итог — надёжно (cmd.done). Повторная доставка той же команды не выполняется.
// Одновременно выполняется не больше MaxConcurrent команд; cmd.cancel
// прерывает команду (итог CANCELLED).
package commands

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Sender — канал к серверу.
type Sender interface {
	Stream(typ string, data any)
	Reliable(typ string, data any) error
}

// Handler — команда: аргументы, вывод (уходит на сервер по мере записи),
// результат или ошибка.
type Handler func(ctx context.Context, args json.RawMessage, out io.Writer) (any, error)

// Error — ошибка команды с кодом.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Errorf — ошибка команды с кодом.
func Errorf(code, format string, args ...any) *Error {
	return &Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// seenTTL — сколько помнить выполненные команды (дедупликация повторной доставки).
const seenTTL = time.Hour

// outputFlush — вывод копится и уходит кусками не реже этого интервала.
const outputFlush = 250 * time.Millisecond

const chunkMax = 64 * 1024

// DefaultMaxConcurrent — команд одновременно по умолчанию.
const DefaultMaxConcurrent = 8

// lateGrace — сколько после срока или отмены ждать, пока обработчик вернётся,
// прежде чем отправить итог без него.
var lateGrace = 5 * time.Second

// errCancelled — причина отмены контекста команды по cmd.cancel.
var errCancelled = errors.New("команда отменена сервером")

// Registry — команды агента.
type Registry struct {
	sender   Sender
	log      *slog.Logger
	mu       sync.Mutex
	handlers map[string]Handler
	seen     map[string]time.Time
	// running — выполняющиеся и ждущие очереди команды: commandId → отмена.
	running map[string]context.CancelCauseFunc
	// slots — места для одновременных команд; limit — их число.
	slots chan struct{}
	limit int
}

// New — пустой реестр.
func New(sender Sender, log *slog.Logger) *Registry {
	return &Registry{sender: sender, log: log, handlers: map[string]Handler{}, seen: map[string]time.Time{},
		running: map[string]context.CancelCauseFunc{}, slots: make(chan struct{}, DefaultMaxConcurrent), limit: DefaultMaxConcurrent}
}

// SetMaxConcurrent — команд одновременно не больше n (остальные ждут); для
// команд, принятых после вызова.
func (r *Registry) SetMaxConcurrent(n int) {
	if n <= 0 {
		n = DefaultMaxConcurrent
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if n != r.limit {
		r.slots, r.limit = make(chan struct{}, n), n
	}
}

// Register — добавить команду.
func (r *Registry) Register(name string, h Handler) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.handlers[name] = h
}

// Unregister — убрать команду: агент её больше не объявляет и не выполняет
// (COMMAND_UNKNOWN).
func (r *Registry) Unregister(name string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.handlers, name)
}

// Names — имена команд по алфавиту.
func (r *Registry) Names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	names := make([]string, 0, len(r.handlers))
	for name := range r.handlers {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// Has — команда зарегистрирована.
func (r *Registry) Has(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	_, ok := r.handlers[name]
	return ok
}

type idKey struct{}

// ID — id выполняемой команды (commandId сервера) из контекста обработчика.
func ID(ctx context.Context) string {
	id, _ := ctx.Value(idKey{}).(string)
	return id
}

func (r *Registry) Declare(caps *message.Capabilities) {
	caps.Commands = &message.CommandsCapability{Names: r.Names()}
}

func (r *Registry) Handles() []string { return []string{message.TypeCmdRun, message.TypeCmdCancel} }

func (r *Registry) Handle(_ context.Context, env message.Envelope) error {
	if env.Type == message.TypeCmdCancel {
		var ref message.CommandRef
		if err := env.Decode(&ref); err != nil {
			return err
		}
		r.mu.Lock()
		cancel := r.running[ref.CommandID]
		r.mu.Unlock()
		if cancel != nil {
			r.log.Info("команда отменена сервером", "commandId", ref.CommandID)
			cancel(errCancelled)
		}
		return nil
	}
	var run message.CommandRun
	if err := env.Decode(&run); err != nil {
		return err
	}
	r.mu.Lock()
	now := time.Now()
	for id, at := range r.seen {
		if now.Sub(at) > seenTTL {
			delete(r.seen, id)
		}
	}
	if _, dup := r.seen[run.CommandID]; dup {
		r.mu.Unlock()
		return nil
	}
	r.seen[run.CommandID] = now
	h := r.handlers[run.Name]
	timeout := time.Duration(run.TimeoutSec) * time.Second
	if timeout <= 0 {
		timeout = time.Minute
	}
	base, cancel := context.WithCancelCause(context.WithValue(context.Background(), idKey{}, run.CommandID))
	ctx, stop := context.WithTimeout(base, timeout)
	r.running[run.CommandID] = cancel
	slots := r.slots
	r.mu.Unlock()

	r.sender.Stream(message.TypeCmdAccept, message.CommandRef{CommandID: run.CommandID})
	go func() {
		defer func() {
			stop()
			cancel(nil)
			r.mu.Lock()
			delete(r.running, run.CommandID)
			r.mu.Unlock()
		}()
		r.execute(ctx, slots, run, h)
	}()
	return nil
}

// execute — дождаться места, выполнить и отправить итог. Срок или отмена —
// итог TIMEOUT или CANCELLED, даже если обработчик не вернулся за lateGrace.
func (r *Registry) execute(ctx context.Context, slots chan struct{}, run message.CommandRun, h Handler) {
	out := newOutput(r.sender, run.CommandID)
	var result any
	var err error
	select {
	case slots <- struct{}{}:
		defer func() { <-slots }()
		if h == nil {
			err = Errorf("COMMAND_UNKNOWN", "Команда %s не поддерживается", run.Name)
			break
		}
		type ret struct {
			result any
			err    error
		}
		done := make(chan ret, 1)
		go func() {
			res, err := safe(ctx, h, run.Args, out)
			done <- ret{res, err}
		}()
		select {
		case res := <-done:
			result, err = res.result, res.err
		case <-ctx.Done():
			select {
			case res := <-done:
				result, err = res.result, res.err
			case <-time.After(lateGrace):
				r.log.Warn("команда не вернулась после срока или отмены — итог без неё", "name", run.Name)
			}
		}
	case <-ctx.Done():
		// Не дождалась места.
	}
	out.Close()
	if ctx.Err() != nil {
		if errors.Is(context.Cause(ctx), errCancelled) {
			err = Errorf(message.ErrCancelled, "Команда отменена")
		} else if err == nil || !errors.Is(err, context.DeadlineExceeded) {
			err = context.DeadlineExceeded
		}
		result = nil
	}

	done := message.CommandDone{CommandID: run.CommandID, OK: err == nil}
	if err != nil {
		var ce *Error
		switch {
		case errors.As(err, &ce):
			done.Error = &message.CommandError{Code: ce.Code, Message: ce.Message}
		case errors.Is(err, context.DeadlineExceeded):
			done.Error = &message.CommandError{Code: "TIMEOUT", Message: "Команда не уложилась в таймаут"}
		default:
			done.Error = &message.CommandError{Code: "COMMAND_FAILED", Message: err.Error()}
		}
		r.log.Warn("команда завершилась ошибкой", "name", run.Name, "err", err)
	} else if result != nil {
		done.Result, _ = json.Marshal(result)
	}
	if err := r.sender.Reliable(message.TypeCmdDone, done); err != nil {
		r.log.Error("итог команды не записан", "err", err)
	}
}

func safe(ctx context.Context, h Handler, args json.RawMessage, out io.Writer) (result any, err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("паника команды: %v", p)
		}
	}()
	return h(ctx, args, out)
}

// output — вывод команды кусками в cmd.output.
type output struct {
	sender Sender
	id     string
	mu     sync.Mutex
	buf    []byte
	timer  *time.Timer
	closed bool
}

func newOutput(sender Sender, id string) *output { return &output{sender: sender, id: id} }

func (o *output) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.closed {
		return 0, io.ErrClosedPipe
	}
	o.buf = append(o.buf, p...)
	for len(o.buf) >= chunkMax {
		o.flushLocked(runeBoundary(o.buf, chunkMax))
	}
	if len(o.buf) > 0 && o.timer == nil {
		o.timer = time.AfterFunc(outputFlush, func() {
			o.mu.Lock()
			defer o.mu.Unlock()
			o.timer = nil
			o.flushLocked(len(o.buf))
		})
	}
	return len(p), nil
}

// runeBoundary — не больше n байт b без разрыва символа UTF-8.
func runeBoundary(b []byte, n int) int {
	if n >= len(b) {
		return len(b)
	}
	for k := n; k > n-utf8.UTFMax && k > 0; k-- {
		if utf8.RuneStart(b[k]) {
			return k
		}
	}
	return n
}

func (o *output) flushLocked(n int) {
	if n == 0 {
		return
	}
	chunk := string(o.buf[:n])
	o.buf = o.buf[n:]
	o.sender.Stream(message.TypeCmdOutput, message.CommandOutput{CommandID: o.id, Chunk: chunk})
}

// Close — дослать остаток до итога.
func (o *output) Close() {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.timer != nil {
		o.timer.Stop()
		o.timer = nil
	}
	o.flushLocked(len(o.buf))
	o.closed = true
}
