package worker

import (
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"unicode/utf8"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// chunkMax — кусок вывода в одном cmd.output (по спецификации — не больше 64 КБ).
const chunkMax = 64 * 1024

// Command — команда, переданная воркеру агентом (cmd.run). Вывод (Write,
// fmt.Fprintf) уходит на сервер по мере записи; срок истёк (cmd.cancel) — ctx
// обработчика отменён, итог не отправляется.
type Command struct {
	ID         string
	Name       string
	Args       json.RawMessage
	TimeoutSec int

	w         *Worker
	ctx       context.Context
	cancel    context.CancelFunc
	cancelled atomic.Bool

	mu      sync.Mutex
	partial []byte // незаконченный символ UTF-8 между вызовами Write
}

func newCommand(w *Worker, run message.CommandRun) *Command {
	ctx, cancel := context.WithCancel(w.ctx)
	return &Command{
		ID: run.CommandID, Name: run.Name, Args: run.Args, TimeoutSec: run.TimeoutSec,
		w: w, ctx: ctx, cancel: cancel,
	}
}

// Write — вывод команды потоком (io.Writer): куски ≤ 64 КБ по границам символов.
func (c *Command) Write(p []byte) (int, error) {
	if c.cancelled.Load() {
		return 0, context.Canceled
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	data := append(c.partial, p...)
	end := len(data)
	// Хвост — начало многобайтного символа: дождаться продолжения.
	for k := 1; k <= utf8.UTFMax-1 && k <= len(data); k++ {
		if utf8.RuneStart(data[len(data)-k]) {
			if !utf8.FullRune(data[len(data)-k:]) {
				end = len(data) - k
			}
			break
		}
	}
	c.partial = append([]byte(nil), data[end:]...)
	if err := c.emit(data[:end]); err != nil {
		return 0, err
	}
	return len(p), nil
}

// emit — куски не больше chunkMax, не разрезая символы. Под c.mu.
func (c *Command) emit(data []byte) error {
	for len(data) > 0 {
		n := len(data)
		if n > chunkMax {
			n = chunkMax
			for n > 0 && !utf8.RuneStart(data[n]) {
				n--
			}
			if n == 0 {
				n = chunkMax
			}
		}
		if err := c.w.send(message.TypeCmdOutput, message.CommandOutput{CommandID: c.ID, Chunk: string(data[:n])}, ""); err != nil {
			return err
		}
		data = data[n:]
	}
	return nil
}

// flush — остаток вывода (недописанный символ) перед итогом.
func (c *Command) flush() {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.partial) > 0 && !c.cancelled.Load() {
		_ = c.emit(c.partial)
	}
	c.partial = nil
}

// abort — cmd.cancel: прервать, итог не отправлять.
func (c *Command) abort() {
	c.cancelled.Store(true)
	c.cancel()
}
