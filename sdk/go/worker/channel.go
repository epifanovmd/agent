package worker

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// requestTimeout — ответ агента на запрос (job.urls) не дольше этого: агент сам
// ждёт сервер до 30 с.
const requestTimeout = 45 * time.Second

// maxLine — предел строки канала (§10): длиннее агент не примет.
const maxLine = 16 << 20

// errClosed — канал с агентом закрыт.
var errClosed = errors.New("worker: канал с агентом закрыт")

// errTooLarge — сообщение длиннее maxLine: не отправлено, канал цел.
var errTooLarge = errors.New("worker: сообщение больше 16 МБ")

// channel — канал IPC: по строке JSON на конверт (§10), отправка из любых
// горутин, ответы на запросы — по re.
type channel struct {
	conn    io.ReadWriteCloser
	scanner *bufio.Scanner

	wmu sync.Mutex

	mu      sync.Mutex
	pending map[string]chan message.Envelope
	closed  bool
}

func newChannel(conn io.ReadWriteCloser) *channel {
	sc := bufio.NewScanner(conn)
	sc.Buffer(make([]byte, 64*1024), maxLine)
	return &channel{conn: conn, scanner: sc, pending: map[string]chan message.Envelope{}}
}

// connFromEnv — канал, унаследованный от агента (AGENT_IPC_FD).
func connFromEnv() (io.ReadWriteCloser, error) {
	raw := os.Getenv("AGENT_IPC_FD")
	if raw == "" {
		return nil, errors.New("worker: воркер запускается агентом (AGENT_IPC_FD не задан): опишите его в workers конфигурации агента")
	}
	fd, err := strconv.Atoi(raw)
	if err != nil {
		return nil, fmt.Errorf("worker: AGENT_IPC_FD=%q: %w", raw, err)
	}
	f := os.NewFile(uintptr(fd), "agent-ipc")
	if f == nil {
		return nil, fmt.Errorf("worker: дескриптор %d недоступен", fd)
	}
	defer f.Close()
	conn, err := net.FileConn(f)
	if err != nil {
		return nil, fmt.Errorf("worker: канал IPC: %w", err)
	}
	return conn, nil
}

func (c *channel) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

// send — конверт агенту.
func (c *channel) send(env message.Envelope) error {
	if c.isClosed() {
		return errClosed
	}
	if env.TS == 0 {
		env.TS = time.Now().UnixMilli()
	}
	raw, err := json.Marshal(env)
	if err != nil {
		return err
	}
	if len(raw)+1 > maxLine {
		return errTooLarge
	}
	c.wmu.Lock()
	defer c.wmu.Unlock()
	_, err = c.conn.Write(append(raw, '\n'))
	return err
}

// message — отправить сообщение типа typ с данными data.
func (c *channel) message(typ string, data any, re string) error {
	env, err := message.New(typ, data)
	if err != nil {
		return err
	}
	env.Re = re
	return c.send(env)
}

// request — запрос с ответом по re; ответ error — *message.Error.
func (c *channel) request(ctx context.Context, typ string, data any) (message.Envelope, error) {
	env, err := message.New(typ, data)
	if err != nil {
		return message.Envelope{}, err
	}
	env.ID = message.NewID()
	reply := make(chan message.Envelope, 1)
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return message.Envelope{}, errClosed
	}
	c.pending[env.ID] = reply
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.pending, env.ID)
		c.mu.Unlock()
	}()
	if err := c.send(env); err != nil {
		return message.Envelope{}, err
	}
	timer := time.NewTimer(requestTimeout)
	defer timer.Stop()
	select {
	case r, ok := <-reply:
		if !ok {
			return message.Envelope{}, errClosed
		}
		if r.Type == message.TypeError {
			var e message.Error
			if err := r.Decode(&e); err != nil {
				return message.Envelope{}, err
			}
			return message.Envelope{}, &e
		}
		return r, nil
	case <-timer.C:
		return message.Envelope{}, &message.Error{Code: "TIMEOUT", Message: "нет ответа агента на " + typ, Retryable: true}
	case <-ctx.Done():
		return message.Envelope{}, ctx.Err()
	}
}

// next — следующее сообщение агента (ответы на запросы разбираются здесь).
// Ошибка — канал закрыт.
func (c *channel) next() (message.Envelope, error) {
	for c.scanner.Scan() {
		var env message.Envelope
		if json.Unmarshal(c.scanner.Bytes(), &env) != nil {
			continue
		}
		if env.Re != "" && c.resolve(env) {
			continue
		}
		return env, nil
	}
	c.close()
	if err := c.scanner.Err(); err != nil {
		return message.Envelope{}, err
	}
	return message.Envelope{}, io.EOF
}

func (c *channel) resolve(env message.Envelope) bool {
	c.mu.Lock()
	reply, ok := c.pending[env.Re]
	if ok {
		delete(c.pending, env.Re)
	}
	c.mu.Unlock()
	if ok {
		reply <- env
	}
	return ok
}

// close — закрыть канал; ждущие запросы получают errClosed.
func (c *channel) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	pending := c.pending
	c.pending = map[string]chan message.Envelope{}
	c.mu.Unlock()
	for _, reply := range pending {
		close(reply)
	}
	_ = c.conn.Close()
}
