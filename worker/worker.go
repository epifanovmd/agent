// Package worker — клиент воркера на Go (docs/PROTOCOL.md, §11–§17): регистрация, поток
// желаемого и команд, итоги, ход, пульс, события и запросы к бэкенду через сокет агента.
// Пакет — удобство: воркер на любом языке реализует те же HTTP-запросы сам.
//
//	w, err := worker.New(worker.Options{Version: "1.0.0", Manifest: manifest,
//		Configure: func(ctx context.Context, key string, data json.RawMessage) error { … },
//		Commands:  map[string]worker.CommandFunc{"report.build": build}})
//	err = w.Run(ctx)
package worker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"sync"
	"time"

	"github.com/epifanovmd/agent/internal/sse"
	"github.com/epifanovmd/agent/protocol"
)

// CommandFunc — выполнение команды: итог — result (JSON) или ошибка (*protocol.ErrorInfo — с
// кодом, другая — COMMAND_FAILED). ctx отменяется по cancel от агента и по сроку команды.
type CommandFunc func(ctx context.Context, c *Call) (any, error)

// Call — команда воркеру.
type Call struct {
	ID       string
	Type     string
	Args     json.RawMessage
	Deadline time.Time
	w        *Worker
}

// Decode — args команды в v.
func (c *Call) Decode(v any) error {
	if len(c.Args) == 0 {
		return nil
	}
	if err := json.Unmarshal(c.Args, v); err != nil {
		return protocol.NewError(protocol.CodeInvalid, "args: "+err.Error())
	}
	return nil
}

// Progress — ход команды (0…1) и сообщение.
func (c *Call) Progress(ctx context.Context, progress float64, message string) error {
	return c.w.post(ctx, protocol.WorkerPathProgress, protocol.Progress{Re: c.ID, Progress: progress, Message: message}, nil)
}

// CodeCommandFailed — код ошибки команды, если CommandFunc вернула не *protocol.ErrorInfo.
const CodeCommandFailed = "COMMAND_FAILED"

// Options — воркер.
type Options struct {
	Version  string
	Manifest protocol.WorkerManifest
	// Configure — применить ключ настроек: data nil — ключ удалён. Ошибка — итог ok: false
	// (*protocol.ErrorInfo — с кодом, другая — CONFIG_REJECTED).
	Configure func(ctx context.Context, key string, data json.RawMessage) error
	// Commands — обработчики команд по типу.
	Commands map[string]CommandFunc
	// Health — самочувствие для пульса (nil — ok: true); busy и метрики — тоже здесь.
	Health func() protocol.Health
	// Socket, Token — сокет агента и токен; пусто — из AGENT_SOCKET и AGENT_WORKER_TOKEN.
	Socket string
	Token  string
	// Log — журнал клиента (nil — stderr через fmt).
	Log func(format string, args ...any)
}

// Worker — связь воркера с агентом.
type Worker struct {
	opts   Options
	client *http.Client
	stream *http.Client

	mu sync.Mutex
	// applied — версии ключей, уже применённых (повтор desired не применяет их снова).
	applied map[string]int64
	// running — идущие команды; done — итоги выполненных (последние journalSize).
	running map[string]context.CancelFunc
	done    map[string]protocol.Result
	order   []string
	// unsent — итоги, которые агент ещё не принял.
	unsent []protocol.Result
	kick   chan struct{}
}

// journalSize — сколько итогов выполненных команд помнит воркер (§15).
const journalSize = protocol.CommandJournal

// ErrNoAgent — воркер запущен не агентом (нет AGENT_SOCKET или AGENT_WORKER_TOKEN).
var ErrNoAgent = errors.New("worker: нет " + protocol.EnvAgentSocket + " или " + protocol.EnvWorkerToken + " — воркер запускает агент")

// New — воркер по opts.
func New(opts Options) (*Worker, error) {
	if opts.Socket == "" {
		opts.Socket = os.Getenv(protocol.EnvAgentSocket)
	}
	if opts.Token == "" {
		opts.Token = os.Getenv(protocol.EnvWorkerToken)
	}
	if opts.Socket == "" || opts.Token == "" {
		return nil, ErrNoAgent
	}
	if opts.Log == nil {
		opts.Log = func(format string, args ...any) { fmt.Fprintf(os.Stderr, format+"\n", args...) }
	}
	if err := (&protocol.Register{Version: opts.Version, Manifest: opts.Manifest}).Check(); err != nil {
		return nil, fmt.Errorf("worker: %w", err)
	}
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", opts.Socket)
	}}
	return &Worker{
		opts: opts, client: &http.Client{Transport: transport, Timeout: 30 * time.Second},
		stream:  &http.Client{Transport: transport},
		applied: map[string]int64{}, running: map[string]context.CancelFunc{}, done: map[string]protocol.Result{},
		kick: make(chan struct{}, 1),
	}, nil
}

// Run — работать до отмены ctx: регистрация, поток, пульс, досылка итогов; обрыв связи с
// агентом — повтор.
func (w *Worker) Run(ctx context.Context) error {
	var wg sync.WaitGroup
	defer wg.Wait()
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	wg.Add(2)
	go func() { defer wg.Done(); w.heartbeat(ctx) }()
	go func() { defer wg.Done(); w.deliver(ctx) }()
	delay := time.Second
	for ctx.Err() == nil {
		err := w.session(ctx)
		if ctx.Err() != nil {
			break
		}
		w.opts.Log("worker: связь с агентом: %v — повтор через %s", err, delay)
		select {
		case <-ctx.Done():
		case <-time.After(delay):
		}
		delay = min(delay*2, 10*time.Second)
	}
	return nil
}

// session — регистрация и поток до обрыва.
func (w *Worker) session(ctx context.Context) error {
	if err := w.post(ctx, protocol.WorkerPathRegister, protocol.Register{Version: w.opts.Version, Manifest: w.opts.Manifest}, nil); err != nil {
		return fmt.Errorf("регистрация: %w", err)
	}
	// Пульс до регистрации агент не принимает: первый — сразу после неё.
	w.beat(ctx)
	req, _ := http.NewRequestWithContext(ctx, http.MethodGet, "http://agent"+protocol.WorkerPathStream, nil)
	req.Header.Set("Authorization", protocol.AuthWorkerScheme+" "+w.opts.Token)
	resp, err := w.stream.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return responseError(resp)
	}
	// Поток молчит дольше StreamSilence — обрыв.
	alive := make(chan struct{}, 1)
	sctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		t := time.NewTimer(protocol.StreamSilence)
		defer t.Stop()
		for {
			select {
			case <-sctx.Done():
				return
			case <-alive:
				t.Reset(protocol.StreamSilence)
			case <-t.C:
				resp.Body.Close()
				return
			}
		}
	}()
	touch := func() {
		select {
		case alive <- struct{}{}:
		default:
		}
	}
	r := sse.NewReader(resp.Body)
	r.OnComment = func(string) { touch() }
	for {
		e, err := r.Next()
		if err != nil {
			if err == io.EOF {
				return errors.New("агент закрыл поток")
			}
			return err
		}
		touch()
		switch e.Type {
		case protocol.SSEDesired:
			var d protocol.WorkerDesiredEvent
			if json.Unmarshal([]byte(e.Data), &d) == nil {
				w.configure(ctx, d.Configs)
			}
		case protocol.SSECommand:
			var c protocol.Command
			if json.Unmarshal([]byte(e.Data), &c) == nil && c.ID != "" {
				w.command(ctx, c)
			}
		case protocol.SSECancel:
			var c protocol.Cancel
			if json.Unmarshal([]byte(e.Data), &c) == nil {
				w.mu.Lock()
				cancel := w.running[c.Re]
				w.mu.Unlock()
				if cancel != nil {
					cancel()
				}
			}
		}
	}
}

// configure — сойтись к настройкам: изменённые ключи применяются, пропавшие удаляются; итоги —
// одной пачкой.
func (w *Worker) configure(ctx context.Context, configs map[string]protocol.ConfigValue) {
	var out []protocol.Applied
	w.mu.Lock()
	prev := w.applied
	w.mu.Unlock()
	next := map[string]int64{}
	for key, v := range configs {
		if ver, ok := prev[key]; ok && ver == v.Version {
			next[key] = ver
			continue
		}
		a := protocol.Applied{Key: key, Version: v.Version, OK: true}
		if w.opts.Configure != nil {
			if err := w.opts.Configure(ctx, key, v.Data); err != nil {
				a.OK, a.Error = false, asError(err, protocol.CodeConfigRejected)
			}
		}
		if a.OK {
			next[key] = v.Version
		}
		out = append(out, a)
	}
	for key := range prev {
		if _, ok := configs[key]; !ok && w.opts.Configure != nil {
			if err := w.opts.Configure(ctx, key, nil); err != nil {
				w.opts.Log("worker: удаление ключа %s: %v", key, err)
			}
		}
	}
	w.mu.Lock()
	w.applied = next
	w.mu.Unlock()
	if len(out) > 0 {
		if err := w.post(ctx, protocol.WorkerPathApplied, out, nil); err != nil {
			w.opts.Log("worker: итоги настроек не отправлены: %v", err)
		}
	}
}

// command — команда: повтор идущей пропускается, выполненной — её итог снова.
func (w *Worker) command(ctx context.Context, c protocol.Command) {
	w.mu.Lock()
	if _, ok := w.running[c.ID]; ok {
		w.mu.Unlock()
		return
	}
	if res, ok := w.done[c.ID]; ok {
		w.mu.Unlock()
		w.queue(res)
		return
	}
	cctx, cancel := context.WithCancel(ctx)
	if c.Deadline > 0 {
		cctx, cancel = context.WithDeadline(ctx, time.UnixMilli(c.Deadline))
	}
	w.running[c.ID] = cancel
	w.mu.Unlock()
	go func() {
		defer cancel()
		res := protocol.Result{Re: c.ID}
		fn := w.opts.Commands[c.Type]
		var result any
		var err error
		if fn == nil {
			err = protocol.NewError(protocol.CodeUnknownCommand, fmt.Sprintf("команда %q не поддерживается", c.Type))
		} else {
			call := &Call{ID: c.ID, Type: c.Type, Args: c.Args, w: w}
			if c.Deadline > 0 {
				call.Deadline = time.UnixMilli(c.Deadline)
			}
			result, err = fn(cctx, call)
		}
		switch {
		case err != nil && cctx.Err() == context.Canceled && ctx.Err() == nil:
			res.Error = protocol.NewError(protocol.CodeCancelled, "команда отменена")
		case err != nil && cctx.Err() == context.DeadlineExceeded:
			res.Error = protocol.NewError(protocol.CodeTimeout, "срок команды истёк")
		case err != nil:
			res.Error = asError(err, CodeCommandFailed)
		default:
			res.OK = true
			if result != nil {
				raw, merr := json.Marshal(result)
				if merr != nil {
					res.OK, res.Error = false, protocol.NewError(CodeCommandFailed, "итог: "+merr.Error())
				} else {
					res.Result = raw
				}
			}
		}
		if ctx.Err() != nil {
			// Воркер останавливается: команду повторит агент после перезапуска.
			w.mu.Lock()
			delete(w.running, c.ID)
			w.mu.Unlock()
			return
		}
		w.mu.Lock()
		delete(w.running, c.ID)
		w.done[c.ID] = res
		w.order = append(w.order, c.ID)
		if len(w.order) > journalSize {
			delete(w.done, w.order[0])
			w.order = w.order[1:]
		}
		w.mu.Unlock()
		w.queue(res)
	}()
}

// queue — итог в очередь отправки.
func (w *Worker) queue(r protocol.Result) {
	w.mu.Lock()
	w.unsent = append(w.unsent, r)
	w.mu.Unlock()
	select {
	case w.kick <- struct{}{}:
	default:
	}
}

// deliver — итоги агенту, пока не примет.
func (w *Worker) deliver(ctx context.Context) {
	for {
		w.mu.Lock()
		batch := w.unsent
		w.mu.Unlock()
		if len(batch) > 0 {
			if err := w.post(ctx, protocol.WorkerPathResults, batch, nil); err != nil {
				w.opts.Log("worker: итоги не отправлены: %v — повтор", err)
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				continue
			}
			w.mu.Lock()
			w.unsent = w.unsent[len(batch):]
			w.mu.Unlock()
			continue
		}
		select {
		case <-ctx.Done():
			return
		case <-w.kick:
		}
	}
}

// beat — пульс: самочувствие из Options.Health.
func (w *Worker) beat(ctx context.Context) {
	h := protocol.Health{OK: true}
	if w.opts.Health != nil {
		h = w.opts.Health()
	}
	_ = w.post(ctx, protocol.WorkerPathHealth, h, nil)
}

// heartbeat — пульс раз в HealthInterval.
func (w *Worker) heartbeat(ctx context.Context) {
	for {
		w.beat(ctx)
		select {
		case <-ctx.Done():
			return
		case <-time.After(protocol.HealthInterval):
		}
	}
}

// Emit — событие типа typ (объявленного в манифесте); агент отвечает после записи на диск.
func (w *Worker) Emit(ctx context.Context, typ string, data any) error {
	e := protocol.WorkerEvent{ID: protocol.NewID(), Type: typ}
	if data != nil {
		raw, err := json.Marshal(data)
		if err != nil {
			return err
		}
		e.Data = raw
	}
	return w.post(ctx, protocol.WorkerPathEvents, []protocol.WorkerEvent{e}, nil)
}

// Request — запрос к бэкенду (§17): ответ data или ошибка (*protocol.ErrorInfo).
func (w *Worker) Request(ctx context.Context, typ string, data any, timeout time.Duration) (json.RawMessage, error) {
	r := protocol.Request{Type: typ, TimeoutMs: timeout.Milliseconds()}
	if data != nil {
		raw, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		r.Data = raw
	}
	var reply protocol.RequestReply
	if err := w.post(ctx, protocol.WorkerPathRequests, r, &reply); err != nil {
		return nil, err
	}
	if !reply.OK {
		if reply.Error == nil {
			reply.Error = protocol.NewError(protocol.CodeInternal, "ответ без ok")
		}
		return nil, reply.Error
	}
	return reply.Data, nil
}

// post — POST в сокет агента; ответ не 2xx — *protocol.ErrorInfo.
func (w *Worker) post(ctx context.Context, path string, body, out any) error {
	raw, err := json.Marshal(body)
	if err != nil {
		return err
	}
	client := w.client
	if path == protocol.WorkerPathRequests {
		client = w.stream
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, "http://agent"+path, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", protocol.AuthWorkerScheme+" "+w.opts.Token)
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return responseError(resp)
	}
	if out != nil {
		return json.NewDecoder(resp.Body).Decode(out)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func responseError(resp *http.Response) error {
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	var ei protocol.ErrorInfo
	if json.Unmarshal(raw, &ei) == nil && ei.Code != "" {
		return &ei
	}
	return fmt.Errorf("HTTP %d", resp.StatusCode)
}

// asError — ошибка с кодом: *protocol.ErrorInfo как есть, другая — с кодом code.
func asError(err error, code string) *protocol.ErrorInfo {
	var ei *protocol.ErrorInfo
	if errors.As(err, &ei) {
		return ei
	}
	return protocol.NewError(code, err.Error())
}
