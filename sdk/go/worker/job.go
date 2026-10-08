package worker

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

const (
	// progressInterval — прогресс и лог уходят агенту не чаще этого.
	progressInterval = 500 * time.Millisecond
	// logBatch — строк лога в одном job.progress.
	logBatch = 100
	// uploadAttempts — попыток загрузки выходного файла (следующие — со свежей ссылкой).
	uploadAttempts = 4
	// urlRefreshMargin — ссылку, истекающую раньше этого, обновить до использования.
	urlRefreshMargin = time.Minute
)

// uploadRetryDelay — пауза перед повтором загрузки (растёт с номером попытки).
var uploadRetryDelay = 5 * time.Second

// httpClient — клиент файлов задач (таймаут — через ctx): системные корни
// плюс CA сервера из AGENT_SERVER_CA_FILE (агент задаёт его по server.caFile).
var httpClient = sync.OnceValues(func() (*http.Client, error) {
	return newHTTPClient(os.Getenv("AGENT_SERVER_CA_FILE"))
})

// newHTTPClient — клиент, доверяющий системным корням и сертификатам PEM из
// caFile (пусто — только системным).
func newHTTPClient(caFile string) (*http.Client, error) {
	if caFile == "" {
		return &http.Client{}, nil
	}
	pem, err := os.ReadFile(caFile)
	if err != nil {
		return nil, fmt.Errorf("AGENT_SERVER_CA_FILE: %w", err)
	}
	pool, err := x509.SystemCertPool()
	if err != nil || pool == nil {
		pool = x509.NewCertPool()
	}
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("AGENT_SERVER_CA_FILE: в %s нет сертификатов PEM", caFile)
	}
	tr := http.DefaultTransport.(*http.Transport).Clone()
	tr.TLSClientConfig = &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}
	return &http.Client{Transport: tr}, nil
}

// Job — задача, выданная воркеру агентом (job.assign). Прогресс, лог и
// события уходят агенту; связь с сервером, повторы и досылка — его забота.
type Job struct {
	ID           string
	Queue        string
	Data         json.RawMessage
	Attempt      int
	LeaseSeconds int

	w         *Worker
	ctx       context.Context
	cancel    context.CancelFunc
	cancelled atomic.Bool
	stop      chan struct{}
	stopOnce  sync.Once

	mu           sync.Mutex
	inputs       map[string]string
	outputs      map[string]message.OutputURL
	urlsExpireAt int64
	progress     *float64
	text         *string
	log          []string
	lastSent     time.Time
	timer        *time.Timer
	eventMu      sync.Mutex // порядок событий и их seq
	eventSeq     int64
	tmpDir       string
	closed       bool
}

func newJob(w *Worker, a message.JobAssign) *Job {
	ctx, cancel := context.WithCancel(w.ctx)
	j := &Job{
		ID: a.JobID, Queue: a.Queue, Data: a.Data, Attempt: a.Attempt, LeaseSeconds: a.LeaseSeconds,
		w: w, ctx: ctx, cancel: cancel, stop: make(chan struct{}),
		inputs: maps.Clone(a.Inputs), outputs: maps.Clone(a.Outputs), urlsExpireAt: a.URLsExpireAt,
	}
	if j.inputs == nil {
		j.inputs = map[string]string{}
	}
	if j.outputs == nil {
		j.outputs = map[string]message.OutputURL{}
	}
	return j
}

func (j *Job) ref() message.JobRef { return message.JobRef{JobID: j.ID, Attempt: j.Attempt} }

// StopRequested — закрывается, когда задачу просят завершить досрочно
// (job.stop): довести шаг и вернуть результат как обычно.
func (j *Job) StopRequested() <-chan struct{} { return j.stop }

// Progress — прогресс 0..1 и (необязательно) что делается сейчас. Частые
// вызовы схлопываются: агенту — не чаще 2 раз в секунду.
func (j *Job) Progress(value float64, text ...string) {
	value = min(max(value, 0), 1)
	j.mu.Lock()
	j.progress = &value
	if len(text) > 0 {
		t := truncate(text[0], 200)
		j.text = &t
	}
	j.mu.Unlock()
	j.schedule(false)
}

// Log — строка лога задачи (хвост виден на сервере).
func (j *Job) Log(line string) {
	j.mu.Lock()
	j.log = append(j.log, truncate(line, 1000))
	urgent := len(j.log) >= logBatch
	j.mu.Unlock()
	j.schedule(urgent)
}

// Event — доменное событие задачи (итог этапа и т. п.): надёжно и по
// порядку, seq нумерует SDK с 1 в пределах попытки. Событие больше 16 МБ не
// отправляется (ошибка и запись в лог).
func (j *Job) Event(typ string, data any) error {
	var raw json.RawMessage
	if data != nil {
		var err error
		if raw, err = json.Marshal(data); err != nil {
			return fmt.Errorf("worker: событие задачи %s: %w", typ, err)
		}
	}
	j.flush()
	j.eventMu.Lock()
	defer j.eventMu.Unlock()
	seq := j.eventSeq + 1
	err := j.w.send(message.TypeJobEvent, message.JobEvent{JobRef: j.ref(), Seq: seq, Type: truncate(typ, 50), Data: raw}, "")
	if err == nil {
		j.eventSeq = seq // не отправленное (больше 16 МБ) номер не занимает
	}
	return err
}

// Inputs — имена входных файлов.
func (j *Job) Inputs() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Sorted(maps.Keys(j.inputs))
}

// Outputs — имена выходных файлов.
func (j *Job) Outputs() []string {
	j.mu.Lock()
	defer j.mu.Unlock()
	return slices.Sorted(maps.Keys(j.outputs))
}

// InputPath — входной файл во временном каталоге задачи (скачивается один
// раз; каталог удаляется по завершении задачи). Имя файла в каталоге — только
// последняя часть name: каталоги и «..» отбрасываются.
func (j *Job) InputPath(ctx context.Context, name string) (string, error) {
	base, err := baseName(name)
	if err != nil {
		return "", err
	}
	if _, ok := j.inputURL(name); !ok {
		return "", fmt.Errorf("worker: нет входного файла %q", name)
	}
	j.mu.Lock()
	if j.tmpDir == "" {
		dir, err := os.MkdirTemp("", "job-"+safePrefix(j.ID)+"-")
		if err != nil {
			j.mu.Unlock()
			return "", err
		}
		j.tmpDir = dir
	}
	target := filepath.Join(j.tmpDir, "inputs", base)
	j.mu.Unlock()
	if _, err := os.Stat(target); err == nil {
		return target, nil
	}
	return target, j.Download(ctx, name, target)
}

// Download — скачать входной файл в path атомарно; ссылка истекла — свежая
// (job.urls) и ещё раз.
func (j *Job) Download(ctx context.Context, name, path string) error {
	if _, ok := j.inputURL(name); !ok {
		return fmt.Errorf("worker: нет входного файла %q", name)
	}
	if j.expiring() {
		if err := j.RefreshURLs(ctx, []string{name}, nil); err != nil {
			j.w.log.Warn("свежая ссылка не получена", "job", j.ID, "input", name, "err", err)
		}
	}
	url, _ := j.inputURL(name)
	err := download(ctx, url, path)
	if err == nil || ctx.Err() != nil {
		return err
	}
	j.w.log.Warn("скачивание не удалось — повтор со свежей ссылкой", "job", j.ID, "input", name, "err", err)
	if rerr := j.RefreshURLs(ctx, []string{name}, nil); rerr != nil {
		return errors.Join(err, rerr)
	}
	url, _ = j.inputURL(name)
	return download(ctx, url, path)
}

// Upload — загрузить выходной файл (PUT по подписанной ссылке). Сбой — повтор
// со свежей ссылкой (job.urls); после 4 попыток — ошибка.
func (j *Job) Upload(ctx context.Context, name string, data []byte) error {
	return j.upload(ctx, name, func() (io.ReadCloser, int64, error) {
		return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
	})
}

// UploadFile — Upload из файла.
func (j *Job) UploadFile(ctx context.Context, name, path string) error {
	return j.upload(ctx, name, func() (io.ReadCloser, int64, error) {
		f, err := os.Open(path)
		if err != nil {
			return nil, 0, err
		}
		st, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, 0, err
		}
		return f, st.Size(), nil
	})
}

func (j *Job) upload(ctx context.Context, name string, open func() (io.ReadCloser, int64, error)) error {
	if _, ok := j.outputURL(name); !ok {
		return fmt.Errorf("worker: нет выходного файла %q", name)
	}
	if j.expiring() {
		if err := j.RefreshURLs(ctx, nil, []string{name}); err != nil {
			j.w.log.Warn("свежая ссылка не получена", "job", j.ID, "output", name, "err", err)
		}
	}
	var err error
	for attempt := 1; attempt <= uploadAttempts; attempt++ {
		target, _ := j.outputURL(name)
		if err = put(ctx, target, open); err == nil {
			return nil
		}
		if attempt == uploadAttempts || ctx.Err() != nil {
			break
		}
		j.w.log.Warn("загрузка не удалась — повтор", "job", j.ID, "output", name, "attempt", attempt, "err", err)
		select {
		case <-time.After(uploadRetryDelay * time.Duration(attempt)):
		case <-ctx.Done():
			return ctx.Err()
		}
		if rerr := j.RefreshURLs(ctx, nil, []string{name}); rerr != nil {
			j.w.log.Warn("свежая ссылка не получена", "job", j.ID, "output", name, "err", rerr)
		}
	}
	return fmt.Errorf("worker: загрузка %s: %w", name, err)
}

// RefreshURLs — свежие подписанные ссылки: перечисленные или все (nil — все).
func (j *Job) RefreshURLs(ctx context.Context, inputs, outputs []string) error {
	reply, err := j.w.ch.request(ctx, message.TypeJobURLs, message.JobURLsRequest{JobRef: j.ref(), Inputs: inputs, Outputs: outputs})
	if err != nil {
		return err
	}
	var urls message.JobURLs
	if err := reply.Decode(&urls); err != nil {
		return err
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	maps.Copy(j.inputs, urls.Inputs)
	maps.Copy(j.outputs, urls.Outputs)
	if urls.ExpiresAt > 0 {
		j.urlsExpireAt = urls.ExpiresAt
	}
	return nil
}

func (j *Job) inputURL(name string) (string, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	u, ok := j.inputs[name]
	return u, ok
}

func (j *Job) outputURL(name string) (message.OutputURL, bool) {
	j.mu.Lock()
	defer j.mu.Unlock()
	u, ok := j.outputs[name]
	return u, ok
}

func (j *Job) expiring() bool {
	j.mu.Lock()
	defer j.mu.Unlock()
	return j.urlsExpireAt > 0 && time.UnixMilli(j.urlsExpireAt).Sub(time.Now()) < urlRefreshMargin
}

// ─── жизненный цикл (Worker) ───────────────────────────────────────────

// abort — job.cancel (или новая попытка): прервать; итог обработчика не
// отправляется, когда обработчик завершится — job.fail CANCELLED (confirmCancel).
func (j *Job) abort() {
	if !j.cancelled.Swap(true) {
		j.w.log.Info("задача отменена", "job", j.ID)
	}
	j.cancel()
}

// requestStop — job.stop.
func (j *Job) requestStop() {
	j.stopOnce.Do(func() {
		j.w.log.Info("задачу просят завершить досрочно", "job", j.ID)
		close(j.stop)
	})
}

// schedule — отправить сейчас или не раньше progressInterval после прошлой.
func (j *Job) schedule(now bool) {
	j.mu.Lock()
	wait := time.Until(j.lastSent.Add(progressInterval))
	if !now && wait > 0 {
		if j.timer == nil && !j.closed {
			j.timer = time.AfterFunc(wait, j.flush)
		}
		j.mu.Unlock()
		return
	}
	j.mu.Unlock()
	j.flush()
}

// flush — накопленный прогресс и лог агенту сейчас.
func (j *Job) flush() {
	j.mu.Lock()
	if j.timer != nil {
		j.timer.Stop()
		j.timer = nil
	}
	if j.closed || j.cancelled.Load() || (j.progress == nil && j.text == nil && len(j.log) == 0) {
		j.mu.Unlock()
		return
	}
	update := message.JobProgress{JobRef: j.ref(), Progress: j.progress, Text: j.text}
	n := min(len(j.log), logBatch)
	if n > 0 {
		update.Log = slices.Clone(j.log[:n])
		j.log = j.log[n:]
	}
	j.progress, j.text = nil, nil
	j.lastSent = time.Now()
	more := len(j.log) > 0
	j.mu.Unlock()
	_ = j.w.send(message.TypeJobProgress, update, "")
	if more {
		j.flush()
	}
}

// close — задача завершена: таймер, временный каталог, ctx.
func (j *Job) close() {
	j.mu.Lock()
	j.closed = true
	if j.timer != nil {
		j.timer.Stop()
		j.timer = nil
	}
	dir := j.tmpDir
	j.mu.Unlock()
	if dir != "" {
		_ = os.RemoveAll(dir)
	}
	j.cancel()
}

// baseName — последняя часть имени файла (разделители «/» и «\»); пусто,
// «.» и «..» — ошибка.
func baseName(name string) (string, error) {
	base := name
	if i := strings.LastIndexAny(base, `/\`); i >= 0 {
		base = base[i+1:]
	}
	if base == "" || base == "." || base == ".." {
		return "", fmt.Errorf("worker: неверное имя входного файла %q", name)
	}
	return base, nil
}

func safePrefix(id string) string {
	out := make([]rune, 0, 8)
	for _, r := range id {
		if len(out) == 8 {
			break
		}
		if r == '/' || r == '\\' || r == '.' {
			r = '_'
		}
		out = append(out, r)
	}
	return string(out)
}

// ─── HTTP ──────────────────────────────────────────────────────────────

// download — GET во временный файл рядом и переименование: файл появляется
// целиком или не появляется.
func download(ctx context.Context, url, path string) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return err
	}
	client, err := httpClient()
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("GET: HTTP %d", resp.StatusCode)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.part")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	if _, err := io.Copy(tmp, resp.Body); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmp.Name(), path)
}

// put — PUT по подписанной ссылке; Content-Type — ровно подписанный.
func put(ctx context.Context, target message.OutputURL, open func() (io.ReadCloser, int64, error)) error {
	body, size, err := open()
	if err != nil {
		return err
	}
	defer body.Close()
	req, err := http.NewRequestWithContext(ctx, http.MethodPut, target.URL, body)
	if err != nil {
		return err
	}
	req.ContentLength = size
	if target.ContentType != "" {
		req.Header.Set("Content-Type", target.ContentType)
	}
	client, err := httpClient()
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("PUT: HTTP %d", resp.StatusCode)
	}
	return nil
}
