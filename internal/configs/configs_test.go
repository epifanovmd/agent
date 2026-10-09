package configs

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/worker"
)

// fakeWorker — воркер на unix-сокете: PUT/DELETE /config/{key}; отклоняет
// значения с полем reject, может «зависнуть» на время.
type fakeWorker struct {
	mu      sync.Mutex
	running bool
	puts    []string // "key@version"
	deletes []string
	hold    chan struct{}
	client  *http.Client
}

func newFakeWorker(t *testing.T) *fakeWorker {
	dir, _ := os.MkdirTemp("", "c")
	t.Cleanup(func() { os.RemoveAll(dir) })
	sock := filepath.Join(dir, "w.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatal(err)
	}
	f := &fakeWorker{running: true}
	mux := http.NewServeMux()
	mux.HandleFunc("PUT /config/{key}", func(w http.ResponseWriter, r *http.Request) {
		var v struct {
			Version int64 `json:"version"`
			Data    struct {
				Reject string `json:"reject"`
				Text   bool   `json:"text"`
				// Reply — тело ответа 200 как есть.
				Reply string `json:"reply"`
			} `json:"data"`
		}
		_ = json.NewDecoder(r.Body).Decode(&v)
		f.mu.Lock()
		hold := f.hold
		f.puts = append(f.puts, r.PathValue("key")+"@"+jsonInt(v.Version))
		f.mu.Unlock()
		if hold != nil {
			<-hold
		}
		switch {
		case v.Data.Reply != "":
			_, _ = io.WriteString(w, v.Data.Reply)
		case v.Data.Text:
			w.WriteHeader(http.StatusBadRequest)
			_, _ = io.WriteString(w, "текстом")
		case v.Data.Reject != "":
			w.WriteHeader(http.StatusUnprocessableEntity)
			_ = json.NewEncoder(w).Encode(map[string]string{"message": v.Data.Reject})
		default:
			w.WriteHeader(http.StatusNoContent)
		}
	})
	mux.HandleFunc("DELETE /config/{key}", func(w http.ResponseWriter, r *http.Request) {
		f.mu.Lock()
		f.deletes = append(f.deletes, r.PathValue("key"))
		f.mu.Unlock()
		w.WriteHeader(http.StatusNotFound)
	})
	srv := &http.Server{Handler: mux}
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(func() { _ = srv.Close() })
	f.client = &http.Client{Transport: &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", sock)
	}}}
	return f
}

func jsonInt(n int64) string { raw, _ := json.Marshal(n); return string(raw) }

func (f *fakeWorker) Has(name string) bool { return name == "report" }
func (f *fakeWorker) Client(name string) (*http.Client, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if name != "report" {
		return nil, worker.ErrUnknown
	}
	if !f.running {
		return nil, worker.ErrUnavailable
	}
	return f.client, nil
}

func (f *fakeWorker) got() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]string(nil), f.puts...)
}

type reports struct {
	mu  sync.Mutex
	all []message.ConfigApplied
}

func (r *reports) add(c message.ConfigApplied) {
	r.mu.Lock()
	r.all = append(r.all, c)
	r.mu.Unlock()
}

func (r *reports) list() []message.ConfigApplied {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]message.ConfigApplied(nil), r.all...)
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("не дождались: %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func open(t *testing.T, dir string, w Workers, r *reports) *Store {
	t.Helper()
	s, err := Open(dir, w, r.add, logx.Discard())
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go s.Run(ctx)
	eventually(t, "Run", func() bool { s.mu.Lock(); defer s.mu.Unlock(); return s.ctx != nil })
	return s
}

func put(key string, version int64, data string) message.ConfigPut {
	return message.ConfigPut{Worker: "report", Key: key, Version: version, Data: json.RawMessage(data)}
}

// config.put (§8): файл 0600, PUT /config/{key} {version, data}, итог —
// config.applied один раз на «версия + итог»; версия не новее — пропуск;
// неизвестный воркер — WORKER_UNKNOWN без сохранения.
func TestPutApplyReport(t *testing.T) {
	dir := t.TempDir()
	w, r := newFakeWorker(t), &reports{}
	s := open(t, dir, w, r)
	s.Put(put("main", 41, `{"a":1}`))
	eventually(t, "применено", func() bool { return len(r.list()) == 1 })
	if c := r.list()[0]; !c.OK || c.Version != 41 || c.Key != "main" || c.Error != nil {
		t.Fatalf("config.applied: %+v", c)
	}
	info, err := os.Stat(filepath.Join(dir, "report", "main.json"))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("файл: %v %v", info, err)
	}
	if v, ok := s.Get("report", "main"); !ok || v.Version != 41 || string(v.Data) != `{"a":1}` {
		t.Fatalf("Get: %+v", v)
	}
	st := s.Status("report")["main"]
	if st.Version != 41 || st.OK == nil || !*st.OK {
		t.Fatalf("status: %+v", st)
	}
	s.Put(put("main", 41, `{"a":2}`))
	s.Put(put("main", 40, `{"a":3}`))
	time.Sleep(50 * time.Millisecond)
	if got := w.got(); len(got) != 1 {
		t.Fatalf("старые версии пропускаются: %v", got)
	}
	s.Put(message.ConfigPut{Worker: "nope", Key: "main", Version: 1, Data: json.RawMessage(`{}`)})
	eventually(t, "WORKER_UNKNOWN", func() bool { return len(r.list()) == 2 })
	if c := r.list()[1]; c.OK || c.Error == nil || c.Error.Code != message.CodeWorkerUnknown {
		t.Fatalf("неизвестный воркер: %+v", c)
	}
	if _, err := os.Stat(filepath.Join(dir, "nope")); err == nil {
		t.Fatal("неизвестному воркеру ничего не сохраняется")
	}
	if v := s.Versions(); v["report"]["main"] != 41 || len(v) != 1 {
		t.Fatalf("hello.configs: %v", v)
	}
}

// Тело ответа 2xx — config.applied.result как есть (JSON до 64 КБ); не JSON
// или больше предела — без result, предупреждение в лог без содержимого.
// Ни значение настроек, ни ответ воркера, ни текст его отказа в лог не
// попадают.
func TestApplyResult(t *testing.T) {
	const secret = "SECRET-CANARY-configs"
	var logBuf bytes.Buffer
	var mu sync.Mutex
	log, _ := logx.New(lockedWriter{&mu, &logBuf}, nil, logx.Options{Level: "debug"})
	w, r := newFakeWorker(t), &reports{}
	s, err := Open(t.TempDir(), w, r.add, log)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go s.Run(ctx)
	reply := func(body string) string {
		raw, _ := json.Marshal(map[string]string{"reply": body, "token": secret})
		return string(raw)
	}
	s.Put(put("a", 1, reply(`{"applied": {"token": "`+secret+`"}}`)))
	s.Put(put("b", 1, reply("не JSON "+secret)))
	s.Put(put("c", 1, reply(`"`+strings.Repeat("x", message.MaxConfigResultBytes)+`"`)))
	s.Put(put("d", 1, `{"reject":"плохой token `+secret+`"}`))
	eventually(t, "четыре итога", func() bool { return len(r.list()) == 4 })
	got := map[string]message.ConfigApplied{}
	for _, c := range r.list() {
		got[c.Key] = c
	}
	if c := got["a"]; !c.OK || string(c.Result) != `{"applied":{"token":"`+secret+`"}}` {
		t.Fatalf("result: %+v %s", c, c.Result)
	}
	for _, key := range []string{"b", "c"} {
		if c := got[key]; !c.OK || c.Result != nil {
			t.Fatalf("%s: без result: %+v", key, c)
		}
	}
	if c := got["d"]; c.OK || c.Error == nil || !strings.Contains(c.Error.Message, secret) {
		t.Fatalf("отказ — с текстом воркера серверу: %+v", c)
	}
	mu.Lock()
	text := logBuf.String()
	mu.Unlock()
	if strings.Contains(text, secret) {
		t.Fatalf("секрет в логе:\n%s", text)
	}
	if !strings.Contains(text, "не JSON") || !strings.Contains(text, "больше предела") {
		t.Fatalf("нет предупреждений:\n%s", text)
	}
}

type lockedWriter struct {
	mu *sync.Mutex
	w  io.Writer
}

func (l lockedWriter) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.w.Write(p)
}

// Отказ воркера — CONFIG_REJECTED с его текстом (JSON message или текст);
// повтор через 25 с без нового config.applied с тем же итогом; запуск
// воркера — передача всех ключей заново.
func TestRejectRetryAndRestart(t *testing.T) {
	defer func(d time.Duration) { retryEvery = d }(retryEvery)
	retryEvery = 50 * time.Millisecond
	w, r := newFakeWorker(t), &reports{}
	s := open(t, t.TempDir(), w, r)
	s.Put(put("limits", 7, `{"reject":"maxItems: должно быть числом"}`))
	s.Put(put("text", 1, `{"text":true}`))
	eventually(t, "оба итога", func() bool { return len(r.list()) == 2 })
	for _, c := range r.list() {
		if c.OK || c.Error.Code != message.CodeConfigRejected {
			t.Fatalf("отказ: %+v", c)
		}
	}
	msgs := map[string]string{}
	for _, c := range r.list() {
		msgs[c.Key] = c.Error.Message
	}
	if msgs["limits"] != "maxItems: должно быть числом" || msgs["text"] != "текстом" {
		t.Fatalf("тексты: %v", msgs)
	}
	eventually(t, "повторы", func() bool { return len(w.got()) >= 6 })
	if len(r.list()) != 2 {
		t.Fatalf("тот же итог — без нового config.applied: %+v", r.list())
	}
	st := s.Status("report")["limits"]
	if st.OK == nil || *st.OK || st.Error.Code != message.CodeConfigRejected {
		t.Fatalf("status: %+v", st)
	}
	// Новая версия принята — новый итог.
	s.Put(put("limits", 8, `{}`))
	eventually(t, "новая версия", func() bool {
		l := r.list()
		return len(l) == 3 && l[2].OK && l[2].Version == 8
	})
	// Перезапуск воркера: все ключи по порядку заново.
	n := len(w.got())
	s.Started("report")
	eventually(t, "ключи после запуска", func() bool { return len(w.got()) >= n+2 })
	got := w.got()[n:]
	if !strings.HasPrefix(got[0], "limits@8") {
		t.Fatalf("порядок ключей: %v", got)
	}
}

// Новая версия во время применения ждёт его окончания; промежуточные
// пропускаются.
func TestSkipIntermediate(t *testing.T) {
	w, r := newFakeWorker(t), &reports{}
	hold := make(chan struct{})
	w.hold = hold
	s := open(t, t.TempDir(), w, r)
	s.Put(put("main", 1, `{}`))
	eventually(t, "применение началось", func() bool { return len(w.got()) == 1 })
	s.Put(put("main", 2, `{}`))
	s.Put(put("main", 3, `{}`))
	w.mu.Lock()
	w.hold = nil
	w.mu.Unlock()
	close(hold)
	eventually(t, "последняя версия", func() bool {
		l := r.list()
		return len(l) == 1 && l[0].Version == 3
	})
	if got := w.got(); len(got) != 2 || got[1] != "main@3" {
		t.Fatalf("воркеру: %v", got)
	}
}

// Воркер не запущен — WORKER_UNAVAILABLE; после запуска — применено.
// Сохранённые ключи переживают перезапуск агента и передаются после
// запуска воркера; итог, уже отправленный для версии, не повторяется.
func TestUnavailableAndReopen(t *testing.T) {
	dir := t.TempDir()
	w, r := newFakeWorker(t), &reports{}
	w.running = false
	s := open(t, dir, w, r)
	s.Put(put("main", 5, `{"x":1}`))
	eventually(t, "недоступен", func() bool { return len(r.list()) == 1 })
	if c := r.list()[0]; c.Error == nil || c.Error.Code != message.CodeWorkerUnavailable {
		t.Fatalf("%+v", c)
	}
	w.mu.Lock()
	w.running = true
	w.mu.Unlock()
	s.Started("report")
	eventually(t, "применено после запуска", func() bool { return len(r.list()) == 2 && r.list()[1].OK })

	r2 := &reports{}
	s2 := open(t, dir, w, r2)
	if v, ok := s2.Get("report", "main"); !ok || v.Version != 5 {
		t.Fatalf("после перезапуска агента: %+v", v)
	}
	if st := s2.Status("report")["main"]; st.OK != nil {
		t.Fatalf("до запуска воркера — применяется: %+v", st)
	}
	n := len(w.got())
	s2.Started("report")
	eventually(t, "передано воркеру", func() bool { return len(w.got()) == n+1 })
	eventually(t, "статус", func() bool { st := s2.Status("report")["main"]; return st.OK != nil && *st.OK })
	if len(r2.list()) != 0 {
		t.Fatalf("тот же итог той же версии не повторяется: %+v", r2.list())
	}
}

// config.delete: файл и ключ удаляются, воркеру — DELETE (404 — тоже
// успех), config.applied нет; следующий config.put — с любой версией.
func TestDelete(t *testing.T) {
	dir := t.TempDir()
	w, r := newFakeWorker(t), &reports{}
	s := open(t, dir, w, r)
	s.Put(put("limits", 9, `{}`))
	eventually(t, "применено", func() bool { return len(r.list()) == 1 })
	s.Delete(message.ConfigDelete{Worker: "report", Key: "limits"})
	eventually(t, "DELETE воркеру", func() bool { w.mu.Lock(); defer w.mu.Unlock(); return len(w.deletes) == 1 })
	if _, err := os.Stat(filepath.Join(dir, "report", "limits.json")); !os.IsNotExist(err) {
		t.Fatalf("файл: %v", err)
	}
	if _, ok := s.Get("report", "limits"); ok || s.Status("report") != nil {
		t.Fatal("ключ удалён")
	}
	s.Put(put("limits", 1, `{}`))
	eventually(t, "любая версия после удаления", func() bool { return len(r.list()) == 2 && r.list()[1].Version == 1 })
}

// Подхваченный после перезапуска агента воркер получает только ключи, чья
// последняя версия не применена; применённые сразу видны в status как ok.
func TestAdopted(t *testing.T) {
	dir := t.TempDir()
	w, r := newFakeWorker(t), &reports{}
	s := open(t, dir, w, r)
	s.Put(put("main", 3, `{}`))
	s.Put(put("limits", 5, `{"reject":"нет"}`))
	eventually(t, "оба итога", func() bool { return len(r.list()) == 2 })

	w2 := newFakeWorker(t)
	again := open(t, dir, w2, &reports{})
	again.Adopted("report")
	eventually(t, "неприменённый передан заново", func() bool { got := w2.got(); return len(got) == 1 && got[0] == "limits@5" })
	time.Sleep(50 * time.Millisecond)
	if got := w2.got(); len(got) != 1 {
		t.Fatalf("применённый ключ не передаётся: %v", got)
	}
	if st := again.Status("report")["main"]; st.OK == nil || !*st.OK || st.Version != 3 {
		t.Fatalf("status применённого: %+v", st)
	}
}

// configRetry воркера (lifecycle.configRetry) — свой срок повтора.
type retryWorker struct{ *fakeWorker }

func (retryWorker) ConfigRetry(string) time.Duration { return 30 * time.Millisecond }

func TestWorkerConfigRetry(t *testing.T) {
	w, r := newFakeWorker(t), &reports{}
	s := open(t, t.TempDir(), retryWorker{w}, r)
	s.Put(put("limits", 1, `{"reject":"нет"}`))
	eventually(t, "повтор по сроку воркера", func() bool { return len(w.got()) >= 3 })
}

// declaredWorker — fakeWorker с манифестом: объявлен только ключ main;
// invalid — воркер не зарегистрирован.
type declaredWorker struct {
	*fakeWorker
	invalid atomic.Bool
}

func (d *declaredWorker) Manifest(string) *message.WorkerManifest {
	return &message.WorkerManifest{Version: "1.0.0", Configs: []message.WorkerManifestConfig{{Key: "main"}}}
}

func (d *declaredWorker) Client(name string) (*http.Client, error) {
	if d.invalid.Load() {
		return nil, fmt.Errorf("%w: GET /manifest: HTTP 404", worker.ErrInvalid)
	}
	return d.fakeWorker.Client(name)
}

// Манифест (§8, §12): ключа нет в manifest.configs — CONFIG_KEY_UNKNOWN,
// воркеру он не передаётся и не повторяется; незарегистрированный воркер —
// WORKER_INVALID.
func TestUndeclaredKey(t *testing.T) {
	w, r := &declaredWorker{fakeWorker: newFakeWorker(t)}, &reports{}
	s := open(t, t.TempDir(), w, r)
	s.retryEvery = 10 * time.Millisecond
	s.Put(put("other", 1, `{}`))
	s.Put(put("main", 1, `{}`))
	eventually(t, "итоги", func() bool { return len(r.list()) == 2 })
	for _, c := range r.list() {
		switch c.Key {
		case "other":
			if c.OK || c.Error == nil || c.Error.Code != message.CodeConfigKeyUnknown {
				t.Fatalf("необъявленный ключ: %+v", c)
			}
		case "main":
			if !c.OK {
				t.Fatalf("объявленный ключ: %+v", c)
			}
		}
	}
	time.Sleep(50 * time.Millisecond)
	if got := w.got(); len(got) != 1 || got[0] != "main@1" {
		t.Fatalf("воркеру переданы: %v", got)
	}

	w.invalid.Store(true)
	s.Put(put("main", 2, `{}`))
	eventually(t, "WORKER_INVALID", func() bool {
		l := r.list()
		return len(l) == 3 && l[2].Error != nil && l[2].Error.Code == message.CodeWorkerInvalid
	})
}
