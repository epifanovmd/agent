// Package configs — настройки воркеров (§8): ключи с версиями на диске
// агента (<dataDir>/configs/<worker>/<key>.json, 0600), передача воркеру
// PUT /config/{key}, итог серверу config.applied — по одному на каждую пару
// «версия + итог», повтор неприменённых и передача всех ключей после
// запуска воркера.
package configs

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/worker"
)

// Сроки; переменные — для тестов.
var (
	retryEvery   = message.ConfigRetry
	applyTimeout = message.ConfigTimeout
)

// Workers — воркеры агента.
type Workers interface {
	Has(name string) bool
	Client(name string) (*http.Client, error)
}

// retrier — свой срок повтора неприменённых настроек у воркера
// (lifecycle.configRetry); 0 — общий.
type retrier interface {
	ConfigRetry(name string) time.Duration
}

// declarer — манифест зарегистрированного воркера (§12): по нему агент не
// передаёт воркеру необъявленные ключи (CONFIG_KEY_UNKNOWN).
type declarer interface {
	Manifest(name string) *message.WorkerManifest
}

// Report — отправить config.applied (важное сообщение).
type Report func(message.ConfigApplied)

// file — содержимое файла ключа: значение и итог последнего применения.
type file struct {
	Version int64           `json:"version"`
	Data    json.RawMessage `json:"data"`
	// Reported — итог, уже отправленный серверу для Version.
	Reported *result `json:"reported,omitempty"`
}

type result struct {
	OK      bool   `json:"ok"`
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

type entry struct {
	file
	// status — итог применения Version; nil — ещё применяется.
	status  *result
	dirty   bool
	retryAt time.Time
}

// Store — настройки всех воркеров.
type Store struct {
	dir     string
	log     *slog.Logger
	workers Workers
	report  Report
	// Сроки на момент открытия (переменные пакета — для тестов).
	retryEvery, applyTimeout time.Duration

	mu   sync.Mutex
	ctx  context.Context
	keys map[string]map[string]*entry
	wake map[string]chan struct{}
}

// Open — настройки из каталога dir (создаётся).
func Open(dir string, workers Workers, report Report, log *slog.Logger) (*Store, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("configs: %w", err)
	}
	s := &Store{dir: dir, log: log, workers: workers, report: report, retryEvery: retryEvery, applyTimeout: applyTimeout,
		keys: map[string]map[string]*entry{}, wake: map[string]chan struct{}{}}
	files, _ := filepath.Glob(filepath.Join(dir, "*", "*.json"))
	for _, path := range files {
		name := filepath.Base(filepath.Dir(path))
		key := strings.TrimSuffix(filepath.Base(path), ".json")
		raw, err := os.ReadFile(path)
		var f file
		if err != nil || json.Unmarshal(raw, &f) != nil || !message.ValidName(name) || !message.ValidName(key) {
			log.Warn("configs: файл настроек повреждён — пропущен", "file", path)
			continue
		}
		// Воркеру ключи передаются после его запуска (Started).
		s.entries(name)[key] = &entry{file: f}
	}
	return s, nil
}

func (s *Store) entries(name string) map[string]*entry {
	m := s.keys[name]
	if m == nil {
		m = map[string]*entry{}
		s.keys[name] = m
	}
	return m
}

func (s *Store) path(name, key string) string { return filepath.Join(s.dir, name, key+".json") }

// Run — передавать настройки воркерам до отмены ctx.
func (s *Store) Run(ctx context.Context) {
	s.mu.Lock()
	s.ctx = ctx
	names := make([]string, 0, len(s.keys))
	for name := range s.keys {
		names = append(names, name)
	}
	s.mu.Unlock()
	for _, name := range names {
		s.kick(name)
	}
	<-ctx.Done()
}

// Versions — версии сохранённых ключей: воркер → ключ → версия (hello.configs).
func (s *Store) Versions() map[string]map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := map[string]map[string]int64{}
	for name, keys := range s.keys {
		for key, e := range keys {
			if out[name] == nil {
				out[name] = map[string]int64{}
			}
			out[name][key] = e.Version
		}
	}
	return out
}

// Status — по каждому ключу воркера: версия и итог применения (§6).
func (s *Store) Status(name string) map[string]message.ConfigStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := s.keys[name]
	if len(keys) == 0 {
		return nil
	}
	out := make(map[string]message.ConfigStatus, len(keys))
	for key, e := range keys {
		st := message.ConfigStatus{Version: e.Version}
		if e.status != nil {
			ok := e.status.OK
			st.OK = &ok
			if !ok {
				st.Error = message.NewError(e.status.Code, e.status.Message)
			}
		}
		out[key] = st
	}
	return out
}

// Get — сохранённое значение ключа (GET /config/{key} сокета агента).
func (s *Store) Get(name, key string) (message.ConfigValue, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	e := s.keys[name][key]
	if e == nil {
		return message.ConfigValue{}, false
	}
	return message.ConfigValue{Version: e.Version, Data: e.Data}, true
}

// Put — config.put (§8): сохранить новую версию и передать воркеру.
func (s *Store) Put(p message.ConfigPut) {
	log := s.log.With("worker", p.Worker, "key", p.Key, "version", p.Version)
	if !s.workers.Has(p.Worker) {
		log.Warn("config.put: воркера нет в настройках агента")
		s.report(message.ConfigApplied{Worker: p.Worker, Key: p.Key, Version: p.Version,
			Error: message.NewError(message.CodeWorkerUnknown, fmt.Sprintf("воркера %q нет в настройках агента", p.Worker))})
		return
	}
	if !message.ValidName(p.Key) || len(p.Data) > message.MaxConfigBytes || !json.Valid(p.Data) {
		log.Warn("config.put: неверное имя ключа или значение — пропущено", "bytes", len(p.Data))
		return
	}
	s.mu.Lock()
	keys := s.entries(p.Worker)
	if e := keys[p.Key]; e != nil && p.Version <= e.Version {
		s.mu.Unlock()
		log.Debug("config.put: версия не новее сохранённой — пропущена", "stored", e.Version)
		return
	}
	e := &entry{file: file{Version: p.Version, Data: slices.Clone(p.Data)}, dirty: true}
	err := s.save(p.Worker, p.Key, e.file)
	if err == nil {
		keys[p.Key] = e
	}
	s.mu.Unlock()
	if err != nil {
		log.Error("config.put: не сохранено на диск", "err", err)
		return
	}
	s.kick(p.Worker)
}

// Delete — config.delete (§8): удалить ключ у себя и у воркера.
func (s *Store) Delete(d message.ConfigDelete) {
	s.mu.Lock()
	_, had := s.keys[d.Worker][d.Key]
	delete(s.keys[d.Worker], d.Key)
	s.mu.Unlock()
	if !message.ValidName(d.Worker) || !message.ValidName(d.Key) {
		return
	}
	if err := os.Remove(s.path(d.Worker, d.Key)); err != nil && !errors.Is(err, os.ErrNotExist) {
		s.log.Error("config.delete: файл не удалён", "worker", d.Worker, "key", d.Key, "err", err)
	}
	if !had {
		return
	}
	go func() {
		client, err := s.workers.Client(d.Worker)
		if err != nil {
			return // воркер не запущен: после запуска ключа у него не будет
		}
		ctx, cancel := context.WithTimeout(context.Background(), s.applyTimeout)
		defer cancel()
		req, _ := http.NewRequestWithContext(ctx, http.MethodDelete, "http://worker"+message.ConfigPathPrefix+d.Key, nil)
		resp, err := client.Do(req)
		if err != nil {
			s.log.Warn("config.delete: воркер не ответил", "worker", d.Worker, "key", d.Key, "err", err)
			return
		}
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<10))
		resp.Body.Close()
		if resp.StatusCode/100 != 2 && resp.StatusCode != http.StatusNotFound {
			s.log.Warn("config.delete: воркер ответил ошибкой", "worker", d.Worker, "key", d.Key,
				"status", resp.StatusCode, "err", worker.ErrorText(body))
		}
	}()
}

// Started — воркер зарегистрирован: передать ему все сохранённые ключи (§8, п. 6).
func (s *Store) Started(name string) {
	s.mu.Lock()
	for _, e := range s.keys[name] {
		e.dirty = true
	}
	s.mu.Unlock()
	s.kick(name)
}

// Adopted — зарегистрирован подхваченный воркер, переживший перезапуск агента: ему передаются
// только ключи, последняя версия которых не применена (применённые он уже
// знает).
func (s *Store) Adopted(name string) {
	s.mu.Lock()
	for _, e := range s.keys[name] {
		if e.Reported != nil && e.Reported.OK {
			e.status = &result{OK: true}
			continue
		}
		e.dirty = true
	}
	s.mu.Unlock()
	s.kick(name)
}

// retry — через сколько повторить неприменённые настройки воркера name.
func (s *Store) retry(name string) time.Duration {
	if r, ok := s.workers.(retrier); ok {
		if d := r.ConfigRetry(name); d > 0 {
			return d
		}
	}
	return s.retryEvery
}

// kick — разбудить передачу настроек воркеру name (запускает её при первом вызове).
func (s *Store) kick(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ctx == nil {
		return // Run ещё не вызван: он разбудит всех сам
	}
	ch := s.wake[name]
	if ch == nil {
		ch = make(chan struct{}, 1)
		s.wake[name] = ch
		go s.loop(s.ctx, name, ch)
	}
	select {
	case ch <- struct{}{}:
	default:
	}
}

// loop — передача настроек одному воркеру: ключи по порядку, по одному;
// новая версия во время применения ждёт его окончания (промежуточные
// пропускаются — передаётся последняя сохранённая).
func (s *Store) loop(ctx context.Context, name string, wake chan struct{}) {
	for {
		key, f, ok := s.next(name)
		if ok {
			res := s.apply(ctx, name, key, f)
			if ctx.Err() != nil {
				return
			}
			s.done(name, key, f, res)
			continue
		}
		var timer <-chan time.Time
		if at := s.nextRetry(name); !at.IsZero() {
			timer = time.After(time.Until(at))
		}
		select {
		case <-ctx.Done():
			return
		case <-wake:
		case <-timer:
		}
	}
}

// next — следующий ключ, который пора передать: dirty или наступил повтор.
func (s *Store) next(name string) (string, file, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := time.Now()
	keys := make([]string, 0, len(s.keys[name]))
	for key := range s.keys[name] {
		keys = append(keys, key)
	}
	slices.Sort(keys)
	for _, key := range keys {
		e := s.keys[name][key]
		if e.dirty || (!e.retryAt.IsZero() && !now.Before(e.retryAt)) {
			e.dirty = false
			e.retryAt = time.Time{}
			return key, e.file, true
		}
	}
	return "", file{}, false
}

func (s *Store) nextRetry(name string) time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	var at time.Time
	for _, e := range s.keys[name] {
		if !e.retryAt.IsZero() && (at.IsZero() || e.retryAt.Before(at)) {
			at = e.retryAt
		}
	}
	return at
}

// apply — PUT /config/{key} {version, data}.
func (s *Store) apply(ctx context.Context, name, key string, f file) result {
	client, err := s.workers.Client(name)
	switch {
	case errors.Is(err, worker.ErrInvalid):
		return result{Code: message.CodeWorkerInvalid, Message: fmt.Sprintf("воркер %s: %v", name, err)}
	case err != nil:
		return result{Code: message.CodeWorkerUnavailable, Message: fmt.Sprintf("воркер %s не запущен", name)}
	}
	// Значение по schema ключа агент не проверяет — только то, что ключ объявлен.
	if d, ok := s.workers.(declarer); ok && !d.Manifest(name).DeclaresConfig(key) {
		return result{Code: message.CodeConfigKeyUnknown,
			Message: fmt.Sprintf("ключа %s нет в манифесте воркера %s (configs)", key, name)}
	}
	body, _ := json.Marshal(message.ConfigValue{Version: f.Version, Data: f.Data})
	actx, cancel := context.WithTimeout(ctx, s.applyTimeout)
	defer cancel()
	req, _ := http.NewRequestWithContext(actx, http.MethodPut, "http://worker"+message.ConfigPathPrefix+key, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		if actx.Err() != nil && ctx.Err() == nil {
			return result{Code: message.CodeTimeout, Message: fmt.Sprintf("воркер не ответил на PUT /config/%s за %s", key, s.applyTimeout)}
		}
		return result{Code: message.CodeWorkerUnavailable, Message: err.Error()}
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode/100 != 2 {
		text := worker.ErrorText(raw)
		if text == "" {
			text = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
		return result{Code: message.CodeConfigRejected, Message: text}
	}
	return result{OK: true}
}

// done — итог применения версии f.Version: статус, повтор при ошибке,
// config.applied, если такого итога для этой версии ещё не было.
func (s *Store) done(name, key string, f file, res result) {
	s.mu.Lock()
	e := s.keys[name][key]
	if e == nil || e.Version != f.Version {
		s.mu.Unlock()
		return // ключ удалён или пришла новая версия — её итог будет свой
	}
	e.status = &res
	// Необъявленный ключ проверяется заново при следующей регистрации воркера.
	if !res.OK && res.Code != message.CodeConfigKeyUnknown {
		e.retryAt = time.Now().Add(s.retry(name))
	}
	send := e.Reported == nil || e.Reported.OK != res.OK || e.Reported.Code != res.Code
	if send {
		e.Reported = &result{OK: res.OK, Code: res.Code}
		if err := s.save(name, key, e.file); err != nil {
			s.log.Error("configs: итог не сохранён на диск", "worker", name, "key", key, "err", err)
		}
	}
	s.mu.Unlock()
	log := s.log.With("worker", name, "key", key, "version", f.Version)
	if res.OK {
		log.Info("настройки применены воркером")
	} else {
		log.Warn("настройки не применены", "code", res.Code, "err", res.Message)
	}
	if send {
		applied := message.ConfigApplied{Worker: name, Key: key, Version: f.Version, OK: res.OK}
		if !res.OK {
			applied.Error = message.NewError(res.Code, res.Message)
		}
		s.report(applied)
	}
}

// save — записать файл ключа атомарно (под s.mu).
func (s *Store) save(name, key string, f file) error {
	dir := filepath.Join(s.dir, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	raw, err := json.Marshal(f)
	if err != nil {
		return err
	}
	path := s.path(name, key)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}
