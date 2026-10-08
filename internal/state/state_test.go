package state

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/sdk/go/message"
)

type fakeReconciler struct {
	calls atomic.Int32
	fail  atomic.Bool
	last  atomic.Int64
}

func (f *fakeReconciler) Domain() string { return "example.app" }
func (f *fakeReconciler) Apply(_ context.Context, v int64, _ json.RawMessage) (any, error) {
	f.calls.Add(1)
	if f.fail.Load() {
		return nil, errors.New("не вышло")
	}
	f.last.Store(v)
	return map[string]int64{"v": v}, nil
}

type sink struct {
	mu  sync.Mutex
	got []message.StateApplied
}

func (s *sink) Reliable(_ string, data any) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, data.(message.StateApplied))
	return nil
}
func (s *sink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.got)
}

func put(v int64) message.Envelope {
	return message.MustNew(message.TypeStatePut, message.StatePut{Domain: "example.app", Version: v, Spec: json.RawMessage(`{}`)})
}

func wait(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatal("не дождались")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestApplyCacheAndAutonomousStart(t *testing.T) {
	dir := t.TempDir()
	r := &fakeReconciler{}
	s := &sink{}
	m := New(dir, s, logx.Discard())
	m.Register(r)
	ctx, cancel := context.WithCancel(context.Background())
	go m.Start(ctx)

	_ = m.Handle(ctx, put(3))
	wait(t, func() bool { return s.count() == 1 && r.last.Load() == 3 })
	_ = m.Handle(ctx, put(2)) // старая версия — без применения
	_ = m.Handle(ctx, put(3)) // повтор применённой — подтверждение снова
	wait(t, func() bool { return s.count() == 2 })
	if r.calls.Load() != 1 {
		t.Fatalf("лишние применения: %d", r.calls.Load())
	}
	var caps message.Capabilities
	m.Declare(&caps)
	if *caps.State.Domains["example.app"] != 3 {
		t.Fatal("в hello — применённая версия")
	}
	cancel()

	// Рестарт без сервера: снимок применяется из кэша.
	r2 := &fakeReconciler{}
	m2 := New(dir, &sink{}, logx.Discard())
	m2.Register(r2)
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go m2.Start(ctx2)
	wait(t, func() bool { return r2.last.Load() == 3 })
}

func TestFailureReportsError(t *testing.T) {
	r := &fakeReconciler{}
	r.fail.Store(true)
	s := &sink{}
	m := New(t.TempDir(), s, logx.Discard())
	m.Register(r)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Start(ctx)
	_ = m.Handle(ctx, put(1))
	wait(t, func() bool { return s.count() == 1 })
	if s.got[0].OK || s.got[0].Error == "" {
		t.Fatalf("ошибка применения: %+v", s.got[0])
	}
}

// Домен воркера появляется после старта: кэш применяется сразу, Kick
// применяет последний снимок заново, повторная регистрация — ошибка.
func TestRegisterAfterStartAndKick(t *testing.T) {
	dir := t.TempDir()
	first := New(dir, &sink{}, logx.Discard())
	_ = first.Register(&fakeReconciler{})
	ctx, cancel := context.WithCancel(context.Background())
	go first.Start(ctx)
	_ = first.Handle(ctx, put(5))
	wait(t, func() bool { c, _ := first.load("example.app"); return c != nil && c.Applied })
	cancel()

	s := &sink{}
	m := New(dir, s, logx.Discard())
	ctx2, cancel2 := context.WithCancel(context.Background())
	defer cancel2()
	go m.Start(ctx2)
	wait(t, func() bool { m.mu.Lock(); defer m.mu.Unlock(); return m.ctx != nil })

	r := &fakeReconciler{}
	if err := m.Register(r); err != nil {
		t.Fatal(err)
	}
	wait(t, func() bool { return r.last.Load() == 5 && s.count() == 1 })
	if err := m.Register(&fakeReconciler{}); err == nil {
		t.Fatal("повторная регистрация домена должна отклоняться")
	}
	m.Kick("example.app")
	wait(t, func() bool { return r.calls.Load() == 2 && s.count() == 2 })
	if !m.Has("example.app") || m.Has("dns") {
		t.Fatal("Has")
	}
}

// availReconciler — исполнитель, который бывает недоступен (воркер не запущен).
type availReconciler struct {
	fakeReconciler
	down   atomic.Bool
	report atomic.Value // string
}

func (a *availReconciler) Available() bool { return !a.down.Load() }
func (a *availReconciler) Apply(ctx context.Context, v int64, spec json.RawMessage) (any, error) {
	if _, err := a.fakeReconciler.Apply(ctx, v, spec); err != nil {
		return nil, err
	}
	if r, _ := a.report.Load().(string); r != "" {
		return map[string]string{"r": r}, nil
	}
	return nil, nil
}

// Повторное применение по интервалу: тот же снимок отдаётся исполнителю
// снова; серверу — только изменившийся итог; без исполнителя — пропуск.
func TestResync(t *testing.T) {
	r := &availReconciler{}
	s := &sink{}
	m := New(t.TempDir(), s, logx.Discard())
	m.retry = 40 * time.Millisecond
	m.SetResync(30 * time.Millisecond)
	_ = m.Register(r)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Start(ctx)

	_ = m.Handle(ctx, put(4))
	wait(t, func() bool { return s.count() == 1 })
	wait(t, func() bool { return r.calls.Load() >= 4 })
	if s.count() != 1 || r.last.Load() != 4 {
		t.Fatalf("итог не менялся — серверу ничего: отправлено %d", s.count())
	}

	// Итог изменился (ошибка) — серверу; та же ошибка повторно — нет.
	r.fail.Store(true)
	wait(t, func() bool { return s.count() == 2 })
	s.mu.Lock()
	failed := s.got[1]
	s.mu.Unlock()
	if failed.OK || failed.Error == "" || failed.Version != 4 {
		t.Fatalf("ошибка повторного применения: %+v", failed)
	}
	r.fail.Store(false)
	r.report.Store("fixed")
	wait(t, func() bool { return s.count() == 3 })
	s.mu.Lock()
	fixed := s.got[2]
	s.mu.Unlock()
	if !fixed.OK || string(fixed.Report) != `{"r":"fixed"}` {
		t.Fatalf("исправлено — ok с отчётом: %+v", fixed)
	}

	// Исполнителя нет — повторное применение пропускается.
	r.down.Store(true)
	time.Sleep(50 * time.Millisecond) // применение, начатое до down, завершается
	calls := r.calls.Load()
	time.Sleep(150 * time.Millisecond)
	if got := r.calls.Load(); got != calls {
		t.Fatalf("без исполнителя применений быть не должно: %d → %d", calls, got)
	}
	if s.count() != 3 {
		t.Fatalf("лишние итоги: %d", s.count())
	}
}

// resync 0 — повторного применения нет.
func TestResyncOff(t *testing.T) {
	r := &fakeReconciler{}
	m := New(t.TempDir(), &sink{}, logx.Discard())
	_ = m.Register(r)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Start(ctx)
	_ = m.Handle(ctx, put(1))
	wait(t, func() bool { return r.calls.Load() == 1 })
	time.Sleep(100 * time.Millisecond)
	if r.calls.Load() != 1 {
		t.Fatalf("resync выключен: %d применений", r.calls.Load())
	}

	// Включён на ходу (перечитывание настроек) — повторы идут без перезапуска.
	m.SetResync(20 * time.Millisecond)
	wait(t, func() bool { return r.calls.Load() >= 3 })

	// Домен снят (воркер удалён) — не объявляется и не применяется.
	m.Unregister(r.Domain())
	time.Sleep(30 * time.Millisecond)
	calls := r.calls.Load()
	time.Sleep(80 * time.Millisecond)
	if m.Has(r.Domain()) || r.calls.Load() != calls {
		t.Fatalf("снятый домен: has=%v, применений %d → %d", m.Has(r.Domain()), calls, r.calls.Load())
	}
}

// specReconciler — запоминает переданный снимок; reportErr — ошибка вместе с отчётом.
type specReconciler struct {
	mu        sync.Mutex
	spec      json.RawMessage
	reportErr bool
}

func (r *specReconciler) Domain() string { return "example.app" }
func (r *specReconciler) Apply(_ context.Context, _ int64, spec json.RawMessage) (any, error) {
	r.mu.Lock()
	r.spec = spec
	r.mu.Unlock()
	if r.reportErr {
		return json.RawMessage(`{"port":8080,"busy":true}`), errors.New("порт занят")
	}
	return nil, nil
}
func (r *specReconciler) got() string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return string(r.spec)
}

func (s *sink) last() message.StateApplied {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.got[len(s.got)-1]
}

// Ошибка применения вместе с отчётом: серверу уходит ok:false, error и report.
func TestFailedWithReport(t *testing.T) {
	r := &specReconciler{reportErr: true}
	s := &sink{}
	m := New(t.TempDir(), s, logx.Discard())
	m.Register(r)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Start(ctx)
	_ = m.Handle(ctx, put(1))
	wait(t, func() bool { return s.count() == 1 })
	got := s.last()
	if got.OK || got.Error != "порт занят" || string(got.Report) != `{"port":8080,"busy":true}` {
		t.Fatalf("итог: %+v (report %s)", got, got.Report)
	}
}

// Запечатанные значения раскрываются только для исполнителя: на диске снимок
// как пришёл; не раскрылось — ok:false с понятной ошибкой, исполнитель не вызван.
func TestUnseal(t *testing.T) {
	dir := t.TempDir()
	r := &specReconciler{}
	s := &sink{}
	m := New(dir, s, logx.Discard())
	m.SetUnseal(func(spec json.RawMessage) (json.RawMessage, error) {
		switch string(spec) {
		case `{"k":{"$sealed":"ok"}}`:
			return json.RawMessage(`{"k":"секрет"}`), nil
		case `{"k":{"$sealed":"bad"}}`:
			return nil, errors.New("чужой ключ")
		}
		return spec, nil
	})
	m.Register(r)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go m.Start(ctx)

	sealedPut := func(v int64, token string) message.Envelope {
		return message.MustNew(message.TypeStatePut, message.StatePut{Domain: "example.app", Version: v,
			Spec: json.RawMessage(`{"k":{"$sealed":"` + token + `"}}`)})
	}
	_ = m.Handle(ctx, sealedPut(1, "ok"))
	wait(t, func() bool { return s.count() == 1 })
	if !s.last().OK || r.got() != `{"k":"секрет"}` {
		t.Fatalf("исполнитель получил %s, итог %+v", r.got(), s.last())
	}
	disk, err := os.ReadFile(filepath.Join(dir, "example.app.json"))
	if err != nil || !strings.Contains(string(disk), `"$sealed":"ok"`) || strings.Contains(string(disk), "секрет") {
		t.Fatalf("на диске: %s %v", disk, err)
	}

	_ = m.Handle(ctx, sealedPut(2, "bad"))
	wait(t, func() bool { return s.count() == 2 })
	got := s.last()
	if got.OK || got.Version != 2 || !strings.Contains(got.Error, ErrUnseal) || !strings.Contains(got.Error, "чужой ключ") {
		t.Fatalf("итог: %+v", got)
	}
	if r.got() != `{"k":"секрет"}` {
		t.Fatalf("исполнитель вызван с нераскрытым: %s", r.got())
	}
}
