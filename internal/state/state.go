// Package state — возможность `state`: желаемое состояние доменов (полный
// снимок с монотонной версией). Снимок сохраняется на диск и применяется
// идемпотентно; после рестарта — из кэша, без сервера; при ошибке — повтор.
package state

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Reconciler — приведение хоста к снимку домена. Apply идемпотентен.
type Reconciler interface {
	Domain() string
	Apply(ctx context.Context, version int64, spec json.RawMessage) (report any, err error)
}

// Availability — необязательно для Reconciler: исполнитель домена сейчас
// принимает снимки (воркер-владелец запущен). Повторное применение по
// интервалу пропускает домен, у которого исполнителя нет.
type Availability interface {
	Available() bool
}

// Sender — канал к серверу.
type Sender interface {
	Reliable(typ string, data any) error
}

// retryEvery — повтор применения после ошибки.
const retryEvery = 30 * time.Second

type cached struct {
	Version int64           `json:"version"`
	Spec    json.RawMessage `json:"spec"`
	Applied bool            `json:"applied"`
}

type domain struct {
	r       Reconciler
	mu      sync.Mutex
	latest  *cached
	applied *int64
	wake    chan struct{}
	cancel  context.CancelFunc
	// sent — последний итог, отправленный серверу (только в цикле домена).
	sent *message.StateApplied
}

// Manager — домены желаемого состояния.
type Manager struct {
	dir    string
	sender Sender
	log    *slog.Logger
	resync time.Duration
	// resyncChanged — закрывается при смене resync (циклы доменов перечитывают).
	resyncChanged chan struct{}
	retry         time.Duration // повтор после ошибки (retryEvery; в тестах — короче)

	// unseal — раскрытие запечатанных значений снимка перед передачей
	// исполнителю (nil — снимок как есть).
	unseal func(json.RawMessage) (json.RawMessage, error)

	mu      sync.Mutex
	domains map[string]*domain
	ctx     context.Context // после Start: новые домены сразу обслуживаются
	wg      sync.WaitGroup
}

// New — менеджер; снимки — в dir.
func New(dir string, sender Sender, log *slog.Logger) *Manager {
	return &Manager{dir: dir, sender: sender, log: log, retry: retryEvery, domains: map[string]*domain{},
		resyncChanged: make(chan struct{})}
}

// ErrUnseal — запечатанное значение снимка не раскрылось ключом агента.
const ErrUnseal = "секрет не расшифрован — бэкенду нужно запечатать заново (ключ агента сменился?)"

// SetUnseal — раскрывать запечатанные значения ({"$sealed": …}) снимка
// функцией fn только в памяти, перед передачей исполнителю; на диске снимок
// остаётся как пришёл. Не раскрылось — state.applied ok:false с ErrUnseal.
// Вызывается до Start.
func (m *Manager) SetUnseal(fn func(json.RawMessage) (json.RawMessage, error)) { m.unseal = fn }

// SetResync — раз в interval заново применять последний применённый снимок
// каждого домена (та же версия): исполнитель исправляет ручные изменения на
// узле. Итог уходит серверу, только если он изменился. 0 — выключено.
// Можно менять на ходу: отсчёт идёт заново с новым интервалом.
func (m *Manager) SetResync(interval time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.resync == interval {
		return
	}
	m.resync = interval
	close(m.resyncChanged)
	m.resyncChanged = make(chan struct{})
}

func (m *Manager) resyncSetting() (time.Duration, <-chan struct{}) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.resync, m.resyncChanged
}

// Unregister — домен больше не обслуживается (его воркер удалён из
// настроек); снимок на диске остаётся.
func (m *Manager) Unregister(name string) {
	m.mu.Lock()
	var cancel context.CancelFunc
	if d, ok := m.domains[name]; ok {
		cancel = d.cancel
	}
	delete(m.domains, name)
	m.mu.Unlock()
	if cancel != nil {
		cancel()
	}
}

// Register — домен. После Start домен начинает обслуживаться сразу: кэш
// применяется без сервера (домены нагрузок появляются после их регистрации).
// Повторная регистрация домена — ошибка.
func (m *Manager) Register(r Reconciler) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	name := r.Domain()
	if _, ok := m.domains[name]; ok {
		return fmt.Errorf("state: домен %s уже зарегистрирован", name)
	}
	d := &domain{r: r, wake: make(chan struct{}, 1)}
	m.domains[name] = d
	if m.ctx != nil {
		m.startLocked(name, d)
	}
	return nil
}

// Has — домен зарегистрирован.
func (m *Manager) Has(name string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.domains[name]
	return ok
}

// Kick — применить последний снимок домена заново (исполнитель домена
// перезапустился и не помнит состояние). Применение идемпотентно.
func (m *Manager) Kick(name string) {
	m.mu.Lock()
	d := m.domains[name]
	m.mu.Unlock()
	if d != nil {
		d.kick()
	}
}

// Empty — нет доменов (возможность не объявляется).
func (m *Manager) Empty() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.domains) == 0
}

func (m *Manager) Declare(caps *message.Capabilities) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.domains) == 0 {
		return
	}
	domains := map[string]*int64{}
	for name, d := range m.domains {
		d.mu.Lock()
		domains[name] = d.applied
		d.mu.Unlock()
	}
	caps.State = &message.StateCapability{Domains: domains}
}

func (m *Manager) Handles() []string { return []string{message.TypeStatePut} }

func (m *Manager) Handle(_ context.Context, env message.Envelope) error {
	var put message.StatePut
	if err := env.Decode(&put); err != nil {
		return err
	}
	m.mu.Lock()
	d, ok := m.domains[put.Domain]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("state: домен %s не поддерживается", put.Domain)
	}
	d.mu.Lock()
	if d.latest != nil && put.Version <= d.latest.Version {
		// Устаревший снимок — мимо; та же версия, уже применённая, — сервер
		// не знает о применении (ack потерялся): сообщить снова.
		applied := put.Version == d.latest.Version && d.applied != nil && *d.applied == put.Version
		d.mu.Unlock()
		if applied {
			return m.sender.Reliable(message.TypeStateApplied, message.StateApplied{Domain: put.Domain, Version: put.Version, OK: true})
		}
		return nil
	}
	d.latest = &cached{Version: put.Version, Spec: put.Spec}
	snapshot := *d.latest
	d.mu.Unlock()
	if err := m.save(put.Domain, snapshot); err != nil {
		m.log.Error("state: снимок не сохранён", "domain", put.Domain, "err", err)
	}
	d.kick()
	return nil
}

// Start — применить кэш (автономный старт) и обслуживать новые снимки до
// отмены ctx, в том числе доменов, зарегистрированных позже.
func (m *Manager) Start(ctx context.Context) error {
	m.mu.Lock()
	m.ctx = ctx
	for name, d := range m.domains {
		m.startLocked(name, d)
	}
	m.mu.Unlock()
	<-ctx.Done()
	m.wg.Wait()
	return nil
}

// startLocked — кэш домена и цикл применения (под m.mu, после Start).
func (m *Manager) startLocked(name string, d *domain) {
	if c, err := m.load(name); err == nil && c != nil {
		d.mu.Lock()
		d.latest = c
		if c.Applied {
			v := c.Version
			d.applied = &v
		}
		d.mu.Unlock()
		d.kick()
	}
	ctx, cancel := context.WithCancel(m.ctx)
	d.cancel = cancel
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		defer cancel()
		m.loop(ctx, name, d)
	}()
}

func (d *domain) kick() {
	select {
	case d.wake <- struct{}{}:
	default:
	}
}

func (m *Manager) loop(ctx context.Context, name string, d *domain) {
	// Повторное применение — через resync после последнего применения (не
	// сразу за применением, растянувшимся дольше интервала).
	var retry, tick <-chan time.Time
	var timer *time.Timer
	var resync time.Duration
	var resyncChanged <-chan struct{}
	setup := func() {
		if timer != nil {
			timer.Stop()
			timer, tick = nil, nil
		}
		resync, resyncChanged = m.resyncSetting()
		if resync > 0 {
			timer = time.NewTimer(resync)
			tick = timer.C
		}
	}
	setup()
	defer func() {
		if timer != nil {
			timer.Stop()
		}
	}()
	again := func() {
		if timer != nil {
			timer.Reset(resync)
		}
	}
	for {
		// quiet — повтор, а не новый снимок: серверу — только изменившийся итог.
		quiet := false
		select {
		case <-ctx.Done():
			return
		case <-resyncChanged:
			setup()
			continue
		case <-d.wake:
			retry = nil
		case <-retry:
			retry, quiet = nil, true
		case <-tick:
			again()
			if retry != nil || !m.resyncable(d) {
				continue // ждёт повтора после ошибки, не применён или исполнителя нет
			}
			quiet = true
		}
		d.mu.Lock()
		if d.latest == nil {
			d.mu.Unlock()
			continue
		}
		snapshot := *d.latest
		d.mu.Unlock()

		report, err := m.apply(ctx, d.r, snapshot)
		again()
		applied := message.StateApplied{Domain: name, Version: snapshot.Version, OK: err == nil}
		if report != nil {
			applied.Report, _ = json.Marshal(report)
		}
		if err != nil {
			applied.Error = err.Error()
		}
		changed := !quiet || !sameResult(d.sent, applied)
		if err != nil {
			if changed {
				m.log.Error("state: применение не удалось — повтор", "domain", name, "version", snapshot.Version, "err", err)
			}
			retry = time.After(m.retry)
		} else {
			d.mu.Lock()
			v := snapshot.Version
			d.applied = &v
			fresh := d.latest.Version == v && !d.latest.Applied
			if fresh {
				d.latest.Applied = true
			}
			saved := *d.latest
			d.mu.Unlock()
			if fresh {
				_ = m.save(name, saved)
			}
			if changed {
				m.log.Info("state: применено", "domain", name, "version", v)
			}
		}
		if !changed {
			continue
		}
		sent := applied
		d.sent = &sent
		if err := m.sender.Reliable(message.TypeStateApplied, applied); err != nil {
			m.log.Error("state: итог не записан", "err", err)
		}
	}
}

// apply — раскрыть запечатанные значения и передать снимок исполнителю.
func (m *Manager) apply(ctx context.Context, r Reconciler, snapshot cached) (any, error) {
	spec := snapshot.Spec
	if m.unseal != nil {
		open, err := m.unseal(spec)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", ErrUnseal, err)
		}
		spec = open
	}
	return r.Apply(ctx, snapshot.Version, spec)
}

// resyncable — повторное применение по интервалу: последний снимок применён
// и исполнитель домена на месте.
func (m *Manager) resyncable(d *domain) bool {
	d.mu.Lock()
	ok := d.latest != nil && d.applied != nil && *d.applied == d.latest.Version
	d.mu.Unlock()
	if a, has := d.r.(Availability); ok && has {
		ok = a.Available()
	}
	return ok
}

// sameResult — итог совпадает с уже отправленным (ok, ошибка, отчёт, версия).
func sameResult(prev *message.StateApplied, cur message.StateApplied) bool {
	return prev != nil && prev.Version == cur.Version && prev.OK == cur.OK &&
		prev.Error == cur.Error && bytes.Equal(prev.Report, cur.Report)
}

func (m *Manager) path(name string) string { return filepath.Join(m.dir, name+".json") }

func (m *Manager) save(name string, c cached) error {
	if err := os.MkdirAll(m.dir, 0o700); err != nil {
		return err
	}
	raw, _ := json.Marshal(c)
	tmp := m.path(name) + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, m.path(name))
}

func (m *Manager) load(name string) (*cached, error) {
	raw, err := os.ReadFile(m.path(name))
	if err != nil {
		return nil, err
	}
	var c cached
	if err := json.Unmarshal(raw, &c); err != nil {
		return nil, err
	}
	return &c, nil
}
