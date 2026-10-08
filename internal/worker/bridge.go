//go:build unix

package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"sort"
	"sync"

	"github.com/epifanovmd/agent/internal/commands"
	"github.com/epifanovmd/agent/internal/state"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// Bridge — куда воркеры подключают свои команды, домены желаемого состояния,
// каналы телеметрии и события (§10 спецификации). Пустое поле — объявления этого
// вида отклоняются.
type Bridge struct {
	Commands  *commands.Registry
	State     *state.Manager
	Telemetry TelemetrySink
	Events    EventSender
	// Changed — возможности агента изменились: сообщить серверу (capabilities).
	Changed func()
	// Narrowed — воркер перестал объявлять часть имён: серверу нужен новый
	// hello (capabilities только добавляет).
	Narrowed func()
}

// TelemetrySink — каналы телеметрии агента (telemetry.Collector).
type TelemetrySink interface {
	Declare(channel string)
	Has(channel string) bool
	Report(channel string, data json.RawMessage)
	Drop(channel string)
	// Undeclare — канал снят (воркер удалён из настроек).
	Undeclare(channel string)
}

// EventSender — надёжная отправка событий (link.Link).
type EventSender interface {
	Reliable(typ string, data any) error
}

// SetBridge — подключить возможности агента; до Start.
func (s *Supervisor) SetBridge(b Bridge) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.bridge = b
}

// Виды имён, которые объявляет воркер.
const (
	kindCommand = "command"
	kindDomain  = "domain"
	kindChannel = "channel"
)

type owned struct{ kind, name string }

// claim — регистрация экземпляра: какие имена воркер получает. Имя
// принадлежит первому объявившему его воркеру; не по правилу имени
// (message.ValidName), зарезервированные и занятые агентом или другим
// воркером — отклоняются. Подключение к агенту — позже,
// в activate: экземпляр должен быть готов принимать вызовы.
func (s *Supervisor) claim(w *worker, reg message.WorkerRegister) (accepted map[owned]bool, rejected []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	accepted = map[owned]bool{}
	try := func(kind, name string, available bool) {
		key := owned{kind, name}
		owner, taken := s.owners[key]
		switch {
		case name == "" || accepted[key]:
			return
		case !message.ValidName(name):
			rejected = append(rejected, name)
			return
		case taken && owner == w.spec.Name:
		case taken || !available || (kind != kindChannel && message.Reserved(name)) || s.bridgedElsewhere(kind, name):
			rejected = append(rejected, name)
			return
		}
		s.owners[key] = w.spec.Name
		accepted[key] = true
	}
	for _, name := range reg.Commands {
		try(kindCommand, name, s.bridge.Commands != nil)
	}
	for _, name := range reg.Domains {
		try(kindDomain, name, s.bridge.State != nil)
	}
	for _, name := range reg.Channels {
		try(kindChannel, name, s.bridge.Telemetry != nil)
	}
	return accepted, rejected
}

// invalidNames — объявленные имена не по правилу message.NamePattern (пустые —
// пропускаются, как в claim).
func invalidNames(reg message.WorkerRegister) []string {
	var bad []string
	check := func(name string) {
		if name != "" && !message.ValidName(name) && !slices.Contains(bad, name) {
			bad = append(bad, name)
		}
	}
	for _, q := range reg.Queues {
		check(q.Name)
	}
	for _, list := range [][]string{reg.Commands, reg.Domains, reg.Channels} {
		for _, name := range list {
			check(name)
		}
	}
	return bad
}

// bridgedElsewhere — имя уже есть у агента, но не от воркеров (Go-код проекта,
// встроенные каналы). Под s.mu.
func (s *Supervisor) bridgedElsewhere(kind, name string) bool {
	switch kind {
	case kindCommand:
		return s.bridge.Commands != nil && s.bridge.Commands.Has(name)
	case kindDomain:
		return s.bridge.State != nil && s.bridge.State.Has(name)
	case kindChannel:
		return s.bridge.Telemetry != nil && s.bridge.Telemetry.Has(name)
	}
	return false
}

// activate — экземпляр воркера принимает вызовы: имена, ещё не подключённые к
// агенту, подключаются (сервер узнаёт о них через capabilities), домены,
// подключённые раньше, получают последний снимок заново.
func (s *Supervisor) activate(w *worker) {
	s.mu.Lock()
	var fresh, kick []owned
	for key, owner := range s.owners {
		if owner != w.spec.Name {
			continue
		}
		if s.bridged[key] {
			if key.kind == kindDomain {
				kick = append(kick, key)
			}
			continue
		}
		s.bridged[key] = true
		fresh = append(fresh, key)
	}
	b := s.bridge
	s.mu.Unlock()

	for _, key := range fresh {
		switch key.kind {
		case kindCommand:
			b.Commands.Register(key.name, s.commandHandler(w.spec.Name, key.name))
		case kindDomain:
			// Регистрация применяет кэш домена: экземпляр уже принимает вызовы.
			if err := b.State.Register(domainProxy{s: s, worker: w.spec.Name, domain: key.name}); err != nil {
				s.log.Error("воркер: домен не подключён", "worker", w.spec.Name, "domain", key.name, "err", err)
			}
		case kindChannel:
			b.Telemetry.Declare(key.name)
		}
	}
	for _, key := range kick {
		b.State.Kick(key.name)
	}
	if len(fresh) > 0 && b.Changed != nil {
		b.Changed()
	}
}

// route — экземпляр воркера, принимающий вызовы: текущий в слоте,
// зарегистрированный, не уходящий. Из нескольких — самый новый.
func (s *Supervisor) route(name string) *instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.workers[name]
	if w == nil {
		return nil
	}
	var best *instance
	for _, sl := range w.slots {
		inst := sl.current
		if inst == nil || !inst.registered() || !inst.Accepting() {
			continue
		}
		if best == nil || inst.started.After(best.started) {
			best = inst
		}
	}
	return best
}

// routeAll — все копии воркера, принимающие вызовы, от самой новой.
func (s *Supervisor) routeAll(name string) []*instance {
	s.mu.Lock()
	defer s.mu.Unlock()
	w := s.workers[name]
	if w == nil {
		return nil
	}
	var out []*instance
	for _, sl := range w.slots {
		if inst := sl.current; inst != nil && inst.registered() && inst.Accepting() {
			out = append(out, inst)
		}
	}
	sort.Slice(out, func(a, b int) bool { return out[a].started.After(out[b].started) })
	return out
}

// owns — имя принадлежит воркеру.
func (s *Supervisor) owns(worker, kind, name string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.owners[owned{kind, name}] == worker
}

// commandHandler — команда воркера: вызов передаётся его экземпляру.
func (s *Supervisor) commandHandler(worker, name string) commands.Handler {
	return func(ctx context.Context, args json.RawMessage, out io.Writer) (any, error) {
		inst := s.route(worker)
		if inst == nil {
			return nil, commands.Errorf("WORKER_UNAVAILABLE", "Воркер %s не запущен", worker)
		}
		return inst.command(ctx, name, args, out)
	}
}

// domainProxy — домен желаемого состояния, который применяет воркер.
type domainProxy struct {
	s      *Supervisor
	worker string
	domain string
}

func (d domainProxy) Domain() string { return d.domain }

// Apply — снимок получают все копии воркера (у каждой своё состояние);
// применён — когда применили все; ошибка любой — ошибка (с её отчётом),
// отчёт успеха — самой новой копии.
func (d domainProxy) Apply(ctx context.Context, version int64, spec json.RawMessage) (any, error) {
	insts := d.s.routeAll(d.worker)
	if len(insts) == 0 {
		return nil, &message.Error{Code: "WORKER_UNAVAILABLE", Message: "воркер " + d.worker + " не запущен"}
	}
	type result struct {
		report any
		err    error
	}
	results := make([]result, len(insts))
	var wg sync.WaitGroup
	for n, inst := range insts {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := inst.applyState(ctx, message.StatePut{Domain: d.domain, Version: version, Spec: spec})
			results[n] = result{r, err}
		}()
	}
	wg.Wait()
	for n, r := range results {
		if r.err != nil {
			if len(insts) > 1 {
				r.err = fmt.Errorf("копия %s: %w", insts[n].id, r.err)
			}
			return r.report, r.err
		}
	}
	return results[0].report, nil
}

// Available — воркер-владелец домена запущен и принимает вызовы
// (state.Availability: повторное применение по интервалу без него пропускается).
func (d domainProxy) Available() bool { return d.s.route(d.worker) != nil }
