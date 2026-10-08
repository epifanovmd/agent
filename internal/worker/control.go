//go:build unix

package worker

import (
	"encoding/json"
	"maps"
	"os"
	"reflect"
	"slices"
	"sync"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// contextEvery — worker.context одному экземпляру не чаще этого.
var contextEvery = 100 * time.Millisecond

// ─── пауза очередей ───────────────────────────────────────────────────

// pauseSet — какие очереди на паузе: все (all, кроме except) или
// перечисленные (queues). Нулевое значение — паузы нет.
type pauseSet struct {
	all    bool
	except map[string]bool
	queues map[string]bool
}

// pause — очереди на паузу; пусто — все.
func (p *pauseSet) pause(queues []string) {
	if len(queues) == 0 {
		*p = pauseSet{all: true}
		return
	}
	for _, q := range queues {
		if p.all {
			delete(p.except, q)
			continue
		}
		if p.queues == nil {
			p.queues = map[string]bool{}
		}
		p.queues[q] = true
	}
}

// resume — снять паузу с очередей; пусто — со всех.
func (p *pauseSet) resume(queues []string) {
	if len(queues) == 0 {
		*p = pauseSet{}
		return
	}
	for _, q := range queues {
		if !p.all {
			delete(p.queues, q)
			continue
		}
		if p.except == nil {
			p.except = map[string]bool{}
		}
		p.except[q] = true
	}
}

func (p *pauseSet) paused(queue string) bool {
	if p.all {
		return !p.except[queue]
	}
	return p.queues[queue]
}

// any — хоть одна очередь из served на паузе (served пусто — по наличию паузы).
func (p *pauseSet) any(served []string) bool {
	if !p.all && len(p.queues) == 0 {
		return false
	}
	if len(served) == 0 {
		return true
	}
	return slices.ContainsFunc(served, p.paused)
}

// pauseJSON — пауза на диске.
type pauseJSON struct {
	All    bool     `json:"all,omitempty"`
	Except []string `json:"except,omitempty"`
	Queues []string `json:"queues,omitempty"`
}

func (p pauseSet) json() pauseJSON {
	return pauseJSON{All: p.all, Except: slices.Sorted(maps.Keys(p.except)), Queues: slices.Sorted(maps.Keys(p.queues))}
}

func (j pauseJSON) set() pauseSet {
	p := pauseSet{all: j.All}
	for _, q := range j.Except {
		if p.except == nil {
			p.except = map[string]bool{}
		}
		p.except[q] = true
	}
	for _, q := range j.Queues {
		if p.queues == nil {
			p.queues = map[string]bool{}
		}
		p.queues[q] = true
	}
	return p
}

// SetPauseFile — хранить паузу сервера в файле path: она переживает
// перезапуск агента. Пауза, сохранённая раньше, применяется сразу (до Start).
func (s *Supervisor) SetPauseFile(path string) {
	saved := map[string]pauseJSON{}
	if raw, err := os.ReadFile(path); err == nil {
		if err := json.Unmarshal(raw, &saved); err != nil {
			s.log.Warn("пауза воркеров не прочитана — без неё", "file", path, "err", err)
			saved = map[string]pauseJSON{}
		}
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pauseFile, s.savedPause = path, saved
	for name, w := range s.workers {
		if p, ok := saved[name]; ok {
			w.pause.mu.Lock()
			w.pause.set = p.set()
			w.pause.mu.Unlock()
		}
	}
}

// savePause — пауза сервера всех воркеров на диск (атомарно).
func (s *Supervisor) savePause() {
	s.mu.Lock()
	path := s.pauseFile
	out := map[string]pauseJSON{}
	for name, w := range s.workers {
		w.pause.mu.Lock()
		if w.pause.set.all || len(w.pause.set.queues) > 0 {
			out[name] = w.pause.set.json()
		}
		w.pause.mu.Unlock()
	}
	s.mu.Unlock()
	if path == "" {
		return
	}
	raw, _ := json.Marshal(out)
	tmp := path + ".tmp"
	err := os.WriteFile(tmp, raw, 0o600)
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		s.log.Error("пауза воркеров не сохранена", "file", path, "err", err)
	}
}

// serverPause — пауза, выставленная сервером (команды worker.pause и
// worker.resume): живёт у воркера, переживает перезапуск экземпляров и
// (с SetPauseFile) агента.
type serverPause struct {
	mu  sync.Mutex
	set pauseSet
}

// Pause — команда worker.pause: места воркера name по очередям queues (пусто —
// по всем) — 0; выданные задачи доделываются. Пауза, выставленная самим
// воркером (worker.pause по IPC), — отдельно: снимает её только воркер.
func (s *Supervisor) Pause(name string, queues []string) error {
	return s.serverPauseDo(name, func(p *pauseSet) { p.pause(queues) })
}

// Resume — команда worker.resume: снять паузу сервера с очередей (пусто — со всех).
func (s *Supervisor) Resume(name string, queues []string) error {
	return s.serverPauseDo(name, func(p *pauseSet) { p.resume(queues) })
}

func (s *Supervisor) serverPauseDo(name string, fn func(*pauseSet)) error {
	s.mu.Lock()
	w, ok := s.workers[name]
	s.mu.Unlock()
	if !ok {
		return ErrUnknownWorker
	}
	w.pause.mu.Lock()
	fn(&w.pause.set)
	w.pause.mu.Unlock()
	s.savePause()
	s.changed()
	return nil
}

// Paused — хоть одна очередь воркера name на паузе (от сервера или от воркера).
func (s *Supervisor) Paused(name string) bool {
	s.mu.Lock()
	w := s.workers[name]
	var insts []*instance
	var served []string
	if w != nil {
		served = w.spec.Queues
		for _, sl := range w.slots {
			if sl.current != nil {
				insts = append(insts, sl.current)
			}
		}
	}
	s.mu.Unlock()
	if w == nil {
		return false
	}
	return w.paused(served, insts)
}

// paused — пауза сервера (по очередям served из настроек; пусто — любая) или
// хоть одного экземпляра.
func (w *worker) paused(served []string, insts []*instance) bool {
	w.pause.mu.Lock()
	server := w.pause.set.any(served)
	w.pause.mu.Unlock()
	if server {
		return true
	}
	for _, inst := range insts {
		inst.mu.Lock()
		self := inst.selfPause.any(slices.Collect(maps.Keys(inst.queues)))
		inst.mu.Unlock()
		if self {
			return true
		}
	}
	return false
}

// QueuePaused — jobs.QueuePauser: очередь на паузе у сервера или у самого
// экземпляра.
func (i *instance) QueuePaused(queue string) bool {
	i.mu.Lock()
	self := i.selfPause.paused(queue)
	i.mu.Unlock()
	if self {
		return true
	}
	i.w.pause.mu.Lock()
	defer i.w.pause.mu.Unlock()
	return i.w.pause.set.paused(queue)
}

// ─── сообщения самоуправления ─────────────────────────────────────────

// control — worker.health, worker.pause, worker.resume, worker.restart.
func (i *instance) control(env message.Envelope) error {
	switch env.Type {
	case message.TypeWorkerHealth:
		var h message.WorkerHealth
		if err := env.Decode(&h); err != nil {
			return err
		}
		i.mu.Lock()
		changed := i.health == nil || *i.health != h
		i.health = &h
		i.mu.Unlock()
		if changed {
			if h.OK {
				i.log.Info("воркер в порядке")
			} else {
				i.log.Warn("воркер сообщил о неполадке", "message", h.Message)
			}
			i.sup.changed()
		}
	case message.TypeWorkerPause, message.TypeWorkerResume:
		var p message.WorkerPause
		if err := env.Decode(&p); err != nil {
			return err
		}
		i.mu.Lock()
		if env.Type == message.TypeWorkerPause {
			i.selfPause.pause(p.Queues)
		} else {
			i.selfPause.resume(p.Queues)
		}
		i.mu.Unlock()
		i.log.Info("воркер: "+env.Type, "queues", p.Queues)
		i.sup.changed()
	case message.TypeWorkerRestart:
		var r message.WorkerRestartRequest
		if err := env.Decode(&r); err != nil {
			return err
		}
		go i.sup.requestRestart(i, r.Reason)
	}
	return nil
}

// requestRestart — воркер просит заменить себя: то же, что команда
// worker.restart (его способом, без паузы и degraded). Повторные просьбы,
// пока идёт замена, и просьбы уходящего экземпляра не учитываются.
func (s *Supervisor) requestRestart(inst *instance, reason string) {
	if s.cleaning || inst.retired.Load() || inst.stopping.Load() {
		return
	}
	s.mu.Lock()
	ctx, w := s.ctx, inst.w
	if ctx == nil || s.stopping || w.restarting || w.updating || s.workers[w.spec.Name] != w {
		s.mu.Unlock()
		return
	}
	w.restarting = true
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		w.restarting = false
		s.mu.Unlock()
	}()
	inst.log.Info("воркер просит замену", "reason", reason)
	if err := s.Restart(ctx, w.spec.Name); err != nil && ctx.Err() == nil {
		inst.log.Error("воркер не заменён по его просьбе", "err", err)
	}
}

// health — оценка воркера по его экземплярам: "" — не сообщали; degraded —
// хоть один не в порядке (message — его причина). Без s.mu.
func health(insts []*instance) (state, msg string) {
	for _, inst := range insts {
		inst.mu.Lock()
		h := inst.health
		inst.mu.Unlock()
		switch {
		case h == nil:
		case !h.OK:
			return message.WorkerHealthDegraded, h.Message
		default:
			state = message.WorkerHealthOK
		}
	}
	return state, ""
}

// ─── контекст воркера ──────────────────────────────────────────────────

// SetContext — контекст агента для воркеров (worker.context): экземпляры
// получают его при изменении, не чаще contextEvery; одинаковый повторно не
// шлётся. Channels — подписки на каналы всех воркеров: каждый экземпляр
// видит только принятые у него каналы.
func (s *Supervisor) SetContext(c message.WorkerContext) {
	if c.Mode == "" {
		c.Mode = message.WorkerModeRun
	}
	c.Agent.Labels = maps.Clone(c.Agent.Labels)
	c.Channels = maps.Clone(c.Channels)
	s.mu.Lock()
	if reflect.DeepEqual(s.wctx, c) {
		s.mu.Unlock()
		return
	}
	s.wctx = c
	var insts []*instance
	for _, w := range s.workers {
		for _, sl := range w.slots {
			if sl.current != nil {
				insts = append(insts, sl.current)
			}
		}
	}
	for inst := range s.leaving {
		insts = append(insts, inst)
	}
	s.mu.Unlock()
	for _, inst := range insts {
		select {
		case inst.ctxWake <- struct{}{}:
		default:
		}
	}
}

func (s *Supervisor) workerContext() message.WorkerContext {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.wctx
}

// pushContext — отправить контекст, если он отличается от отправленного.
// Подписки на каналы — только каналов этого экземпляра.
func (i *instance) pushContext() {
	c := i.sup.workerContext()
	var own map[string]int64
	for ch, ms := range c.Channels {
		if i.has(kindChannel, ch) {
			if own == nil {
				own = map[string]int64{}
			}
			own[ch] = ms
		}
	}
	c.Channels = own
	i.mu.Lock()
	if i.sentCtx != nil && reflect.DeepEqual(*i.sentCtx, c) {
		i.mu.Unlock()
		return
	}
	i.sentCtx = &c
	i.mu.Unlock()
	if err := i.send(message.MustNew(message.TypeWorkerContext, c)); err != nil {
		i.log.Debug("воркер: worker.context не отправлен", "err", err)
	}
}

// contextLoop — изменения контекста после первого (он уходит сразу после
// worker.ready): не чаще contextEvery, до выхода экземпляра.
func (i *instance) contextLoop() {
	for {
		select {
		case <-i.exited:
			return
		case <-time.After(contextEvery):
		}
		select {
		case <-i.exited:
			return
		case <-i.ctxWake:
		}
		i.pushContext()
	}
}
