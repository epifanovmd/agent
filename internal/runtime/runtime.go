// Package runtime — оркестратор агента: собирает возможности (задачи,
// команды, состояние, воркеры), держит связь, шлёт status и metrics,
// управляет режимами (работа, drain, обновление) и штатной остановкой.
// Связь продолжает работать во время остановки: итоги задач успевают уйти.
package runtime

import (
	"context"
	"log/slog"
	"maps"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Sender — чем возможности говорят с сервером (реализует link.Link).
type Sender interface {
	Stream(typ string, data any)
	Reliable(typ string, data any) error
	Request(ctx context.Context, typ string, data any, out any) error
	Connected() bool
}

// Capability — модуль агента: объявляет себя в hello и обрабатывает
// сообщения сервера своих типов. Handle не должен блокироваться надолго:
// сообщения обрабатываются по порядку одной горутиной.
type Capability interface {
	Declare(caps *message.Capabilities)
	Handles() []string
	Handle(ctx context.Context, env message.Envelope) error
}

// StatusContributor — вклад в status: слоты, задачи, воркеры, деградация.
type StatusContributor interface {
	ContributeStatus(st *message.Status)
}

// JobsReporter — задачи, выполняемые сейчас (для hello: сверка на сервере).
type JobsReporter interface {
	RunningJobs() []message.JobRef
}

// Starter — фоновая работа возможности на время жизни агента.
type Starter interface {
	Start(ctx context.Context) error
}

// Stopper — штатная остановка: перестать брать работу, доработать текущую.
type Stopper interface {
	Stop(ctx context.Context)
}

// Drainer — перестать брать новую работу (drain) или снова брать (resume).
type Drainer interface {
	SetAccepting(accepting bool)
}

// MetricsSource — телеметрия агента.
type MetricsSource interface {
	Collect(ctx context.Context) message.Metrics
}

// MetricsExtender — источник метрик принимает группы метрик узла из сводной
// подписки сервера в дополнение к настройке агента (необязательно для
// MetricsSource). true — изменился набор каналов телеметрии.
type MetricsExtender interface {
	SetExtra(groups []string) bool
}

// InventorySource — сведения об узле, меняющиеся редко (необязательно для MetricsSource).
type InventorySource interface {
	Inventory(ctx context.Context) message.Inventory
}

// defaultBacklog — точек метрик, собранных без связи, не больше (около часа
// при интервале 5 с); старые вытесняются.
const defaultBacklog = 720

// defaultInventoryEvery — как часто проверять, изменились ли сведения об узле.
const defaultInventoryEvery = 10 * time.Minute

// sameInventory — сведения не изменились (подменяется в тестах).
var sameInventory = func(a, b message.Inventory) bool {
	a.CollectedAt, b.CollectedAt = 0, 0
	return reflect.DeepEqual(a, b)
}

// Info — сведения об агенте для hello.
type Info struct {
	Name     string
	Version  string
	SDK      string
	CodeHash string
	Labels   map[string]string
	Host     message.Host
	// EncryptionKey — открытый ключ X25519 агента (base64) для hello.
	EncryptionKey string
}

// Runtime — агент.
type Runtime struct {
	info    Info
	log     *slog.Logger
	sender  Sender
	metrics MetricsSource
	outbox  func() int
	bootID  string
	started time.Time

	mu       sync.Mutex
	caps     []Capability
	handlers map[string]Capability
	cfg      message.SessionConfig
	// sub — сводная подписка сервера (пустая — подписок нет).
	sub message.Subscription

	draining atomic.Bool
	updating atomic.Bool
	// capsGen — поколение возможностей (растёт при CapabilitiesChanged);
	// helloGen — поколение, объявленное в hello текущей сессии.
	capsGen  atomic.Int64
	helloGen atomic.Int64
	changed  chan struct{}
	inbox    chan message.Envelope
	// metricsWake, statusWake — интервал изменился (config): применить сразу.
	metricsWake chan struct{}
	statusWake  chan struct{}
	// inventoryWake — сессия открыта: отправить сведения об узле.
	inventoryWake chan struct{}
	// backlog — метрики, собранные без связи (дошлются с backfill), не
	// больше backlogLimit (0 — не копить).
	backlog      []message.Metrics
	backlogLimit int
	// inventoryEvery — интервал проверки сведений об узле (0 — не отправлять);
	// inventoryReset — интервал изменился.
	inventoryEvery time.Duration
	inventoryReset chan struct{}
	// clockOffset — часы сервера − часы агента, мс (по welcome.serverTime).
	clockOffset atomic.Int64
	// inventory — последние отправленные сведения об узле.
	inventory *message.Inventory
	onWelcome []func(message.Welcome)
	// onLogLevel — порог отправки лога из сводной подписки.
	onLogLevel func(level string)
	// online — сессия открыта (welcome получен, связь не потеряна).
	online atomic.Bool
	// onContext — изменилось то, что видят воркеры (worker.context).
	onContext []func()
}

// New — агент; связь (Sender) подключается SetSender до Run.
func New(info Info, log *slog.Logger) *Runtime {
	return &Runtime{
		info:           info,
		log:            log,
		bootID:         message.NewBootID(),
		started:        time.Now(),
		handlers:       map[string]Capability{},
		cfg:            message.SessionConfig{StatusIntervalMs: 15_000, MetricsIntervalMs: 15_000},
		changed:        make(chan struct{}, 1),
		metricsWake:    make(chan struct{}, 1),
		statusWake:     make(chan struct{}, 1),
		inventoryWake:  make(chan struct{}, 1),
		inventoryReset: make(chan struct{}, 1),
		backlogLimit:   defaultBacklog,
		inventoryEvery: defaultInventoryEvery,
		inbox:          make(chan message.Envelope, 256),
		outbox:         func() int { return 0 },
	}
}

// SetTelemetry — точек метрик без связи не больше backlog (0 — не копить);
// сведения об узле раз в inventory (0 — не отправлять). Действует сразу.
func (r *Runtime) SetTelemetry(backlog int, inventory time.Duration) {
	r.mu.Lock()
	r.backlogLimit = backlog
	if len(r.backlog) > backlog {
		r.backlog = r.backlog[len(r.backlog)-backlog:]
	}
	changed := r.inventoryEvery != inventory
	r.inventoryEvery = inventory
	r.mu.Unlock()
	if changed {
		select {
		case r.inventoryReset <- struct{}{}:
		default:
		}
	}
}

// SetIdentity — имя и метки для следующего hello (сессию заново открывает
// вызывающий).
func (r *Runtime) SetIdentity(name string, labels map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.info.Name, r.info.Labels = name, labels
}

// OnLogLevel — fn получает порог отправки лога из сводной подписки сервера
// ("" — подписка лог не просит) при каждом welcome и каждой новой сводной.
func (r *Runtime) OnLogLevel(fn func(level string)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onLogLevel = fn
}

// SetOutbox — число неподтверждённых надёжных сообщений (в status).
func (r *Runtime) SetOutbox(count func() int) { r.outbox = count }

// SetSender — канал к серверу.
func (r *Runtime) SetSender(s Sender) { r.sender = s }

// SetMetrics — источник телеметрии.
func (r *Runtime) SetMetrics(m MetricsSource) { r.metrics = m }

// Register — добавить возможность.
func (r *Runtime) Register(c Capability) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.caps = append(r.caps, c)
	for _, t := range c.Handles() {
		r.handlers[t] = c
	}
}

// WhenWelcomed — вызывать fn при каждом открытии сессии.
func (r *Runtime) WhenWelcomed(fn func(message.Welcome)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onWelcome = append(r.onWelcome, fn)
}

// Changed — состояние изменилось: отправить status вне очереди.
func (r *Runtime) Changed() {
	select {
	case r.changed <- struct{}{}:
	default:
	}
}

// CapabilitiesChanged — возможности изменились после hello (воркер
// объявил команды, домены, каналы): сервер узнаёт о них сообщением
// capabilities сейчас или, если сессия ещё открывается, сразу после welcome.
func (r *Runtime) CapabilitiesChanged() {
	r.capsGen.Add(1)
	if r.sender != nil && r.sender.Connected() {
		r.sendCapabilities()
	}
}

func (r *Runtime) sendCapabilities() {
	gen := r.capsGen.Load()
	var caps message.Capabilities
	r.each(func(c Capability) { c.Declare(&caps) })
	r.sender.Stream(message.TypeCapabilities, caps)
	r.helloGen.Store(gen)
}

// Drain — перестать брать новую работу; текущая дорабатывается.
func (r *Runtime) Drain() { r.setAccepting(false) }

// Resume — снова брать работу.
func (r *Runtime) Resume() { r.setAccepting(true) }

// SetUpdating — идёт самообновление (status.state = updating).
func (r *Runtime) SetUpdating(v bool) {
	r.updating.Store(v)
	r.Changed()
}

func (r *Runtime) setAccepting(accepting bool) {
	r.draining.Store(!accepting)
	r.each(func(c Capability) {
		if d, ok := c.(Drainer); ok {
			d.SetAccepting(accepting)
		}
	})
	r.Changed()
}

func (r *Runtime) each(fn func(Capability)) {
	r.mu.Lock()
	caps := append([]Capability(nil), r.caps...)
	r.mu.Unlock()
	for _, c := range caps {
		fn(c)
	}
}

// ─── link.Handler ──────────────────────────────────────────────────────

// Hello — приветствие сессии: возможности и выполняющиеся задачи на сейчас.
func (r *Runtime) Hello() message.Hello {
	r.mu.Lock()
	info := r.info
	r.mu.Unlock()
	h := message.Hello{
		Versions: message.Versions,
		Agent: message.HelloAgent{
			Name:          info.Name,
			Version:       info.Version,
			SDK:           info.SDK,
			CodeHash:      info.CodeHash,
			BootID:        r.bootID,
			StartedAt:     r.started.UnixMilli(),
			EncryptionKey: info.EncryptionKey,
		},
		Host:   info.Host,
		Labels: info.Labels,
		Jobs:   []message.JobRef{},
	}
	r.helloGen.Store(r.capsGen.Load())
	r.each(func(c Capability) {
		c.Declare(&h.Capabilities)
		if j, ok := c.(JobsReporter); ok {
			h.Jobs = append(h.Jobs, j.RunningJobs()...)
		}
	})
	return h
}

// OnWelcome — сессия открыта: настройки сервера, немедленный status.
func (r *Runtime) OnWelcome(w message.Welcome) {
	if w.ServerTime > 0 {
		r.clockOffset.Store(w.ServerTime - time.Now().UnixMilli())
	}
	r.applyConfig(w.Config)
	// Новая сессия: сводная подписка — из welcome; нет — подписок нет.
	var sub message.Subscription
	if w.Config.Subscription != nil {
		sub = *w.Config.Subscription
	}
	r.setSubscription(sub)
	r.mu.Lock()
	hooks := append([]func(message.Welcome){}, r.onWelcome...)
	r.mu.Unlock()
	for _, fn := range hooks {
		fn(w)
	}
	if r.capsGen.Load() != r.helloGen.Load() {
		// Воркер зарегистрировался между hello и welcome.
		r.sendCapabilities()
	}
	r.online.Store(true)
	r.contextChanged()
	// Метрики, собранные без связи, и сведения об узле (сервер мог перезапуститься).
	go r.flushBacklog()
	select {
	case r.inventoryWake <- struct{}{}:
	default:
	}
	r.Changed()
}

// Online — сессия с сервером открыта.
func (r *Runtime) Online() bool { return r.online.Load() }

// MetricsInterval — действующая частота метрик: настройка сервера,
// ужесточённая сводной подпиской (метрики и каналы показателей).
func (r *Runtime) MetricsInterval() time.Duration {
	_, metrics := r.intervals()
	return metrics
}

// StatusInterval — действующая частота статуса (с учётом подписки).
func (r *Runtime) StatusInterval() time.Duration {
	status, _ := r.intervals()
	return status
}

// Subscription — текущая сводная подписка (копия; пустая — подписок нет).
func (r *Runtime) Subscription() message.Subscription {
	r.mu.Lock()
	defer r.mu.Unlock()
	sub := r.sub
	sub.Metrics = slices.Clone(sub.Metrics)
	sub.Channels = maps.Clone(sub.Channels)
	return sub
}

// WhenContextChanged — fn вызывается, когда могло измениться то, что видят
// воркеры: связь, частота метрик, порог лога, подписки на каналы. Повторы возможны.
func (r *Runtime) WhenContextChanged(fn func()) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.onContext = append(r.onContext, fn)
}

func (r *Runtime) contextChanged() {
	r.mu.Lock()
	hooks := append([]func(){}, r.onContext...)
	r.mu.Unlock()
	for _, fn := range hooks {
		fn()
	}
}

// OnMessage — сообщение сервера: по порядку, в горутину обработки.
func (r *Runtime) OnMessage(env message.Envelope) {
	r.inbox <- env
}

// OnDisconnect — связь потеряна: работа продолжается автономно.
func (r *Runtime) OnDisconnect(error) {
	if r.online.Swap(false) {
		r.contextChanged()
	}
}

func (r *Runtime) applyConfig(c message.SessionConfig) {
	st, mt := r.intervals()
	r.mu.Lock()
	if c.StatusIntervalMs > 0 {
		r.cfg.StatusIntervalMs = c.StatusIntervalMs
	}
	if c.MetricsIntervalMs > 0 {
		r.cfg.MetricsIntervalMs = c.MetricsIntervalMs
	}
	r.mu.Unlock()
	r.wakeLoops(st, mt)
}

// setSubscription — новая сводная подписка: частоты, группы метрик, порог
// лога; воркеры узнают о ней из worker.context.
func (r *Runtime) setSubscription(sub message.Subscription) {
	sub.Metrics = slices.Clone(sub.Metrics)
	sub.Channels = maps.Clone(sub.Channels)
	st, mt := r.intervals()
	r.mu.Lock()
	r.sub = sub
	fn := r.onLogLevel
	r.mu.Unlock()
	r.wakeLoops(st, mt)
	if ext, ok := r.metrics.(MetricsExtender); ok && ext.SetExtra(sub.Metrics) {
		r.CapabilitiesChanged()
	}
	if fn != nil {
		fn(sub.LogLevel)
	}
	r.contextChanged()
}

// wakeLoops — действующая частота изменилась (была status, metrics):
// цикл статуса или метрик берёт новую сразу.
func (r *Runtime) wakeLoops(status, metrics time.Duration) {
	st, mt := r.intervals()
	wake := func(ch chan struct{}) {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
	if st != status {
		wake(r.statusWake)
	}
	if mt != metrics {
		wake(r.metricsWake)
	}
}

// intervals — действующие частоты: настройки сервера, ужесточённые сводной
// подпиской; частота метрик — ещё и не реже подписок на каналы (показатели
// воркеров уходят в metrics).
func (r *Runtime) intervals() (status, metrics time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	st, mt := r.cfg.StatusIntervalMs, r.cfg.MetricsIntervalMs
	tighten := func(cur *int64, v int64) {
		if v > 0 && v < *cur {
			*cur = v
		}
	}
	tighten(&st, r.sub.StatusIntervalMs)
	tighten(&mt, r.sub.MetricsIntervalMs)
	for _, v := range r.sub.Channels {
		tighten(&mt, v)
	}
	return time.Duration(st) * time.Millisecond, time.Duration(mt) * time.Millisecond
}

// ─── Работа ────────────────────────────────────────────────────────────

// Run — фоновая работа возможностей, связь (link), обработка сообщений,
// status и metrics до отмены ctx; затем штатная остановка возможностей не
// дольше stopTimeout и досылка итогов.
func (r *Runtime) Run(ctx context.Context, link func(ctx context.Context) error, stopTimeout time.Duration) error {
	var wg sync.WaitGroup
	r.each(func(c Capability) {
		if s, ok := c.(Starter); ok {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := s.Start(ctx); err != nil && ctx.Err() == nil {
					r.log.Error("агент: возможность остановилась с ошибкой", "err", err)
				}
			}()
		}
	})

	linkCtx, stopLink := context.WithCancel(context.Background())
	linkDone := make(chan struct{})
	go func() {
		defer close(linkDone)
		_ = link(linkCtx)
	}()
	go r.dispatch(linkCtx)
	go r.statusLoop(linkCtx)
	go r.metricsLoop(linkCtx)
	go r.inventoryLoop(linkCtx)

	<-ctx.Done()
	r.log.Info("агент: остановка — новая работа не берётся, текущая дорабатывается", "timeout", stopTimeout)
	r.Drain()
	stopCtx, cancel := context.WithTimeout(context.Background(), stopTimeout)
	defer cancel()
	r.each(func(c Capability) {
		if s, ok := c.(Stopper); ok {
			s.Stop(stopCtx)
		}
	})
	wg.Wait()
	// Последний status и досылка итогов, пока связь есть.
	r.sendStatus()
	flushUntil := time.Now().Add(3 * time.Second)
	for time.Now().Before(flushUntil) && r.outbox() > 0 && r.sender.Connected() {
		time.Sleep(100 * time.Millisecond)
	}
	stopLink()
	<-linkDone
	return nil
}

func (r *Runtime) dispatch(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case env := <-r.inbox:
			r.mu.Lock()
			c := r.handlers[env.Type]
			r.mu.Unlock()
			if c == nil {
				if env.Type != message.TypeConfig {
					r.log.Debug("агент: сообщение без обработчика", "type", env.Type)
					continue
				}
				var cfg message.SessionConfig
				if env.Decode(&cfg) == nil {
					r.applyConfig(cfg)
					if cfg.Subscription != nil {
						r.setSubscription(*cfg.Subscription)
					} else {
						r.contextChanged()
					}
				}
				continue
			}
			if err := c.Handle(ctx, env); err != nil {
				r.log.Warn("агент: сообщение сервера не обработано", "type", env.Type, "err", err)
			}
		}
	}
}

func (r *Runtime) statusLoop(ctx context.Context) {
	for {
		interval, _ := r.intervals()
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-r.statusWake:
			timer.Stop()
			continue
		case <-timer.C:
		case <-r.changed:
			timer.Stop()
			// Несколько изменений подряд — один status.
			time.Sleep(50 * time.Millisecond)
		}
		r.sendStatus()
	}
}

// metricsLoop — метрики раз в metricsIntervalMs; без связи — в backlog
// (дошлются после welcome); новый интервал из config — сразу.
func (r *Runtime) metricsLoop(ctx context.Context) {
	if r.metrics == nil {
		return
	}
	for {
		_, interval := r.intervals()
		timer := time.NewTimer(interval)
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-r.metricsWake:
			timer.Stop()
			continue
		case <-timer.C:
		}
		m := r.metrics.Collect(ctx)
		if r.sender.Connected() {
			r.flushBacklog()
			r.stamp(&m)
			r.sender.Stream(message.TypeMetrics, m)
			continue
		}
		r.mu.Lock()
		if r.backlogLimit > 0 {
			r.backlog = append(r.backlog, m)
			if len(r.backlog) > r.backlogLimit {
				r.backlog = r.backlog[len(r.backlog)-r.backlogLimit:]
			}
		}
		r.mu.Unlock()
	}
}

// flushBacklog — метрики, собранные без связи, по порядку с backfill.
func (r *Runtime) flushBacklog() {
	r.mu.Lock()
	backlog := r.backlog
	r.backlog = nil
	r.mu.Unlock()
	for _, m := range backlog {
		m.Backfill = true
		r.stamp(&m)
		r.sender.Stream(message.TypeMetrics, m)
	}
}

// stamp — смещение часов: время точки на сервере — collectedAt + смещение.
func (r *Runtime) stamp(m *message.Metrics) {
	offset := r.clockOffset.Load()
	m.ClockOffsetMs = &offset
}

// inventoryLoop — сведения об узле: после каждого welcome и при изменении
// (проверка раз в inventoryEvery; 0 — сведения не отправляются).
func (r *Runtime) inventoryLoop(ctx context.Context) {
	src, ok := r.metrics.(InventorySource)
	if !ok {
		return
	}
	for {
		r.mu.Lock()
		every := r.inventoryEvery
		r.mu.Unlock()
		var tick <-chan time.Time
		var timer *time.Timer
		if every > 0 {
			timer = time.NewTimer(every)
			tick = timer.C
		}
		force := false
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			return
		case <-r.inventoryReset:
			if timer != nil {
				timer.Stop()
			}
			continue
		case <-r.inventoryWake:
			force = true
		case <-tick:
		}
		if timer != nil {
			timer.Stop()
		}
		if every <= 0 || !r.sender.Connected() {
			continue
		}
		inv := src.Inventory(ctx)
		r.mu.Lock()
		same := r.inventory != nil && sameInventory(*r.inventory, inv)
		if !same || force {
			r.inventory = &inv
		}
		r.mu.Unlock()
		if !same || force {
			r.sender.Stream(message.TypeInventory, inv)
		}
	}
}

func (r *Runtime) buildStatus() message.Status {
	st := message.Status{Slots: map[string]int{}, Jobs: []message.StatusJob{}, Workers: []message.StatusWorker{}, Outbox: r.outbox()}
	r.each(func(c Capability) {
		if s, ok := c.(StatusContributor); ok {
			s.ContributeStatus(&st)
		}
	})
	switch {
	case r.updating.Load():
		st.State = message.StateUpdating
	case r.draining.Load():
		st.State = message.StateDraining
		for q := range st.Slots {
			st.Slots[q] = 0
		}
	case st.State != "":
	case len(st.Jobs) > 0:
		st.State = message.StateBusy
	default:
		st.State = message.StateIdle
	}
	return st
}

func (r *Runtime) sendStatus() {
	if r.sender == nil {
		return
	}
	r.sender.Stream(message.TypeStatus, r.buildStatus())
}

// Status — текущий status (для команд и тестов).
func (r *Runtime) Status() message.Status { return r.buildStatus() }
