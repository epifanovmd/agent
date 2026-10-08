package server

import (
	"maps"
	"reflect"
	"regexp"
	"slices"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Подписка — «присылай это, так часто, столько времени». У агента их
// несколько (у каждого подписчика своя), сервер сводит действующие в одну
// (message.Subscription) и шлёт агенту в welcome и config.

// IntervalSpec — частота: IntervalMs, мс (не меньше 200).
type IntervalSpec struct {
	IntervalMs int64 `json:"intervalMs"`
}

// MetricsSpec — метрики узла: IntervalMs — частота, мс (0 — не менять, иначе
// не меньше 200); Groups — группы метрик узла (message.MetricGroups) сверх
// настройки агента telemetry.metrics.
type MetricsSpec struct {
	IntervalMs int64    `json:"intervalMs,omitempty"`
	Groups     []string `json:"groups,omitempty"`
}

// LogsSpec — отправка лога агента с уровня Level (debug | info | warn | error).
type LogsSpec struct {
	Level string `json:"level"`
}

// SubscribeRequest — подписка (Agents.Subscribe). ID пустой — сервер создаёт
// новый; существующий — продлить срок и заменить содержимое. TTL — срок (0 —
// 30 с). Status, Metrics, Logs, Channels (канал показателей воркера →
// частота) — что присылать; nil — подписка этого не касается.
type SubscribeRequest struct {
	ID       string
	TTL      time.Duration
	Status   *IntervalSpec
	Metrics  *MetricsSpec
	Logs     *LogsSpec
	Channels map[string]IntervalSpec
}

// Subscription — подписка в записи агента (Agent.Subscriptions): общая для
// процессов с общим Store. Until — срок, мс UTC.
type Subscription struct {
	ID       string                  `json:"id"`
	Until    int64                   `json:"until"`
	Status   *IntervalSpec           `json:"status,omitempty"`
	Metrics  *MetricsSpec            `json:"metrics,omitempty"`
	Logs     *LogsSpec               `json:"logs,omitempty"`
	Channels map[string]IntervalSpec `json:"channels,omitempty"`
}

// clone — глубокая копия.
func (s Subscription) clone() Subscription {
	if s.Status != nil {
		v := *s.Status
		s.Status = &v
	}
	if s.Metrics != nil {
		v := *s.Metrics
		v.Groups = slices.Clone(v.Groups)
		s.Metrics = &v
	}
	if s.Logs != nil {
		v := *s.Logs
		s.Logs = &v
	}
	s.Channels = maps.Clone(s.Channels)
	return s
}

// Сроки подписки.
const (
	defaultSubscriptionTTL = 30 * time.Second
	minSubscriptionMs      = 200
)

// metricGroupName — форма имени группы метрик узла (cpu, cpu.cores, diskio …):
// проверяется только форма — группы новых версий агента принимаются.
var metricGroupName = regexp.MustCompile(`^[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*$`)

// logRank — подробность уровня лога: меньше — подробнее.
var logRank = map[string]int{message.LogDebug: 0, message.LogInfo: 1, message.LogWarn: 2, message.LogError: 3}

func validInterval(ms int64) bool { return ms >= minSubscriptionMs }

func invalidSub(msg string) error { return protoErr("MESSAGE_INVALID", msg) }

// subscriptionOf — подписка из запроса с проверками (без ID и Until).
func subscriptionOf(req SubscribeRequest) (Subscription, error) {
	var sub Subscription
	if req.Status != nil {
		if !validInterval(req.Status.IntervalMs) {
			return sub, invalidSub("status.intervalMs — не меньше 200 мс")
		}
		sub.Status = &IntervalSpec{IntervalMs: req.Status.IntervalMs}
	}
	if req.Metrics != nil {
		ms := req.Metrics.IntervalMs
		if ms != 0 && !validInterval(ms) {
			return sub, invalidSub("metrics.intervalMs — не меньше 200 мс")
		}
		m := &MetricsSpec{IntervalMs: ms}
		for _, g := range req.Metrics.Groups {
			if !metricGroupName.MatchString(g) {
				return sub, invalidSub("Неверное имя группы метрик " + g)
			}
			if !slices.Contains(m.Groups, g) {
				m.Groups = append(m.Groups, g)
			}
		}
		sub.Metrics = m
	}
	if req.Logs != nil {
		if _, ok := logRank[req.Logs.Level]; !ok {
			return sub, invalidSub("logs.level — debug, info, warn или error")
		}
		sub.Logs = &LogsSpec{Level: req.Logs.Level}
	}
	for ch, iv := range req.Channels {
		if !message.ValidName(ch) {
			return sub, invalidName("канал", ch)
		}
		if !validInterval(iv.IntervalMs) {
			return sub, invalidSub("channels." + ch + ".intervalMs — не меньше 200 мс")
		}
		if sub.Channels == nil {
			sub.Channels = map[string]IntervalSpec{}
		}
		sub.Channels[ch] = iv
	}
	return sub, nil
}

// Subscribe — подписка на агента: что присылать чаще и подробнее обычного
// (SubscribeRequest), на срок TTL. Подписка — в записи агента
// (Agent.Subscriptions, видна всем процессам с общим Store); агенту этого
// процесса — config со сводной подпиской, если она изменилась; агент,
// подключившийся позже, получает её в welcome; другой процесс — после
// Refresh. Повторный вызов с тем же ID продлевает срок и заменяет
// содержимое. Истёкшие подписки удаляет сверка. Интервал меньше 200 мс, имя
// группы, канала или уровень не по правилу — MESSAGE_INVALID; агента нет —
// AGENT_NOT_FOUND; отозван — AGENT_REVOKED.
func (a *Agents) Subscribe(agentID string, req SubscribeRequest) (Subscription, error) {
	sub, err := subscriptionOf(req)
	if err != nil {
		return Subscription{}, err
	}
	ttl := req.TTL
	if ttl <= 0 {
		ttl = defaultSubscriptionTTL
	}
	sub.ID = req.ID
	if sub.ID == "" {
		sub.ID = newID()
	}
	a.mu.Lock()
	defer a.unlock()
	agent, err := a.store.GetAgent(agentID)
	if err != nil {
		return Subscription{}, protoErr("AGENT_NOT_FOUND", "Агент не найден")
	}
	if agent.Revoked {
		return Subscription{}, protoErr("AGENT_REVOKED", "Агент отозван")
	}
	ts := now()
	sub.Until = ts + ttl.Milliseconds()
	subs := activeSubscriptions(agent.Subscriptions, ts)
	subs = slices.DeleteFunc(subs, func(s Subscription) bool { return s.ID == sub.ID })
	agent.Subscriptions = append(subs, sub)
	if err := a.store.UpdateAgent(agent); err != nil {
		return Subscription{}, err
	}
	if ss := a.sessions[agentID]; ss != nil {
		a.applySubscription(ss, agent)
	}
	return sub.clone(), nil
}

// Unsubscribe — снять подписку id; агенту этого процесса — config, если
// сводная изменилась. Подписки нет — ничего; агента нет — AGENT_NOT_FOUND.
func (a *Agents) Unsubscribe(agentID, id string) error {
	a.mu.Lock()
	defer a.unlock()
	agent, err := a.store.GetAgent(agentID)
	if err != nil {
		return protoErr("AGENT_NOT_FOUND", "Агент не найден")
	}
	n := len(agent.Subscriptions)
	agent.Subscriptions = slices.DeleteFunc(agent.Subscriptions, func(s Subscription) bool { return s.ID == id })
	if len(agent.Subscriptions) == n {
		return nil
	}
	if len(agent.Subscriptions) == 0 {
		agent.Subscriptions = nil
	}
	if err := a.store.UpdateAgent(agent); err != nil {
		return err
	}
	if ss := a.sessions[agentID]; ss != nil {
		a.applySubscription(ss, agent)
	}
	return nil
}

// activeSubscriptions — подписки с непрошедшим сроком (копия списка).
func activeSubscriptions(subs []Subscription, ts int64) []Subscription {
	var out []Subscription
	for _, s := range subs {
		if s.Until > ts {
			out = append(out, s)
		}
	}
	return out
}

// hasExpired — среди подписок есть истёкшие.
func hasExpired(subs []Subscription, ts int64) bool {
	return slices.ContainsFunc(subs, func(s Subscription) bool { return s.Until <= ts })
}

// summarize — сводная подписка по действующим подпискам: частоты — минимум,
// группы — объединение в порядке появления, уровень лога — самый подробный,
// каналы — минимум по каждому; next — ближайший срок (0 — подписок нет).
func summarize(subs []Subscription, ts int64) (sum message.Subscription, next int64) {
	minMs := func(cur, v int64) int64 {
		if v > 0 && (cur == 0 || v < cur) {
			return v
		}
		return cur
	}
	for _, s := range subs {
		if s.Until <= ts {
			continue
		}
		if next == 0 || s.Until < next {
			next = s.Until
		}
		if s.Status != nil {
			sum.StatusIntervalMs = minMs(sum.StatusIntervalMs, s.Status.IntervalMs)
		}
		if s.Metrics != nil {
			sum.MetricsIntervalMs = minMs(sum.MetricsIntervalMs, s.Metrics.IntervalMs)
			for _, g := range s.Metrics.Groups {
				if !slices.Contains(sum.Metrics, g) {
					sum.Metrics = append(sum.Metrics, g)
				}
			}
		}
		if s.Logs != nil {
			if r, ok := logRank[s.Logs.Level]; ok && (sum.LogLevel == "" || r < logRank[sum.LogLevel]) {
				sum.LogLevel = s.Logs.Level
			}
		}
		for ch, iv := range s.Channels {
			if sum.Channels == nil {
				sum.Channels = map[string]int64{}
			}
			sum.Channels[ch] = minMs(sum.Channels[ch], iv.IntervalMs)
		}
	}
	return sum, next
}

// applySubscription — сводная подписка сессии по записи агента: изменилась —
// агенту config {subscription} (пустая — подписок нет). Под a.mu.
func (a *Agents) applySubscription(ss *session, agent *Agent) {
	if ss.closed || !ss.greeted {
		return
	}
	sum, next := summarize(agent.Subscriptions, now())
	ss.subUntil = next
	if reflect.DeepEqual(sum, ss.sub) {
		return
	}
	ss.sub = sum
	ss.send(message.TypeConfig, message.SessionConfig{Subscription: &sum}, "")
}

// expireSubscriptions — сверка (sweep): у подписок сессии прошёл ближайший
// срок — перечитать запись агента (могли продлить в другом процессе),
// удалить истёкшие, агенту — новая сводная. Под a.mu.
func (a *Agents) expireSubscriptions(ss *session, ts int64) {
	if ss.closed || !ss.greeted || ss.subUntil == 0 || ts < ss.subUntil {
		return
	}
	agent, err := a.store.GetAgent(ss.agentID)
	if err != nil {
		return
	}
	a.pruneSubscriptions(agent, ts)
	a.applySubscription(ss, agent)
}

// pruneSubscriptions — удалить из записи агента истёкшие подписки. Под a.mu.
func (a *Agents) pruneSubscriptions(agent *Agent, ts int64) {
	if !hasExpired(agent.Subscriptions, ts) {
		return
	}
	agent.Subscriptions = activeSubscriptions(agent.Subscriptions, ts)
	a.storeAgent(agent)
}

// sweepOfflineSubscriptions — сверка: у агента без связи (сессии нет нигде)
// истёкшие подписки удаляет любой процесс. Под a.mu.
func (a *Agents) sweepOfflineSubscriptions(list []*Agent, ts int64) {
	for _, agent := range list {
		if agent.Online || a.sessions[agent.ID] != nil || !hasExpired(agent.Subscriptions, ts) {
			continue
		}
		cur, err := a.store.GetAgent(agent.ID) // другой процесс мог обновить запись
		if err != nil || cur.Online {
			continue
		}
		a.pruneSubscriptions(cur, ts)
	}
}
