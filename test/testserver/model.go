package testserver

import (
	"encoding/json"

	"github.com/epifanovmd/agent/internal/message"
)

// Модель — JSON серверного SDK (agent-sdk/server); время — миллисекунды Unix.

// LogEntry — запись журнала агента или воркера.
type LogEntry = message.LogEntry

// Agent — агент, как его видит бэкенд, с событиями его воркеров (по порядку прихода).
type Agent struct {
	ID      string            `json:"id"`
	Name    string            `json:"name"`
	Labels  map[string]string `json:"labels"`
	Online  bool              `json:"online"`
	Revoked bool              `json:"revoked"`
	Version string            `json:"version"`
	BootID  string            `json:"bootId"`
	Workers []AgentWorker     `json:"workers"`
	Hello   *message.Hello    `json:"hello,omitempty"`
	Status  *message.Status   `json:"status,omitempty"`
	Alerts  []Alert           `json:"alerts"`
	Events  []AgentEvent      `json:"events,omitempty"`
}

// Worker — воркер агента по имени (ok — есть).
func (a Agent) Worker(name string) (AgentWorker, bool) {
	for _, w := range a.Workers {
		if w.Name == name {
			return w, true
		}
	}
	return AgentWorker{}, false
}

// AgentWorker — воркер агента: из hello и последнего status.
type AgentWorker = message.WorkerStatus

// Alert — проблема агента.
type Alert struct {
	Key    string `json:"key"`
	Type   string `json:"type"`
	Worker string `json:"worker,omitempty"`
}

// AgentEvent — событие воркера.
type AgentEvent struct {
	ID      string          `json:"id"`
	AgentID string          `json:"agentId"`
	Worker  string          `json:"worker"`
	Type    string          `json:"type"`
	Data    json.RawMessage `json:"data,omitempty"`
	At      int64           `json:"at"`
}

// MetricsPoint — точка метрик агента (событие metrics).
type MetricsPoint struct {
	message.Metrics
	At int64 `json:"at"`
}

// ConfigRecord — желаемое значение ключа настроек.
type ConfigRecord struct {
	Worker  string          `json:"worker"`
	Key     string          `json:"key"`
	Version int64           `json:"version"`
	Data    json.RawMessage `json:"data"`
}

// Состояния ключа настроек (ConfigStatus.State).
const (
	ConfigPending  = "pending"
	ConfigApplying = "applying"
	ConfigApplied  = "applied"
	ConfigFailed   = "failed"
	ConfigDeleting = "deleting"
	// ConfigDeleted — агент подтвердил удаление ключа (только в событии config).
	ConfigDeleted = "deleted"
)

// ConfigStatus — статус ключа настроек: желаемая, доставленная и применённая версии.
type ConfigStatus struct {
	Worker    string             `json:"worker"`
	Key       string             `json:"key"`
	Version   *int64             `json:"version"`
	Delivered int64              `json:"delivered"`
	Applied   int64              `json:"applied"`
	State     string             `json:"state"`
	Error     *message.ErrorInfo `json:"error,omitempty"`
}

// FetchInit — параметры запроса к воркеру.
type FetchInit struct {
	Method  string            `json:"method,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    string            `json:"body,omitempty"`
	// BodyBase64 — двоичное тело.
	BodyBase64 string `json:"bodyBase64,omitempty"`
	TimeoutMs  int64  `json:"timeoutMs,omitempty"`
	// AbortAfterMs — отменить запрос через столько мс.
	AbortAfterMs int64 `json:"abortAfterMs,omitempty"`
}

// FetchResponse — ответ воркера; Error — ошибка после заголовка ответа.
type FetchResponse struct {
	Status     int               `json:"status"`
	Headers    map[string]string `json:"headers"`
	Body       string            `json:"body"`
	BodyBase64 string            `json:"bodyBase64"`
	// Chunks — сколько кусков тела пришло; FirstChunkAt, EndAt — когда пришёл первый и
	// закончилось тело (мс Unix).
	Chunks       int    `json:"chunks"`
	FirstChunkAt int64  `json:"firstChunkAt"`
	EndAt        int64  `json:"endAt"`
	Error        *Error `json:"error,omitempty"`
}

// WatchOptions — наблюдатель (Agents.watch).
type WatchOptions struct {
	ID                string `json:"id,omitempty"`
	MetricsIntervalMs int64  `json:"metricsIntervalMs,omitempty"`
	LogLevel          string `json:"logLevel,omitempty"`
	TTLMs             int64  `json:"ttlMs,omitempty"`
}

// WatchRef — id наблюдателя и срок (мс Unix).
type WatchRef struct {
	ID    string `json:"id"`
	Until int64  `json:"until"`
}

// UpdateResult — итог updateWorker и updateAgent.
type UpdateResult = message.UpdateResult

// ReplaceResult — итог restartWorker и updateWorker: Deferred — замена отложена, пока воркер
// занят (итог — событие action с ActionID).
type ReplaceResult struct {
	Deferred bool   `json:"deferred"`
	Pending  string `json:"pending,omitempty"`
	ActionID string `json:"actionId,omitempty"`
	Version  string `json:"version,omitempty"`
}

// ActionRecord — итог действия (событие action); Deferred — пришёл в action.done.
type ActionRecord struct {
	ID       string             `json:"id"`
	Name     string             `json:"name"`
	Status   string             `json:"status"`
	Deferred bool               `json:"deferred,omitempty"`
	Result   json.RawMessage    `json:"result,omitempty"`
	Error    *message.ErrorInfo `json:"error,omitempty"`
}

// JobResult — итог runJob.
type JobResult struct {
	JobID    string             `json:"jobId"`
	ID       string             `json:"id,omitempty"`
	State    string             `json:"state"`
	Progress *float64           `json:"progress,omitempty"`
	Result   json.RawMessage    `json:"result,omitempty"`
	Error    *message.ErrorInfo `json:"error,omitempty"`
}
