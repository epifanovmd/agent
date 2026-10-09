// Package message — сообщения агента, сервера и воркера: конверт, типы
// содержимого сообщений WebSocket и тела HTTP-запросов между агентом и
// воркером. Нормативный документ — sdk/spec/README.md.
package message

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
)

// Subprotocol — канал WebSocket (заголовок Sec-WebSocket-Protocol, §2).
const Subprotocol = "agent.v2"

// Пути сервера (§2, §11).
const (
	LinkPath     = "/api/v1/agent-link"
	EnrollPath   = "/api/v1/agent-link/enroll"
	ManifestPath = "/api/v1/agent-link/releases/manifest.json"
)

// Коды закрытия WebSocket (§2).
const (
	CloseNormal       = 1000
	CloseGoingAway    = 1001
	CloseRestart      = 1012
	CloseInvalid      = 4400
	CloseUnauthorized = 4401
	CloseReplaced     = 4409
	CloseOverloaded   = 4429
)

// Envelope — конверт сообщения (§3). ID — у важных сообщений и запросов,
// Re — id сообщения, к которому относится это (ответ, отмена), Seq — номер
// сообщения потока.
type Envelope struct {
	Type string          `json:"type"`
	ID   string          `json:"id,omitempty"`
	Re   string          `json:"re,omitempty"`
	Seq  int64           `json:"seq,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

// New — конверт с содержимым data (nil — без data).
func New(typ string, data any) (Envelope, error) {
	env := Envelope{Type: typ}
	if data == nil {
		return env, nil
	}
	raw, err := json.Marshal(data)
	if err != nil {
		return Envelope{}, fmt.Errorf("message: %s: %w", typ, err)
	}
	env.Data = raw
	return env, nil
}

// MustNew — New для данных, которые заведомо сериализуются (структуры пакета).
func MustNew(typ string, data any) Envelope {
	env, err := New(typ, data)
	if err != nil {
		panic(err)
	}
	return env
}

// Decode — содержимое в структуру; нет data — как пустой объект.
// Незнакомые поля пропускаются (§1).
func (e Envelope) Decode(v any) error {
	if len(e.Data) == 0 {
		return json.Unmarshal([]byte("{}"), v)
	}
	if err := json.Unmarshal(e.Data, v); err != nil {
		return fmt.Errorf("message: %s: %w", e.Type, err)
	}
	return nil
}

// NewID — уникальный id сообщения (128 бит, hex).
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// NewBootID — id запуска агента: с него начинается нумерация seq (§3).
func NewBootID() string {
	var b [8]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}
