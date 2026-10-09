//go:build unix

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Методы проверки цели.
const (
	MethodICMP = "icmp"
	MethodTCP  = "tcp"
)

// Значения по умолчанию и пределы настроек.
const (
	defaultIntervalSec = 30
	defaultCount       = 3
	defaultTimeoutMs   = 1000
	maxTargets         = 256
	maxCount           = 100
	maxIntervalSec     = 3600
	minTimeoutMs       = 50
	maxTimeoutMs       = 30000
	maxIDLen           = 128
	maxHostLen         = 253
)

// Target — цель проверки: id — имя цели для бэкенда (например, id агента-адресата), host —
// адрес или имя узла, port — порт TCP (для icmp — запасной путь, если ICMP недоступен).
type Target struct {
	ID     string `json:"id"`
	Host   string `json:"host"`
	Port   int    `json:"port,omitempty"`
	Method string `json:"method,omitempty"`
}

// Spec — настройки netprobe (ключ targets).
type Spec struct {
	Targets     []Target `json:"targets"`
	IntervalSec int      `json:"intervalSec,omitempty"`
	Count       int      `json:"count,omitempty"`
	TimeoutMs   int      `json:"timeoutMs,omitempty"`
}

func (s Spec) timeout() time.Duration { return time.Duration(s.TimeoutMs) * time.Millisecond }

// parseSpec — настройки: неизвестные поля не мешают, ошибки — с указанием цели.
func parseSpec(raw json.RawMessage) (Spec, error) {
	var s Spec
	if len(raw) > 0 && string(raw) != "null" {
		if err := json.Unmarshal(raw, &s); err != nil {
			return Spec{}, fmt.Errorf("spec: %w", err)
		}
	}
	if err := s.normalize(); err != nil {
		return Spec{}, err
	}
	return s, nil
}

// normalize — значения по умолчанию и проверка пределов.
func (s *Spec) normalize() error {
	if s.IntervalSec == 0 {
		s.IntervalSec = defaultIntervalSec
	}
	if s.Count == 0 {
		s.Count = defaultCount
	}
	if s.TimeoutMs == 0 {
		s.TimeoutMs = defaultTimeoutMs
	}
	switch {
	case s.IntervalSec < 1 || s.IntervalSec > maxIntervalSec:
		return fmt.Errorf("intervalSec — от 1 до %d", maxIntervalSec)
	case s.Count < 1 || s.Count > maxCount:
		return fmt.Errorf("count — от 1 до %d", maxCount)
	case s.TimeoutMs < minTimeoutMs || s.TimeoutMs > maxTimeoutMs:
		return fmt.Errorf("timeoutMs — от %d до %d", minTimeoutMs, maxTimeoutMs)
	case len(s.Targets) > maxTargets:
		return fmt.Errorf("targets — не больше %d", maxTargets)
	}
	seen := map[string]bool{}
	for i := range s.Targets {
		t := &s.Targets[i]
		if t.Method == "" {
			t.Method = MethodICMP
		}
		if err := t.check(); err != nil {
			return fmt.Errorf("targets[%d]: %w", i, err)
		}
		if seen[t.ID] {
			return fmt.Errorf("targets[%d]: id %q повторяется", i, t.ID)
		}
		seen[t.ID] = true
	}
	return nil
}

func (t Target) check() error {
	switch {
	case t.ID == "" || len(t.ID) > maxIDLen:
		return fmt.Errorf("id — от 1 до %d символов", maxIDLen)
	case t.Host == "" || len(t.Host) > maxHostLen:
		return fmt.Errorf("host — от 1 до %d символов", maxHostLen)
	case t.Host[0] == '-':
		return errors.New("host не может начинаться с «-»")
	case t.Port < 0 || t.Port > 65535:
		return errors.New("port — от 1 до 65535")
	case t.Method != MethodICMP && t.Method != MethodTCP:
		return fmt.Errorf("method %q — icmp или tcp", t.Method)
	case t.Method == MethodTCP && t.Port == 0:
		return errors.New("для method tcp нужен port")
	}
	return nil
}
