package message

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"
	"unicode"
	"unicode/utf8"
)

// WorkerManifest — ответ GET /manifest воркера (§12): что воркер умеет.
// Обязательно только Version.
type WorkerManifest struct {
	Version     string                 `json:"version,omitempty"`
	Description string                 `json:"description,omitempty"`
	Configs     []WorkerManifestConfig `json:"configs,omitempty"`
	Routes      []WorkerManifestRoute  `json:"routes,omitempty"`
	Events      []WorkerManifestEvent  `json:"events,omitempty"`
	Jobs        []WorkerManifestJob    `json:"jobs,omitempty"`
}

// WorkerManifestJob — тип задачи воркера (§12); Schema — JSON Schema поля data.
type WorkerManifestJob struct {
	Type        string          `json:"type"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
}

// WorkerManifestConfig — ключ настроек; Schema — JSON Schema значения.
type WorkerManifestConfig struct {
	Key         string          `json:"key"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
}

// WorkerManifestRoute — маршрут для fetch; {name} в Path — один сегмент пути.
type WorkerManifestRoute struct {
	Method      string `json:"method"`
	Path        string `json:"path"`
	Description string `json:"description,omitempty"`
}

// DeclaresConfig — ключ key есть в Configs.
func (m *WorkerManifest) DeclaresConfig(key string) bool {
	return m != nil && slices.ContainsFunc(m.Configs, func(c WorkerManifestConfig) bool { return c.Key == key })
}

// DeclaresEvent — воркеру можно слать событие typ (§12): типы job.* — только
// события задач и только при непустом Jobs, остальные — из Events.
func (m *WorkerManifest) DeclaresEvent(typ string) bool {
	if m == nil {
		return false
	}
	if strings.HasPrefix(typ, JobEventPrefix) {
		return len(m.Jobs) > 0 && IsJobEvent(typ)
	}
	return slices.ContainsFunc(m.Events, func(e WorkerManifestEvent) bool { return e.Type == typ })
}

// IsJobEvent — typ — событие задачи (job.progress, job.done, job.failed, job.cancelled).
func IsJobEvent(typ string) bool {
	switch typ {
	case JobEventProgress, JobEventDone, JobEventFailed, JobEventCancelled:
		return true
	}
	return false
}

// WorkerManifestEvent — тип события, которое шлёт воркер.
type WorkerManifestEvent struct {
	Type        string `json:"type"`
	Description string `json:"description,omitempty"`
}

// Пределы манифеста (§16).
const (
	MaxManifestConfigs = 64
	MaxManifestItems   = 256
	MaxManifestVersion = 64
	MaxManifestText    = 1024
	MaxManifestPath    = 512
)

var methodRe = regexp.MustCompile(`^[A-Z]{1,16}$`)

// ParseWorkerManifest — тело ответа GET /manifest в пределах §16; незнакомые поля
// пропускаются.
func ParseWorkerManifest(raw []byte) (*WorkerManifest, error) {
	if len(raw) > MaxManifestBytes {
		return nil, fmt.Errorf("больше %d байт", MaxManifestBytes)
	}
	var m WorkerManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		return nil, fmt.Errorf("неверный JSON: %w", err)
	}
	for i := range m.Configs {
		if string(bytes.TrimSpace(m.Configs[i].Schema)) == "null" {
			m.Configs[i].Schema = nil
		}
	}
	for i := range m.Jobs {
		if string(bytes.TrimSpace(m.Jobs[i].Schema)) == "null" {
			m.Jobs[i].Schema = nil
		}
	}
	if err := m.Check(); err != nil {
		return nil, err
	}
	return &m, nil
}

// Check — поля манифеста в пределах §16.
func (m *WorkerManifest) Check() error {
	if m.Version == "" {
		return errors.New("version: обязательно")
	}
	if err := maxChars("version", m.Version, MaxManifestVersion); err != nil {
		return err
	}
	if err := maxChars("description", m.Description, MaxManifestText); err != nil {
		return err
	}
	switch {
	case len(m.Configs) > MaxManifestConfigs:
		return fmt.Errorf("configs: больше %d", MaxManifestConfigs)
	case len(m.Routes) > MaxManifestItems:
		return fmt.Errorf("routes: больше %d", MaxManifestItems)
	case len(m.Events) > MaxManifestItems:
		return fmt.Errorf("events: больше %d", MaxManifestItems)
	case len(m.Jobs) > MaxManifestItems:
		return fmt.Errorf("jobs: больше %d", MaxManifestItems)
	}
	for i, c := range m.Configs {
		at := fmt.Sprintf("configs[%d]", i)
		if !ValidName(c.Key) {
			return fmt.Errorf("%s.key: %q не по правилу %s", at, c.Key, NamePattern)
		}
		if err := maxChars(at+".description", c.Description, MaxManifestText); err != nil {
			return err
		}
		if len(c.Schema) > 0 && !isObject(c.Schema) {
			return fmt.Errorf("%s.schema: нужен объект", at)
		}
	}
	for i, r := range m.Routes {
		at := fmt.Sprintf("routes[%d]", i)
		if !methodRe.MatchString(r.Method) {
			return fmt.Errorf("%s.method: %q не по правилу %s", at, r.Method, methodRe)
		}
		if !strings.HasPrefix(r.Path, "/") || utf8.RuneCountInString(r.Path) > MaxManifestPath ||
			strings.IndexFunc(r.Path, unicode.IsSpace) >= 0 {
			return fmt.Errorf("%s.path: %q — с / в начале, без пробелов, до %d символов", at, r.Path, MaxManifestPath)
		}
		if err := maxChars(at+".description", r.Description, MaxManifestText); err != nil {
			return err
		}
	}
	for i, e := range m.Events {
		at := fmt.Sprintf("events[%d]", i)
		if !ValidEventType(e.Type) {
			return fmt.Errorf("%s.type: %q не по правилу %s", at, e.Type, EventTypePattern)
		}
		if strings.HasPrefix(e.Type, JobEventPrefix) {
			return fmt.Errorf("%s.type: типы %s* зарезервированы для событий задач", at, JobEventPrefix)
		}
		if err := maxChars(at+".description", e.Description, MaxManifestText); err != nil {
			return err
		}
	}
	for i, j := range m.Jobs {
		at := fmt.Sprintf("jobs[%d]", i)
		if !ValidEventType(j.Type) {
			return fmt.Errorf("%s.type: %q не по правилу %s", at, j.Type, EventTypePattern)
		}
		if err := maxChars(at+".description", j.Description, MaxManifestText); err != nil {
			return err
		}
		if len(j.Schema) > 0 && !isObject(j.Schema) {
			return fmt.Errorf("%s.schema: нужен объект", at)
		}
	}
	return nil
}

func maxChars(field, s string, limit int) error {
	if utf8.RuneCountInString(s) > limit {
		return fmt.Errorf("%s: длиннее %d символов", field, limit)
	}
	return nil
}

// isObject — raw — объект JSON.
func isObject(raw json.RawMessage) bool {
	raw = bytes.TrimSpace(raw)
	return len(raw) > 0 && raw[0] == '{'
}
