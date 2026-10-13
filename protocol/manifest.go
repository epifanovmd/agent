package protocol

import (
	"bytes"
	"encoding/json"
	"fmt"
	"unicode/utf8"
)

// WorkerManifest — что воркер умеет (§13): ключи настроек, команды, события и запросы к
// бэкенду со схемами JSON Schema (их проверяет бэкенд; агент сверяет только имена и типы).
type WorkerManifest struct {
	Description string            `json:"description,omitempty"`
	Configs     []ManifestConfig  `json:"configs,omitempty"`
	Commands    []ManifestCommand `json:"commands,omitempty"`
	Events      []ManifestEvent   `json:"events,omitempty"`
	Requests    []ManifestRequest `json:"requests,omitempty"`
}

// ManifestConfig — ключ настроек; Schema — схема значения.
type ManifestConfig struct {
	Key         string          `json:"key"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
}

// ManifestCommand — тип команды; Schema — схема args, Result — схема итога.
type ManifestCommand struct {
	Type        string          `json:"type"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
	Result      json.RawMessage `json:"result,omitempty"`
}

// ManifestEvent — тип события; Schema — схема data.
type ManifestEvent struct {
	Type        string          `json:"type"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
}

// ManifestRequest — тип запроса к бэкенду; Schema — схема data, Response — схема ответа.
type ManifestRequest struct {
	Type        string          `json:"type"`
	Description string          `json:"description,omitempty"`
	Schema      json.RawMessage `json:"schema,omitempty"`
	Response    json.RawMessage `json:"response,omitempty"`
}

// Пределы манифеста.
const (
	MaxManifestItems = 256
	MaxManifestText  = 1024
	MaxVersionChars  = 64
)

// DeclaresConfig — ключ key объявлен.
func (m *WorkerManifest) DeclaresConfig(key string) bool {
	if m == nil {
		return false
	}
	for _, c := range m.Configs {
		if c.Key == key {
			return true
		}
	}
	return false
}

// DeclaresCommand — тип команды объявлен.
func (m *WorkerManifest) DeclaresCommand(typ string) bool {
	if m == nil {
		return false
	}
	for _, c := range m.Commands {
		if c.Type == typ {
			return true
		}
	}
	return false
}

// DeclaresEvent — тип события объявлен.
func (m *WorkerManifest) DeclaresEvent(typ string) bool {
	if m == nil {
		return false
	}
	for _, e := range m.Events {
		if e.Type == typ {
			return true
		}
	}
	return false
}

// DeclaresRequest — тип запроса объявлен.
func (m *WorkerManifest) DeclaresRequest(typ string) bool {
	if m == nil {
		return false
	}
	for _, r := range m.Requests {
		if r.Type == typ {
			return true
		}
	}
	return false
}

// Check — регистрация воркера в пределах: версия, имена и типы по правилам, схемы — объекты.
func (r *Register) Check() error {
	if utf8.RuneCountInString(r.Version) > MaxVersionChars {
		return fmt.Errorf("version: длиннее %d символов", MaxVersionChars)
	}
	return r.Manifest.Check()
}

// Check — манифест в пределах.
func (m *WorkerManifest) Check() error {
	if err := maxChars("description", m.Description, MaxManifestText); err != nil {
		return err
	}
	for name, n := range map[string]int{"configs": len(m.Configs), "commands": len(m.Commands),
		"events": len(m.Events), "requests": len(m.Requests)} {
		if n > MaxManifestItems {
			return fmt.Errorf("%s: больше %d", name, MaxManifestItems)
		}
	}
	seen := map[string]bool{}
	for i, c := range m.Configs {
		at := fmt.Sprintf("configs[%d]", i)
		if !ValidName(c.Key) {
			return fmt.Errorf("%s.key: %q не по правилу %s", at, c.Key, NamePattern)
		}
		if err := item(seen, "configs", c.Key, at, c.Description, c.Schema); err != nil {
			return err
		}
	}
	for i, c := range m.Commands {
		at := fmt.Sprintf("commands[%d]", i)
		if !ValidEventType(c.Type) {
			return fmt.Errorf("%s.type: %q не по правилу %s", at, c.Type, EventTypePattern)
		}
		if err := item(seen, "commands", c.Type, at, c.Description, c.Schema, c.Result); err != nil {
			return err
		}
	}
	for i, e := range m.Events {
		at := fmt.Sprintf("events[%d]", i)
		if !ValidEventType(e.Type) {
			return fmt.Errorf("%s.type: %q не по правилу %s", at, e.Type, EventTypePattern)
		}
		if e.Type == EventCommandProgress {
			return fmt.Errorf("%s.type: %s — событие агента", at, EventCommandProgress)
		}
		if err := item(seen, "events", e.Type, at, e.Description, e.Schema); err != nil {
			return err
		}
	}
	for i, r := range m.Requests {
		at := fmt.Sprintf("requests[%d]", i)
		if !ValidEventType(r.Type) {
			return fmt.Errorf("%s.type: %q не по правилу %s", at, r.Type, EventTypePattern)
		}
		if err := item(seen, "requests", r.Type, at, r.Description, r.Schema, r.Response); err != nil {
			return err
		}
	}
	return nil
}

// item — общее у элементов манифеста: повтор имени, длина описания, схемы — объекты.
func item(seen map[string]bool, list, name, at, description string, schemas ...json.RawMessage) error {
	if seen[list+"\n"+name] {
		return fmt.Errorf("%s: %q повторяется", at, name)
	}
	seen[list+"\n"+name] = true
	if err := maxChars(at+".description", description, MaxManifestText); err != nil {
		return err
	}
	for _, s := range schemas {
		if t := bytes.TrimSpace(s); len(t) > 0 && string(t) != "null" && t[0] != '{' {
			return fmt.Errorf("%s: схема — объект JSON", at)
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
