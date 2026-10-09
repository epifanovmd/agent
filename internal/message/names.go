package message

import (
	"fmt"
	"regexp"
	"unicode/utf8"
)

// NamePattern — правило имени воркера и ключа настроек (§1).
const NamePattern = `^[a-z][a-z0-9-]{0,31}$`

// EventTypePattern — правило типа события (§1).
const EventTypePattern = `^[a-z][a-z0-9._-]{0,63}$`

// Пределы имени агента и меток (§1).
const (
	MaxAgentName = 128
	MaxLabels    = 64
	MaxLabelLen  = 256
)

var (
	nameRe      = regexp.MustCompile(NamePattern)
	eventTypeRe = regexp.MustCompile(EventTypePattern)
)

// ValidName — имя воркера или ключа настроек соответствует NamePattern.
func ValidName(name string) bool { return nameRe.MatchString(name) }

// ValidEventType — тип события соответствует EventTypePattern.
func ValidEventType(typ string) bool { return eventTypeRe.MatchString(typ) }

// ValidAgentName — непустое имя агента не длиннее MaxAgentName символов.
func ValidAgentName(name string) bool {
	return name != "" && utf8.RuneCountInString(name) <= MaxAgentName
}

// CheckLabels — метки в пределах: не больше MaxLabels, ключ непустой, ключ и
// значение — не длиннее MaxLabelLen символов.
func CheckLabels(labels map[string]string) error {
	if len(labels) > MaxLabels {
		return fmt.Errorf("меток %d, можно не больше %d", len(labels), MaxLabels)
	}
	for k, v := range labels {
		if k == "" || utf8.RuneCountInString(k) > MaxLabelLen {
			return fmt.Errorf("ключ метки %q: непустой и не длиннее %d символов", k, MaxLabelLen)
		}
		if utf8.RuneCountInString(v) > MaxLabelLen {
			return fmt.Errorf("метка %s: значение длиннее %d символов", k, MaxLabelLen)
		}
	}
	return nil
}

// LogLevelRank — подробность уровня лога: debug — 0, error — 3; незнакомый — -1.
func LogLevelRank(level string) int {
	switch level {
	case LogDebug:
		return 0
	case LogInfo:
		return 1
	case LogWarn:
		return 2
	case LogError:
		return 3
	}
	return -1
}
