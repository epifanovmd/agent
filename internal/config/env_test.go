package config

import (
	"reflect"
	"testing"
)

// Каждая переменная AGENT_* меняет в настройках значение и задана один раз.
func TestEnvVars(t *testing.T) {
	seen := map[string]bool{}
	for _, e := range envVars {
		if seen[e.Name] {
			t.Errorf("%s — дважды", e.Name)
		}
		seen[e.Name] = true
		changed := false
		for _, v := range []string{"7", "7s", "k=v", "true"} {
			cfg := Defaults()
			if e.apply(&cfg, v) == nil && !reflect.DeepEqual(cfg, Defaults()) {
				changed = true
				break
			}
		}
		if !changed {
			t.Errorf("%s ничего не меняет", e.Name)
		}
	}
}
