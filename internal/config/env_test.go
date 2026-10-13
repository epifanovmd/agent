package config

import (
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Каждая переменная AGENT_* меняет в настройках ровно тот ключ, что записан у
// неё в Path: по Path config check помечает, откуда значение.
func TestEnvVarPaths(t *testing.T) {
	tree := func(c Config) map[string]any {
		raw, err := yaml.Marshal(c)
		if err != nil {
			t.Fatal(err)
		}
		var m map[string]any
		if err := yaml.Unmarshal(raw, &m); err != nil {
			t.Fatal(err)
		}
		return m
	}
	at := func(m map[string]any, path string) any {
		var v any = m
		for _, k := range strings.Split(path, ".") {
			mm, _ := v.(map[string]any)
			v = mm[k]
		}
		return v
	}
	seen := map[string]bool{}
	for _, e := range envVars {
		if seen[e.Name] {
			t.Errorf("%s — дважды", e.Name)
		}
		seen[e.Name] = true
		changed := false
		for _, v := range []string{"7", "7s", "k=v"} {
			cfg := Defaults()
			before := tree(cfg)
			if e.apply(&cfg, v) != nil {
				continue
			}
			after := tree(cfg)
			if reflect.DeepEqual(at(before, e.Path), at(after, e.Path)) {
				continue
			}
			changed = true
			delete(before, strings.Split(e.Path, ".")[0])
			delete(after, strings.Split(e.Path, ".")[0])
			if !reflect.DeepEqual(before, after) {
				t.Errorf("%s меняет не только %s", e.Name, e.Path)
			}
			break
		}
		if !changed {
			t.Errorf("%s не меняет %s", e.Name, e.Path)
		}
	}
}
