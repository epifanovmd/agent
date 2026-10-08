// Package examples загружает образцы сообщений sdk/spec/examples для тестов
// SDK. Каждый файл каталога (кроме sealed.json) — объект «имя → {from, to,
// message}»; имена уникальны во всём каталоге.
package examples

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"testing"
)

// Стороны связи в from/to.
const (
	Agent  = "agent"
	Server = "server"
	Worker = "worker"
)

// Example — образец: кто шлёт, кому и конверт целиком.
type Example struct {
	Name    string          `json:"-"`
	From    string          `json:"from"`
	To      string          `json:"to"`
	Message json.RawMessage `json:"message"`
}

var (
	once    sync.Once
	all     map[string]Example
	dir     string
	loadErr error
)

// Dir — каталог образцов: EXAMPLES_DIR или sdk/spec/examples, найденный
// вверх от текущего каталога (go test запускается в каталоге пакета).
func Dir() (string, error) {
	if d := os.Getenv("EXAMPLES_DIR"); d != "" {
		return d, nil
	}
	cwd, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for d := cwd; ; d = filepath.Dir(d) {
		candidate := filepath.Join(d, "sdk", "spec", "examples")
		if st, err := os.Stat(candidate); err == nil && st.IsDir() {
			return candidate, nil
		}
		if filepath.Dir(d) == d {
			return "", errors.New("каталог sdk/spec/examples не найден")
		}
	}
}

func load() {
	dir, loadErr = Dir()
	if loadErr != nil {
		return
	}
	files, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		loadErr = err
		return
	}
	all = map[string]Example{}
	for _, file := range files {
		if filepath.Base(file) == "sealed.json" {
			continue
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			loadErr = err
			return
		}
		var part map[string]Example
		if err := json.Unmarshal(raw, &part); err != nil {
			loadErr = errors.New(filepath.Base(file) + ": " + err.Error())
			return
		}
		for name, ex := range part {
			if _, dup := all[name]; dup {
				loadErr = errors.New("образец " + name + " повторяется")
				return
			}
			ex.Name = name
			all[name] = ex
		}
	}
}

func loaded(t testing.TB) map[string]Example {
	t.Helper()
	once.Do(load)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	return all
}

// Get — образец по имени (например "hello", "cmd.run.rotateKey", "event@worker").
func Get(t testing.TB, name string) Example {
	t.Helper()
	ex, ok := loaded(t)[name]
	if !ok {
		t.Fatalf("нет образца %s", name)
	}
	return ex
}

// Message — конверт образца по имени.
func Message(t testing.TB, name string) json.RawMessage {
	t.Helper()
	return Get(t, name).Message
}

// Between — все образцы направления from → to, по имени.
func Between(t testing.TB, from, to string) []Example {
	t.Helper()
	var out []Example
	for _, ex := range loaded(t) {
		if ex.From == from && ex.To == to {
			out = append(out, ex)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	if len(out) == 0 {
		t.Fatalf("нет образцов %s → %s", from, to)
	}
	return out
}

// Sealed — содержимое sealed.json: тестовые ключи агента и запечатанные значения.
func Sealed(t testing.TB) []byte {
	t.Helper()
	loaded(t)
	raw, err := os.ReadFile(filepath.Join(dir, "sealed.json"))
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
