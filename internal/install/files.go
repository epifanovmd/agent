package install

import (
	"bufio"
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/epifanovmd/agent/internal/fsutil"
)

func isDir(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.IsDir()
}

// writeFileAtomic — создать каталог и записать через временный файл рядом.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return fsutil.WriteFile(path, data, perm)
}

func yamlUnmarshal(raw []byte, v any) error {
	return yaml.Unmarshal([]byte(os.ExpandEnv(string(raw))), v)
}

// newWorker — воркер со сборкой, поставленный установкой (--worker).
type newWorker struct {
	Name      string        `yaml:"name"`
	Release   bool          `yaml:"release"`
	Command   []string      `yaml:"command,omitempty,flow"`
	Lifecycle *newLifecycle `yaml:"lifecycle,omitempty"`
}

type newLifecycle struct {
	StopTimeout string `yaml:"stopTimeout"`
}

// newConfig — agent.yaml, который создаёт установка: заданные значения, без
// значений по умолчанию (их показывает agent config check).
func newConfig(server, caFile, name, dataDir string, workers []newWorker) []byte {
	type srv struct {
		URL           string `yaml:"url"`
		AllowInsecure bool   `yaml:"allowInsecure,omitempty"`
		CAFile        string `yaml:"caFile,omitempty"`
	}
	type logs struct {
		Format string `yaml:"format"`
	}
	doc := struct {
		Server  srv         `yaml:"server"`
		Name    string      `yaml:"name,omitempty"`
		DataDir string      `yaml:"dataDir"`
		Log     logs        `yaml:"log"`
		Workers []newWorker `yaml:"workers,omitempty"`
	}{srv{server, strings.HasPrefix(strings.ToLower(server), "http://"), caFile}, name, dataDir, logs{"json"}, workers}
	var b bytes.Buffer
	b.WriteString("# Настройки агента; итоговые значения — agent config check.\n")
	enc := yaml.NewEncoder(&b)
	enc.SetIndent(2)
	if err := enc.Encode(doc); err != nil {
		panic("agent.yaml: " + err.Error())
	}
	_ = enc.Close()
	return b.Bytes()
}

// journal — журнал установки экземпляра (/etc/agent[-ИМЯ]/install-state):
// строки «вид значение». package — пакет поставлен установкой, requires —
// пакет нужен воркерам экземпляра, user — пользователь службы создан
// установкой, sysctl — параметр ядра агента, sysctl-prev — его значение до
// установки, service-user — пользователь
// службы.
type journal struct {
	path  string
	lines []string
}

const journalHeader = "# Журнал установки агента: package — пакет поставлен установкой, requires — пакет нужен воркерам, user — пользователь службы создан установкой, sysctl — параметр ядра агента, sysctl-prev — его значение до установки, service-user — пользователь службы"

func loadJournal(path string) (*journal, error) {
	j := &journal{path: path}
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return j, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if l := sc.Text(); l != "" && !strings.HasPrefix(l, "#") {
			j.lines = append(j.lines, l)
		}
	}
	return j, sc.Err()
}

// values — значения записей вида kind по порядку.
func (j *journal) values(kind string) []string {
	var out []string
	for _, l := range j.lines {
		if k, v, ok := strings.Cut(l, " "); ok && k == kind {
			out = append(out, v)
		}
	}
	return out
}

func (j *journal) last(kind string) string {
	v := j.values(kind)
	if len(v) == 0 {
		return ""
	}
	return v[len(v)-1]
}

func (j *journal) has(kind, value string) bool {
	for _, v := range j.values(kind) {
		if v == value {
			return true
		}
	}
	return false
}

// add — запись, если такой ещё нет.
func (j *journal) add(kind, value string) {
	if !j.has(kind, value) {
		j.lines = append(j.lines, kind+" "+value)
	}
}

// set — единственная запись вида kind.
func (j *journal) set(kind, value string) {
	j.drop(func(k, _ string) bool { return k == kind })
	j.lines = append(j.lines, kind+" "+value)
}

// drop — убрать записи, для которых match(вид, значение).
func (j *journal) drop(match func(kind, value string) bool) {
	kept := j.lines[:0]
	for _, l := range j.lines {
		k, v, _ := strings.Cut(l, " ")
		if !match(k, v) {
			kept = append(kept, l)
		}
	}
	j.lines = kept
}

func (j *journal) save() error {
	body := journalHeader + "\n"
	for _, l := range j.lines {
		body += l + "\n"
	}
	return writeFileAtomic(j.path, []byte(body), 0o644)
}
