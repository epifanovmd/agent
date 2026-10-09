package install

import (
	"bufio"
	"errors"
	"os"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"
)

func isDir(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.IsDir()
}

func isFile(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

func sameFile(a, b string) bool {
	sa, err1 := os.Stat(a)
	sb, err2 := os.Stat(b)
	return err1 == nil && err2 == nil && os.SameFile(sa, sb)
}

// writeFileAtomic — записать через временный файл рядом и переименовать.
func writeFileAtomic(path string, data []byte, perm os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	tmp := path + ".new"
	if err := os.WriteFile(tmp, data, perm); err != nil {
		return err
	}
	if err := os.Chmod(tmp, perm); err != nil {
		os.Remove(tmp)
		return err
	}
	return os.Rename(tmp, path)
}

func yamlUnmarshal(raw []byte, v any) error {
	return yaml.Unmarshal([]byte(os.ExpandEnv(string(raw))), v)
}

// journal — журнал установки экземпляра (/etc/agent[-ИМЯ]/install-state):
// строки «вид значение». package — пакет поставлен установкой, requires —
// пакет нужен воркерам экземпляра, user — пользователь службы создан
// установкой, sysctl — параметр ядра агента, sysctl-prev — его значение до
// установки, kill-mode — режим остановки службы, service-user — пользователь
// службы.
type journal struct {
	path  string
	lines []string
}

const journalHeader = "# Журнал установки агента: package — пакет поставлен установкой, requires — пакет нужен воркерам, user — пользователь службы создан установкой, sysctl — параметр ядра агента, sysctl-prev — его значение до установки, kill-mode — режим остановки службы, service-user — пользователь службы"

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
