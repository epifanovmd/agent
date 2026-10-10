package scaffold

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"text/template"

	"github.com/epifanovmd/agent/internal/message"
)

//go:embed worker
var workerFiles embed.FS

// Lang — язык заготовки воркера: база (не правится, её обновляет sync) и файл воркера.
type Lang struct {
	Name, Base, Main string
	files            []langFile
}

// langFile — файл заготовки: шаблон, имя в папке воркера, подставлять ли имя, права.
type langFile struct {
	from, to string
	template bool
	perm     os.FileMode
	// noModule — только если у папки агента ещё нет go.mod (воркер — свой модуль).
	noModule bool
}

// Langs — языки agent worker new; первый — по умолчанию.
var Langs = []Lang{
	{Name: "python", Base: "agent_worker.py", Main: "worker.py", files: []langFile{
		{from: "agent_worker.py", to: "agent_worker.py", perm: 0o644},
		{from: "worker.py", to: "worker.py", template: true, perm: 0o644},
		{from: "run", to: "run", perm: 0o755},
	}},
	{Name: "go", Base: "agent_worker.go", Main: "worker.go", files: []langFile{
		{from: "agent_worker.go.tmpl", to: "agent_worker.go", perm: 0o644},
		{from: "worker.go.tmpl", to: "worker.go", template: true, perm: 0o644},
		{from: "run", to: "run", perm: 0o755},
		{from: "build", to: "build", perm: 0o755},
		{from: "gitignore", to: ".gitignore", perm: 0o644},
		{from: "go.mod.tmpl", to: "go.mod", template: true, perm: 0o644, noModule: true},
	}},
}

// LangByName — язык заготовки по имени.
func LangByName(name string) (Lang, error) {
	for _, l := range Langs {
		if l.Name == name {
			return l, nil
		}
	}
	names := make([]string, len(Langs))
	for i, l := range Langs {
		names[i] = l.Name
	}
	return Lang{}, fmt.Errorf("--lang %q — %s", name, strings.Join(names, " | "))
}

// reBase — строка версии базы в её файле.
var reBase = regexp.MustCompile(`agent-worker-base: (\d+)`)

// WorkerOptions — новый воркер в папке агента.
type WorkerOptions struct {
	// Root — папка агента (там agent.yaml и workers/).
	Root string
	// Config — файл настроек, куда дописать воркер ("" — не дописывать).
	Config string
	Name   string
	// Lang — язык заготовки ("" — первый из Langs).
	Lang  string
	Force bool
}

// NewWorker — воркер из заготовки: workers/<имя>/ с базой (agent_worker.*), файлом воркера
// (наследник базы), run и VERSION (у Go — ещё build и go.mod); в файле настроек —
// `- path: workers/<имя>`.
// Вернёт созданные файлы и дописан ли воркер в настройки.
func NewWorker(o WorkerOptions) ([]string, bool, error) {
	if !message.ValidName(o.Name) {
		return nil, false, fmt.Errorf("имя воркера %q — строчная латиница, цифры и «-», первая — буква, до 32 символов", o.Name)
	}
	if o.Lang == "" {
		o.Lang = Langs[0].Name
	}
	lang, err := LangByName(o.Lang)
	if err != nil {
		return nil, false, err
	}
	dir := filepath.Join(o.Root, "workers", o.Name)
	if _, err := os.Stat(dir); err == nil && !o.Force {
		return nil, false, fmt.Errorf("%s уже есть (перезаписать — --force)", dir)
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, false, err
	}
	data := struct{ Name, Class string }{o.Name, className(o.Name)}
	var created []string
	_, rootModule := os.Stat(filepath.Join(o.Root, "go.mod"))
	for _, f := range lang.files {
		if f.noModule && rootModule == nil {
			continue
		}
		raw, err := workerFiles.ReadFile("worker/" + lang.Name + "/" + f.from)
		if err != nil {
			return nil, false, err
		}
		if f.template {
			t, err := template.New(f.from).Parse(string(raw))
			if err != nil {
				return nil, false, err
			}
			var b bytes.Buffer
			if err := t.Execute(&b, data); err != nil {
				return nil, false, err
			}
			raw = b.Bytes()
		}
		if err := writeFile(filepath.Join(dir, f.to), raw, f.perm); err != nil {
			return nil, false, err
		}
		created = append(created, filepath.Join("workers", o.Name, f.to))
	}
	if err := writeFile(filepath.Join(dir, "VERSION"), []byte("0.1.0\n"), 0o644); err != nil {
		return nil, false, err
	}
	created = append(created, filepath.Join("workers", o.Name, "VERSION"))
	if o.Config == "" {
		return created, false, nil
	}
	added, err := addWorker(o.Config, "workers/"+o.Name)
	return created, added, err
}

// className — имя класса воркера: report-builder → ReportBuilder.
func className(name string) string {
	var b strings.Builder
	for _, part := range strings.Split(name, "-") {
		if part != "" {
			b.WriteString(strings.ToUpper(part[:1]) + part[1:])
		}
	}
	s := b.String()
	if s == "" || (s[0] >= '0' && s[0] <= '9') {
		s = "Worker" + s
	}
	return s
}

var (
	reWorkersKey   = regexp.MustCompile(`(?m)^workers:[ \t]*(#.*)?$`)
	reWorkersEmpty = regexp.MustCompile(`(?m)^workers:[ \t]*\[\][ \t]*(#.*)?$`)
)

// addWorker — `- path: <path>` в список workers файла настроек (текстом: комментарии и
// порядок остаются). Уже есть — false.
func addWorker(file, path string) (bool, error) {
	raw, err := os.ReadFile(file)
	if err != nil {
		return false, err
	}
	text := string(raw)
	if regexp.MustCompile(`(?m)^\s*-?\s*path:\s*["']?` + regexp.QuoteMeta(path) + `["']?\s*(#.*)?$`).MatchString(text) {
		return false, nil
	}
	item := "  - path: " + path
	switch {
	case reWorkersEmpty.MatchString(text):
		loc := reWorkersEmpty.FindStringIndex(text)
		text = text[:loc[0]] + "workers:\n" + item + text[loc[1]:]
	case reWorkersKey.MatchString(text):
		loc := reWorkersKey.FindStringIndex(text)
		text = text[:loc[1]] + "\n" + item + text[loc[1]:]
	default:
		text = strings.TrimRight(text, "\n") + "\n\nworkers:\n" + item + "\n"
	}
	return true, writeFile(file, []byte(text), 0o644)
}

// BaseVersion — версия базы воркера языка lang в этой программе (из её файла).
func BaseVersion(lang Lang) int {
	raw, _ := workerFiles.ReadFile("worker/" + lang.Name + "/" + lang.files[0].from)
	return baseVersion(raw)
}

func baseVersion(raw []byte) int {
	if m := reBase.FindSubmatch(raw); m != nil {
		n, _ := strconv.Atoi(string(m[1]))
		return n
	}
	return 0
}

// Synced — итог agent worker sync для одной базы.
type Synced struct {
	File     string
	From, To int
}

// SyncBases — обновить базы воркеров папки агента (workers/*/agent_worker.*) до версии
// этой программы. Базы новее — не трогает (их сделал более новый агент).
func SyncBases(root string) ([]Synced, error) {
	var out []Synced
	dirs, err := os.ReadDir(filepath.Join(root, "workers"))
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	for _, d := range dirs {
		if !d.IsDir() {
			continue
		}
		for _, lang := range Langs {
			path := filepath.Join(root, "workers", d.Name(), lang.Base)
			raw, err := os.ReadFile(path)
			if err != nil {
				continue
			}
			want, _ := workerFiles.ReadFile("worker/" + lang.Name + "/" + lang.files[0].from)
			have, next := baseVersion(raw), baseVersion(want)
			if have == 0 || have > next || bytes.Equal(raw, want) {
				continue
			}
			if err := writeFile(path, want, 0o644); err != nil {
				return out, err
			}
			out = append(out, Synced{File: filepath.Join("workers", d.Name(), lang.Base), From: have, To: next})
		}
	}
	return out, nil
}
