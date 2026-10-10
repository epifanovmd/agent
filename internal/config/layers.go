package config

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"
)

// Цепочка файлов настроек. Файл может опираться на другой (extends): сначала
// читается базовый, поверх него — этот. Словари сливаются по ключам, списки
// заменяются целиком; воркеры (workers) сливаются по имени. ${ИМЯ} и
// переменные AGENT_* берутся из окружения процесса, а если там их нет — из
// файлов переменных envFiles. Относительные пути (extends, envFiles, dataDir,
// server.caFile, server.certFile, server.keyFile, workers[].dir,
// workers[].path) — от файла, в котором записаны.

const notMapping = "файл настроек — словарь ключей (server:, workers: …)"

// maxExtends — сколько файлов может быть в цепочке extends.
const maxExtends = 8

// Pos — где задано значение: файл и строка.
type Pos struct {
	File string
	Line int
}

func (p Pos) String() string {
	if p.Line > 0 {
		return fmt.Sprintf("%s:%d", ShortPath(p.File), p.Line)
	}
	return ShortPath(p.File)
}

// ShortPath — путь для текстов: от текущего каталога, если файл внутри него.
func ShortPath(path string) string {
	wd, err := os.Getwd()
	if err != nil || wd == "/" {
		return path
	}
	if rel, err := filepath.Rel(wd, path); err == nil && !strings.HasPrefix(rel, "..") {
		return rel
	}
	return path
}

// Source — прочитанная цепочка файлов настроек.
type Source struct {
	// Files — файлы настроек от базового к заданному.
	Files []string
	// EnvFiles — прочитанные файлы переменных, от менее важного к более важному.
	EnvFiles []string
	// Env — значения из EnvFiles; окружение процесса важнее их.
	Env map[string]string
	// Origin — где задано каждое поле итоговых настроек: "server.url",
	// "workers[0].dir".
	Origin map[string]Pos
	// Warnings — неизвестные поля и ненайденные файлы переменных.
	Warnings []Problem
	// Errors — с такими файлами агент не запустится (чтение, разметка, типы).
	Errors []Problem

	node *yaml.Node
}

// Lookup — значение переменной: окружение процесса, иначе файлы переменных.
func (s *Source) Lookup(name string) (string, bool) {
	if v, ok := os.LookupEnv(name); ok {
		return v, true
	}
	v, ok := s.Env[name]
	return v, ok
}

// expand — ${ИМЯ} в тексте файла; незаданная переменная — пустая строка.
func (s *Source) expand(text string) string {
	return os.Expand(text, func(name string) string {
		v, _ := s.Lookup(name)
		return v
	})
}

// Decode — итоговые настройки файлов поверх cfg (без переменных AGENT_* и проверки).
func (s *Source) Decode(cfg *Config) error {
	if s.node == nil {
		return nil
	}
	return s.node.Decode(cfg)
}

// layerMeta — ключи цепочки в файле.
type layerMeta struct {
	Extends  string   `yaml:"extends"`
	EnvFiles []string `yaml:"envFiles"`
}

// fromFile — путь value относительно каталога файла file (абсолютный — как есть).
func fromFile(file, value string) string {
	if value == "" || filepath.IsAbs(value) {
		return value
	}
	return filepath.Join(filepath.Dir(file), value)
}

// ReadSource — цепочка файлов настроек, начиная с path. Ошибки — в Errors.
func ReadSource(path string) *Source {
	s := &Source{Env: map[string]string{}, Origin: map[string]Pos{}}
	abs, err := filepath.Abs(path)
	if err != nil {
		abs = path
	}
	chain, errs := readChain(abs)
	if len(errs) > 0 {
		s.Errors = errs
		return s
	}
	for _, l := range chain {
		s.Files = append(s.Files, l.file)
	}

	// Файлы переменных: более поздний (в списке и в цепочке) важнее.
	for _, l := range chain {
		for _, name := range l.meta.EnvFiles {
			file := fromFile(l.file, name)
			vars, err := readEnvFile(file)
			switch {
			case errors.Is(err, fs.ErrNotExist):
				s.Warnings = append(s.Warnings, Problem{File: l.file, Text: fmt.Sprintf("envFiles: файла %s нет — значения из него не учтены", ShortPath(file))})
				continue
			case err != nil:
				s.Warnings = append(s.Warnings, Problem{File: l.file, Text: fmt.Sprintf("envFiles: %v — значения из него не учтены", err)})
				continue
			}
			s.EnvFiles = append(s.EnvFiles, file)
			for _, kv := range vars {
				s.Env[kv[0]] = kv[1]
			}
		}
	}

	files := map[*yaml.Node]string{}
	for _, l := range chain {
		src := s.expand(l.raw)
		for _, p := range unknownFields(src) {
			p.File = l.file
			s.Warnings = append(s.Warnings, p)
		}
		probe := Defaults()
		if err := yaml.Unmarshal([]byte(src), &probe); err != nil {
			for _, p := range yamlProblems(err) {
				p.File = l.file
				s.Errors = append(s.Errors, p)
			}
			continue
		}
		var doc yaml.Node
		_ = yaml.Unmarshal([]byte(src), &doc)
		root := mappingOf(&doc)
		if root == nil {
			s.Errors = append(s.Errors, Problem{File: l.file, Text: notMapping})
			continue
		}
		dropKeys(root, "extends", "envFiles")
		resolvePaths(root, l.file)
		markFile(root, l.file, files)
		if s.node == nil {
			s.node = root
		} else {
			mergeMap(s.node, root, true)
		}
	}
	if s.node != nil {
		s.origins(s.node, "", files)
	}
	if len(s.Files) == 1 {
		// Один файл — в текстах только номера строк, как раньше.
		for i := range s.Errors {
			s.Errors[i].File = ""
		}
		for i := range s.Warnings {
			if s.Warnings[i].File == s.Files[0] {
				s.Warnings[i].File = ""
			}
		}
	}
	return s
}

// ${ИМЯ} и $ИМЯ в тексте, пока значения не подставлены, — метки: внутри
// { … } и [ … ] YAML не принимает «{» и «$» в значении без кавычек.
var (
	reVar    = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}|\$([A-Za-z_][A-Za-z0-9_]*)`)
	reMasked = regexp.MustCompile(`__agentvar__([A-Za-z0-9_]+?)__agentvar__`)
)

func maskVars(s string) string {
	return reVar.ReplaceAllStringFunc(s, func(m string) string {
		sub := reVar.FindStringSubmatch(m)
		return "__agentvar__" + sub[1] + sub[2] + "__agentvar__"
	})
}

func unmaskVars(s string) string { return reMasked.ReplaceAllString(s, "$${$1}") }

// layer — файл цепочки: путь, текст, ключи цепочки.
type layer struct {
	file string
	raw  string
	meta layerMeta
}

// readChain — файлы цепочки extends от базового к file (текст как есть).
func readChain(file string) ([]layer, []Problem) {
	var chain []layer
	var errs []Problem
	for file != "" {
		if slices.ContainsFunc(chain, func(l layer) bool { return l.file == file }) {
			errs = append(errs, Problem{File: file, Text: "extends: файлы ссылаются друг на друга по кругу"})
			return nil, errs
		}
		if len(chain) == maxExtends {
			errs = append(errs, Problem{File: file, Text: fmt.Sprintf("extends: в цепочке больше %d файлов", maxExtends)})
			return nil, errs
		}
		raw, err := os.ReadFile(file)
		if err != nil {
			errs = append(errs, Problem{File: file, Text: fmt.Sprintf("файл настроек: %v", err)})
			return nil, errs
		}
		var doc yaml.Node
		if err := yaml.Unmarshal([]byte(maskVars(string(raw))), &doc); err != nil {
			for _, p := range yamlProblems(err) {
				p.File = file
				errs = append(errs, p)
			}
			return nil, errs
		}
		var meta layerMeta
		root := mappingOf(&doc)
		if root == nil {
			errs = append(errs, Problem{File: file, Line: doc.Content[0].Line, Text: notMapping})
			return nil, errs
		}
		if err := root.Decode(&meta); err != nil {
			for _, p := range yamlProblems(err) {
				p.File = file
				errs = append(errs, p)
			}
			return nil, errs
		}
		chain = append(chain, layer{file: file, raw: string(raw), meta: meta})
		file = fromFile(file, meta.Extends)
	}
	slices.Reverse(chain)
	return chain, nil
}

// readEnvFile — пары KEY=VALUE файла переменных.
func readEnvFile(path string) ([][2]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	vars, err := ParseEnv(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", ShortPath(path), err)
	}
	return vars, nil
}

// mappingOf — словарь верхнего уровня документа; пустой файл — пустой словарь,
// не словарь — nil.
func mappingOf(doc *yaml.Node) *yaml.Node {
	if doc.Kind == 0 || (doc.Kind == yaml.DocumentNode && len(doc.Content) == 0) {
		return &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	}
	if doc.Kind == yaml.DocumentNode {
		doc = doc.Content[0]
	}
	if doc.Kind != yaml.MappingNode {
		return nil
	}
	return doc
}

// mapIndex — индекс ключа key в словаре m (-1 — нет).
func mapIndex(m *yaml.Node, key string) int {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return i
		}
	}
	return -1
}

// mapValue — значение ключа key словаря m (nil — нет).
func mapValue(m *yaml.Node, key string) *yaml.Node {
	if m == nil || m.Kind != yaml.MappingNode {
		return nil
	}
	if i := mapIndex(m, key); i >= 0 {
		return m.Content[i+1]
	}
	return nil
}

func dropKeys(m *yaml.Node, keys ...string) {
	for _, key := range keys {
		if i := mapIndex(m, key); i >= 0 {
			m.Content = slices.Delete(m.Content, i, i+2)
		}
	}
}

// resolvePaths — относительные пути файла file — от его каталога.
func resolvePaths(root *yaml.Node, file string) {
	fix := func(n *yaml.Node) {
		if n != nil && n.Kind == yaml.ScalarNode && n.Tag != "!!null" {
			n.Value = fromFile(file, n.Value)
		}
	}
	fix(mapValue(root, "dataDir"))
	server := mapValue(root, "server")
	for _, key := range []string{"caFile", "certFile", "keyFile"} {
		fix(mapValue(server, key))
	}
	if workers := mapValue(root, "workers"); workers != nil && workers.Kind == yaml.SequenceNode {
		for _, w := range workers.Content {
			fix(mapValue(w, "dir"))
			fix(mapValue(w, "path"))
		}
	}
}

// markFile — файл каждого узла дерева (для Origin).
func markFile(n *yaml.Node, file string, files map[*yaml.Node]string) {
	files[n] = file
	for _, c := range n.Content {
		markFile(c, file, files)
	}
}

// mergeMap — over поверх base: словари — по ключам, остальное заменяется;
// top — верхний уровень (там workers сливаются по имени).
func mergeMap(base, over *yaml.Node, top bool) {
	for i := 0; i+1 < len(over.Content); i += 2 {
		k, v := over.Content[i], over.Content[i+1]
		j := mapIndex(base, k.Value)
		switch {
		case j < 0:
			base.Content = append(base.Content, k, v)
		case top && k.Value == "workers" && base.Content[j+1].Kind == yaml.SequenceNode && v.Kind == yaml.SequenceNode:
			mergeWorkers(base.Content[j+1], v)
		case base.Content[j+1].Kind == yaml.MappingNode && v.Kind == yaml.MappingNode:
			mergeMap(base.Content[j+1], v, false)
		default:
			base.Content[j], base.Content[j+1] = k, v
		}
	}
}

// workerName — имя воркера в элементе списка workers: name, иначе имя папки
// path ("" — нет).
func workerName(n *yaml.Node) string {
	if v := mapValue(n, "name"); v != nil && v.Kind == yaml.ScalarNode && v.Value != "" {
		return v.Value
	}
	if v := mapValue(n, "path"); v != nil && v.Kind == yaml.ScalarNode && v.Value != "" {
		return filepath.Base(v.Value)
	}
	return ""
}

// mergeWorkers — воркер с тем же именем дополняется, новый — добавляется в конец.
func mergeWorkers(base, over *yaml.Node) {
	for _, w := range over.Content {
		name := workerName(w)
		i := slices.IndexFunc(base.Content, func(b *yaml.Node) bool { return name != "" && workerName(b) == name })
		if i >= 0 && base.Content[i].Kind == yaml.MappingNode && w.Kind == yaml.MappingNode {
			mergeMap(base.Content[i], w, false)
			continue
		}
		base.Content = append(base.Content, w)
	}
}

// origins — Origin итогового дерева: у значения — его строка, у словаря и
// списка — строка ключа.
func (s *Source) origins(n *yaml.Node, path string, files map[*yaml.Node]string) {
	pos := func(n *yaml.Node) Pos { return Pos{File: files[n], Line: n.Line} }
	switch n.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			p := k.Value
			if path != "" {
				p = path + "." + p
			}
			if v.Kind == yaml.ScalarNode || v.Kind == yaml.AliasNode {
				s.Origin[p] = pos(v)
			} else {
				s.Origin[p] = pos(k)
			}
			s.origins(v, p, files)
		}
	case yaml.SequenceNode:
		for i, c := range n.Content {
			p := fmt.Sprintf("%s[%d]", path, i)
			s.Origin[p] = pos(c)
			s.origins(c, p, files)
		}
	}
}

// originOf — где задано поле, о котором текст ошибки Validate
// («workers[1].user: …»): само поле или ближайшее объемлющее.
func (s *Source) originOf(text string) (Pos, bool) {
	p := rePath.FindString(text)
	for p != "" {
		if pos, ok := s.Origin[p]; ok {
			return pos, true
		}
		i := strings.LastIndexAny(p, ".[")
		if i < 0 {
			break
		}
		p = p[:i]
	}
	return Pos{}, false
}
