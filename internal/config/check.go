package config

import (
	"errors"
	"fmt"
	"io"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"
)

// Problem — замечание или ошибка в настройках: файл ("" — единственный файл
// настроек), строка (0 — неизвестна) и понятный текст.
type Problem struct {
	File string
	Line int
	Text string
}

func (p Problem) String() string {
	switch {
	case p.File != "" && p.Line > 0:
		return fmt.Sprintf("%s, строка %d: %s", ShortPath(p.File), p.Line, p.Text)
	case p.File != "":
		return fmt.Sprintf("%s: %s", ShortPath(p.File), p.Text)
	case p.Line > 0:
		return fmt.Sprintf("строка %d: %s", p.Line, p.Text)
	}
	return p.Text
}

// CheckResult — итог agent config check.
type CheckResult struct {
	// Path — проверенный файл ("" — без файла, только окружение).
	Path string
	// Source — прочитанная цепочка файлов (nil — без файла).
	Source *Source
	// Config — итоговые настройки: умолчания, файлы, окружение.
	Config Config
	// FromEnv — переменные AGENT_* окружения процесса, которые заменили значения файлов.
	FromEnv []string
	// FromEnvFiles — переменные AGENT_* из файлов переменных (envFiles).
	FromEnvFiles []string
	// Warnings — неизвестные поля: агент их не читает (опечатка или поле
	// другой версии), но работать это не мешает.
	Warnings []Problem
	// Errors — с такими настройками агент не запустится.
	Errors []Problem
}

var (
	reLine    = regexp.MustCompile(`^(?:yaml: )?(?:line|строка) (\d+): (.*)$`)
	reUnknown = regexp.MustCompile(`^field (\S+) not found in type`)
	rePath    = regexp.MustCompile(`^[A-Za-z]+(?:\[\d+\])?(?:\.[A-Za-z]+(?:\[\d+\])?)*`)
)

// Check — проверить настройки, как их прочитает agent run: цепочку файлов
// (extends, envFiles, подстановка ${ENV}), переменные AGENT_*, допустимые
// значения. В отличие от Load, сообщает неизвестные поля, файлы и номера строк.
func Check(path string) CheckResult {
	res := CheckResult{Path: path, Config: Defaults()}
	src := &Source{}
	if path != "" {
		src = ReadSource(path)
		res.Source = src
		res.Warnings = src.Warnings
		if len(src.Errors) > 0 {
			res.Errors = src.Errors
			return res
		}
		if err := src.Decode(&res.Config); err != nil {
			res.Errors = append(res.Errors, yamlProblems(err)...)
			return res
		}
	}
	for _, e := range envVars {
		if v, ok := os.LookupEnv(e.Name); ok && v != "" {
			res.FromEnv = append(res.FromEnv, e.Name)
		} else if v := src.Env[e.Name]; v != "" {
			res.FromEnvFiles = append(res.FromEnvFiles, e.Name)
		}
	}
	applyEnv(&res.Config, src.Lookup)
	if err := res.Config.Validate(); err != nil {
		for _, text := range strings.Split(err.Error(), "\n") {
			p := Problem{Text: text}
			if pos, ok := src.originOf(text); ok {
				p.Line = pos.Line
				if len(src.Files) > 1 {
					p.File = pos.File
				}
			}
			res.Errors = append(res.Errors, p)
		}
	}
	return res
}

// Unknown — неизвестные поля файлов настроек (для предупреждения при запуске).
func Unknown(path string) []Problem {
	return ReadSource(path).Warnings
}

// unknownFields — поля, которых нет в Config (строгое чтение).
func unknownFields(src string) []Problem {
	probe := Defaults()
	dec := yaml.NewDecoder(strings.NewReader(src))
	dec.KnownFields(true)
	err := dec.Decode(&probe)
	var te *yaml.TypeError
	if err == nil || errors.Is(err, io.EOF) || !errors.As(err, &te) {
		return nil
	}
	var out []Problem
	for _, e := range te.Errors {
		line, text := splitLine(e)
		if m := reUnknown.FindStringSubmatch(text); m != nil {
			out = append(out, Problem{Line: line, Text: fmt.Sprintf("неизвестное поле «%s» — агент его не читает (опечатка или поле другой версии)", m[1])})
		}
	}
	return out
}

// yamlProblems — ошибки разбора YAML по-русски, с номерами строк.
func yamlProblems(err error) []Problem {
	var te *yaml.TypeError
	if errors.As(err, &te) {
		var out []Problem
		for _, e := range te.Errors {
			line, text := splitLine(e)
			if reUnknown.MatchString(text) {
				continue
			}
			out = append(out, Problem{Line: line, Text: "значение не того типа: " + text})
		}
		return out
	}
	line, text := splitLine(err.Error())
	if strings.HasPrefix(err.Error(), "yaml:") {
		text = "ошибка разметки YAML: " + text
	}
	return []Problem{{Line: line, Text: text}}
}

func splitLine(s string) (int, string) {
	if m := reLine.FindStringSubmatch(s); m != nil {
		n, _ := strconv.Atoi(m[1])
		return n, m[2]
	}
	return 0, strings.TrimPrefix(s, "yaml: ")
}

// Annotated — итоговые настройки cfg деревом YAML с пометкой, откуда каждое
// значение: файл и строка или переменная окружения. Без пометки — по умолчанию.
func (r CheckResult) Annotated(cfg Config) (*yaml.Node, error) {
	var root yaml.Node
	if err := root.Encode(cfg); err != nil {
		return nil, err
	}
	from := map[string]string{}
	if r.Source != nil {
		for p, pos := range r.Source.Origin {
			from[p] = pos.String()
		}
	}
	for _, e := range envVars {
		if slices.Contains(r.FromEnvFiles, e.Name) {
			from[e.Path] = e.Name + " из файла переменных"
		}
		if slices.Contains(r.FromEnv, e.Name) {
			from[e.Path] = e.Name
		}
	}
	annotate(&root, "", from)
	return &root, nil
}

// annotate — пометки у значений (и у списков — на строке ключа).
func annotate(n *yaml.Node, path string, from map[string]string) {
	switch n.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			p := k.Value
			if path != "" {
				p = path + "." + p
			}
			if text, ok := from[p]; ok {
				switch v.Kind {
				case yaml.ScalarNode:
					v.LineComment = text
				case yaml.SequenceNode:
					k.LineComment = text
				}
			}
			annotate(v, p, from)
		}
	case yaml.SequenceNode:
		for i, c := range n.Content {
			annotate(c, fmt.Sprintf("%s[%d]", path, i), from)
		}
	}
}
