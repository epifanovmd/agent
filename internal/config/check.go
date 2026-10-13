package config

import (
	"errors"
	"fmt"
	"io"
	"regexp"
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
	// Warnings — неизвестные поля: агент их не читает (опечатка или поле
	// другой версии), но работать это не мешает.
	Warnings []Problem
	// Errors — с такими настройками агент не запустится.
	Errors []Problem
}

var (
	reLine    = regexp.MustCompile(`^(?:yaml: )?(?:line|строка) (\d+): (.*)$`)
	reUnknown = regexp.MustCompile(`^field (\S+) not found in type`)
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
	applyEnv(&res.Config, src.Lookup)
	if err := res.Config.Validate(); err != nil {
		for _, text := range strings.Split(err.Error(), "\n") {
			res.Errors = append(res.Errors, Problem{Text: text})
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
