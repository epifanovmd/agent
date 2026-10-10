package config

import (
	"errors"
	"fmt"
	"io"
	"os"
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

// EnvVars — переменные окружения, которые читает агент (applyEnv).
var EnvVars = []string{
	"AGENT_SERVER_URL", "AGENT_SERVER_URLS", "AGENT_SERVER_CA_FILE", "AGENT_SERVER_CERT_FILE", "AGENT_SERVER_KEY_FILE",
	"AGENT_DATA_DIR", "AGENT_NAME", "AGENT_LABELS", "AGENT_ENROLL_TOKEN",
	"AGENT_LOG_LEVEL", "AGENT_LOG_FORMAT", "AGENT_LOG_FORWARD",
	"AGENT_TELEMETRY_METRICS", "AGENT_TELEMETRY_DISKS", "AGENT_TELEMETRY_EXCLUDE_INTERFACES",
	"AGENT_UPDATE_MODE", "AGENT_UPDATE_PUBLIC_KEY", "AGENT_UPDATE_PUBLIC_KEYS", "AGENT_UPDATE_RELEASES", "AGENT_UPDATE_CHECK_INTERVAL",
	"AGENT_SERVER_RECONNECT_MIN", "AGENT_SERVER_RECONNECT_MAX", "AGENT_SERVER_STREAM_BUFFER",
	"AGENT_OUTBOX_MAX_MESSAGES", "AGENT_LOG_BUFFER",
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
	for _, name := range EnvVars {
		if v, ok := os.LookupEnv(name); ok && v != "" {
			res.FromEnv = append(res.FromEnv, name)
		} else if v := src.Env[name]; v != "" {
			res.FromEnvFiles = append(res.FromEnvFiles, name)
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

// envPaths — какое поле настроек задаёт переменная AGENT_* (для пометок config check).
var envPaths = map[string]string{
	"AGENT_SERVER_URL": "server.url", "AGENT_SERVER_URLS": "server.urls", "AGENT_SERVER_CA_FILE": "server.caFile",
	"AGENT_SERVER_CERT_FILE": "server.certFile", "AGENT_SERVER_KEY_FILE": "server.keyFile",
	"AGENT_DATA_DIR": "dataDir", "AGENT_NAME": "name", "AGENT_ENROLL_TOKEN": "enroll.token",
	"AGENT_LOG_LEVEL": "log.level", "AGENT_LOG_FORMAT": "log.format", "AGENT_LOG_FORWARD": "log.forward",
	"AGENT_TELEMETRY_METRICS": "telemetry.metrics", "AGENT_TELEMETRY_DISKS": "telemetry.disks",
	"AGENT_TELEMETRY_EXCLUDE_INTERFACES": "telemetry.excludeInterfaces",
	"AGENT_UPDATE_MODE":                  "update.mode", "AGENT_UPDATE_PUBLIC_KEY": "update.publicKey", "AGENT_UPDATE_PUBLIC_KEYS": "update.publicKeys",
	"AGENT_UPDATE_RELEASES":      "update.releases",
	"AGENT_SERVER_RECONNECT_MIN": "server.reconnect.min", "AGENT_SERVER_RECONNECT_MAX": "server.reconnect.max",
	"AGENT_SERVER_STREAM_BUFFER": "server.streamBuffer", "AGENT_OUTBOX_MAX_MESSAGES": "outbox.maxMessages",
	"AGENT_LOG_BUFFER": "log.buffer",
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
	for _, name := range r.FromEnvFiles {
		if p, ok := envPaths[name]; ok {
			from[p] = name + " из файла переменных"
		}
	}
	for _, name := range r.FromEnv {
		if p, ok := envPaths[name]; ok {
			from[p] = name
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
