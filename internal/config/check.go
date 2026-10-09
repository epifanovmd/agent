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

// Problem — замечание или ошибка в настройках: строка файла (0 — неизвестна)
// и понятный текст.
type Problem struct {
	Line int
	Text string
}

func (p Problem) String() string {
	if p.Line > 0 {
		return fmt.Sprintf("строка %d: %s", p.Line, p.Text)
	}
	return p.Text
}

// CheckResult — итог agent config check.
type CheckResult struct {
	// Path — проверенный файл ("" — без файла, только окружение).
	Path string
	// Config — итоговые настройки: умолчания, файл, окружение.
	Config Config
	// FromEnv — переменные AGENT_*, которые заменили значения файла.
	FromEnv []string
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
	"AGENT_UPDATE_MODE", "AGENT_UPDATE_PUBLIC_KEY", "AGENT_UPDATE_PUBLIC_KEYS",
	"AGENT_SERVER_RECONNECT_MIN", "AGENT_SERVER_RECONNECT_MAX", "AGENT_SERVER_STREAM_BUFFER",
	"AGENT_OUTBOX_MAX_MESSAGES", "AGENT_LOG_BUFFER",
}

var (
	reLine    = regexp.MustCompile(`^(?:yaml: )?(?:line|строка) (\d+): (.*)$`)
	reUnknown = regexp.MustCompile(`^field (\S+) not found in type`)
	rePath    = regexp.MustCompile(`^[A-Za-z]+(?:\[\d+\])?(?:\.[A-Za-z]+(?:\[\d+\])?)*`)
)

// Check — проверить настройки, как их прочитает agent run: файл path (с
// подстановкой ${ENV}), переменные AGENT_*, допустимые значения. В отличие
// от Load, сообщает неизвестные поля и номера строк.
func Check(path string) CheckResult {
	res := CheckResult{Path: path, Config: Defaults()}
	var lines map[string]int
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			res.Errors = append(res.Errors, Problem{Text: fmt.Sprintf("файл настроек: %v", err)})
			return res
		}
		src := os.ExpandEnv(string(raw))
		res.Warnings = unknownFields(src)
		if err := yaml.Unmarshal([]byte(src), &res.Config); err != nil {
			res.Errors = append(res.Errors, yamlProblems(err)...)
			return res
		}
		lines = fieldLines(src)
	}
	for _, name := range EnvVars {
		if v, ok := os.LookupEnv(name); ok && v != "" {
			res.FromEnv = append(res.FromEnv, name)
		}
	}
	applyEnv(&res.Config)
	if err := res.Config.Validate(); err != nil {
		for _, text := range strings.Split(err.Error(), "\n") {
			res.Errors = append(res.Errors, Problem{Line: lineOf(lines, text), Text: text})
		}
	}
	return res
}

// Unknown — неизвестные поля файла настроек (для предупреждения при запуске).
func Unknown(path string) []Problem {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	return unknownFields(os.ExpandEnv(string(raw)))
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

// fieldLines — строка каждого ключа файла: "server.url", "workers[0]",
// "workers[0].stopTimeout".
func fieldLines(src string) map[string]int {
	var root yaml.Node
	if yaml.Unmarshal([]byte(src), &root) != nil {
		return nil
	}
	out := map[string]int{}
	var walk func(n *yaml.Node, path string)
	walk = func(n *yaml.Node, path string) {
		switch n.Kind {
		case yaml.DocumentNode:
			for _, c := range n.Content {
				walk(c, path)
			}
		case yaml.MappingNode:
			for i := 0; i+1 < len(n.Content); i += 2 {
				p := n.Content[i].Value
				if path != "" {
					p = path + "." + p
				}
				out[p] = n.Content[i].Line
				walk(n.Content[i+1], p)
			}
		case yaml.SequenceNode:
			for i, c := range n.Content {
				p := fmt.Sprintf("%s[%d]", path, i)
				out[p] = c.Line
				walk(c, p)
			}
		}
	}
	walk(&root, "")
	return out
}

// lineOf — строка поля, о котором ошибка Validate («workers[1].user: …»):
// само поле или ближайшее объемлющее.
func lineOf(lines map[string]int, text string) int {
	p := rePath.FindString(text)
	for p != "" {
		if n, ok := lines[p]; ok {
			return n
		}
		i := strings.LastIndexAny(p, ".[")
		if i < 0 {
			break
		}
		p = p[:i]
	}
	return 0
}
