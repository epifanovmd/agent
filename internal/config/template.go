package config

import (
	"bytes"
	_ "embed"
	"strconv"
	"strings"
	"text/template"
)

// TemplateWorker — воркер, который записывается в создаваемый файл настроек.
type TemplateWorker struct {
	Name        string
	Release     bool
	Command     []string
	StopTimeout string
}

// TemplateOptions — значения для создаваемого файла настроек; пустые поля
// остаются закомментированными примерами.
type TemplateOptions struct {
	ServerURL string
	Token     string
	Name      string
	DataDir   string
	CAFile    string
	LogFormat string
	Workers   []TemplateWorker
	// uncomment — примеры без «#» (проверка, что примеры — верный YAML).
	uncomment bool
}

//go:embed agent.yaml.tmpl
var templateText string

var configTemplate = template.Must(template.New("agent.yaml").Funcs(template.FuncMap{
	"quote": quote,
	"dur":   dur,
	"join":  strings.Join,
	"commandList": func(cmd []string) string {
		parts := make([]string, len(cmd))
		for i, c := range cmd {
			parts[i] = strconv.Quote(c)
		}
		return strings.Join(parts, ", ")
	},
}).Parse(templateText))

// templateData — значения шаблона agent.yaml.tmpl.
type templateData struct {
	TemplateOptions
	// Ex — «# » перед примером; пусто — примеры без комментария.
	Ex        string
	Uncomment bool
	D         Config
	L         Lifecycle
	// Значения по умолчанию из полей-указателей и констант.
	BusyTimeout, HealthInterval Duration
	HealthFailures              int
	LogMaxSize                  string
	LogMaxFiles                 int
	Routes                      string
}

// Template — файл настроек с пояснениями (agent.yaml.tmpl): заданные значения
// и закомментированные примеры всех разделов со значениями по умолчанию
// (agent init, agent install).
func Template(o TemplateOptions) []byte {
	l := DefaultLifecycle()
	data := templateData{
		TemplateOptions: o, Ex: "# ", Uncomment: o.uncomment, D: Defaults(), L: l,
		BusyTimeout: *l.BusyTimeout, HealthInterval: *l.Health.Interval, HealthFailures: *l.Health.Failures,
		LogMaxSize: DefaultLogs().MaxSize.String(), LogMaxFiles: DefaultLogMaxFiles, Routes: RoutesStrict,
	}
	if o.uncomment {
		data.Ex = ""
	}
	var b bytes.Buffer
	if err := configTemplate.Execute(&b, data); err != nil {
		panic("agent.yaml.tmpl: " + err.Error())
	}
	return b.Bytes()
}

func dur(d Duration) string {
	s := d.Std().String()
	// 10m0s → 10m, 1h0m0s → 1h.
	for _, suf := range []string{"0s", "0m"} {
		if strings.HasSuffix(s, "m"+suf) || strings.HasSuffix(s, "h"+suf) {
			s = strings.TrimSuffix(s, suf)
		}
	}
	return s
}

// quote — значение YAML: как есть, если это безопасно, иначе в кавычках.
func quote(s string) string {
	plain := s != "" && strings.TrimSpace(s) == s &&
		!strings.ContainsAny(s[:1], "-?:,[]{}#&*!|>'\"%@`") &&
		!strings.Contains(s, ": ") && !strings.Contains(s, " #") && !strings.HasSuffix(s, ":") &&
		!strings.ContainsAny(s, "\n\t\"'\\$") &&
		s != "true" && s != "false" && s != "null" && s != "~" && s != "yes" && s != "no"
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		plain = false
	}
	if plain {
		return s
	}
	return strconv.Quote(s)
}
