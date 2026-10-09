package message

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Образец sdk/spec/examples (§14): сообщение WebSocket (Message) или
// HTTP-запрос с ответом (Request, Response).
type sample struct {
	From     string          `json:"from"`
	To       string          `json:"to"`
	Message  json.RawMessage `json:"message,omitempty"`
	Request  *httpRequest    `json:"request,omitempty"`
	Response *httpResponse   `json:"response,omitempty"`
}

type httpRequest struct {
	Method  string            `json:"method"`
	Path    string            `json:"path"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}

type httpResponse struct {
	Status  int               `json:"status"`
	Headers map[string]string `json:"headers,omitempty"`
	Body    json.RawMessage   `json:"body,omitempty"`
}

// examplesDir — sdk/spec/examples, найденный вверх от каталога пакета.
func examplesDir(t *testing.T) string {
	t.Helper()
	d, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		c := filepath.Join(d, "sdk", "spec", "examples")
		if st, err := os.Stat(c); err == nil && st.IsDir() {
			return c
		}
		if filepath.Dir(d) == d {
			t.Fatal("каталог sdk/spec/examples не найден")
		}
		d = filepath.Dir(d)
	}
}

// strict — разбор без незнакомых полей.
func strict(raw []byte, v any) error {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if err := dec.Decode(v); err != nil {
		return err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return errors.New("лишние данные после JSON")
	}
	return nil
}

func loadSamples(t *testing.T) map[string]sample {
	t.Helper()
	files, err := filepath.Glob(filepath.Join(examplesDir(t), "*.json"))
	if err != nil || len(files) == 0 {
		t.Fatalf("нет образцов: %v", err)
	}
	all := map[string]sample{}
	for _, file := range files {
		raw, err := os.ReadFile(file)
		if err != nil {
			t.Fatal(err)
		}
		var part map[string]sample
		if err := strict(raw, &part); err != nil {
			t.Fatalf("%s: %v", filepath.Base(file), err)
		}
		for name, s := range part {
			if _, dup := all[name]; dup {
				t.Fatalf("образец %s повторяется", name)
			}
			if (s.Message == nil) == (s.Request == nil || s.Response == nil) {
				t.Fatalf("%s: нужно либо message, либо request и response", name)
			}
			all[name] = s
		}
	}
	return all
}

func normalize(t *testing.T, raw []byte) any {
	t.Helper()
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// roundTrip — raw строго разбирается в v и сериализуется обратно без потерь.
func roundTrip(t *testing.T, what string, raw []byte, v any) {
	t.Helper()
	if err := strict(raw, v); err != nil {
		t.Fatalf("%s: %v\n%s", what, err, raw)
	}
	again, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(normalize(t, raw), normalize(t, again)) {
		t.Fatalf("%s: поля потеряны или искажены:\nобразец: %s\nGo:      %s", what, raw, again)
	}
}

// Содержимое сообщений по направлению; nil — сообщение без data.
var serverToAgent = map[string]func() any{
	TypeWelcome:      func() any { return &Welcome{} },
	TypeConfigPut:    func() any { return &ConfigPut{} },
	TypeConfigDelete: func() any { return &ConfigDelete{} },
	TypeFetch:        func() any { return &Fetch{} },
	TypeFetchCancel:  nil,
	TypeWatch:        func() any { return &Watch{} },
	TypeAction:       func() any { return &Action{} },
	TypeAck:          func() any { return &Ack{} },
	TypeError:        func() any { return &Error{} },
}

var agentToServer = map[string]func() any{
	TypeHello:         func() any { return &Hello{} },
	TypeStatus:        func() any { return &Status{} },
	TypeMetrics:       func() any { return &Metrics{} },
	TypeLog:           func() any { return &Log{} },
	TypeEvent:         func() any { return &Event{} },
	TypeConfigApplied: func() any { return &ConfigApplied{} },
	TypeFetchHead:     func() any { return &FetchHead{} },
	TypeFetchChunk:    func() any { return &FetchChunk{} },
	TypeFetchEnd:      func() any { return &FetchEnd{} },
	TypeActionResult:  func() any { return &ActionResult{} },
}

// Аргументы и итоги действий; nil — их нет.
var actionArgs = map[string]func() any{
	ActionWorkerRestart:  func() any { return &WorkerRestartArgs{} },
	ActionWorkerUpdate:   func() any { return &WorkerUpdateArgs{} },
	ActionAgentUpdate:    func() any { return &AgentUpdateArgs{} },
	ActionAgentRotateKey: nil,
	ActionAgentLogs:      func() any { return &AgentLogsArgs{} },
}

var actionResults = map[string]func() any{
	ActionWorkerRestart:  nil,
	ActionWorkerUpdate:   func() any { return &UpdateResult{} },
	ActionAgentUpdate:    func() any { return &UpdateResult{} },
	ActionAgentRotateKey: func() any { return &RotateKeyResult{} },
	ActionAgentLogs:      func() any { return &LogsResult{} },
}

// decodeOptional — raw (если есть) строго в тип фабрики; нет фабрики — raw
// должен отсутствовать.
func decodeOptional(t *testing.T, what string, raw json.RawMessage, factory func() any) {
	t.Helper()
	if factory == nil {
		if len(raw) != 0 {
			t.Fatalf("%s: лишнее содержимое %s", what, raw)
		}
		return
	}
	if len(raw) == 0 {
		t.Fatalf("%s: нет содержимого", what)
	}
	roundTrip(t, what, raw, factory())
}

// Каждое сообщение образцов строго разбирается в типы пакета и обратно;
// все типы и действия покрыты образцами, класс доставки виден по конверту.
func TestExamplesMessages(t *testing.T) {
	samples := loadSamples(t)
	actionByID := map[string]string{}
	for _, s := range samples {
		if s.Message == nil {
			continue
		}
		var env Envelope
		_ = json.Unmarshal(s.Message, &env)
		if env.Type == TypeAction {
			var a Action
			_ = env.Decode(&a)
			actionByID[env.ID] = a.Name
		}
	}
	seen := map[string]bool{}
	seenArgs, seenResults := map[string]bool{}, map[string]bool{}
	names := make([]string, 0, len(samples))
	for name := range samples {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		s := samples[name]
		if s.Message == nil {
			continue
		}
		t.Run(name, func(t *testing.T) {
			var types map[string]func() any
			switch {
			case s.From == "server" && s.To == "agent":
				types = serverToAgent
			case s.From == "agent" && s.To == "server":
				types = agentToServer
			default:
				t.Fatalf("направление %s → %s", s.From, s.To)
			}
			var env Envelope
			roundTrip(t, "конверт", s.Message, &env)
			factory, ok := types[env.Type]
			if !ok {
				t.Fatalf("тип %s (%s → %s) не описан в Go", env.Type, s.From, s.To)
			}
			seen[s.From+">"+env.Type] = true
			if factory == nil {
				if len(env.Data) != 0 {
					t.Fatalf("у %s не бывает data", env.Type)
				}
			} else {
				roundTrip(t, "data", env.Data, factory())
			}
			if s.From == "agent" {
				checkClass(t, env)
			} else if (env.Type == TypeFetch || env.Type == TypeAction) && env.ID == "" {
				t.Fatalf("запрос %s без id", env.Type)
			}
			switch env.Type {
			case TypeAction:
				var a Action
				_ = env.Decode(&a)
				factory, ok := actionArgs[a.Name]
				if !ok {
					t.Fatalf("незнакомое действие %s", a.Name)
				}
				seenArgs[a.Name] = true
				decodeOptional(t, "args "+a.Name, a.Args, factory)
			case TypeActionResult:
				var r ActionResult
				_ = env.Decode(&r)
				act := actionByID[env.Re]
				if act == "" {
					t.Fatalf("нет образца action с id %s", env.Re)
				}
				if r.OK == (r.Error != nil) {
					t.Fatalf("ok и error противоречат друг другу")
				}
				if r.OK {
					seenResults[act] = true
					decodeOptional(t, "result "+act, r.Result, actionResults[act])
				}
			}
		})
	}
	for dir, types := range map[string]map[string]func() any{"server": serverToAgent, "agent": agentToServer} {
		for typ := range types {
			if !seen[dir+">"+typ] {
				t.Errorf("нет образца %s от %s", typ, dir)
			}
		}
	}
	for act := range actionArgs {
		if !seenArgs[act] || !seenResults[act] {
			t.Errorf("нет образцов action и action.result для %s", act)
		}
	}
}

func checkClass(t *testing.T, env Envelope) {
	t.Helper()
	switch ClassOf(env.Type) {
	case ClassImportant:
		if env.ID == "" || env.Seq != 0 {
			t.Fatalf("важное %s: нужен id и нет seq", env.Type)
		}
	case ClassStream:
		if env.Seq < 1 || env.ID != "" {
			t.Fatalf("поток %s: нужен seq и нет id", env.Type)
		}
	case ClassReply:
		if env.Re == "" || env.ID != "" || env.Seq != 0 {
			t.Fatalf("ответ %s: нужен только re", env.Type)
		}
	}
}

// Маршрут HTTP-образца: тело запроса, тело успешного ответа и ответа с
// ошибкой; nil — тела нет.
type route struct {
	from, to, method, path string
	req, ok, fail          func() any
}

var routes = []route{
	{"agent", "server", "POST", EnrollPath, func() any { return &Enroll{} }, func() any { return &EnrollResult{} }, errorInfo},
	{"agent", "worker", "PUT", ConfigPathPrefix, func() any { return &ConfigValue{} }, nil, workerError},
	{"agent", "worker", "DELETE", ConfigPathPrefix, nil, nil, workerError},
	{"agent", "worker", "GET", MetricsPath, nil, func() any { return new(json.RawMessage) }, workerError},
	{"agent", "worker", "GET", HealthPath, nil, func() any { return &Health{} }, workerError},
	{"agent", "worker", "POST", CleanupPath, nil, nil, workerError},
	{"agent", "worker", "GET", WorkerManifestPath, nil, func() any { return &WorkerManifest{} }, workerError},
	{"worker", "agent", "POST", EventsPath, func() any { return &EventPost{} }, nil, errorInfo},
	{"worker", "agent", "GET", ConfigPathPrefix, nil, func() any { return &ConfigValue{} }, errorInfo},
	{"worker", "agent", "GET", ContextPath, nil, func() any { return &Context{} }, errorInfo},
}

func errorInfo() any   { return &ErrorInfo{} }
func workerError() any { return &WorkerError{} }

func findRoute(s sample) (int, bool) {
	for i, r := range routes {
		if r.from != s.From || r.to != s.To || r.method != s.Request.Method {
			continue
		}
		if r.path == ConfigPathPrefix {
			if key, ok := strings.CutPrefix(s.Request.Path, ConfigPathPrefix); ok && ValidName(key) {
				return i, true
			}
			continue
		}
		if s.Request.Path == r.path {
			return i, true
		}
	}
	return 0, false
}

// isText — тело-строка JSON: текст, а не объект.
func isText(raw json.RawMessage) bool { return len(raw) > 0 && raw[0] == '"' }

// HTTP-образцы (регистрация, агент ↔ воркер) строго разбираются в типы
// пакета; каждый маршрут покрыт образцом.
func TestExamplesHTTP(t *testing.T) {
	hit := map[int]bool{}
	for name, s := range loadSamples(t) {
		if s.Request == nil {
			continue
		}
		t.Run(name, func(t *testing.T) {
			i, ok := findRoute(s)
			if !ok {
				t.Fatalf("маршрут %s %s (%s → %s) не описан", s.Request.Method, s.Request.Path, s.From, s.To)
			}
			hit[i] = true
			r := routes[i]
			decodeOptional(t, "тело запроса", s.Request.Body, r.req)
			body := s.Response.Body
			if s.Response.Status >= 200 && s.Response.Status < 300 {
				factory := r.ok
				if strings.HasSuffix(name, "@"+BuiltinSysmetrics) {
					factory = func() any { return &HostMetrics{} }
				}
				decodeOptional(t, "тело ответа", body, factory)
				if r.path == WorkerManifestPath {
					if _, err := ParseWorkerManifest(body); err != nil {
						t.Fatalf("манифест не по §12, §16: %v", err)
					}
				}
				return
			}
			if r.to == "worker" && isText(body) {
				return
			}
			decodeOptional(t, "тело ошибки", body, r.fail)
		})
	}
	for i, r := range routes {
		if !hit[i] {
			t.Errorf("нет образца %s %s (%s → %s)", r.method, r.path, r.from, r.to)
		}
	}
}

// Все коды ошибок в образцах описаны в Codes; все имена воркеров и ключей
// соответствуют правилу.
func TestExamplesCodesAndNames(t *testing.T) {
	known := map[string]bool{}
	for _, c := range Codes {
		known[c] = true
	}
	var walk func(name string, v any)
	walk = func(name string, v any) {
		switch v := v.(type) {
		case map[string]any:
			if c, ok := v["code"].(string); ok && !known[c] {
				t.Errorf("%s: незнакомый код %s", name, c)
			}
			for _, k := range []string{"worker", "key"} {
				if n, ok := v[k].(string); ok && !ValidName(n) {
					t.Errorf("%s: неверное имя %s=%q", name, k, n)
				}
			}
			for _, x := range v {
				walk(name, x)
			}
		case []any:
			for _, x := range v {
				walk(name, x)
			}
		}
	}
	for name, s := range loadSamples(t) {
		raw, _ := json.Marshal(s)
		walk(name, normalize(t, raw))
	}
}
