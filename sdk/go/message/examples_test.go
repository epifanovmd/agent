package message

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/sdk/go/internal/examples"
)

// readExample — конверт образца sdk/spec/examples по имени.
func readExample(t *testing.T, name string) Envelope {
	t.Helper()
	var env Envelope
	if err := json.Unmarshal(examples.Message(t, name), &env); err != nil {
		t.Fatal(err)
	}
	return env
}

var agentToServer = map[string]func() any{
	TypeHello:        func() any { return &Hello{} },
	TypeStatus:       func() any { return &Status{} },
	TypeMetrics:      func() any { return &Metrics{} },
	TypeJobAccept:    func() any { return &JobRef{} },
	TypeJobReject:    func() any { return &JobReject{} },
	TypeJobProgress:  func() any { return &JobProgress{} },
	TypeJobEvent:     func() any { return &JobEvent{} },
	TypeJobURLs:      func() any { return &JobURLsRequest{} },
	TypeJobComplete:  func() any { return &JobComplete{} },
	TypeJobFail:      func() any { return &JobFail{} },
	TypeCmdAccept:    func() any { return &CommandRef{} },
	TypeCmdOutput:    func() any { return &CommandOutput{} },
	TypeCmdDone:      func() any { return &CommandDone{} },
	TypeStateApplied: func() any { return &StateApplied{} },
	TypeCapabilities: func() any { return &Capabilities{} },
	TypeEvent:        func() any { return &Event{} },
	TypeInventory:    func() any { return &Inventory{} },
	TypeLog:          func() any { return &LogBatch{} },
}

var serverToAgent = map[string]func() any{
	TypeWelcome:   func() any { return &Welcome{} },
	TypeConfig:    func() any { return &SessionConfig{} },
	TypeAck:       func() any { return &Ack{} },
	TypeError:     func() any { return &Error{} },
	TypeJobAssign: func() any { return &JobAssign{} },
	TypeJobURLs:   func() any { return &JobURLs{} },
	TypeJobCancel: func() any { return &JobRef{} },
	TypeJobStop:   func() any { return &JobRef{} },
	TypeCmdRun:    func() any { return &CommandRun{} },
	TypeStatePut:  func() any { return &StatePut{} },
}

// Сообщения, которые есть только в IPC нагрузок (§10).
var workerToAgent = map[string]func() any{
	TypeWorkerRegister: func() any { return &WorkerRegister{} },
	TypeTelemetry:      func() any { return &Telemetry{} },
	TypeEvent:          func() any { return &Event{} },
	TypeStateApplied:   func() any { return &StateApplied{} },
	TypeWorkerCleaned:  func() any { return &WorkerCleaned{} },
	TypeWorkerHealth:   func() any { return &WorkerHealth{} },
	TypeWorkerPause:    func() any { return &WorkerPause{} },
	TypeWorkerResume:   func() any { return &WorkerResume{} },
	TypeWorkerRestart:  func() any { return &WorkerRestartRequest{} },
}

var agentToWorker = map[string]func() any{
	TypeWorkerReady:   func() any { return &WorkerReady{} },
	TypeWorkerDrain:   func() any { return &struct{}{} },
	TypeCmdCancel:     func() any { return &CommandRef{} },
	TypeStatePut:      func() any { return &StatePut{} },
	TypeWorkerCleanup: func() any { return &struct{}{} },
	TypeWorkerContext: func() any { return &WorkerContext{} },
}

func normalize(t *testing.T, raw []byte) any {
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		t.Fatal(err)
	}
	return v
}

// Каждый образец разбирается в структуру своего типа и сериализуется обратно
// без потери и искажения полей: Go-контракт совпадает со спецификацией.
func TestExamplesRoundTrip(t *testing.T) {
	for _, dir := range []struct {
		from, to string
		types    map[string]func() any
	}{
		{examples.Agent, examples.Server, agentToServer},
		{examples.Server, examples.Agent, serverToAgent},
		{examples.Worker, examples.Agent, workerToAgent},
		{examples.Agent, examples.Worker, agentToWorker},
	} {
		types := dir.types
		seen := map[string]bool{}
		for _, ex := range examples.Between(t, dir.from, dir.to) {
			raw := []byte(ex.Message)
			t.Run(dir.from+"-"+dir.to+"/"+ex.Name, func(t *testing.T) {
				var env Envelope
				if err := json.Unmarshal(raw, &env); err != nil {
					t.Fatal(err)
				}
				factory, ok := types[env.Type]
				if !ok {
					t.Fatalf("тип %s не описан в Go", env.Type)
				}
				seen[env.Type] = true
				payload := factory()
				if err := env.Decode(payload); err != nil {
					t.Fatal(err)
				}
				again, err := json.Marshal(payload)
				if err != nil {
					t.Fatal(err)
				}
				if want, got := normalize(t, env.Data), normalize(t, again); !reflect.DeepEqual(want, got) {
					t.Fatalf("поля потеряны или искажены:\nобразец: %s\nGo:      %s", env.Data, again)
				}
				// Конверт тоже сериализуется без потерь.
				envAgain, _ := json.Marshal(env)
				if !reflect.DeepEqual(normalize(t, raw), normalize(t, envAgain)) {
					t.Fatalf("конверт искажён: %s", envAgain)
				}
			})
		}
		for typ := range types {
			if !seen[typ] && !strings.HasPrefix(typ, "_") {
				t.Errorf("нет образца %s → %s: %s", dir.from, dir.to, typ)
			}
		}
	}
}

// worker.update: args и итог образцов разбираются в WorkerUpdate и
// WorkerUpdateResult без потерь; status.workers[].release — в StatusWorker.
func TestExamplesWorkerUpdate(t *testing.T) {
	roundTrip := func(file string, field func(Envelope) json.RawMessage, v any) {
		t.Helper()
		env := readExample(t, file)
		part := field(env)
		if err := json.Unmarshal(part, v); err != nil {
			t.Fatal(err)
		}
		again, _ := json.Marshal(v)
		if !reflect.DeepEqual(normalize(t, part), normalize(t, again)) {
			t.Fatalf("%s: поля потеряны:\nобразец: %s\nGo:      %s", file, part, again)
		}
	}
	var run CommandRun
	roundTrip("cmd.run.workerUpdate", func(e Envelope) json.RawMessage {
		_ = e.Decode(&run)
		return run.Args
	}, &WorkerUpdate{})
	if run.Name != CommandWorkerUpdate {
		t.Fatalf("имя команды: %s", run.Name)
	}
	var done CommandDone
	roundTrip("cmd.done.workerUpdate", func(e Envelope) json.RawMessage {
		_ = e.Decode(&done)
		return done.Result
	}, &WorkerUpdateResult{})
	var st Status
	roundTrip("status.release", func(e Envelope) json.RawMessage { return e.Data }, &st)
	if len(st.Workers) != 1 || !st.Workers[0].Release {
		t.Fatalf("status.workers[].release: %+v", st.Workers)
	}
}

// Управление воркером: args worker.pause — в WorkerPauseArgs,
// status.workers[].health/message/paused — в StatusWorker без потерь.
func TestExamplesWorkerControl(t *testing.T) {
	read := func(name string) Envelope { return readExample(t, name) }
	same := func(file string, part json.RawMessage, v any) {
		t.Helper()
		if err := json.Unmarshal(part, v); err != nil {
			t.Fatal(err)
		}
		again, _ := json.Marshal(v)
		if !reflect.DeepEqual(normalize(t, part), normalize(t, again)) {
			t.Fatalf("%s: поля потеряны:\nобразец: %s\nGo:      %s", file, part, again)
		}
	}
	var run CommandRun
	if err := read("cmd.run.workerPause").Decode(&run); err != nil || run.Name != CommandWorkerPause {
		t.Fatalf("cmd.run: %v %+v", err, run)
	}
	var args WorkerPauseArgs
	same("cmd.run.workerPause", run.Args, &args)
	if args.Name == "" || len(args.Queues) != 1 {
		t.Fatalf("args: %+v", args)
	}
	var st Status
	same("status.workers", read("status.workers").Data, &st)
	if w := st.Workers[0]; w.Health != WorkerHealthDegraded || w.Message == "" || !w.Paused {
		t.Fatalf("status.workers: %+v", w)
	}
	var wc WorkerContext
	same("worker.context.cleanup", read("worker.context.cleanup").Data, &wc)
	if wc.Mode != WorkerModeCleanup {
		t.Fatalf("mode: %s", wc.Mode)
	}
}

// config.subscription: сводная подписка без потерь полей; пустая — не nil
// (подписок нет); без поля — nil (не менять). worker.context — с channels.
func TestExamplesSubscription(t *testing.T) {
	read := func(name string) Envelope { return readExample(t, name) }
	var cfg SessionConfig
	env := read("config.subscription")
	if err := env.Decode(&cfg); err != nil || cfg.Subscription == nil {
		t.Fatalf("subscription: %v %+v", err, cfg)
	}
	want := Subscription{MetricsIntervalMs: 1000, Metrics: []string{"diskio", "sockets"}, LogLevel: LogDebug,
		Channels: map[string]int64{"example.app": 1000}}
	if !reflect.DeepEqual(*cfg.Subscription, want) {
		t.Fatalf("subscription: %+v", *cfg.Subscription)
	}
	if again, _ := json.Marshal(cfg); !reflect.DeepEqual(normalize(t, env.Data), normalize(t, again)) {
		t.Fatalf("поля потеряны: %s", again)
	}
	var empty SessionConfig
	if err := read("config.subscription.empty").Decode(&empty); err != nil || empty.Subscription == nil ||
		!reflect.DeepEqual(*empty.Subscription, Subscription{}) {
		t.Fatalf("пустая: %v %+v", err, empty.Subscription)
	}
	if b, _ := json.Marshal(SessionConfig{Subscription: &Subscription{}}); string(b) != `{"subscription":{}}` {
		t.Fatalf("пустая: %s", b)
	}
	var plain SessionConfig
	if err := json.Unmarshal([]byte(`{"metricsIntervalMs":2000}`), &plain); err != nil || plain.Subscription != nil {
		t.Fatalf("без subscription: %v %+v", err, plain.Subscription)
	}
	var w Welcome
	if err := read("welcome.subscription").Decode(&w); err != nil || w.Config.Subscription == nil ||
		w.Config.Subscription.Channels["example.app"] != 1000 {
		t.Fatalf("welcome: %v %+v", err, w.Config.Subscription)
	}
	var wc WorkerContext
	env = read("worker.context")
	if err := env.Decode(&wc); err != nil || wc.Channels["example.app"] != 1000 {
		t.Fatalf("worker.context: %v %+v", err, wc)
	}
	if again, _ := json.Marshal(wc); !reflect.DeepEqual(normalize(t, env.Data), normalize(t, again)) {
		t.Fatalf("worker.context: поля потеряны: %s", again)
	}
}

// Manifest.Worker — старшая по версии запись воркера под платформу.
func TestManifestWorker(t *testing.T) {
	m := Manifest{Workers: []WorkerArtifact{
		{Name: "w", Version: "1.10.0", OS: "linux", Arch: "amd64"},
		{Name: "w", Version: "1.9.2", OS: "linux", Arch: "amd64"},
		{Name: "w", Version: "3", OS: "linux", Arch: "arm64"},
	}}
	if got := m.Worker("w", "linux", "amd64"); got == nil || got.Version != "1.10.0" {
		t.Fatalf("Worker: %+v", got)
	}
	if m.Worker("w", "darwin", "amd64") != nil || m.Worker("x", "linux", "amd64") != nil {
		t.Fatal("лишняя запись")
	}
	for _, c := range []struct {
		a, b string
		want int
	}{{"1.10.0", "1.9.9", 1}, {"2.0.0", "2.0.0", 0}, {"1.2", "1.2.1", -1}} {
		if got := CompareVersions(c.a, c.b); got != c.want {
			t.Errorf("CompareVersions(%s, %s) = %d", c.a, c.b, got)
		}
	}
}
