package message

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNewDecode(t *testing.T) {
	env := MustNew(TypeConfigDelete, ConfigDelete{Worker: "report", Key: "main"})
	raw, _ := json.Marshal(env)
	if string(raw) != `{"type":"config.delete","data":{"worker":"report","key":"main"}}` {
		t.Fatalf("конверт: %s", raw)
	}
	var d ConfigDelete
	if err := env.Decode(&d); err != nil || d.Key != "main" {
		t.Fatalf("Decode: %v %+v", err, d)
	}
	bare := MustNew(TypeFetchCancel, nil)
	if raw, _ := json.Marshal(bare); string(raw) != `{"type":"fetch.cancel"}` {
		t.Fatalf("без data: %s", raw)
	}
	var w Watch
	if err := bare.Decode(&w); err != nil || !w.Empty() {
		t.Fatalf("пустой Decode: %v %+v", err, w)
	}
	if err := (Envelope{Type: TypeWatch, Data: json.RawMessage(`[1]`)}).Decode(&w); err == nil ||
		!strings.Contains(err.Error(), TypeWatch) {
		t.Fatalf("ошибка без типа: %v", err)
	}
	if _, err := New(TypeEvent, func() {}); err == nil {
		t.Fatal("ждали ошибку сериализации")
	}
	if len(NewID()) != 32 || len(NewBootID()) != 16 || NewID() == NewID() {
		t.Fatal("NewID / NewBootID")
	}
}

func TestClassOf(t *testing.T) {
	for typ, want := range map[string]Class{
		TypeEvent: ClassImportant, TypeConfigApplied: ClassImportant, TypeActionResult: ClassImportant,
		TypeStatus: ClassStream, TypeMetrics: ClassStream, TypeLog: ClassStream,
		TypeFetchHead: ClassReply, TypeFetchChunk: ClassReply, TypeFetchEnd: ClassReply,
		TypeHello: ClassControl,
	} {
		if got := ClassOf(typ); got != want {
			t.Errorf("ClassOf(%s) = %d, ждали %d", typ, got, want)
		}
	}
}

func TestNames(t *testing.T) {
	for _, n := range []string{"a", "report", "echo-2", "a" + strings.Repeat("b", 31)} {
		if !ValidName(n) {
			t.Errorf("ValidName(%q) = false", n)
		}
	}
	for _, n := range []string{"", "1a", "-a", "Report", "a.b", "a_b", "a b", "пример", "a" + strings.Repeat("b", 32), "a\n"} {
		if ValidName(n) {
			t.Errorf("ValidName(%q) = true", n)
		}
	}
	for _, n := range []string{"report.sent", "a", "a-b_c.d"} {
		if !ValidEventType(n) {
			t.Errorf("ValidEventType(%q) = false", n)
		}
	}
	for _, n := range []string{"", ".a", "Report", "a/b", "a" + strings.Repeat("b", 64)} {
		if ValidEventType(n) {
			t.Errorf("ValidEventType(%q) = true", n)
		}
	}
	if !ValidAgentName("узел-01") || ValidAgentName("") || ValidAgentName(strings.Repeat("я", 129)) {
		t.Error("ValidAgentName")
	}
	if err := CheckLabels(map[string]string{"zone": "eu"}); err != nil {
		t.Error(err)
	}
	many := map[string]string{}
	for i := range MaxLabels + 1 {
		many[strings.Repeat("k", i+1)] = "v"
	}
	for _, bad := range []map[string]string{{"": "v"}, {"k": strings.Repeat("v", 257)}, many} {
		if CheckLabels(bad) == nil {
			t.Errorf("CheckLabels(%d меток): ждали ошибку", len(bad))
		}
	}
}

func TestLogLevelRank(t *testing.T) {
	if !(LogLevelRank(LogDebug) < LogLevelRank(LogInfo) && LogLevelRank(LogInfo) < LogLevelRank(LogWarn) &&
		LogLevelRank(LogWarn) < LogLevelRank(LogError)) || LogLevelRank("trace") != -1 {
		t.Fatal("LogLevelRank")
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
