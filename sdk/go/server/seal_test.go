package server

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/epifanovmd/agent/sdk/go/internal/examples"
	"github.com/epifanovmd/agent/sdk/go/message"
	"github.com/epifanovmd/agent/sdk/go/sealed"
)

type sealedExample struct {
	AgentKey struct {
		PrivateKey string `json:"privateKey"`
		PublicKey  string `json:"publicKey"`
	} `json:"agentKey"`
	Samples []struct {
		Sealed json.RawMessage `json:"sealed"`
		Value  json.RawMessage `json:"value"`
	} `json:"samples"`
}

// Seal — ключом из hello.agent.encryptionKey (тестовая пара образца sealed.json):
// закрытый ключ агента раскрывает каждое значение образца; образец раскрывается
// тем же кодом. Нет ключа — SEAL_NOT_AVAILABLE, нет агента — AGENT_NOT_FOUND.
func TestSeal(t *testing.T) {
	var fx sealedExample
	if err := json.Unmarshal(examples.Sealed(t), &fx); err != nil {
		t.Fatal(err)
	}
	priv, err := sealed.ParsePrivateKey(fx.AgentKey.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	agents := newTestAgents(t, Options{})
	id, _ := enroll(t, agents, "a")
	if _, err := agents.Seal(id, "x"); protoCode(err) != "SEAL_NOT_AVAILABLE" {
		t.Fatalf("до hello: %v", err)
	}
	if _, err := agents.Seal("nobody", "x"); protoCode(err) != "AGENT_NOT_FOUND" {
		t.Fatalf("нет агента: %v", err)
	}
	// hello без ключа шифрования — тоже нельзя.
	open(t, agents, id, helloEnv("b", message.Capabilities{}))
	if _, err := agents.Seal(id, "x"); protoCode(err) != "SEAL_NOT_AVAILABLE" {
		t.Fatalf("hello без ключа: %v", err)
	}
	hello := message.MustNew(message.TypeHello, message.Hello{
		Versions: []int{1}, Agent: message.HelloAgent{Name: "a", Version: "1", BootID: "b", EncryptionKey: fx.AgentKey.PublicKey},
		Jobs: []message.JobRef{},
	})
	open(t, agents, id, hello)

	same := func(a, b json.RawMessage) bool {
		var x, y any
		return json.Unmarshal(a, &x) == nil && json.Unmarshal(b, &y) == nil && reflect.DeepEqual(x, y)
	}
	for i, s := range fx.Samples {
		got, err := sealed.Unseal(priv, s.Sealed)
		if err != nil || !same(got, s.Value) {
			t.Fatalf("образец %d: %s, %v", i, got, err)
		}
		box, err := agents.Seal(id, s.Value)
		if err != nil {
			t.Fatalf("seal %d: %v", i, err)
		}
		var obj map[string]string
		if json.Unmarshal(box, &obj) != nil || len(obj) != 1 || obj["$sealed"] == "" {
			t.Fatalf("seal %d: не {\"$sealed\": …}: %s", i, box)
		}
		got, err = sealed.Unseal(priv, box)
		if err != nil || !same(got, s.Value) {
			t.Fatalf("seal %d раскрыт: %s, %v", i, got, err)
		}
	}
	// Значение Go (не RawMessage) — внутри снимка.
	box, err := agents.Seal(id, map[string]string{"password": "p"})
	if err != nil {
		t.Fatal(err)
	}
	spec, _ := json.Marshal(map[string]any{"db": box, "port": 1})
	got, err := sealed.Unseal(priv, spec)
	if err != nil || !same(got, json.RawMessage(`{"db":{"password":"p"},"port":1}`)) {
		t.Fatalf("снимок: %s, %v", got, err)
	}
	if _, err := agents.Seal(id, json.RawMessage(`{`)); protoCode(err) != "MESSAGE_INVALID" {
		t.Fatalf("некорректный JSON: %v", err)
	}
}
