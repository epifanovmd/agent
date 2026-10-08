package sealed

import (
	"bytes"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/sdk/go/internal/examples"
)

type sealedExample struct {
	AgentKey struct {
		PrivateKey string `json:"privateKey"`
		PublicKey  string `json:"publicKey"`
	} `json:"agentKey"`
	Samples []struct {
		Value  json.RawMessage `json:"value"`
		Sealed json.RawMessage `json:"sealed"`
	} `json:"samples"`
	Snapshot struct {
		Spec     json.RawMessage `json:"spec"`
		Unsealed json.RawMessage `json:"unsealed"`
	} `json:"snapshot"`
	WrongKey json.RawMessage `json:"wrongKey"`
}

func loadSealed(t *testing.T) sealedExample {
	t.Helper()
	var f sealedExample
	if err := json.Unmarshal(examples.Sealed(t), &f); err != nil {
		t.Fatal(err)
	}
	return f
}

func sameJSON(t *testing.T, a, b []byte) bool {
	t.Helper()
	var x, y any
	if json.Unmarshal(a, &x) != nil || json.Unmarshal(b, &y) != nil {
		t.Fatalf("не JSON: %s | %s", a, b)
	}
	return reflect.DeepEqual(x, y)
}

// Образец sdk/spec/examples/sealed.json раскрывается тестовым ключом агента;
// значение, запечатанное другим ключом, — ErrOpen.
func TestSealedExample(t *testing.T) {
	f := loadSealed(t)
	priv, err := ParsePrivateKey(f.AgentKey.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if EncodeKey(priv.PublicKey().Bytes()) != f.AgentKey.PublicKey {
		t.Fatal("открытый ключ образца не от его закрытого")
	}
	for _, s := range f.Samples {
		got, err := Unseal(priv, s.Sealed)
		if err != nil {
			t.Fatalf("%s: %v", s.Sealed, err)
		}
		if !sameJSON(t, got, s.Value) {
			t.Fatalf("раскрыто %s, ждали %s", got, s.Value)
		}
	}
	got, err := Unseal(priv, f.Snapshot.Spec)
	if err != nil || !sameJSON(t, got, f.Snapshot.Unsealed) {
		t.Fatalf("снимок: %s %v", got, err)
	}
	if _, err := Unseal(priv, f.WrongKey); !errors.Is(err, ErrOpen) {
		t.Fatalf("чужой ключ: %v", err)
	}
	// Образец state.put с запечатанными значениями (state.put.sealed).
	raw := examples.Message(t, "state.put.sealed")
	var put struct {
		Data struct {
			Spec json.RawMessage `json:"spec"`
		} `json:"data"`
	}
	_ = json.Unmarshal(raw, &put)
	if got, err := Unseal(priv, put.Data.Spec); err != nil || !sameJSON(t, got, f.Snapshot.Unsealed) {
		t.Fatalf("state.put.sealed: %s %v", got, err)
	}
}

// Seal → Unseal на свежей паре; числа не теряют точность, HTML не экранируется.
func TestSealUnseal(t *testing.T) {
	priv, err := GenerateKey()
	if err != nil {
		t.Fatal(err)
	}
	pub := EncodeKey(priv.PublicKey().Bytes())
	secret, err := Seal(pub, map[string]any{"password": "a<b>&c", "n": 12345678901234567})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(string(secret), `{"$sealed":"v1.`) {
		t.Fatalf("формат: %s", secret)
	}
	doc := []byte(`{"list":[1,{"x":` + string(secret) + `}],"big":12345678901234567890,"s":"<"}`)
	got, err := Unseal(priv, doc)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"big":12345678901234567890,"list":[1,{"x":{"n":12345678901234567,"password":"a<b>&c"}}],"s":"<"}`
	if string(got) != want {
		t.Fatalf("раскрыто:\n%s\nждали:\n%s", got, want)
	}

	// Без запечатанных — документ как есть (байт в байт).
	plain := []byte(`{ "a": 1, "$sealedNot": 2 }`)
	if got, err := Unseal(priv, plain); err != nil || !bytes.Equal(got, plain) {
		t.Fatalf("без секретов: %s %v", got, err)
	}
	// Объект с $sealed и другими полями — не запечатанное значение.
	mixed := []byte(`{"$sealed":"x","other":1}`)
	if got, err := Unseal(priv, mixed); err != nil || !bytes.Equal(got, mixed) {
		t.Fatalf("смешанный объект: %s %v", got, err)
	}
}

func TestOpenErrors(t *testing.T) {
	priv, _ := GenerateKey()
	other, _ := GenerateKey()
	token, err := SealBytes(other.PublicKey(), []byte(`"x"`))
	if err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{token, "v2.a.b.c", "v1.!!.b.c", "v1", token[:len(token)-4]} {
		if _, err := OpenBytes(priv, bad); !errors.Is(err, ErrOpen) {
			t.Errorf("%q: %v", bad, err)
		}
	}
	doc := []byte(`{"a":[{"$sealed":"` + token + `"}]}`)
	_, err = Unseal(priv, doc)
	if !errors.Is(err, ErrOpen) || !strings.Contains(err.Error(), "$.a[0]") {
		t.Fatalf("путь в ошибке: %v", err)
	}
	if _, err := Seal("не ключ", 1); err == nil {
		t.Fatal("Seal с неверным ключом")
	}
}
