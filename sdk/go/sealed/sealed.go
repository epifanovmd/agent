// Package sealed — запечатанные значения в снимках состояния: сервер
// шифрует секрет открытым ключом агента (hello.agent.encryptionKey), агент
// раскрывает его своим закрытым ключом только в памяти, перед передачей
// воркеру. На диске агента и в БД сервера значение остаётся запечатанным.
//
// Формат: объект {"$sealed": "v1.<eph>.<nonce>.<ct>"} на месте любого
// значения JSON; части — base64url без паддинга: eph — одноразовый открытый
// ключ X25519, ключ шифра — HKDF-SHA256(ECDH(eph, ключ агента), salt пустая,
// info "agent sealed v1"), шифр AES-256-GCM (nonce 12 байт, AAD пустой), ct —
// шифротекст JSON-значения с тегом. Ключ агента в hello — base64 (стандартный,
// с паддингом), 32 байта.
package sealed

import (
	"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/ecdh"
	"crypto/hkdf"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
)

// Field — имя единственного поля запечатанного объекта.
const Field = "$sealed"

// version — префикс запечатанной строки.
const version = "v1"

// info — параметр HKDF.
const info = "agent sealed v1"

// ErrOpen — значение не раскрывается: чужой ключ, повреждено или незнакомый формат.
var ErrOpen = errors.New("sealed: значение не расшифровано")

var b64 = base64.RawURLEncoding

// GenerateKey — новая пара ключей X25519 агента.
func GenerateKey() (*ecdh.PrivateKey, error) { return ecdh.X25519().GenerateKey(rand.Reader) }

// EncodeKey — ключ (открытый или закрытый, 32 байта) в base64 для hello и файлов.
func EncodeKey(key []byte) string { return base64.StdEncoding.EncodeToString(key) }

// ParsePublicKey — открытый ключ X25519 из base64 (hello.agent.encryptionKey).
func ParsePublicKey(encoded string) (*ecdh.PublicKey, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil || len(raw) != 32 {
		return nil, fmt.Errorf("sealed: ключ агента — base64 32 байт: %q", encoded)
	}
	return ecdh.X25519().NewPublicKey(raw)
}

// ParsePrivateKey — закрытый ключ X25519 из base64.
func ParsePrivateKey(encoded string) (*ecdh.PrivateKey, error) {
	raw, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
	if err != nil || len(raw) != 32 {
		return nil, errors.New("sealed: закрытый ключ — base64 32 байт")
	}
	return ecdh.X25519().NewPrivateKey(raw)
}

// Seal — запечатать value (любое значение, сериализуемое в JSON) ключом
// агента encryptionKey (base64 из hello.agent.encryptionKey). Итог —
// {"$sealed": "v1.…"}, его можно класть в снимок состояния на место value.
func Seal(encryptionKey string, value any) (json.RawMessage, error) {
	pub, err := ParsePublicKey(encryptionKey)
	if err != nil {
		return nil, err
	}
	plain, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("sealed: значение не сериализуется: %w", err)
	}
	token, err := SealBytes(pub, plain)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]string{Field: token})
}

// SealBytes — запечатанная строка "v1.<eph>.<nonce>.<ct>" для plaintext
// (JSON-значения).
func SealBytes(pub *ecdh.PublicKey, plaintext []byte) (string, error) {
	eph, err := GenerateKey()
	if err != nil {
		return "", err
	}
	aead, err := aeadFor(eph, pub)
	if err != nil {
		return "", err
	}
	nonce := make([]byte, aead.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return "", err
	}
	ct := aead.Seal(nil, nonce, plaintext, nil)
	return strings.Join([]string{version, b64.EncodeToString(eph.PublicKey().Bytes()),
		b64.EncodeToString(nonce), b64.EncodeToString(ct)}, "."), nil
}

// OpenBytes — раскрыть запечатанную строку закрытым ключом агента; ошибка — ErrOpen.
func OpenBytes(priv *ecdh.PrivateKey, token string) ([]byte, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 4 || parts[0] != version {
		return nil, fmt.Errorf("%w: незнакомый формат", ErrOpen)
	}
	ephRaw, err1 := b64.DecodeString(parts[1])
	nonce, err2 := b64.DecodeString(parts[2])
	ct, err3 := b64.DecodeString(parts[3])
	if err := errors.Join(err1, err2, err3); err != nil {
		return nil, fmt.Errorf("%w: повреждено", ErrOpen)
	}
	eph, err := ecdh.X25519().NewPublicKey(ephRaw)
	if err != nil {
		return nil, fmt.Errorf("%w: повреждено", ErrOpen)
	}
	aead, err := aeadFor(priv, eph)
	if err != nil || len(nonce) != aead.NonceSize() {
		return nil, fmt.Errorf("%w: повреждено", ErrOpen)
	}
	plain, err := aead.Open(nil, nonce, ct, nil)
	if err != nil {
		return nil, fmt.Errorf("%w: чужой ключ или повреждено", ErrOpen)
	}
	return plain, nil
}

// aeadFor — AES-256-GCM с ключом HKDF(ECDH(priv, pub)).
func aeadFor(priv *ecdh.PrivateKey, pub *ecdh.PublicKey) (cipher.AEAD, error) {
	shared, err := priv.ECDH(pub)
	if err != nil {
		return nil, err
	}
	key, err := hkdf.Key(sha256.New, shared, nil, info, 32)
	if err != nil {
		return nil, err
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	return cipher.NewGCM(block)
}

// Contains — в документе может быть запечатанное значение (быстрая
// проверка по тексту, без разбора).
func Contains(doc []byte) bool { return bytes.Contains(doc, []byte(`"`+Field+`"`)) }

// Unseal — документ JSON, в котором каждое запечатанное значение (в любом
// месте, рекурсивно) заменено раскрытым. Без запечатанных — doc как есть.
// Не раскрылось хотя бы одно — ошибка ErrOpen (с путём до значения).
func Unseal(priv *ecdh.PrivateKey, doc json.RawMessage) (json.RawMessage, error) {
	if !Contains(doc) {
		return doc, nil
	}
	v, err := decode(doc)
	if err != nil {
		return nil, fmt.Errorf("sealed: документ не JSON: %w", err)
	}
	found := false
	v, err = walk(priv, v, "", &found)
	if err != nil {
		return nil, err
	}
	if !found {
		return doc, nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return json.RawMessage(bytes.TrimRight(buf.Bytes(), "\n")), nil
}

func decode(raw []byte) (any, error) {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber() // числа без потери точности
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	return v, nil
}

// token — строка запечатанного объекта {"$sealed": "…"} (ровно одно поле).
func token(v any) (string, bool) {
	obj, ok := v.(map[string]any)
	if !ok || len(obj) != 1 {
		return "", false
	}
	s, ok := obj[Field].(string)
	return s, ok
}

func walk(priv *ecdh.PrivateKey, v any, path string, found *bool) (any, error) {
	if t, ok := token(v); ok {
		*found = true
		plain, err := OpenBytes(priv, t)
		if err != nil {
			return nil, fmt.Errorf("%w (%s)", err, pathOrRoot(path))
		}
		out, err := decode(plain)
		if err != nil {
			return nil, fmt.Errorf("%w: внутри не JSON (%s)", ErrOpen, pathOrRoot(path))
		}
		return out, nil
	}
	switch x := v.(type) {
	case map[string]any:
		for k, item := range x {
			next, err := walk(priv, item, path+"."+k, found)
			if err != nil {
				return nil, err
			}
			x[k] = next
		}
	case []any:
		for i, item := range x {
			next, err := walk(priv, item, fmt.Sprintf("%s[%d]", path, i), found)
			if err != nil {
				return nil, err
			}
			x[i] = next
		}
	}
	return v, nil
}

func pathOrRoot(path string) string {
	if path == "" {
		return "$"
	}
	return "$" + path
}
