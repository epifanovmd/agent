package releases

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"os"
)

// SigningKeyEnv — переменная с закрытым ключом подписи сборок (seed Ed25519, base64).
const SigningKeyEnv = "AGENT_SIGNING_KEY"

// SigningKey — ключ подписи из AGENT_SIGNING_KEY (nil — не задан).
func SigningKey() (ed25519.PrivateKey, error) {
	seed := os.Getenv(SigningKeyEnv)
	if seed == "" {
		return nil, nil
	}
	raw, err := base64.StdEncoding.DecodeString(seed)
	if err != nil || len(raw) != ed25519.SeedSize {
		return nil, errors.New(SigningKeyEnv + " — base64 seed Ed25519 (agent keygen)")
	}
	return ed25519.NewKeyFromSeed(raw), nil
}

// NewKeyPair — новая пара ключей подписи: закрытый (seed) и открытый, base64.
func NewKeyPair() (private, public string, err error) {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return "", "", err
	}
	return base64.StdEncoding.EncodeToString(priv.Seed()), base64.StdEncoding.EncodeToString(pub), nil
}

// PublicKeyOf — открытый ключ закрытого priv, base64.
func PublicKeyOf(priv ed25519.PrivateKey) string {
	return base64.StdEncoding.EncodeToString(priv.Public().(ed25519.PublicKey))
}
