// Package identity — ключ агента: регистрация по токену, хранение
// `agentId.secret` в файле с правами 0600 и смена секрета (agent.rotateKey)
// через ожидающий секрет.
package identity

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

const fileName = "credentials.json"

// Credentials — учётные данные агента. PendingSecret — новый секрет после
// agent.rotateKey, ещё не признанный сервером (§10).
type Credentials struct {
	AgentID       string `json:"agentId"`
	Secret        string `json:"secret"`
	PendingSecret string `json:"pendingSecret,omitempty"`
}

// Authorization — значение заголовка Authorization (основной секрет).
func (c Credentials) Authorization() string { return header(c.AgentID, c.Secret) }

func header(agentID, secret string) string { return "Agent " + agentID + "." + secret }

// NewSecret — случайный секрет в формате сервера (24 байта, hex).
func NewSecret() (string, error) {
	var raw [24]byte
	if _, err := rand.Read(raw[:]); err != nil {
		return "", fmt.Errorf("identity: %w", err)
	}
	return hex.EncodeToString(raw[:]), nil
}

// SecretHash — sha256 секрета (hex), как его хранит сервер.
func SecretHash(secret string) string {
	sum := sha256.Sum256([]byte(secret))
	return hex.EncodeToString(sum[:])
}

// Store — файл учётных данных в каталоге данных агента.
type Store struct{ path string }

// NewStore — хранилище в каталоге dir.
func NewStore(dir string) *Store { return &Store{path: filepath.Join(dir, fileName)} }

// Load — сохранённые учётные данные; ok=false — агент ещё не зарегистрирован.
func (s *Store) Load() (Credentials, bool, error) {
	raw, err := os.ReadFile(s.path)
	if errors.Is(err, os.ErrNotExist) {
		return Credentials{}, false, nil
	}
	if err != nil {
		return Credentials{}, false, fmt.Errorf("identity: %w", err)
	}
	var c Credentials
	if err := json.Unmarshal(raw, &c); err != nil || c.AgentID == "" || c.Secret == "" {
		return Credentials{}, false, fmt.Errorf("identity: файл %s повреждён", s.path)
	}
	return c, true, nil
}

// Save — записать атомарно с правами 0600.
func (s *Store) Save(c Credentials) error {
	if err := os.MkdirAll(filepath.Dir(s.path), 0o700); err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	raw, _ := json.Marshal(c)
	tmp := s.path + ".tmp"
	if err := os.WriteFile(tmp, raw, 0o600); err != nil {
		return fmt.Errorf("identity: %w", err)
	}
	return os.Rename(tmp, s.path)
}

// Forget — удалить (учётные данные отозваны).
func (s *Store) Forget() error {
	if err := os.Remove(s.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("identity: %w", err)
	}
	return nil
}

// ErrTokenRejected — токен регистрации не принят: повтор не поможет.
var ErrTokenRejected = errors.New("identity: токен регистрации отклонён")

// RateLimited — сервер просит повторить регистрацию позже (429 с Retry-After).
type RateLimited struct{ After time.Duration }

func (e *RateLimited) Error() string {
	return fmt.Sprintf("identity: регистрация: много попыток, повтор через %s", e.After)
}

// Enroll — обменять токен регистрации на ключ агента (§2).
func Enroll(ctx context.Context, client *http.Client, baseURL string, req message.Enroll) (Credentials, error) {
	body, _ := json.Marshal(req)
	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, strings.TrimRight(baseURL, "/")+message.EnrollPath, bytes.NewReader(body))
	if err != nil {
		return Credentials{}, err
	}
	httpReq.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(httpReq)
	if err != nil {
		return Credentials{}, fmt.Errorf("identity: регистрация: %w", err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	var e message.ErrorInfo
	_ = json.Unmarshal(raw, &e)
	switch {
	case resp.StatusCode == http.StatusOK || resp.StatusCode == http.StatusCreated:
		var r message.EnrollResult
		if err := json.Unmarshal(raw, &r); err != nil || r.AgentID == "" || r.Secret == "" {
			// Тело не выводится: в нём может быть секрет агента.
			return Credentials{}, fmt.Errorf("identity: неожиданный ответ регистрации (HTTP %d, %d байт)", resp.StatusCode, len(raw))
		}
		return Credentials{AgentID: r.AgentID, Secret: r.Secret}, nil
	case resp.StatusCode == http.StatusTooManyRequests:
		after := 30 * time.Second
		if n, err := strconv.Atoi(strings.TrimSpace(resp.Header.Get("Retry-After"))); err == nil && n > 0 {
			after = time.Duration(n) * time.Second
		}
		return Credentials{}, &RateLimited{After: min(after, 10*time.Minute)}
	case resp.StatusCode == http.StatusUnauthorized || resp.StatusCode == http.StatusBadRequest ||
		resp.StatusCode == http.StatusRequestEntityTooLarge:
		return Credentials{}, fmt.Errorf("%w: HTTP %d %s %s", ErrTokenRejected, resp.StatusCode, e.Code, e.Message)
	case e.Code != "":
		return Credentials{}, fmt.Errorf("identity: регистрация: HTTP %d %s %s", resp.StatusCode, e.Code, e.Message)
	default:
		// Тело не выводится: ответ не от сервера агентов (прокси) может повторять запрос с токеном.
		return Credentials{}, fmt.Errorf("identity: регистрация: HTTP %d (%d байт)", resp.StatusCode, len(raw))
	}
}

// Keys — действующие учётные данные агента с ожидающим секретом: подключение
// сначала с ожидающим, отказ — откат на основной; принят ожидающий — он
// становится основным. Безопасен для вызова из любых горутин.
type Keys struct {
	store *Store

	mu    sync.Mutex
	creds Credentials
	// skipPending — ожидающий секрет отклонён в этой серии подключений.
	skipPending bool
	// accepted — заголовок, с которым открыта последняя сессия.
	accepted string
}

// NewKeys — учётные данные creds, изменения сохраняются в store.
func NewKeys(store *Store, creds Credentials) *Keys {
	return &Keys{store: store, creds: creds}
}

// Credentials — текущее содержимое (копия).
func (k *Keys) Credentials() Credentials {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.creds
}

// Authorization — заголовок для следующего подключения: ожидающий секрет,
// если он есть и ещё не отклонён, иначе основной.
func (k *Keys) Authorization() string {
	k.mu.Lock()
	defer k.mu.Unlock()
	return k.authorization()
}

// Session — заголовок, принятый сервером в последней сессии (для запросов
// вне подключения, например загрузки обновления); до первой сессии — как
// Authorization.
func (k *Keys) Session() string {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.accepted != "" {
		return k.accepted
	}
	return k.authorization()
}

func (k *Keys) authorization() string {
	if k.creds.PendingSecret != "" && !k.skipPending {
		return header(k.creds.AgentID, k.creds.PendingSecret)
	}
	return k.creds.Authorization()
}

// Fallback — сервер отклонил текущий заголовок (401/4401). true — есть другой
// секрет (основной вместо ожидающего): переподключиться сразу; false —
// отклонены все, нужна повторная регистрация. После false следующая серия
// подключений снова начинается с ожидающего.
func (k *Keys) Fallback() bool {
	k.mu.Lock()
	defer k.mu.Unlock()
	if k.creds.PendingSecret != "" && !k.skipPending {
		k.skipPending = true
		return true
	}
	k.skipPending = false
	return false
}

// Accepted — сессия открыта с заголовком authorization. Это ожидающий
// секрет — он становится основным, прежний удаляется из файла. Принят
// основной — следующее подключение снова начнётся с ожидающего: сервер мог
// узнать его хеш в этой сессии.
func (k *Keys) Accepted(authorization string) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.skipPending = false
	k.accepted = authorization
	if k.creds.PendingSecret == "" || authorization != header(k.creds.AgentID, k.creds.PendingSecret) {
		return nil
	}
	next := Credentials{AgentID: k.creds.AgentID, Secret: k.creds.PendingSecret}
	if err := k.store.Save(next); err != nil {
		return err
	}
	k.creds = next
	return nil
}

// Rotate — новый ожидающий секрет (прежний ожидающий заменяется), сохранён в
// файл до ответа; возвращает его sha256 — сам секрет не покидает агента.
func (k *Keys) Rotate() (secretHash string, err error) {
	secret, err := NewSecret()
	if err != nil {
		return "", err
	}
	k.mu.Lock()
	defer k.mu.Unlock()
	next := k.creds
	next.PendingSecret = secret
	if err := k.store.Save(next); err != nil {
		return "", err
	}
	k.creds = next
	k.skipPending = false
	return SecretHash(secret), nil
}

// Replace — новые учётные данные (регистрация): сохранить и использовать.
func (k *Keys) Replace(c Credentials) error {
	k.mu.Lock()
	defer k.mu.Unlock()
	if err := k.store.Save(c); err != nil {
		return err
	}
	k.creds = c
	k.skipPending = false
	k.accepted = ""
	return nil
}
