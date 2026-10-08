package server

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Files — провайдер файлов задач: бэкенд отдаёт подписанные ссылки своего
// хранилища (S3 и т. п.). Если провайдер — ещё и http.Handler, Agents
// обслуживает им /files/.
type Files interface {
	// Inputs — входные файлы запроса задачи (имя → содержимое или URL, по
	// провайдеру); вызывается при постановке.
	Inputs(job *Job, inputs map[string]string) error
	// URLs — ссылки на файлы задачи: inputs — имя → url, outputs — имя →
	// {url, contentType}, срок; baseURL — адрес сервера, по которому агент до
	// него дошёл.
	URLs(job *Job, baseURL string) (message.JobURLs, error)
}

// FilesPrefix — путь файлов MemoryFiles: /files/<jobId>/(in|out)/<имя>.
const FilesPrefix = "/files/"

// MemoryFiles — файлы задач в памяти (по умолчанию): inputs запроса задачи —
// содержимое; GET/PUT /files/<jobId>/(in|out)/<имя> обслуживает транспорт Agents.
// Ссылки без подписи — для разработки.
type MemoryFiles struct {
	// TTL — срок ссылок (по умолчанию час).
	TTL time.Duration

	mu    sync.Mutex
	files map[string][]byte
}

// NewMemoryFiles — файлы в памяти.
func NewMemoryFiles() *MemoryFiles {
	return &MemoryFiles{TTL: time.Hour, files: map[string][]byte{}}
}

func (m *MemoryFiles) Inputs(job *Job, inputs map[string]string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for name, content := range inputs {
		m.files[job.ID+"/in/"+name] = []byte(content)
	}
	return nil
}

func (m *MemoryFiles) URLs(job *Job, baseURL string) (message.JobURLs, error) {
	base := strings.TrimRight(baseURL, "/") + FilesPrefix + url.PathEscape(job.ID)
	out := message.JobURLs{
		Inputs:    map[string]string{},
		Outputs:   map[string]message.OutputURL{},
		ExpiresAt: time.Now().Add(m.ttl()).UnixMilli(),
	}
	for _, name := range job.Inputs {
		out.Inputs[name] = base + "/in/" + url.PathEscape(name)
	}
	for _, name := range job.Outputs {
		out.Outputs[name] = message.OutputURL{URL: base + "/out/" + url.PathEscape(name), ContentType: "application/octet-stream"}
	}
	return out, nil
}

func (m *MemoryFiles) ttl() time.Duration {
	if m.TTL > 0 {
		return m.TTL
	}
	return time.Hour
}

// Get — файл по ключу <jobId>/(in|out)/<имя>.
func (m *MemoryFiles) Get(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[key]
	return data, ok
}

// Put — сохранить файл по ключу.
func (m *MemoryFiles) Put(key string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[key] = data
}

// ServeHTTP — GET и PUT /files/<ключ>.
func (m *MemoryFiles) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key, ok := strings.CutPrefix(r.URL.Path, FilesPrefix)
	if !ok || key == "" {
		http.NotFound(w, r)
		return
	}
	switch r.Method {
	case http.MethodGet:
		data, ok := m.Get(key)
		if !ok {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(data)
	case http.MethodPut:
		data, err := io.ReadAll(io.LimitReader(r.Body, 1<<30))
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		m.Put(key, data)
		w.WriteHeader(http.StatusOK)
	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

var _ Files = (*MemoryFiles)(nil)
