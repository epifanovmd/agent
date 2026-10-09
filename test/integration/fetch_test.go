//go:build unix

package integration

import (
	"bytes"
	"encoding/base64"
	"slices"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/test/testserver"
)

// Запрос к воркеру через агента: обычный (метод, заголовки, путь с query, статус), потоковый
// (куски приходят по мере записи), большой двоичный ответ, двоичное тело запроса, таймаут,
// отмена (запрос к воркеру прерывается), воркер неизвестен, воркер не запущен, служебный путь.
func TestFetch(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	down := config.Worker{Name: "down", Command: []string{"/bin/false"}} // падает сразу: не запущен
	s.start(s.config(s.worker("w"), down))
	a := s.running("w")

	res := s.fetch(a.ID, "w", "/echo?x=1", testserver.FetchInit{
		Method: "PUT", Headers: map[string]string{"Content-Type": "application/json"}, Body: `{"a":1}`,
	})
	if res.Status != 201 || res.Body != `{"a":1}` || res.Headers["x-method"] != "PUT" || res.Headers["x-query"] != "x=1" ||
		res.Headers["content-type"] != "application/json" {
		t.Fatalf("обычный запрос: %+v", res)
	}

	stream := s.fetch(a.ID, "w", "/stream", testserver.FetchInit{})
	if stream.Body != "первая часть\nвторая часть\n" || stream.Chunks < 2 || stream.EndAt-stream.FirstChunkAt < 800 {
		t.Fatalf("потоковый ответ: куски %d, первый за %d мс до конца, тело %q",
			stream.Chunks, stream.EndAt-stream.FirstChunkAt, stream.Body)
	}

	big := s.fetch(a.ID, "w", "/big", testserver.FetchInit{})
	raw, _ := base64.StdEncoding.DecodeString(big.BodyBase64)
	if !bytes.Equal(raw, bigBody()) || big.Chunks < 5 {
		t.Fatalf("двоичный ответ: %d байт, кусков %d", len(raw), big.Chunks)
	}

	bin := make([]byte, 256)
	for i := range bin {
		bin[i] = byte(255 - i)
	}
	echo := s.fetch(a.ID, "w", "/echo", testserver.FetchInit{
		Method: "POST", Headers: map[string]string{"Content-Type": "application/octet-stream"},
		BodyBase64: base64.StdEncoding.EncodeToString(bin),
	})
	if got, _ := base64.StdEncoding.DecodeString(echo.BodyBase64); !bytes.Equal(got, bin) {
		t.Fatalf("двоичное тело запроса: %x", got)
	}

	start := time.Now()
	_, err := s.server.Fetch(a.ID, "w", "/sleep?id=timeout", testserver.FetchInit{TimeoutMs: 500})
	if code := errorCode(err); code != "TIMEOUT" || time.Since(start) > 5*time.Second {
		t.Fatalf("таймаут: %q за %s", code, time.Since(start))
	}
	_, err = s.server.Fetch(a.ID, "w", "/sleep?id=cancel", testserver.FetchInit{AbortAfterMs: 300})
	if code := errorCode(err); code != "CANCELLED" {
		t.Fatalf("отмена: %q", code)
	}
	eventually(t, "воркер увидел отмену и истечение срока", func() bool {
		got := s.lines("w", "cancelled.log")
		return slices.Contains(got, "timeout") && slices.Contains(got, "cancel")
	})

	for path, want := range map[string]string{"/metrics": "PATH_FORBIDDEN", "/config/main": "PATH_FORBIDDEN"} {
		if _, err := s.server.Fetch(a.ID, "w", path, testserver.FetchInit{}); errorCode(err) != want {
			t.Fatalf("%s: %v, нужен %s", path, err, want)
		}
	}
	if _, err := s.server.Fetch(a.ID, "nope", "/", testserver.FetchInit{}); errorCode(err) != "WORKER_UNKNOWN" {
		t.Fatalf("неизвестный воркер: %v", err)
	}
	if _, err := s.server.Fetch(a.ID, "down", "/", testserver.FetchInit{}); errorCode(err) != "WORKER_UNAVAILABLE" {
		t.Fatalf("воркер не запущен: %v", err)
	}
}
