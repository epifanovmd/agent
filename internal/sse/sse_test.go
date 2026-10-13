package sse

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// Записанное Writer читается Reader: события по порядку, комментарии отдельно.
func TestRoundTrip(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s, err := NewWriter(w)
		if err != nil {
			t.Error(err)
			return
		}
		_ = s.Send(Event{ID: "18", Type: "desired", Data: `{"rev":18}`})
		_ = s.Comment("ping")
		_ = s.Send(Event{Type: "command", Data: "a\nb"})
	}))
	defer srv.Close()
	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.Header.Get("Content-Type") != ContentType {
		t.Fatalf("тип: %q", resp.Header.Get("Content-Type"))
	}
	var comments []string
	r := NewReader(resp.Body)
	r.OnComment = func(text string) { comments = append(comments, text) }
	e1, err := r.Next()
	if err != nil || e1 != (Event{ID: "18", Type: "desired", Data: `{"rev":18}`}) {
		t.Fatalf("первое: %+v %v", e1, err)
	}
	e2, err := r.Next()
	if err != nil || e2.Type != "command" || e2.Data != "a\nb" {
		t.Fatalf("второе: %+v %v", e2, err)
	}
	if _, err := r.Next(); err != io.EOF {
		t.Fatalf("конец: %v", err)
	}
	if strings.Join(comments, ",") != "ok,ping" {
		t.Fatalf("комментарии: %v", comments)
	}
}

// Оборванное событие — ошибка, а не неполное событие.
func TestTruncated(t *testing.T) {
	r := NewReader(strings.NewReader("event: command\ndata: {\"id\""))
	if _, err := r.Next(); err != io.ErrUnexpectedEOF {
		t.Fatalf("ошибка: %v", err)
	}
}
