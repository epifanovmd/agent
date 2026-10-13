// Package sse — поток Server-Sent Events (text/event-stream): запись событий и комментариев на
// стороне сервера, чтение событий на стороне клиента.
package sse

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
)

// ContentType — тип ответа потока.
const ContentType = "text/event-stream"

// Event — событие потока: ID — поле id, Type — event, Data — data (строки через \n).
type Event struct {
	ID   string
	Type string
	Data string
}

// Writer — запись в ответ потока; безопасен для вызова из разных горутин.
type Writer struct {
	mu sync.Mutex
	w  http.ResponseWriter
	rc *http.ResponseController
}

// NewWriter — заголовки потока и первый комментарий («ok»).
func NewWriter(w http.ResponseWriter) (*Writer, error) {
	w.Header().Set("Content-Type", ContentType)
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("X-Accel-Buffering", "no")
	w.WriteHeader(http.StatusOK)
	s := &Writer{w: w, rc: http.NewResponseController(w)}
	if err := s.Comment("ok"); err != nil {
		return nil, err
	}
	return s, nil
}

// Send — событие; data — одна строка (JSON без переводов строк) или несколько.
func (s *Writer) Send(e Event) error {
	var b strings.Builder
	if e.ID != "" {
		fmt.Fprintf(&b, "id: %s\n", oneLine(e.ID))
	}
	if e.Type != "" {
		fmt.Fprintf(&b, "event: %s\n", oneLine(e.Type))
	}
	for _, line := range strings.Split(e.Data, "\n") {
		fmt.Fprintf(&b, "data: %s\n", line)
	}
	b.WriteString("\n")
	return s.write(b.String())
}

// Comment — комментарий (ping): клиент его пропускает.
func (s *Writer) Comment(text string) error { return s.write(": " + oneLine(text) + "\n\n") }

func (s *Writer) write(text string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := io.WriteString(s.w, text); err != nil {
		return err
	}
	return s.rc.Flush()
}

func oneLine(s string) string { return strings.NewReplacer("\r", " ", "\n", " ").Replace(s) }

// Reader — чтение событий потока.
type Reader struct {
	r *bufio.Reader
	// OnComment — вызывается на каждый комментарий (признак жизни потока); может быть nil.
	OnComment func(text string)
}

// NewReader — чтение из r.
func NewReader(r io.Reader) *Reader { return &Reader{r: bufio.NewReaderSize(r, 64<<10)} }

// ErrLineTooLong — строка потока длиннее предела.
var ErrLineTooLong = errors.New("sse: строка потока слишком длинная")

// maxLine — предел длины строки потока (data события целиком в одной строке).
const maxLine = 32 << 20

// Next — следующее событие; конец потока — io.EOF.
func (r *Reader) Next() (Event, error) {
	var e Event
	var data []string
	seen := false
	for {
		line, err := r.readLine()
		if err != nil {
			if errors.Is(err, io.EOF) && seen {
				return Event{}, io.ErrUnexpectedEOF
			}
			return Event{}, err
		}
		if line == "" {
			if !seen {
				continue
			}
			e.Data = strings.Join(data, "\n")
			return e, nil
		}
		if strings.HasPrefix(line, ":") {
			if r.OnComment != nil {
				r.OnComment(strings.TrimSpace(line[1:]))
			}
			continue
		}
		field, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		seen = true
		switch field {
		case "id":
			e.ID = value
		case "event":
			e.Type = value
		case "data":
			data = append(data, value)
		}
	}
}

func (r *Reader) readLine() (string, error) {
	var b strings.Builder
	for {
		part, isPrefix, err := r.r.ReadLine()
		if err != nil {
			return "", err
		}
		b.Write(part)
		if b.Len() > maxLine {
			return "", ErrLineTooLong
		}
		if !isPrefix {
			return strings.TrimSuffix(b.String(), "\r"), nil
		}
	}
}
