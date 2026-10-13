//go:build unix

package integration

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/epifanovmd/agent/test/testserver"
)

// Задачи воркера (§12): события job.* агент принимает, только если манифест объявляет jobs;
// задача — fetch POST /jobs: быстрая — итог в ответе, долгая — 202 и итог событиями job.*.
func TestJobs(t *testing.T) {
	t.Parallel()
	s := newStand(t)
	cfg := s.config(s.worker("plain"), s.worker("jobs"))
	if err := os.WriteFile(filepath.Join(s.stateDir("jobs"), "jobs"), nil, 0o644); err != nil {
		t.Fatal(err)
	}
	n := s.start(cfg)
	s.running("plain")
	a := s.running("jobs")

	// Без jobs в манифесте события задач не принимаются; с jobs — только стандартные.
	if _, codes := n.direct("plain", "POST", "/emit?type=job.done&n=1"); codes != "400" {
		t.Fatalf("job.done без jobs: %s", codes)
	}
	if _, codes := n.direct("jobs", "POST", "/emit?type=job.other&n=1"); codes != "400" {
		t.Fatalf("job.other: %s", codes)
	}
	n.emit("jobs", "job.progress", 0, 1)

	job := func(body string) testserver.FetchResponse {
		return s.fetch(a.ID, "jobs", "/jobs", testserver.FetchInit{Method: "POST",
			Headers: map[string]string{"Content-Type": "application/json"}, Body: body})
	}
	if res := job(`{"type":"example.quick","data":{"n":1}}`); res.Status != 200 || res.Body != `{"result":{"echo":{"n":1}}}` {
		t.Fatalf("быстрая задача: %+v", res)
	}
	if res := job(`{"type":"example.long","jobId":"j-1"}`); res.Status != 202 || !strings.Contains(res.Body, `"w-j-1"`) {
		t.Fatalf("долгая задача: %+v", res)
	}
	var result struct {
		JobID  string
		Result struct{ PID int }
	}
	eventually(t, "итог долгой задачи — событие job.done", func() bool {
		b, _ := s.server.Agent(agentName)
		for _, e := range b.Events {
			if e.Type == "job.done" && json.Unmarshal(e.Data, &result) == nil && result.JobID == "j-1" {
				return result.Result.PID != 0
			}
		}
		return false
	})

	// Агент сам сверяет задачи с манифестом (§7): тип не из jobs — JOB_UNKNOWN, задачи у
	// воркера без jobs — ROUTE_UNDECLARED; до воркера такие запросы не доходят.
	post := testserver.FetchInit{Method: "POST", Headers: map[string]string{"Content-Type": "application/json"},
		Body: `{"type":"example.none","jobId":"j-2"}`}
	if _, err := s.server.Fetch(a.ID, "jobs", "/jobs", post); errorCode(err) != "JOB_UNKNOWN" {
		t.Fatalf("fetch POST /jobs с типом не из манифеста: %v", err)
	}
	if _, err := s.server.Fetch(a.ID, "plain", "/jobs/w-j-1", testserver.FetchInit{}); errorCode(err) != "ROUTE_UNDECLARED" {
		t.Fatalf("GET /jobs/{id} у воркера без jobs: %v", err)
	}
	if res := s.fetch(a.ID, "jobs", "/jobs/w-j-1/cancel", testserver.FetchInit{Method: "POST"}); res.Status != 404 {
		t.Fatalf("POST /jobs/{id}/cancel доходит до воркера: %+v", res)
	}
}
