//go:build unix

package integration

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/epifanovmd/agent/test/testserver"
)

// Задачи воркера (§12): события job.* агент принимает, только если манифест объявляет jobs;
// runJob сервера — быстрая задача (итог сразу) и долгая (итог по событиям job.*).
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

	var quick testserver.JobResult
	if err := s.server.Action("runJob", a.ID, &quick, "jobs", map[string]any{"type": "example.quick", "data": map[string]int{"n": 1}}); err != nil {
		t.Fatal(err)
	}
	if quick.State != "done" || quick.ID != "" || string(quick.Result) != `{"echo":{"n":1}}` {
		t.Fatalf("быстрая задача: %+v %s", quick, quick.Result)
	}

	var long testserver.JobResult
	if err := s.server.Action("runJob", a.ID, &long, "jobs", map[string]any{"type": "example.long", "jobId": "j-1", "timeoutMs": 10_000}); err != nil {
		t.Fatal(err)
	}
	var result struct{ PID int }
	if long.JobID != "j-1" || long.ID != "w-j-1" || long.State != "done" || json.Unmarshal(long.Result, &result) != nil || result.PID == 0 {
		t.Fatalf("долгая задача: %+v %s", long, long.Result)
	}
	if err := s.server.Action("runJob", a.ID, nil, "jobs", map[string]any{"type": "example.none"}); errorCode(err) != "JOB_UNKNOWN" {
		t.Fatalf("тип не из манифеста: %v", err)
	}
	if err := s.server.Action("runJob", a.ID, nil, "plain", map[string]any{"type": "example.quick"}); errorCode(err) != "JOB_UNKNOWN" {
		t.Fatalf("воркер без jobs: %v", err)
	}
}
