package worker

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// worker.ping (A→W) → worker.pong (W→A) по образцам; worker.register — с ping.
func TestPingPongMatchesExamples(t *testing.T) {
	w, a := newWorker(t, "report")
	w.Channel("example.app")
	done := make(chan error, 1)
	go func() { done <- w.Run(context.Background()) }()
	t.Cleanup(func() { a.conn.Close(); <-done })
	reg := a.expect(message.TypeWorkerRegister, nil)
	var r map[string]any
	_ = json.Unmarshal(reg.Data, &r)
	if r["ping"] != true {
		t.Fatalf("worker.register без ping: %s", reg.Data)
	}
	ping := example(t, "worker.ping")
	a.send(ping["type"].(string), ping["data"], ping["id"].(string))
	env := a.expect(message.TypeWorkerPong, nil)
	pong := example(t, "worker.pong")
	var got map[string]any
	_ = json.Unmarshal(env.Data, &got)
	if env.Type != pong["type"] || env.Re != pong["re"] || !reflect.DeepEqual(got, pong["data"]) {
		t.Fatalf("worker.pong: re=%s %s", env.Re, env.Data)
	}
}

// Повторная доставка job.assign: та же попытка — без последствий; новая
// попытка той же задачи — прежняя отменяется (job.fail CANCELLED), новая
// выполняется.
func TestJobRedelivery(t *testing.T) {
	w, a := newWorker(t, "jobs")
	var runs atomic.Int32
	cancelled := make(chan int, 4)
	w.Job("example.q", 2, func(ctx context.Context, job *Job) (any, error) {
		runs.Add(1)
		if job.Attempt == 0 {
			<-ctx.Done()
			cancelled <- job.Attempt
			return nil, ctx.Err()
		}
		return map[string]int{"attempt": job.Attempt}, nil
	})
	start(t, w, a)
	a.send(message.TypeJobAssign, assign("j", 0), "")
	time.Sleep(50 * time.Millisecond)
	a.send(message.TypeJobAssign, assign("j", 0), "") // та же попытка
	a.quiet(150 * time.Millisecond)
	if runs.Load() != 1 {
		t.Fatalf("повтор той же попытки запустил обработчик: %d", runs.Load())
	}
	a.send(message.TypeJobAssign, assign("j", 1), "")
	select {
	case at := <-cancelled:
		if at != 0 {
			t.Fatalf("отменена попытка %d", at)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("прежняя попытка не отменена")
	}
	// Прежняя попытка — job.fail CANCELLED, новая — job.complete (в любом порядке).
	var gotCancel, gotComplete bool
	for !gotCancel || !gotComplete {
		env := a.next()
		switch env.Type {
		case message.TypeJobFail:
			var f message.JobFail
			_ = env.Decode(&f)
			if gotCancel || f.Attempt != 0 || f.Code != message.ErrCancelled {
				t.Fatalf("job.fail: %+v", f)
			}
			gotCancel = true
		case message.TypeJobComplete:
			var c message.JobComplete
			_ = env.Decode(&c)
			if gotComplete || c.Attempt != 1 || string(c.Result) != `{"attempt":1}` {
				t.Fatalf("job.complete: %+v", c)
			}
			gotComplete = true
		default:
			t.Fatalf("лишнее сообщение %s: %s", env.Type, env.Data)
		}
	}
	a.quiet(150 * time.Millisecond)
}

// Итог больше 16 МБ: задача — job.fail RESULT_TOO_LARGE, команда — ошибка с
// тем же кодом, состояние — ok: false; большое событие отбрасывается, канал цел.
func TestResultTooLarge(t *testing.T) {
	big := strings.Repeat("x", maxLine)
	w, a := newWorker(t, "big")
	w.Job("example.q", 1, func(context.Context, *Job) (any, error) { return big, nil })
	w.Command("example.big", func(context.Context, *Command) (any, error) { return big, nil })
	w.State("example.big", func(context.Context, int64, json.RawMessage) (any, error) { return big, nil })
	w.Channel("example.app")
	start(t, w, a)

	a.send(message.TypeJobAssign, assign("j", 3), "")
	var f message.JobFail
	a.expect(message.TypeJobFail, &f)
	if f.JobID != "j" || f.Attempt != 3 || f.Code != CodeResultTooLarge || f.Retryable {
		t.Fatalf("job.fail: %+v", f)
	}

	a.send(message.TypeCmdRun, message.CommandRun{CommandID: "c", Name: "example.big"}, "")
	var done message.CommandDone
	a.expect(message.TypeCmdDone, &done)
	if done.OK || done.Error == nil || done.Error.Code != CodeResultTooLarge || done.Result != nil {
		t.Fatalf("cmd.done: %+v", done)
	}

	a.send(message.TypeStatePut, message.StatePut{Domain: "example.big", Version: 1, Spec: json.RawMessage(`{}`)}, "p")
	var applied message.StateApplied
	env := a.expect(message.TypeStateApplied, &applied)
	if env.Re != "p" || applied.OK || !strings.HasPrefix(applied.Error, CodeResultTooLarge) {
		t.Fatalf("state.applied: %s", env.Data)
	}

	if err := w.Event("example.big", big); err == nil {
		t.Fatal("большое событие: ждали ошибку")
	}
	if err := w.Report("example.app", big); err == nil {
		t.Fatal("большая телеметрия: ждали ошибку")
	}
	if err := w.Event("example.small", 1); err != nil {
		t.Fatal(err)
	}
	var e message.Event
	a.expect(message.TypeEvent, &e)
	if e.Type != "example.small" {
		t.Fatalf("после большого — %+v", e)
	}
}

// Канал, отклонённый агентом, не опрашивается; nil от источника — без точки.
func TestTelemetryRejectedAndNil(t *testing.T) {
	w, a := newWorker(t, "tele")
	var rejectedCalls, nilCalls atomic.Int32
	w.Telemetry("example.taken", 20*time.Millisecond, func() any { rejectedCalls.Add(1); return 1 })
	w.Telemetry("example.empty", 20*time.Millisecond, func() any {
		nilCalls.Add(1)
		var m map[string]int
		return m
	})
	go func() { _ = w.Run(context.Background()) }()
	t.Cleanup(func() { a.conn.Close() })
	a.expect(message.TypeWorkerRegister, nil)
	a.send(message.TypeWorkerReady, message.WorkerReady{AgentVersion: "t", Rejected: []string{"example.taken"}}, "")
	a.quiet(200 * time.Millisecond)
	if rejectedCalls.Load() != 0 {
		t.Fatalf("отклонённый канал опрошен %d раз", rejectedCalls.Load())
	}
	if nilCalls.Load() == 0 {
		t.Fatal("источник не опрошен")
	}
}

// Имя входного файла в каталоге задачи — только последняя часть.
func TestBaseName(t *testing.T) {
	for in, want := range map[string]string{
		"source": "source", "a/b/c.txt": "c.txt", "../../etc/passwd": "passwd", `..\..\x`: "x",
	} {
		if got, err := baseName(in); err != nil || got != want {
			t.Errorf("%q: %q %v", in, got, err)
		}
	}
	for _, bad := range []string{"", ".", "..", "a/..", "dir/"} {
		if _, err := baseName(bad); err == nil {
			t.Errorf("%q: ждали ошибку", bad)
		}
	}
}
