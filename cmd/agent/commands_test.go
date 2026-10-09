package main

import (
	"bytes"
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/app"
	"github.com/epifanovmd/agent/internal/message"
)

// agent status: работает — версия, связь, воркеры, последняя ошибка связи;
// не работает — причина и ошибка последнего завершения.
func TestPrintStatus(t *testing.T) {
	now := time.Date(2026, 1, 2, 15, 0, 0, 0, time.Local)
	yes, no := true, false
	var out bytes.Buffer
	printStatus(&out, statusReport{Running: true, DataDir: "/var/lib/agent", Status: &app.AgentStatus{
		PID: 42, Version: "1.0.0", Name: "node-01", AgentID: "ag-1", Online: false, Server: "https://api.example.com",
		StartedAt: now.Add(-3 * time.Hour).UnixMilli(), Outbox: 2,
		Workers: []message.WorkerStatus{
			{Name: "sysmetrics", State: message.WorkerRunning, Builtin: true},
			{Name: "report", State: message.WorkerBackoff, Version: "1.2.0", Release: true, Restarts: 3,
				Health: &message.Health{OK: false, Message: "нет связи"},
				Configs: map[string]message.ConfigStatus{"main": {Version: 4, OK: &yes}, "limits": {Version: 2, OK: &no,
					Error: message.NewError(message.CodeConfigRejected, "x")}}},
		},
		LastError: "dial: connection refused", LastErrorAt: now.Add(-5 * time.Minute).UnixMilli(),
	}}, now)
	text := out.String()
	for _, want := range []string{
		"Агент работает: pid 42, версия 1.0.0, запущен 2026-01-02 12:00:00 (3 ч 0 мин назад)",
		"node-01 (id ag-1)", "Связь:      нет — подключается к https://api.example.com",
		"ждут подтверждения сервера: 2", "sysmetrics     running   встроенный",
		"report         backoff   версия 1.2.0, из выпуска, перезапусков: 3, нездоров нет связи, limits v2 ошибка CONFIG_REJECTED, main v4 применено",
		"Последняя ошибка связи или регистрации 2026-01-02 14:55:00 (5 мин назад):\n  dial: connection refused",
	} {
		if !strings.Contains(text, want) {
			t.Errorf("нет %q:\n%s", want, text)
		}
	}

	out.Reset()
	printStatus(&out, statusReport{DataDir: "/var/lib/agent", Error: "агент с этим dataDir не запущен",
		LastExit: &app.ExitError{At: now.Add(-time.Minute).UnixMilli(), Error: "токен отклонён\n<html>"}}, now)
	if text := out.String(); !strings.Contains(text, "Агент не работает: агент с этим dataDir не запущен.") ||
		!strings.Contains(text, "Последний раз завершился с ошибкой 2026-01-02 14:59:00 (1 мин назад):\n  токен отклонён\n") ||
		strings.Contains(text, "<html>") {
		t.Fatalf("не работает:\n%s", text)
	}
}

// --instance: файл настроек экземпляра; вместе с -config или с плохим именем — ошибка.
func TestInstanceFlag(t *testing.T) {
	parse := func(args ...string) (string, error) {
		fs := flag.NewFlagSet("t", flag.ContinueOnError)
		tg := configFlag(fs)
		if err := fs.Parse(args); err != nil {
			return "", err
		}
		return tg.path()
	}
	if p, err := parse("--instance", "web"); err != nil || p != "/etc/agent-web/agent.yaml" {
		t.Fatalf("%q %v", p, err)
	}
	if p, err := parse("-config", "/srv/a.yaml"); err != nil || p != "/srv/a.yaml" {
		t.Fatalf("%q %v", p, err)
	}
	for _, args := range [][]string{{"--instance", "Web"}, {"--instance", "web", "-config", "/srv/a.yaml"}} {
		if _, err := parse(args...); err == nil {
			t.Errorf("%v: нет ошибки", args)
		}
	}
}
