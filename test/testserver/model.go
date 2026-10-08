// Package testserver — сервер для тестов агента: Agents из sdk/go/server в памяти
// (MemoryStore + MemoryFiles) и методы, которыми тесты ставят задачи, шлют
// команды, меняют состояние и читают итоги. Своей логики связи с агентами нет.
package testserver

import (
	"github.com/epifanovmd/agent/sdk/go/server"
)

// Статусы задачи.
const (
	JobQueued    = server.JobQueued
	JobRunning   = server.JobRunning
	JobCompleted = server.JobCompleted
	JobFailed    = server.JobFailed
	JobCancelled = server.JobCancelled
)

// Статусы команды.
const (
	CommandPending   = server.CommandPending
	CommandRunning   = server.CommandRunning
	CommandSucceeded = server.CommandSucceeded
	CommandFailed    = server.CommandFailed
)

// Модель — из SDK.
type (
	Job            = server.Job
	JobEvent       = server.JobEvent
	JobError       = server.JobError
	Command        = server.Command
	DesiredState   = server.DesiredState
	AgentEvent     = server.AgentEvent
	MetricsPoint   = server.MetricsPoint
	EnqueueRequest = server.JobRequest
	CommandRequest = server.CommandRequest
)

// Agent — снимок агента с его событиями (события агента и воркеров).
type Agent struct {
	server.Agent
	Events []server.AgentEvent `json:"events,omitempty"`
}
