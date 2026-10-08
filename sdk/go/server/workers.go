package server

import (
	"slices"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// workerControlTimeout — срок команд worker.pause и worker.resume.
const workerControlTimeout = 30

// PauseWorker — команда worker.pause {name, queues}: воркер name не получает
// новых задач очередей queues (без queues — всех своих), выданные
// доделываются. Пауза от сервера и от самого воркера (Pause в SDK воркера)
// независимы: ResumeWorker снимает только серверную. Срок 30 с. Нет агента —
// AGENT_NOT_FOUND, отозван — AGENT_REVOKED, имя воркера или очереди не по
// правилу — MESSAGE_INVALID, агент не объявил worker.pause —
// COMMAND_NOT_SUPPORTED.
func (a *Agents) PauseWorker(agentID, name string, queues ...string) (*Command, error) {
	return a.controlWorker("", message.CommandWorkerPause, AuditWorkerPause, agentID, name, queues)
}

// ResumeWorker — команда worker.resume {name, queues}: снять серверную паузу
// (PauseWorker) с очередей queues воркера name (без queues — со всех).
// Ошибки — как у PauseWorker.
func (a *Agents) ResumeWorker(agentID, name string, queues ...string) (*Command, error) {
	return a.controlWorker("", message.CommandWorkerResume, AuditWorkerResume, agentID, name, queues)
}

// PauseWorker — Agents.PauseWorker от имени actor.
func (x *Actor) PauseWorker(agentID, name string, queues ...string) (*Command, error) {
	return x.agents.controlWorker(x.name, message.CommandWorkerPause, AuditWorkerPause, agentID, name, queues)
}

// ResumeWorker — Agents.ResumeWorker от имени actor.
func (x *Actor) ResumeWorker(agentID, name string, queues ...string) (*Command, error) {
	return x.agents.controlWorker(x.name, message.CommandWorkerResume, AuditWorkerResume, agentID, name, queues)
}

func (a *Agents) controlWorker(actor, command, action, agentID, name string, queues []string) (*Command, error) {
	agent, err := a.Agent(agentID)
	if err != nil {
		return nil, protoErr("AGENT_NOT_FOUND", "Агент не найден")
	}
	if agent.Revoked {
		return nil, protoErr("AGENT_REVOKED", "Агент отозван")
	}
	if !message.ValidName(name) {
		return nil, invalidName("name", name)
	}
	for _, q := range queues {
		if !message.ValidName(q) {
			return nil, invalidName("queues", q)
		}
	}
	if !declaresCommand(agent.Capabilities, command) {
		return nil, protoErr("COMMAND_NOT_SUPPORTED", "Агент не объявил команду "+command)
	}
	queues = slices.Clone(queues)
	return a.newCommand(actor, CommandRequest{
		AgentID: agentID, Name: command, TimeoutSec: workerControlTimeout,
		Args: message.WorkerPauseArgs{Name: name, Queues: queues},
	}, func(cmd *Command) {
		details := map[string]any{"worker": name, "commandId": cmd.ID}
		if len(queues) > 0 {
			details["queues"] = queues
		}
		a.audit(actor, action, agentID, agentID, details)
	})
}
