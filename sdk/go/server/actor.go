package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"strings"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Actor — изменяющие методы Agents от имени actor (Agents.By): actor
// попадает в Job.Actor, Command.Actor, DesiredState.Actor и AuditEntry.Actor.
type Actor struct {
	agents *Agents
	name   string
}

// By — изменяющие методы от имени actor (пользователь, сервис):
//
//	agents.By("ivan").SetState("example.kv", spec, "")
//
// Методы Agents без By — с пустым actor.
func (a *Agents) By(actor string) *Actor { return &Actor{agents: a, name: actor} }

// Name — от чьего имени действия.
func (x *Actor) Name() string { return x.name }

// Enqueue — Agents.Enqueue от имени actor.
func (x *Actor) Enqueue(req JobRequest) (*Job, error) { return x.agents.enqueue(x.name, req) }

// CancelJob — Agents.CancelJob от имени actor.
func (x *Actor) CancelJob(id string) error { return x.agents.signalJob(x.name, id, false) }

// StopJob — Agents.StopJob от имени actor.
func (x *Actor) StopJob(id string) error { return x.agents.signalJob(x.name, id, true) }

// Command — Agents.Command от имени actor.
func (x *Actor) Command(req CommandRequest) (*Command, error) { return x.agents.command(x.name, req) }

// Call — Agents.Call от имени actor.
func (x *Actor) Call(ctx context.Context, req CommandRequest) (*Command, error) {
	return x.agents.call(ctx, x.name, req)
}

// SetState — Agents.SetState от имени actor.
func (x *Actor) SetState(domain string, spec any, agentID string) (*DesiredState, error) {
	return x.agents.setState(x.name, domain, spec, agentID)
}

// DeleteState — Agents.DeleteState от имени actor.
func (x *Actor) DeleteState(domain, agentID string) (*DesiredState, error) {
	return x.agents.deleteState(x.name, domain, agentID)
}

// RollbackState — Agents.RollbackState от имени actor.
func (x *Actor) RollbackState(domain string, version int64, agentID string) (*DesiredState, error) {
	return x.agents.rollbackState(x.name, domain, version, agentID)
}

// Revoke — Agents.Revoke от имени actor.
func (x *Actor) Revoke(agentID string) error { return x.agents.revoke(x.name, agentID) }

// UpdateAgent — Agents.UpdateAgent от имени actor.
func (x *Actor) UpdateAgent(agentID string) (*Command, error) {
	return x.agents.updateAgent(x.name, agentID)
}

// UpdateWorker — Agents.UpdateWorker от имени actor.
func (x *Actor) UpdateWorker(agentID, name string) (*Command, error) {
	return x.agents.updateWorker(x.name, agentID, name)
}

// RotateKey — Agents.RotateKey от имени actor.
func (x *Actor) RotateKey(agentID string) (*Command, error) {
	return x.agents.rotateKey(x.name, agentID)
}

// audit — запись аудита для OnAudit (под a.mu; уходит при снятии блокировки).
func (a *Agents) audit(actor, action, agentID, target string, details map[string]any) {
	if a.opts.OnAudit == nil {
		return
	}
	a.audits = append(a.audits, AuditEntry{
		At: now(), Actor: actor, Action: action, AgentID: agentID, Target: target, Details: details,
	})
}

// ─── смена ключа агента ────────────────────────────────────────────────

// RotateKey — смена секрета агента без новой регистрации (id сохраняется):
// команда agent.rotateKey, срок 60 с. Агент создаёт новый секрет и отвечает
// его sha256 (result.secretHash) — он сохраняется как ожидающий
// (Agent.PendingSecretHash), сессия закрывается кодом 1012; агент
// подключается с новым секретом — тот становится основным, старый перестаёт
// действовать. Нет агента — AGENT_NOT_FOUND, отозван — AGENT_REVOKED, не
// объявил agent.rotateKey — COMMAND_NOT_SUPPORTED.
func (a *Agents) RotateKey(agentID string) (*Command, error) { return a.rotateKey("", agentID) }

func (a *Agents) rotateKey(actor, agentID string) (*Command, error) {
	agent, err := a.Agent(agentID)
	if err != nil {
		return nil, protoErr("AGENT_NOT_FOUND", "Агент не найден")
	}
	if agent.Revoked {
		return nil, protoErr("AGENT_REVOKED", "Агент отозван")
	}
	if !declaresCommand(agent.Capabilities, message.CommandRotateKey) {
		return nil, protoErr("COMMAND_NOT_SUPPORTED", "Агент не объявил команду "+message.CommandRotateKey)
	}
	return a.newCommand(actor, CommandRequest{AgentID: agentID, Name: message.CommandRotateKey, TimeoutSec: 60},
		func(cmd *Command) {
			a.audit(actor, AuditAgentRotateKey, agentID, agentID, map[string]any{"commandId": cmd.ID})
		})
}

// rotated — успешный cmd.done команды agent.rotateKey: новый sha256 секрета —
// ожидающий; true — сессию закрыть (1012) после ack. Под a.mu.
func (a *Agents) rotated(agentID string, result json.RawMessage) bool {
	var r message.RotateKeyResult
	if json.Unmarshal(result, &r) != nil || !validHash(r.SecretHash) {
		a.log.Warn("agent.rotateKey: нет корректного secretHash в итоге", "agent", agentID)
		return false
	}
	agent, err := a.store.GetAgent(agentID)
	if err != nil || agent.Revoked {
		return false
	}
	agent.PendingSecretHash = strings.ToLower(r.SecretHash)
	if !a.storeAgent(agent) {
		return false
	}
	a.log.Info("агенту выдан новый секрет — ждём подключения с ним", "agent", agent.Name)
	return true
}

// validHash — sha256 в hex (64 знака).
func validHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	_, err := hex.DecodeString(s)
	return err == nil
}
