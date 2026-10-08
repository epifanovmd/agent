package server

import (
	"encoding/json"

	"github.com/epifanovmd/agent/sdk/go/sealed"
)

// Seal — запечатать value (любое значение JSON) открытым ключом агента
// (hello.agent.encryptionKey из записи агента): итог {"$sealed": "v1.…"}
// кладётся в снимок состояния на место value — в Store и на диске агента
// секрет остаётся зашифрованным, раскрывает его только агент перед передачей
// воркеру. Нет агента — AGENT_NOT_FOUND; агент не сообщал ключ (не
// подключался или hello без encryptionKey) — SEAL_NOT_AVAILABLE. Ключ агента
// сменился — запечатать заново (агент ответит state.applied ok: false).
func (a *Agents) Seal(agentID string, value any) (json.RawMessage, error) {
	a.mu.Lock()
	agent, err := a.store.GetAgent(agentID)
	a.mu.Unlock()
	if err != nil {
		return nil, protoErr("AGENT_NOT_FOUND", "Агент не найден")
	}
	if agent.Hello == nil || agent.Hello.Agent.EncryptionKey == "" {
		return nil, protoErr("SEAL_NOT_AVAILABLE", "Агент не сообщил ключ шифрования (не подключался или версия без секретов)")
	}
	pub, err := sealed.ParsePublicKey(agent.Hello.Agent.EncryptionKey)
	if err != nil {
		return nil, protoErr("SEAL_NOT_AVAILABLE", "Ключ шифрования агента некорректен: "+err.Error())
	}
	plain, err := marshalAny(value)
	if err != nil {
		return nil, protoErr("MESSAGE_INVALID", "value: "+err.Error())
	}
	if plain == nil {
		plain = json.RawMessage("null")
	}
	token, err := sealed.SealBytes(pub, plain)
	if err != nil {
		return nil, err
	}
	return json.Marshal(map[string]string{sealed.Field: token})
}
