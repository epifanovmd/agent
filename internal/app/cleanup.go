//go:build unix

package app

import (
	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/identity"
	"github.com/epifanovmd/agent/internal/logx"
	"github.com/epifanovmd/agent/sdk/go/message"
)

// CleanupContext — контекст воркеров для `agent cleanup` (worker.context с
// mode cleanup): связи с сервером нет; id — из сохранённых учётных данных
// (пусто — агент не регистрировался).
func CleanupContext(cfg config.Config, version string) message.WorkerContext {
	var id string
	if creds, ok, err := identity.NewStore(cfg.DataDir).Load(); err == nil && ok {
		id = creds.AgentID
	}
	return message.WorkerContext{
		Mode:     message.WorkerModeCleanup,
		Agent:    message.WorkerContextAgent{ID: id, Name: cfg.Name, Version: version, Labels: cfg.Labels},
		LogLevel: logx.NewForwarder(cfg.Log.Forward).Level(),
	}
}
