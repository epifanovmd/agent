//go:build unix

package app

import (
	"context"
	"path/filepath"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/worker"
)

// Cleanup — `agent cleanup` (вызывает `agent uninstall`, §13): без связи с
// сервером каждый воркер из настроек подхватывается (работает после
// остановки агента) или запускается, получает POST /cleanup и
// останавливается. Сокет агента на это время работает (online: false).
func Cleanup(ctx context.Context, cfg config.Config, version string) ([]worker.CleanupResult, error) {
	a, err := New(cfg, version)
	if err != nil {
		return nil, err
	}
	defer a.Close()
	if creds, ok, err := a.auth.store.Load(); err == nil && ok {
		a.auth.keys.Store(newKeys(a, creds))
	}
	sup := worker.New(cfg.Workers, worker.Options{
		RunDir: a.runDir, DataDir: cfg.DataDir, AgentSocket: filepath.Join(a.runDir, AgentSocketName), AgentVersion: version, Log: a.log,
	})
	a.workers = sup
	sock, err := listenSocket(a)
	if err != nil {
		return nil, err
	}
	defer sock.Close()
	return sup.Cleanup(ctx), nil
}

// StopWorkers — `agent stop-workers`: остановить воркеры, оставшиеся
// работать после остановки агента (lifecycle.onAgentStop: keep). Агент с
// этим dataDir не должен работать. Итог — имена остановленных.
func StopWorkers(cfg config.Config) ([]string, error) {
	lock, err := LockDataDir(cfg.DataDir)
	if err != nil {
		return nil, err
	}
	defer lock.Unlock()
	return worker.StopProcesses(cfg.DataDir), nil
}
