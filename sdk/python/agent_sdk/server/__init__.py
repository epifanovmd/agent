"""Бэкенд, к которому подключаются агенты (роль server контракта SDK, §2–§7 спецификации).

``Agents`` — движок связи с агентами на asyncio, транспорт-независимый: бэкенд передаёт ему
запросы своего HTTP-сервера (любого: Starlette/FastAPI, aiohttp, свой)::

    from agent_sdk.server import Agents

    agents = Agents(enroll_token="demo-token")

    reply = await agents.handle_enroll(raw_body, remote=client_ip)           # POST …/enroll (тело ≤ body_limit)
    status, body = reply                                                      # reply.headers — Retry-After у 429
    status, body = await agents.handle_sync(authorization, raw_body, is_gone,
                                            remote=client_ip)                  # POST …/sync
    await agents.serve_websocket(authorization, websocket, remote=client_ip)   # WS …/agent-link

    job = await agents.enqueue("example.echo", {"text": "hi"}, max_attempts=2)
    cmd = await agents.call("agent.logs", {"lines": 100})
    await agents.set_state("example.kv", {"greeting": "hello"})
    agents.on("change", lambda change: print(change.kind, change.id))
    agents.on("audit", lambda entry: print(entry.actor, entry.action, entry.target))
    agents.on("alert", lambda alert: print(alert.type, alert.agent_name, alert.active))
    await agents.by("ivan").set_state("example.kv", {"greeting": "hi"})       # actor в снимке и аудите
    await agents.rollback_state("example.kv", version)                         # история — state_history

    points = await agents.list_metrics(agent_id, since=ms)                     # история метрик
    sub = await agents.subscribe(agent_id, ttl_ms=30000, metrics={"intervalMs": 1000, "groups": ["diskio"]},
                                 logs={"level": "debug"},
                                 channels={"example.app": {"intervalMs": 1000}})  # {id, until}; продлить — тот же id
    await agents.unsubscribe(agent_id, sub["id"])                              # снять подписку
    await agents.pause_worker(agent_id, "report")                              # воркер не берёт задачи; resume_worker
    agents.on("log", lambda agent_id, entries: print(agent_id, len(entries)))  # логи агентов (SDK их не хранит)
    sealed = await agents.seal(agent_id, {"password": "…"})                     # {"$sealed": …}, нужен extra crypto
    status, body, content_type = await agents.handle_release(path, base_url)  # GET …/releases/*, …/install.sh

    await agents.cancel_command(cmd.id)                                        # агенту — cmd.cancel
    await agents.revoke(agent_id); await agents.delete_agent(agent_id)         # удалить — только отозванного
    await agents.prune(jobs_older_than_ms=30 * 86400_000)                      # уборка старых записей
    page = await agents.list_jobs(limit=50, after=last_id)                     # постранично
    active = await agents.alerts()                                             # активные проблемы (из Store)

Готовый транспорт без зависимостей — ASGI (uvicorn, FastAPI/Starlette)::

    from agent_sdk.server.asgi import AgentsApp
    app = AgentsApp(agents, fallback=api)
"""

from .files import Files, MemoryFiles
from .agents import Actor, Agents
from .session import AgentsError, HttpReply, Session
from .model import (
    Agent, AgentEvent, Alert, AuditEntry, Change, Command, DesiredState, Job, MetricsPoint, UpdateCandidate,
    WorkerUpdateCandidate,
)
from .store import MemoryStore, Store

__all__ = [
    "Actor", "Agent", "AgentEvent", "Alert", "AuditEntry", "Change", "Command", "DesiredState", "Files", "Agents",
    "AgentsError", "HttpReply", "Job", "MemoryFiles", "MemoryStore", "MetricsPoint", "Session", "Store",
    "UpdateCandidate", "WorkerUpdateCandidate",
]
