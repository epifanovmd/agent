"""Небольшой бэкенд на FastAPI с готовым ASGI-транспортом ``agent_sdk.server.asgi``.

Не входит в стенд и тесты: нужны пакеты ``fastapi`` и ``uvicorn``. Маршруты агента (регистрация,
HTTP sync, WebSocket, выпуск, файлы задач) обслуживает ``AgentsApp``, остальное — FastAPI::

    pip install fastapi uvicorn
    cd examples/server-python && ENROLL_TOKEN=demo-token \\
        uvicorn fastapi_app:app --port 18090 --ws-ping-interval 20

За прокси — ``TRUST_PROXY=1``: адрес агента из ``X-Forwarded-For``, адрес сервера — из
``X-Forwarded-Host`` и ``X-Forwarded-Proto``.
"""

from __future__ import annotations

import contextlib
import os
import sys
from pathlib import Path
from typing import Any, AsyncIterator, Dict, List, Optional

# Без установки: SDK из репозитория (установленный agent-sdk тоже подойдёт).
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "sdk" / "python"))

from fastapi import Body, FastAPI, Header, Request  # noqa: E402
from fastapi.responses import JSONResponse  # noqa: E402

from agent_sdk.server import Agents, AgentsError  # noqa: E402
from agent_sdk.server.asgi import AgentsApp  # noqa: E402

agents = Agents(enroll_token=os.environ.get("ENROLL_TOKEN", "demo-token"),
                trust_proxy=os.environ.get("TRUST_PROXY", "").lower() in ("1", "true"))


@contextlib.asynccontextmanager
async def lifespan(_: FastAPI) -> AsyncIterator[None]:
    yield
    await agents.close()  # агенты переподключатся сразу (1012)


api = FastAPI(lifespan=lifespan)


@api.exception_handler(AgentsError)
async def agents_error(_: Request, err: AgentsError) -> JSONResponse:
    return JSONResponse(err.to_dict(), status_code=err.status)


def by(actor: Optional[str]) -> Any:
    """Действия от имени ``X-Actor`` (поле ``actor`` и аудит)."""
    return agents.by((actor or "").strip()[:100] or "web")


@api.get("/api/agents")
async def list_agents() -> List[Dict[str, Any]]:
    return [a.to_dict() for a in await agents.list_agents()]


@api.delete("/api/agents/{agent_id}")
async def delete_agent(agent_id: str, x_actor: Optional[str] = Header(None)) -> Dict[str, Any]:
    await by(x_actor).delete_agent(agent_id)
    return {}


@api.post("/api/jobs", status_code=201)
async def enqueue(body: Dict[str, Any] = Body(...), x_actor: Optional[str] = Header(None)) -> Dict[str, Any]:
    job = await by(x_actor).enqueue(body.get("queue"), body.get("data"), max_attempts=body.get("maxAttempts") or 1,
                                    agent_id=body.get("agentId"))
    return job.to_dict()


@api.get("/api/jobs")
async def list_jobs(limit: int = 50, after: Optional[str] = None) -> List[Dict[str, Any]]:
    return [j.to_dict() for j in await agents.list_jobs(limit=limit, after=after)]


@api.post("/api/commands", status_code=201)
async def command(body: Dict[str, Any] = Body(...), x_actor: Optional[str] = Header(None)) -> Dict[str, Any]:
    cmd = await by(x_actor).command(body.get("name"), body.get("args"), timeout_sec=body.get("timeoutSec") or 60,
                                    agent_id=body.get("agentId"))
    return cmd.to_dict()


@api.post("/api/commands/{command_id}/cancel")
async def cancel_command(command_id: str, x_actor: Optional[str] = Header(None)) -> Dict[str, Any]:
    return (await by(x_actor).cancel_command(command_id)).to_dict()


@api.get("/api/alerts")
async def alerts() -> List[Dict[str, Any]]:
    return [a.to_dict() for a in await agents.alerts()]


#: Приложение для uvicorn: маршруты агента — AgentsApp, остальное — FastAPI.
app = AgentsApp(agents, fallback=api)
