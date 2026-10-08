"""Пример бэкенда на Python: agent_sdk.server + только стандартная библиотека.

Агенты подключаются по HTTP sync (WebSocket для агентов в этом примере нет: на
upgrade — 404, агент в режиме auto сам переходит на HTTP sync). Сервер раздаёт задачи,
команды и желаемое состояние, принимает итоги, телеметрию и события; API
приложения — то же, что у Node-примера (examples/server), поэтому работает тот
же веб-интерфейс (examples/web) и его самопроверка (examples/API.md; поток для
интерфейса — WebSocket /api/ws на своём минимальном RFC 6455 в http11.py).

    PORT=18090 ENROLL_TOKEN=demo-token python3 examples/server-python/main.py

Выпуск агента (обновления, install.sh): RELEASES_DIR — каталог выпуска (dist/<VERSION>),
PUBLIC_KEY — ключ проверки релизов (base64), подставляется в install.sh.
TRUST_PROXY=1 — сервер за прокси: адрес агента берётся из X-Forwarded-For.
"""

from __future__ import annotations

import asyncio
import json
import mimetypes
import os
import re
import signal
import sys
import time
from pathlib import Path
from typing import Any, Dict, Optional

HERE = Path(__file__).resolve().parent
# Без установки: SDK из репозитория (установленный agent-sdk тоже подойдёт).
sys.path.insert(0, str(HERE.parents[1] / "sdk" / "python"))

from agent_sdk.message import (  # noqa: E402
    ENROLL_PATH, INSTALL_PATH, LINK_PATH, RELEASES_PATH, SYNC_PATH, now_ms,
)
from agent_sdk.server import Agents, AgentsError, Job  # noqa: E402

from http11 import Request, Response, WebSocket, accept_websocket, serve, websocket_refusal  # noqa: E402
from live import Live  # noqa: E402

#: Ping интерфейсу по WebSocket, чтобы прокси не рвали тихое соединение, с.
WS_PING = 30.0


def log(msg: str, extra: Optional[Dict[str, Any]] = None) -> None:
    stamp = time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime())
    print(stamp, msg, json.dumps(extra, ensure_ascii=False) if extra else "", flush=True)


def as_json(status: int, body: Any, headers: Optional[Dict[str, str]] = None) -> Response:
    raw = json.dumps(body, ensure_ascii=False).encode()
    return Response(status, raw, {"Content-Type": "application/json; charset=utf-8", **(headers or {})})


def parse_json(req: Request) -> Any:
    return json.loads(req.body) if req.body else {}


def actor_of(req: Request) -> str:
    """Кто действует: заголовок ``X-Actor`` (в демо — без проверки), иначе ``web``."""
    return (req.headers.get("x-actor") or "").strip()[:100] or "web"


def base_url(req: Request) -> str:
    """Адрес сервера, по которому агент до него дошёл (ссылки на файлы задач)."""
    return f"{req.headers.get('x-forwarded-proto', 'http')}://{req.headers.get('host', 'localhost')}"


class App:
    def __init__(self, agents: Agents, web_dir: Path, enroll_token: str = "<ENROLL_TOKEN>") -> None:
        self.agents = agents
        self.web_dir = web_dir.resolve()
        #: Токен регистрации — для команды установки агента в /api/releases.
        self.enroll_token = enroll_token
        #: Поток для интерфейса (WebSocket /api/ws).
        self.live = Live(agents, snapshot=self.snapshot, job_view=self.job_view, log=log)

    async def job_view(self, job: Job) -> Dict[str, Any]:
        """Задача для интерфейса — с ``files`` (загруженные выходы)."""
        item = job.to_dict()
        item["files"] = await self.agents.files.outputs(job)
        return item

    async def snapshot(self) -> Dict[str, Any]:
        """Всё для интерфейса (examples/web/src/types.ts ``Snapshot``), новые — первыми."""
        jobs = [await self.job_view(job) for job in await self.agents.list_jobs()]
        return {
            "serverTime": now_ms(),
            "agents": [a.to_dict() for a in await self.agents.list_agents()],
            "jobs": jobs,
            "commands": [c.to_dict() for c in await self.agents.list_commands()],
            "states": [s.to_dict() for s in await self.agents.list_states()],
            "events": [e.to_dict() for e in await self.agents.list_events(500)],
            "alerts": [a.to_dict() for a in self.agents.alerts()],
        }

    async def __call__(self, req: Request) -> Optional[Response]:
        try:
            return await self.route(req)
        except AgentsError as err:
            return as_json(err.status, err.to_dict())
        except (ValueError, TypeError) as err:
            return as_json(400, {"code": "MESSAGE_INVALID", "message": str(err)})

    async def route(self, req: Request) -> Optional[Response]:
        method, path = req.method, req.path
        # ── связь с агентом ──
        if path == ENROLL_PATH and method == "POST":
            # Адрес клиента — для ограничения неудачных регистраций (429 с Retry-After).
            reply = await self.agents.handle_enroll(req.body, remote=req.remote or None)
            status, body = reply
            return as_json(status, body, reply.headers)
        if path == SYNC_PATH and method == "POST":
            # Адрес клиента и X-Forwarded-For — для Agent.address (заголовок — только с trust_proxy).
            status, body = await self.agents.handle_sync(req.headers.get("authorization"), req.body,
                                                      req.disconnected, base_url=base_url(req),
                                                      remote=req.remote or None,
                                                      forwarded_for=req.headers.get("x-forwarded-for"))
            return None if status == 499 else as_json(status, body)
        if path == LINK_PATH:
            # WebSocket не поддерживается: 404 на upgrade — агент переходит на HTTP sync.
            return as_json(404, {"code": "NOT_FOUND", "message": "WebSocket нет: HTTP sync"})
        if path.startswith("/files/") and method in ("GET", "HEAD", "PUT"):
            status, data = await self.agents.handle_file(method, path[len("/files/"):], req.body)
            return Response(status, data, {"Content-Type": "application/octet-stream"})
        # Выпуск агента — публично: сборки подписаны, секретов в них нет.
        if (path == INSTALL_PATH or path.startswith(RELEASES_PATH + "/")) and method in ("GET", "HEAD"):
            status, data, kind = await self.agents.handle_release(path, base_url(req))
            return Response(status, data, {"Content-Type": kind})

        # ── API приложения ── (изменяющие действия — от имени X-Actor: поле actor и аудит)
        by = self.agents.by(actor_of(req))
        if path == "/api/snapshot" and method == "GET":
            return as_json(200, await self.snapshot())
        if path == "/api/ws":
            refusal = websocket_refusal(req)
            if refusal is not None:
                return refusal
            await self.websocket(await accept_websocket(req))
            return None
        if path == "/api/jobs" and method == "POST":
            b = parse_json(req)
            job = await by.enqueue(
                b.get("queue"), b.get("data"), max_attempts=b.get("maxAttempts") or 1,
                lease_seconds=b.get("leaseSeconds") or 60, inputs=b.get("inputs"), outputs=b.get("outputs"),
                agent_id=b.get("agentId"))
            return as_json(201, job.to_dict())
        m = re.fullmatch(r"/api/jobs/([^/]+)", path)
        if m and method == "GET":
            job = await self.agents.get_job(m.group(1))
            return as_json(200, job.to_dict()) if job else as_json(404, {"code": "JOB_NOT_FOUND",
                                                                          "message": "Задача не найдена"})
        m = re.fullmatch(r"/api/jobs/([^/]+)/(cancel|stop)", path)
        if m and method == "POST":
            signal_job = by.cancel_job if m.group(2) == "cancel" else by.stop_job
            return as_json(200, (await signal_job(m.group(1))).to_dict())
        if path == "/api/commands" and method == "POST":
            b = parse_json(req)
            cmd = await by.command(b.get("name"), b.get("args"), timeout_sec=b.get("timeoutSec") or 60,
                                   agent_id=b.get("agentId"))
            return as_json(201, cmd.to_dict())
        m = re.fullmatch(r"/api/commands/([^/]+)", path)
        if m and method == "GET":
            cmd = await self.agents.get_command(m.group(1))
            return as_json(200, cmd.to_dict()) if cmd else as_json(404, {"code": "COMMAND_NOT_FOUND",
                                                                          "message": "Команда не найдена"})
        # История снимков раздела (новые — первыми) и откат к версии (новая версия, тот же spec).
        m = re.fullmatch(r"/api/state/([^/]+)/history", path)
        if m and method == "GET":
            limit = req.query.get("limit")
            history = await self.agents.state_history(m.group(1), agent_id=req.query.get("agentId") or None,
                                                      limit=int(limit) if limit else 20)
            return as_json(200, [s.to_dict() for s in history])
        m = re.fullmatch(r"/api/state/([^/]+)/rollback", path)
        if m and method == "POST":
            b = parse_json(req) or {}
            version = b.get("version")
            if not isinstance(version, int) or isinstance(version, bool):
                return as_json(400, {"code": "MESSAGE_INVALID", "message": "Нужна version (число)"})
            st = await by.rollback_state(m.group(1), version, agent_id=b.get("agentId") or None)
            return as_json(200, st.to_dict())
        m = re.fullmatch(r"/api/state/(.+)", path)
        if m and method == "PUT":
            st = await by.set_state(m.group(1), parse_json(req), agent_id=req.query.get("agentId"))
            return as_json(200, st.to_dict())
        if m and method == "DELETE":
            # Без agentId — общий снимок; с agentId — личный (общий тогда переиздаётся).
            st = await by.delete_state(m.group(1), agent_id=req.query.get("agentId"))
            return as_json(200, {"state": st.to_dict() if st else None})
        # Подписка на агента (тело — как у agents.subscribe) и её снятие.
        m = re.fullmatch(r"/api/agents/([^/]+)/subscriptions", path)
        if m and method == "POST":
            b = parse_json(req) or {}
            if not isinstance(b, dict):
                return as_json(400, {"code": "MESSAGE_INVALID", "message": "Тело — объект подписки"})
            sub = await self.agents.subscribe(
                m.group(1), id=b.get("id"), ttl_ms=b.get("ttlMs", 30_000), status=b.get("status"),
                metrics=b.get("metrics"), logs=b.get("logs"), channels=b.get("channels"))
            return as_json(200, sub)
        m = re.fullmatch(r"/api/agents/([^/]+)/subscriptions/([^/]+)", path)
        if m and method == "DELETE":
            await self.agents.unsubscribe(m.group(1), m.group(2))
            return as_json(200, {})
        # Агенты: история метрик, отзыв, обновление, смена ключа.
        m = re.fullmatch(r"/api/agents/([^/]+)/(metrics|revoke|update|rotate-key)", path)
        if m:
            agent_id, action = m.group(1), m.group(2)
            if method == "GET" and action == "metrics":
                if await self.agents.get_agent(agent_id) is None:
                    return as_json(404, {"code": "AGENT_NOT_FOUND", "message": "Агент не найден"})
                since = req.query.get("since")
                points = await self.agents.list_metrics(agent_id, since=int(float(since)) if since else None)
                return as_json(200, [p.to_dict() for p in points])
            if method == "POST" and action == "revoke":
                return as_json(200, (await by.revoke(agent_id)).to_dict())
            if method == "POST" and action == "update":
                return as_json(201, (await by.update_agent(agent_id)).to_dict())
            if method == "POST" and action == "rotate-key":
                return as_json(201, (await by.rotate_key(agent_id)).to_dict())
        # Воркер из выпуска (release: true в agent.yaml узла): обновить до версии манифеста.
        m = re.fullmatch(r"/api/agents/([^/]+)/workers/([^/]+)/(update|pause|resume)", path)
        if m and method == "POST":
            agent_id, name, action = m.groups()
            if action == "update":
                return as_json(201, (await by.update_worker(agent_id, name)).to_dict())
            # Пауза воркера: не брать новые задачи очередей queues (без них — всех).
            queues = (parse_json(req) or {}).get("queues")
            control = by.pause_worker if action == "pause" else by.resume_worker
            return as_json(201, (await control(agent_id, name, queues)).to_dict())
        # Выпуск агента: манифест, кандидаты на обновление агентов и воркеров, команда установки.
        if path == "/api/releases" and method == "GET":
            install = self.agents.install_command(base_url=self.agents.base_url or base_url(req),
                                                  token=self.enroll_token)
            return as_json(200, {"release": await self.agents.release(),
                                 "candidates": [c.to_dict() for c in await self.agents.update_candidates()],
                                 "workerCandidates": [c.to_dict()
                                                      for c in await self.agents.worker_update_candidates()],
                                 "installCommand": install})
        if path.startswith("/api/"):
            return as_json(404, {"code": "NOT_FOUND", "message": "Нет такого метода"})
        if method in ("GET", "HEAD"):
            return self.static(path)
        return as_json(405, {"code": "METHOD_NOT_ALLOWED", "message": "Метод не поддерживается"})

    async def websocket(self, ws: WebSocket) -> None:
        """Поток для интерфейса (examples/API.md): snapshot, изменения, метрики подписанных агентов."""
        async def keepalive() -> None:
            try:
                while not ws.closed:
                    await asyncio.sleep(WS_PING)
                    await ws.ping()
            except (ConnectionError, OSError):
                pass

        pinger = asyncio.ensure_future(keepalive())
        try:
            await self.live.serve(ws)
        finally:
            pinger.cancel()
            await ws.close()

    def static(self, path: str) -> Response:
        """Собранный интерфейс (examples/web/dist); неизвестный путь — index.html (SPA)."""
        index = self.web_dir / "index.html"
        if not index.exists():
            return Response(200, "Интерфейс не собран: cd examples/web && npm install && npm run build\n".encode(),
                            {"Content-Type": "text/plain; charset=utf-8"})
        file = (self.web_dir / path.lstrip("/")).resolve()
        if not str(file).startswith(str(self.web_dir)) or not file.is_file():
            file = index
        kind = mimetypes.guess_type(file.name)[0] or "application/octet-stream"
        if kind.startswith("text/") or kind == "application/javascript":
            kind += "; charset=utf-8"
        return Response(200, file.read_bytes(), {"Content-Type": kind})


async def main() -> None:
    port = int(os.environ.get("PORT", "8080"))
    host = os.environ.get("HOST", "0.0.0.0")
    token = os.environ.get("ENROLL_TOKEN", "demo-token")
    web_dir = Path(os.environ.get("WEB_DIR", HERE.parent / "web" / "dist"))
    releases_dir = os.environ.get("RELEASES_DIR") or None
    agents = Agents(enroll_token=token, log=log,
              status_interval_ms=int(os.environ.get("STATUS_INTERVAL_MS", "5000")),
              metrics_interval_ms=int(os.environ.get("METRICS_INTERVAL_MS", "5000")),
              # История метрик: точка в Store не чаще раза в 5 с (0 — каждая точка).
              metrics_store_interval_ms=int(os.environ.get("METRICS_STORE_INTERVAL_MS", "5000")),
              releases_dir=str(Path(releases_dir).resolve()) if releases_dir else None,
              public_key=os.environ.get("PUBLIC_KEY", ""),
              trust_proxy=os.environ.get("TRUST_PROXY", "").lower() in ("1", "true"))
    app = App(agents, web_dir, token)
    server = await serve(app, host, port)
    log(f"сервер (Python) на :{port}", {"enrollToken": token, "webDir": str(web_dir), "releasesDir": releases_dir})

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    await stop.wait()
    # Остановка: агенты переподключатся сразу (1012), их итоги ждут в outbox.
    await app.live.close()
    await agents.close()
    server.close()
    log("сервер остановлен")


if __name__ == "__main__":
    asyncio.run(main())
