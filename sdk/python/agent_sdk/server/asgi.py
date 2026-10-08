"""ASGI-приложение для маршрутов агента — без зависимостей (uvicorn, hypercorn, FastAPI/Starlette).

::

    from agent_sdk.server import Agents
    from agent_sdk.server.asgi import AgentsApp

    agents = Agents(enroll_token="…")
    app = AgentsApp(agents, fallback=api)   # api — своё ASGI-приложение (FastAPI и т. п.)
    # uvicorn module:app --ws-ping-interval 20

Маршруты агента:

- ``POST /api/v1/agent-link/enroll`` — регистрация (``handle_enroll``);
- ``POST /api/v1/agent-link/sync`` — HTTP sync (``handle_sync``; клиент ушёл — ответа нет);
- WebSocket ``/api/v1/agent-link`` — сессия (``serve_websocket``): канал ``agent.v1`` и учётные
  данные проверяются до upgrade (401/426 — если сервер умеет ответ на отказ
  ``websocket.http.response``, иначе отказ закрытием);
- ``GET /api/v1/agent-link/releases/…``, ``GET /api/v1/agent-link/install.sh`` — выпуск агента;
- ``GET``/``PUT /files/<ключ>`` — файлы задач (``handle_file``; ``MemoryFiles``).

Тело читается не больше ``agents.body_limit(path)``: ``Content-Length`` больше — ``413`` без
чтения, тело больше по ходу чтения — тоже ``413``. Остальные запросы (и ``lifespan``) уходят в
``fallback``; без него — ``404``, а ``lifespan`` по остановке закрывает ``agents``.

Подключить внутрь FastAPI/Starlette можно и так: ``api.mount("/", AgentsApp(agents))`` —
тогда ``Mount`` в конце списка маршрутов.
"""

from __future__ import annotations

import asyncio
import contextlib
import json
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple

from ..message import ENROLL_PATH, INSTALL_PATH, LINK_PATH, RELEASES_PATH, SYNC_PATH, WS_CHANNEL
from .agents import Agents

Scope = Dict[str, Any]
Receive = Callable[[], Awaitable[Dict[str, Any]]]
Send = Callable[[Dict[str, Any]], Awaitable[None]]
ASGIApp = Callable[[Scope, Receive, Send], Awaitable[None]]

#: Префикс файлов задач (``MemoryFiles``).
FILES_PREFIX = "/files/"


class _TooLarge(Exception):
    pass


class _Gone(Exception):
    pass


class AgentsApp:
    """ASGI-приложение маршрутов агента поверх ``Agents``; ``fallback`` — всё остальное."""

    def __init__(self, agents: Agents, fallback: Optional[ASGIApp] = None) -> None:
        self.agents = agents
        self.fallback = fallback

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        kind = scope.get("type")
        if kind == "lifespan":
            if self.fallback is not None:
                await self.fallback(scope, receive, send)
            else:
                await self._lifespan(receive, send)
            return
        path = _path(scope)
        if kind == "websocket" and path == LINK_PATH:
            await self._websocket(scope, receive, send)
            return
        if kind == "http" and self._agent_route(scope["method"], path):
            await self._http(scope, path, receive, send)
            return
        if self.fallback is not None:
            await self.fallback(scope, receive, send)
            return
        if kind == "websocket":
            await send({"type": "websocket.close", "code": 1000})
            return
        if kind == "http":
            await _reply_json(send, 404, {"code": "NOT_FOUND", "message": "Нет такого пути"})

    @staticmethod
    def _agent_route(method: str, path: str) -> bool:
        if path in (ENROLL_PATH, SYNC_PATH):
            return method == "POST"
        if path == INSTALL_PATH or path.startswith(RELEASES_PATH + "/"):
            return method in ("GET", "HEAD")
        if path.startswith(FILES_PREFIX):
            return method in ("GET", "HEAD", "PUT")
        return False

    async def _lifespan(self, receive: Receive, send: Send) -> None:
        while True:
            message = await receive()
            if message["type"] == "lifespan.startup":
                await send({"type": "lifespan.startup.complete"})
            elif message["type"] == "lifespan.shutdown":
                await self.agents.close()
                await send({"type": "lifespan.shutdown.complete"})
                return

    # ── HTTP ────────────────────────────────────────────────────────────

    async def _http(self, scope: Scope, path: str, receive: Receive, send: Send) -> None:
        agents = self.agents
        method = scope["method"]
        headers = _headers(scope)
        remote = _remote(scope)
        forwarded_for = headers.get("x-forwarded-for")
        base_url = agents.request_base(headers.get("host"), secure=scope.get("scheme") == "https",
                                       forwarded_host=headers.get("x-forwarded-host"),
                                       forwarded_proto=headers.get("x-forwarded-proto"))
        limit = agents.body_limit(path)
        try:
            body = await _read_body(headers, receive, limit) if method in ("POST", "PUT") else b""
        except _TooLarge:
            if path == ENROLL_PATH:
                reply = agents.enroll_too_large(remote, forwarded_for=forwarded_for)
                status, data = reply
                await _reply_json(send, status, data, reply.headers)
            else:
                await _reply_json(send, 413, {"code": "MESSAGE_INVALID", "message": "Слишком большой запрос"})
            return
        except _Gone:
            return

        if path == ENROLL_PATH:
            reply = await agents.handle_enroll(body, remote, forwarded_for=forwarded_for)
            status, data = reply
            await _reply_json(send, status, data, reply.headers)
            return
        if path == SYNC_PATH:
            gone = asyncio.Event()

            async def watch() -> None:
                while True:
                    message = await receive()
                    if message["type"] == "http.disconnect":
                        gone.set()
                        return

            watcher = asyncio.ensure_future(watch())
            try:
                status, data = await agents.handle_sync(headers.get("authorization"), body, gone.is_set,
                                                        base_url=base_url, remote=remote,
                                                        forwarded_for=forwarded_for)
            finally:
                watcher.cancel()
                with contextlib.suppress(BaseException):
                    await watcher
            if status != 499:  # клиент ушёл — отвечать некому
                await _reply_json(send, status, data)
            return
        if path.startswith(FILES_PREFIX):
            status, raw = await agents.handle_file(method, path[len(FILES_PREFIX):], body)
            await _reply(send, status, raw, "application/octet-stream", head=method == "HEAD")
            return
        status, raw, content_type = await agents.handle_release(path, base_url)
        await _reply(send, status, raw, content_type, head=method == "HEAD")

    # ── WebSocket ───────────────────────────────────────────────────────

    async def _websocket(self, scope: Scope, receive: Receive, send: Send) -> None:
        agents = self.agents
        headers = _headers(scope)
        message = await receive()
        if message["type"] != "websocket.connect":
            return
        can_deny = "websocket.http.response" in (scope.get("extensions") or {})
        if WS_CHANNEL not in (scope.get("subprotocols") or []):
            await _deny(send, can_deny, 426, "Нужен канал agent.v1")
            return
        authorization = headers.get("authorization")
        if await agents.authenticate(authorization) is None:
            await _deny(send, can_deny, 401, "Неверные учётные данные")
            return
        await send({"type": "websocket.accept", "subprotocol": WS_CHANNEL})
        base_url = agents.request_base(headers.get("host"), secure=scope.get("scheme") in ("https", "wss"),
                                       forwarded_host=headers.get("x-forwarded-host"),
                                       forwarded_proto=headers.get("x-forwarded-proto"))
        await agents.serve_websocket(authorization, ASGIConnection(receive, send), base_url=base_url,
                                     remote=_remote(scope), forwarded_for=headers.get("x-forwarded-for"))


class ASGIConnection:
    """Принятый ASGI WebSocket с интерфейсом, который ждёт ``agents.serve_websocket``."""

    def __init__(self, receive: Receive, send: Send) -> None:
        self._receive = receive
        self._send = send
        self.closed = False

    async def receive_text(self) -> str:
        while True:
            message = await self._receive()
            if message["type"] == "websocket.disconnect":
                self.closed = True
                raise ConnectionError("WebSocket закрыт")
            if message["type"] != "websocket.receive":
                continue
            if message.get("text") is not None:
                return str(message["text"])
            raw = message.get("bytes")
            if raw is not None:
                return bytes(raw).decode("utf-8")

    async def send_text(self, text: str) -> None:
        if self.closed:
            raise ConnectionError("WebSocket закрыт")
        await self._send({"type": "websocket.send", "text": text})

    async def close(self, code: int = 1000) -> None:
        if self.closed:
            return
        self.closed = True
        with contextlib.suppress(Exception):
            await self._send({"type": "websocket.close", "code": code})


# ── помощники ───────────────────────────────────────────────────────────────

def _path(scope: Scope) -> str:
    """Полный путь запроса: при монтировании (``root_path``) — вместе с префиксом."""
    path = scope.get("path") or "/"
    root = scope.get("root_path") or ""
    if root and not path.startswith(root):
        path = root.rstrip("/") + path
    return path


def _headers(scope: Scope) -> Dict[str, str]:
    """Заголовки (имена — в нижнем регистре); повторы — через запятую."""
    out: Dict[str, str] = {}
    for name, value in scope.get("headers") or []:
        key = name.decode("latin-1").lower()
        text = value.decode("latin-1")
        out[key] = f"{out[key]}, {text}" if key in out else text
    return out


def _remote(scope: Scope) -> Optional[str]:
    client = scope.get("client")
    return str(client[0]) if isinstance(client, (list, tuple)) and client else None


async def _read_body(headers: Dict[str, str], receive: Receive, limit: int) -> bytes:
    """Тело не больше ``limit``: ``Content-Length`` больше — сразу ``_TooLarge`` (без чтения)."""
    length = headers.get("content-length")
    if length is not None:
        try:
            if int(length) > limit:
                raise _TooLarge()
        except ValueError:
            pass
    chunks: List[bytes] = []
    size = 0
    while True:
        message = await receive()
        if message["type"] == "http.disconnect":
            raise _Gone()
        if message["type"] != "http.request":
            continue
        chunk = message.get("body") or b""
        size += len(chunk)
        if size > limit:
            raise _TooLarge()
        chunks.append(chunk)
        if not message.get("more_body"):
            return b"".join(chunks)


async def _reply(send: Send, status: int, body: bytes, content_type: str,
                 headers: Optional[Dict[str, str]] = None, *, head: bool = False) -> None:
    raw: List[Tuple[bytes, bytes]] = [(b"content-type", content_type.encode("latin-1")),
                                      (b"content-length", str(len(body)).encode())]
    raw += [(k.lower().encode("latin-1"), str(v).encode("latin-1")) for k, v in (headers or {}).items()]
    await send({"type": "http.response.start", "status": status, "headers": raw})
    await send({"type": "http.response.body", "body": b"" if head else body})


async def _reply_json(send: Send, status: int, body: Any, headers: Optional[Dict[str, str]] = None) -> None:
    raw = json.dumps(body, ensure_ascii=False).encode()
    await _reply(send, status, raw, "application/json; charset=utf-8", headers)


async def _deny(send: Send, can_deny: bool, status: int, message: str) -> None:
    """Отказ в upgrade: ответ HTTP, если сервер это умеет, иначе закрытие до accept (403)."""
    if not can_deny:
        await send({"type": "websocket.close", "code": 1008})
        return
    code = "AGENT_CREDENTIALS_INVALID" if status == 401 else "UPGRADE_REQUIRED"
    body = json.dumps({"code": code, "message": message}, ensure_ascii=False).encode()
    await send({"type": "websocket.http.response.start", "status": status,
                "headers": [(b"content-type", b"application/json; charset=utf-8"),
                            (b"content-length", str(len(body)).encode())]})
    await send({"type": "websocket.http.response.body", "body": body})
