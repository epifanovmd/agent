"""Адаптер ``Agents`` к библиотеке ``websockets`` (≥ 13, ``websockets.asyncio``), если она установлена.

::

    from agent_sdk.server.websockets_adapter import serve
    server = await serve(agents, "0.0.0.0", 8080)   # WebSocket /api/v1/agent-link
    await server.serve_forever()

Учётные данные и канал проверяются до upgrade (401/426), ping — 20 с.
HTTP-маршруты (enroll, sync, файлы) — своим HTTP-сервером.
"""

from __future__ import annotations

from typing import Any, Optional

from ..message import LINK_PATH, WS_CHANNEL


class WebsocketsConnection:
    """Соединение ``websockets`` с интерфейсом, который ждёт ``agents.serve_websocket``."""

    def __init__(self, ws: Any) -> None:
        self.ws = ws

    async def receive_text(self) -> str:
        raw = await self.ws.recv()
        return raw.decode() if isinstance(raw, (bytes, bytearray)) else raw

    async def send_text(self, text: str) -> None:
        await self.ws.send(text)

    async def close(self, code: int = 1000) -> None:
        await self.ws.close(code)


def _authorization(ws: Any) -> Optional[str]:
    request = getattr(ws, "request", None)
    headers = getattr(request, "headers", None) or getattr(ws, "request_headers", None) or {}
    return headers.get("Authorization")


async def serve(agents: Any, host: str, port: int, **kwargs: Any) -> Any:
    """WebSocket-сервер ``websockets`` для агентов: путь ``/api/v1/agent-link``."""
    from http import HTTPStatus

    from websockets.asyncio.server import serve as ws_serve  # type: ignore[import-not-found]

    async def process_request(connection: Any, request: Any) -> Any:
        if request.path.split("?", 1)[0] != LINK_PATH:
            return connection.respond(HTTPStatus.NOT_FOUND, "Not Found\n")
        offered = [p.strip() for p in request.headers.get("Sec-WebSocket-Protocol", "").split(",")]
        if WS_CHANNEL not in offered:
            return connection.respond(HTTPStatus.UPGRADE_REQUIRED, "Upgrade Required\n")
        if await agents.authenticate(request.headers.get("Authorization")) is None:
            return connection.respond(HTTPStatus.UNAUTHORIZED, "Unauthorized\n")
        return None

    async def handler(ws: Any) -> None:
        host_header = ws.request.headers.get("Host", "")
        peer = getattr(ws, "remote_address", None)
        await agents.serve_websocket(_authorization(ws), WebsocketsConnection(ws),
                                  base_url=f"http://{host_header}" if host_header else "",
                                  remote=str(peer[0]) if isinstance(peer, (tuple, list)) and peer else None,
                                  forwarded_for=ws.request.headers.get("X-Forwarded-For"))

    kwargs.setdefault("ping_interval", 20)
    kwargs.setdefault("ping_timeout", 10)
    kwargs.setdefault("max_size", 16 << 20)
    return await ws_serve(handler, host, port, subprotocols=[WS_CHANNEL],
                          process_request=process_request, **kwargs)
