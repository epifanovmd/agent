"""Минимальный HTTP/1.1-сервер на asyncio (только стандартная библиотека).

Ровно столько, сколько нужно примеру: keep-alive, тело по Content-Length или
chunked, ответы целиком и WebSocket (RFC 6455: handshake, текстовые сообщения,
ping/pong/close, ограничение размера). Не продакшен: без TLS, HTTP/2 и расширений
WebSocket (сжатие) — для продакшена берите uvicorn/aiohttp, ``Agents`` от транспорта не зависит.
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import struct
from dataclasses import dataclass, field
from typing import Awaitable, Callable, Dict, List, Optional, Tuple
from urllib.parse import parse_qs, unquote, urlsplit

#: Наибольшее тело запроса.
MAX_BODY = 64 << 20
#: Наибольшая строка запроса или заголовка.
MAX_LINE = 64 << 10

REASONS = {
    101: "Switching Protocols", 200: "OK", 201: "Created", 204: "No Content", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found",
    405: "Method Not Allowed", 409: "Conflict", 413: "Payload Too Large", 426: "Upgrade Required", 429: "Too Many Requests", 500: "Internal Server Error",
}


@dataclass
class Request:
    method: str
    path: str
    query: Dict[str, str]
    headers: Dict[str, str]  # имена — в нижнем регистре
    body: bytes
    reader: asyncio.StreamReader
    writer: asyncio.StreamWriter
    #: Адрес клиента (IP соединения; пусто — неизвестен).
    remote: str = ""

    def disconnected(self) -> bool:
        """Клиент закрыл соединение (для long-poll)."""
        return self.reader.at_eof() or self.writer.is_closing()


@dataclass
class Response:
    status: int = 200
    body: bytes = b""
    headers: Dict[str, str] = field(default_factory=dict)


#: Обработчик: ответ целиком или ``None`` — ответил сам (поток), соединение закрыть.
Handler = Callable[[Request], Awaitable[Optional[Response]]]


def head(status: int, headers: Dict[str, str]) -> bytes:
    """Строка статуса и заголовки."""
    lines = [f"HTTP/1.1 {status} {REASONS.get(status, 'Status')}"]
    lines += [f"{k}: {v}" for k, v in headers.items()]
    return ("\r\n".join(lines) + "\r\n\r\n").encode("latin-1")


async def serve(handler: Handler, host: str, port: int) -> asyncio.AbstractServer:
    async def connection(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            while True:
                request = await _read_request(reader, writer)
                if request is None:
                    return
                try:
                    response = await handler(request)
                except Exception as err:  # noqa: BLE001 — ошибка обработчика — 500, соединение живо
                    response = Response(500, f"{type(err).__name__}: {err}".encode())
                if response is None:
                    return
                keep = request.headers.get("connection", "").lower() != "close"
                headers = {"Content-Length": str(len(response.body)),
                           "Connection": "keep-alive" if keep else "close", **response.headers}
                writer.write(head(response.status, headers) + (b"" if request.method == "HEAD" else response.body))
                await writer.drain()
                if not keep:
                    return
        except _BadRequest as err:
            writer.write(head(err.status, {"Content-Length": "0", "Connection": "close"}))
        except (ConnectionError, asyncio.IncompleteReadError, asyncio.LimitOverrunError):
            pass
        finally:
            writer.close()

    return await asyncio.start_server(connection, host, port, limit=MAX_LINE)


class _BadRequest(Exception):
    def __init__(self, status: int = 400) -> None:
        super().__init__(status)
        self.status = status


async def _read_request(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> Optional[Request]:
    line = await reader.readline()
    if not line:
        return None
    try:
        method, target, _version = line.decode("latin-1").split()
    except ValueError:
        raise _BadRequest() from None
    headers: Dict[str, str] = {}
    while True:
        raw = await reader.readline()
        if raw in (b"\r\n", b"\n", b""):
            break
        name, _, value = raw.decode("latin-1").partition(":")
        headers[name.strip().lower()] = value.strip()
    if headers.get("expect", "").lower() == "100-continue":
        writer.write(b"HTTP/1.1 100 Continue\r\n\r\n")
    if headers.get("transfer-encoding", "").lower() == "chunked":
        body = await _read_chunked(reader)
    else:
        length = int(headers.get("content-length") or 0)
        if length > MAX_BODY:
            raise _BadRequest(413)
        body = await reader.readexactly(length) if length else b""
    url = urlsplit(target)
    query = {k: v[-1] for k, v in parse_qs(url.query).items()}
    peer = writer.get_extra_info("peername")
    remote = str(peer[0]) if isinstance(peer, (tuple, list)) and peer else ""
    return Request(method.upper(), unquote(url.path), query, headers, body, reader, writer, remote)


async def _read_chunked(reader: asyncio.StreamReader) -> bytes:
    chunks = []
    size_total = 0
    while True:
        size = int((await reader.readline()).split(b";")[0].strip() or b"0", 16)
        if size == 0:
            while (await reader.readline()) not in (b"\r\n", b"\n", b""):
                pass
            return b"".join(chunks)
        size_total += size
        if size_total > MAX_BODY:
            raise _BadRequest(413)
        chunks.append(await reader.readexactly(size))
        await reader.readline()


# ── WebSocket (RFC 6455) ────────────────────────────────────────────────────

#: GUID из RFC 6455 для Sec-WebSocket-Accept.
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
#: Наибольшее сообщение от клиента (вместе с продолжениями), байт.
WS_MAX_MESSAGE = 1 << 20

OP_CONT, OP_TEXT, OP_BINARY, OP_CLOSE, OP_PING, OP_PONG = 0x0, 0x1, 0x2, 0x8, 0x9, 0xA

#: Коды закрытия.
CLOSE_NORMAL, CLOSE_PROTOCOL, CLOSE_UNSUPPORTED, CLOSE_INVALID_DATA, CLOSE_TOO_BIG = 1000, 1002, 1003, 1007, 1009


class WebSocketError(Exception):
    """Нарушение протокола клиентом: соединение закрывается с ``code``."""

    def __init__(self, code: int, reason: str = "") -> None:
        super().__init__(reason or str(code))
        self.code = code
        self.reason = reason


def websocket_accept(key: str) -> str:
    """Значение Sec-WebSocket-Accept для Sec-WebSocket-Key клиента."""
    return base64.b64encode(hashlib.sha1((key + WS_GUID).encode("ascii")).digest()).decode("ascii")


def websocket_refusal(req: Request) -> Optional[Response]:
    """Почему запрос — не годный WebSocket upgrade (ответ с ошибкой), или ``None`` — годный."""
    tokens = {t.strip().lower() for t in req.headers.get("connection", "").split(",")}
    if req.method != "GET" or req.headers.get("upgrade", "").lower() != "websocket" or "upgrade" not in tokens:
        return Response(400, b"WebSocket upgrade expected")
    if req.headers.get("sec-websocket-version") != "13":
        return Response(426, b"", {"Sec-WebSocket-Version": "13"})
    try:
        if len(base64.b64decode(req.headers.get("sec-websocket-key", ""), validate=True)) != 16:
            raise ValueError
    except ValueError:
        return Response(400, b"Bad Sec-WebSocket-Key")
    return None


async def accept_websocket(req: Request, max_message: int = WS_MAX_MESSAGE) -> "WebSocket":
    """Ответ 101 на годный upgrade (проверка — ``websocket_refusal``) → соединение."""
    req.writer.write(head(101, {"Upgrade": "websocket", "Connection": "Upgrade",
                                "Sec-WebSocket-Accept": websocket_accept(req.headers["sec-websocket-key"])}))
    await req.writer.drain()
    return WebSocket(req.reader, req.writer, max_message)


def _apply_mask(data: bytes, key: bytes) -> bytes:
    if not data:
        return data
    n = len(data)
    stream = (key * (n // 4 + 1))[:n]
    return (int.from_bytes(data, "big") ^ int.from_bytes(stream, "big")).to_bytes(n, "big")


def encode_frame(opcode: int, payload: bytes = b"", *, fin: bool = True, mask: Optional[bytes] = None) -> bytes:
    """Кадр WebSocket. Сервер шлёт без маски; ``mask`` (4 байта) — кадр клиента (для тестов)."""
    n = len(payload)
    first = (0x80 if fin else 0) | opcode
    bit = 0x80 if mask else 0
    if n < 126:
        out = struct.pack("!BB", first, bit | n)
    elif n < 1 << 16:
        out = struct.pack("!BBH", first, bit | 126, n)
    else:
        out = struct.pack("!BBQ", first, bit | 127, n)
    if mask:
        return out + mask + _apply_mask(payload, mask)
    return out + payload


async def read_frame(reader: asyncio.StreamReader, max_size: int = WS_MAX_MESSAGE,
                     require_mask: bool = True) -> Tuple[bool, int, bytes]:
    """Кадр от клиента → ``(fin, opcode, payload)``; кадры клиента обязаны быть маскированы."""
    b0, b1 = await reader.readexactly(2)
    fin, opcode, masked, n = bool(b0 & 0x80), b0 & 0x0F, bool(b1 & 0x80), b1 & 0x7F
    if b0 & 0x70:
        raise WebSocketError(CLOSE_PROTOCOL, "RSV без расширений")
    if require_mask and not masked:
        raise WebSocketError(CLOSE_PROTOCOL, "кадр клиента без маски")
    if opcode >= 0x8 and (n > 125 or not fin):
        raise WebSocketError(CLOSE_PROTOCOL, "управляющий кадр длинный или фрагментирован")
    if n == 126:
        (n,) = struct.unpack("!H", await reader.readexactly(2))
    elif n == 127:
        (n,) = struct.unpack("!Q", await reader.readexactly(8))
    if n > max_size:
        raise WebSocketError(CLOSE_TOO_BIG, "сообщение слишком большое")
    key = await reader.readexactly(4) if masked else b""
    payload = await reader.readexactly(n) if n else b""
    return fin, opcode, _apply_mask(payload, key) if masked else payload


class WebSocket:
    """Серверная сторона WebSocket: текстовые сообщения, ответ на ping, закрытие."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter,
                 max_message: int = WS_MAX_MESSAGE) -> None:
        self.reader = reader
        self.writer = writer
        self.max_message = max_message
        #: Соединение закрыто (кадр close отправлен или получен, или обрыв).
        self.closed = False
        #: Код закрытия (от клиента или наш).
        self.close_code: Optional[int] = None
        self._lock = asyncio.Lock()

    async def send_text(self, text: str) -> None:
        """Текстовое сообщение одним кадром; закрытое соединение — ``ConnectionError``."""
        await self._send(OP_TEXT, text.encode("utf-8"))

    async def ping(self, data: bytes = b"") -> None:
        await self._send(OP_PING, data)

    async def _send(self, opcode: int, payload: bytes) -> None:
        if self.closed:
            raise ConnectionError("WebSocket закрыт")
        async with self._lock:
            self.writer.write(encode_frame(opcode, payload))
            await self.writer.drain()

    async def receive(self) -> Optional[str]:
        """Следующее текстовое сообщение клиента; ``None`` — соединение закрыто.

        Ping — ответ pong, pong пропускается, close — ответный close. Нарушение
        протокола, бинарное сообщение или превышение размера — закрытие с кодом.
        """
        parts: List[bytes] = []
        size = 0
        kind: Optional[int] = None
        while not self.closed:
            try:
                fin, opcode, payload = await read_frame(self.reader, self.max_message)
                if opcode == OP_PING:
                    await self._send(OP_PONG, payload)
                    continue
                if opcode == OP_PONG:
                    continue
                if opcode == OP_CLOSE:
                    code = struct.unpack("!H", payload[:2])[0] if len(payload) >= 2 else CLOSE_NORMAL
                    await self.close(code if 1000 <= code < 5000 else CLOSE_PROTOCOL)
                    self.close_code = code
                    return None
                if opcode in (OP_TEXT, OP_BINARY):
                    if kind is not None:
                        raise WebSocketError(CLOSE_PROTOCOL, "новое сообщение до конца прежнего")
                    kind = opcode
                elif opcode == OP_CONT:
                    if kind is None:
                        raise WebSocketError(CLOSE_PROTOCOL, "продолжение без начала")
                else:
                    raise WebSocketError(CLOSE_PROTOCOL, f"неизвестный opcode {opcode}")
                size += len(payload)
                if size > self.max_message:
                    raise WebSocketError(CLOSE_TOO_BIG, "сообщение слишком большое")
                parts.append(payload)
                if not fin:
                    continue
                if kind == OP_BINARY:
                    raise WebSocketError(CLOSE_UNSUPPORTED, "только текстовые сообщения")
                try:
                    return b"".join(parts).decode("utf-8")
                except UnicodeDecodeError:
                    raise WebSocketError(CLOSE_INVALID_DATA, "не UTF-8") from None
            except WebSocketError as err:
                await self.close(err.code, err.reason)
                return None
            except (ConnectionError, asyncio.IncompleteReadError, OSError):
                self.closed = True
                return None
        return None

    async def close(self, code: int = CLOSE_NORMAL, reason: str = "") -> None:
        """Отправить close (однажды) и закрыть TCP-соединение."""
        if self.closed:
            return
        try:
            await self._send(OP_CLOSE, struct.pack("!H", code)
                             + reason.encode("utf-8")[:120].decode("utf-8", "ignore").encode("utf-8"))
        except (ConnectionError, OSError):
            pass
        self.closed = True
        self.close_code = code
        self.writer.close()
