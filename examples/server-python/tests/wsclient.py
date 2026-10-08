"""Минимальный клиент WebSocket для тестов: кадры клиента — с маской."""

from __future__ import annotations

import asyncio
import base64
import json
import os
import struct
from typing import Any, Dict, Optional, Tuple

from http11 import OP_CLOSE, OP_TEXT, encode_frame, read_frame, websocket_accept


class Client:
    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter, head: str) -> None:
        self.reader, self.writer, self.head = reader, writer, head

    @classmethod
    async def connect(cls, port: int, path: str = "/api/ws", headers: Optional[Dict[str, str]] = None,
                      key: Optional[str] = None) -> "Client":
        reader, writer = await asyncio.open_connection("127.0.0.1", port)
        key = key or base64.b64encode(os.urandom(16)).decode()
        hdrs = {"Host": f"127.0.0.1:{port}", "Upgrade": "websocket", "Connection": "Upgrade",
                "Sec-WebSocket-Key": key, "Sec-WebSocket-Version": "13", **(headers or {})}
        writer.write((f"GET {path} HTTP/1.1\r\n" + "".join(f"{k}: {v}\r\n" for k, v in hdrs.items())
                      + "\r\n").encode())
        head = (await reader.readuntil(b"\r\n\r\n")).decode()
        client = cls(reader, writer, head)
        if " 101 " in head.split("\r\n")[0]:
            assert f"Sec-WebSocket-Accept: {websocket_accept(key)}" in head, head
        return client

    @property
    def status(self) -> int:
        return int(self.head.split()[1])

    def send_frame(self, opcode: int, payload: bytes, fin: bool = True, masked: bool = True) -> None:
        self.writer.write(encode_frame(opcode, payload, fin=fin, mask=os.urandom(4) if masked else None))

    def send(self, message: Dict[str, Any]) -> None:
        self.send_frame(OP_TEXT, json.dumps(message).encode())

    async def frame(self, timeout: float = 3.0) -> Tuple[bool, int, bytes]:
        """Кадр сервера (без маски)."""
        return await asyncio.wait_for(read_frame(self.reader, 1 << 26, require_mask=False), timeout)

    async def message(self, timeout: float = 3.0) -> Dict[str, Any]:
        fin, opcode, payload = await self.frame(timeout)
        assert fin and opcode == OP_TEXT, (fin, opcode, payload)
        return json.loads(payload)

    async def until(self, pred: Any, timeout: float = 3.0) -> Dict[str, Any]:
        """Первое сообщение, для которого pred(msg) — истина."""
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while True:
            msg = await self.message(max(0.01, deadline - loop.time()))
            if pred(msg):
                return msg

    async def close_code(self, timeout: float = 3.0) -> int:
        """Ждать кадр close сервера → код."""
        while True:
            _, opcode, payload = await self.frame(timeout)
            if opcode == OP_CLOSE:
                return struct.unpack("!H", payload[:2])[0]

    def close(self) -> None:
        self.writer.close()
