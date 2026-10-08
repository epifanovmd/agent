"""http11: WebSocket по RFC 6455 — handshake, кадры клиента с маской, ping/pong/close, ограничения."""

from __future__ import annotations

import asyncio
import os
import struct
import unittest
from typing import List, Optional

from http11 import (
    OP_BINARY, OP_CLOSE, OP_CONT, OP_PING, OP_PONG, OP_TEXT, Request, Response, accept_websocket, encode_frame,
    read_frame, serve, websocket_accept, websocket_refusal,
)

from tests.wsclient import Client


class FrameTest(unittest.IsolatedAsyncioTestCase):
    def test_accept_rfc_example(self) -> None:
        # Пример из RFC 6455 §1.3.
        self.assertEqual(websocket_accept("dGhlIHNhbXBsZSBub25jZQ=="), "s3pPLMBiTxaQ9kYGzzhZRbK+xOo=")

    def test_server_frame_unmasked_and_lengths(self) -> None:
        self.assertEqual(encode_frame(OP_TEXT, b"Hello"), b"\x81\x05Hello")
        self.assertEqual(encode_frame(OP_TEXT, b"x" * 200)[:4], b"\x81\x7e\x00\xc8")
        self.assertEqual(encode_frame(OP_TEXT, b"x" * 70000)[:10], b"\x81\x7f" + struct.pack("!Q", 70000))
        # Маскированный кадр клиента из RFC 6455 §5.7.
        self.assertEqual(encode_frame(OP_TEXT, b"Hello", mask=bytes.fromhex("37fa213d")),
                         bytes.fromhex("818537fa213d7f9f4d5158"))

    async def test_read_masked_client_frame(self) -> None:
        reader = asyncio.StreamReader()
        reader.feed_data(bytes.fromhex("818537fa213d7f9f4d5158"))
        self.assertEqual(await read_frame(reader), (True, OP_TEXT, b"Hello"))
        big = os.urandom(70000)
        reader.feed_data(encode_frame(OP_BINARY, big, fin=False, mask=b"abcd"))
        self.assertEqual(await read_frame(reader), (False, OP_BINARY, big))


class ServerTest(unittest.IsolatedAsyncioTestCase):
    """Эхо-сервер на http11.serve: сообщения клиента возвращаются с префиксом."""

    async def asyncSetUp(self) -> None:
        self.received: List[Optional[str]] = []
        self.codes: List[Optional[int]] = []

        async def handler(req: Request) -> Optional[Response]:
            if req.path != "/ws":
                return Response(200, b"plain")
            refusal = websocket_refusal(req)
            if refusal is not None:
                return refusal
            ws = await accept_websocket(req, max_message=1000)
            while True:
                text = await ws.receive()
                self.received.append(text)
                if text is None:
                    self.codes.append(ws.close_code)
                    return None
                await ws.send_text("эхо: " + text)

        self.server = await serve(handler, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self) -> None:
        self.server.close()

    async def test_handshake_echo_ping_close(self) -> None:
        c = await Client.connect(self.port, "/ws", key="dGhlIHNhbXBsZSBub25jZQ==")
        self.assertEqual(c.status, 101)
        self.assertIn("Sec-WebSocket-Accept: s3pPLMBiTxaQ9kYGzzhZRbK+xOo=", c.head)
        c.send_frame(OP_TEXT, "привет".encode())
        self.assertEqual(await c.frame(), (True, OP_TEXT, "эхо: привет".encode()))
        # Сообщение из фрагментов (continuation) и ping между ними.
        c.send_frame(OP_TEXT, b"one ", fin=False)
        c.send_frame(OP_PING, b"p1")
        c.send_frame(OP_CONT, b"two", fin=True)
        self.assertEqual(await c.frame(), (True, OP_PONG, b"p1"))
        self.assertEqual(await c.frame(), (True, OP_TEXT, "эхо: one two".encode()))
        c.send_frame(OP_PONG, b"")  # непрошеный pong пропускается
        c.send_frame(OP_CLOSE, struct.pack("!H", 1000) + b"bye")
        self.assertEqual(await c.close_code(), 1000)
        self.assertEqual(await c.reader.read(), b"", "сервер закрыл соединение")
        self.assertEqual(self.received, ["привет", "one two", None])
        self.assertEqual(self.codes, [1000])
        c.close()

    async def test_protocol_violations_close_with_code(self) -> None:
        cases = [
            ("без маски", lambda c: c.send_frame(OP_TEXT, b"x", masked=False), 1002),
            ("слишком большое", lambda c: c.send_frame(OP_TEXT, b"x" * 1001), 1009),
            ("фрагменты больше лимита", lambda c: (c.send_frame(OP_TEXT, b"x" * 600, fin=False),
                                                    c.send_frame(OP_CONT, b"x" * 600)), 1009),
            ("бинарное", lambda c: c.send_frame(OP_BINARY, b"\x00"), 1003),
            ("не UTF-8", lambda c: c.send_frame(OP_TEXT, b"\xff\xfe"), 1007),
            ("продолжение без начала", lambda c: c.send_frame(OP_CONT, b"x"), 1002),
            ("неизвестный opcode", lambda c: c.send_frame(0x3, b""), 1002),
            ("RSV", lambda c: c.writer.write(b"\xc1\x80abcd"), 1002),
        ]
        for name, send, code in cases:
            with self.subTest(name):
                c = await Client.connect(self.port, "/ws")
                send(c)
                self.assertEqual(await c.close_code(), code)
                c.close()

    async def test_bad_handshake(self) -> None:
        c = await Client.connect(self.port, "/ws", headers={"Sec-WebSocket-Version": "8"})
        self.assertEqual(c.status, 426)
        self.assertIn("Sec-WebSocket-Version: 13", c.head)
        c.close()
        c = await Client.connect(self.port, "/ws", key="short")
        self.assertEqual(c.status, 400)
        c.close()
        c = await Client.connect(self.port, "/ws", headers={"Upgrade": "h2c"})
        self.assertEqual(c.status, 400)
        c.close()
        # Обычный HTTP на том же сервере работает.
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        writer.write(b"GET / HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        self.assertTrue((await reader.read()).endswith(b"plain"))
        writer.close()


if __name__ == "__main__":
    unittest.main()
