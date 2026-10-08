"""Маршруты примера: отмена команды, удаление агента, предел тела, адрес сервера за прокси."""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Dict, Optional, Tuple

from agent_sdk.message import ENROLL_PATH, SYNC_PATH
from agent_sdk.server import Agents
from http11 import serve
from main import App

from tests.test_admin import TOKEN, Agent


class RoutesCase(unittest.IsolatedAsyncioTestCase):
    agents_options: Dict[str, Any] = {}

    async def asyncSetUp(self) -> None:
        self.agents = Agents(enroll_token=TOKEN, offline_grace_ms=0, log=lambda msg, extra: None,
                             **self.agents_options)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.app = App(self.agents, Path(tmp.name))
        self.server = await serve(self.app, "127.0.0.1", 0, body_limit=self.agents.body_limit)
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self) -> None:
        await self.app.live.close()
        await self.agents.close()
        self.server.close()

    async def http(self, method: str, path: str, body: Any = None,
                   headers: Optional[Dict[str, str]] = None) -> Tuple[int, Any]:
        def call() -> Tuple[int, Any]:
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json", **(headers or {})})
            try:
                with urllib.request.urlopen(req, timeout=3) as resp:
                    return resp.status, json.loads(resp.read())
            except urllib.error.HTTPError as err:
                with err:
                    return err.code, json.loads(err.read())

        return await asyncio.to_thread(call)

    async def raw(self, head: str) -> Tuple[int, Dict[str, Any]]:
        """Запрос без тела (заголовки — как есть) → статус и JSON ответа."""
        reader, writer = await asyncio.open_connection("127.0.0.1", self.port)
        writer.write(head.encode())
        await writer.drain()
        data = await asyncio.wait_for(reader.read(), 3)
        writer.close()
        status_line, _, rest = data.partition(b"\r\n")
        return int(status_line.split()[1]), json.loads(rest.partition(b"\r\n\r\n")[2])


class CommandAndAgentRoutesTest(RoutesCase):
    async def test_cancel_command(self) -> None:
        agent = await Agent(self.agents).start(commands=["example.run"])
        status, cmd = await self.http("POST", "/api/commands", {"name": "example.run", "agentId": agent.id})
        self.assertEqual(status, 201, cmd)
        await agent.sync()  # cmd.run ушёл агенту
        status, body = await self.http("POST", f"/api/commands/{cmd['id']}/cancel", headers={"X-Actor": "ivan"})
        self.assertEqual(status, 200, body)
        self.assertEqual((body["status"], body["error"]["code"]), ("cancelled", "CANCELLED"))
        self.assertIn({"commandId": cmd["id"]}, [m["data"] for m in await agent.sync() if m["type"] == "cmd.cancel"])
        status, body = await self.http("POST", f"/api/commands/{cmd['id']}/cancel")
        self.assertEqual((status, body["code"]), (409, "COMMAND_NOT_ACTIVE"))
        status, body = await self.http("POST", "/api/commands/nope/cancel")
        self.assertEqual((status, body["code"]), (404, "COMMAND_NOT_FOUND"))

    async def test_delete_agent(self) -> None:
        agent = await Agent(self.agents).start()
        status, body = await self.http("DELETE", f"/api/agents/{agent.id}")
        self.assertEqual((status, body["code"]), (409, "AGENT_NOT_REVOKED"))
        status, _ = await self.http("POST", f"/api/agents/{agent.id}/revoke")
        self.assertEqual(status, 200)
        status, body = await self.http("DELETE", f"/api/agents/{agent.id}")
        self.assertEqual((status, body), (200, {}))
        self.assertIsNone(await self.agents.get_agent(agent.id))
        status, body = await self.http("DELETE", f"/api/agents/{agent.id}")
        self.assertEqual((status, body["code"]), (404, "AGENT_NOT_FOUND"))


class BodyLimitTest(RoutesCase):
    agents_options = {"max_body": 1024}

    async def test_enroll_over_limit_not_read(self) -> None:
        status, body = await self.raw(f"POST {ENROLL_PATH} HTTP/1.1\r\nHost: x\r\nContent-Length: 70000\r\n\r\n")
        self.assertEqual((status, body["code"]), (413, "MESSAGE_INVALID"))
        self.assertEqual(list(self.agents._enroll_failures), ["127.0.0.1"], "в счёт неудачных регистраций")

    async def test_sync_over_max_body(self) -> None:
        status, body = await self.raw(f"POST {SYNC_PATH} HTTP/1.1\r\nHost: x\r\nContent-Length: 2048\r\n\r\n")
        self.assertEqual((status, body["code"]), (413, "MESSAGE_INVALID"))
        status, _ = await self.raw(f"POST {SYNC_PATH} HTTP/1.1\r\nHost: x\r\nTransfer-Encoding: chunked\r\n"
                                   "Connection: close\r\n\r\n800\r\n" + "x" * 2048 + "\r\n0\r\n\r\n")
        self.assertEqual(status, 413)


class ForwardedHeadersTest(RoutesCase):
    async def test_forwarded_host_ignored_without_trust_proxy(self) -> None:
        status, body = await self.http("GET", "/api/releases", headers={"X-Forwarded-Host": "example.org",
                                                                        "X-Forwarded-Proto": "https"})
        self.assertEqual(status, 200, body)
        self.assertIn(f"'http://127.0.0.1:{self.port}/api/v1/agent-link/install.sh'", body["installCommand"])


class ForwardedHeadersTrustedTest(RoutesCase):
    agents_options = {"trust_proxy": True}

    async def test_forwarded_host_with_trust_proxy(self) -> None:
        status, body = await self.http("GET", "/api/releases", headers={"X-Forwarded-Host": "example.org",
                                                                        "X-Forwarded-Proto": "https"})
        self.assertEqual(status, 200, body)
        self.assertIn("'https://example.org/api/v1/agent-link/install.sh'", body["installCommand"])


if __name__ == "__main__":
    unittest.main()
