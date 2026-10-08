"""ASGI-приложение маршрутов агента (agent_sdk.server.asgi) — на голом ASGI, без сервера."""

from __future__ import annotations

import asyncio
import json
import unittest
from typing import Any, Dict, List, Optional, Tuple

from agent_sdk.message import ENROLL_PATH, INSTALL_PATH, LINK_PATH, SYNC_PATH, WS_CHANNEL
from agent_sdk.server import Agents
from agent_sdk.server.asgi import AgentsApp

from tests.server_helpers import TOKEN, hello


def http_scope(method: str, path: str, headers: Optional[Dict[str, str]] = None, **extra: Any) -> Dict[str, Any]:
    return {"type": "http", "method": method, "path": path, "root_path": "", "scheme": "http",
            "client": ("10.0.0.1", 5000),
            "headers": [(k.lower().encode(), v.encode()) for k, v in (headers or {}).items()], **extra}


async def call(app: Any, scope: Dict[str, Any], chunks: Optional[List[bytes]] = None,
               ) -> Tuple[int, Dict[str, str], bytes, int]:
    """Запрос → (статус, заголовки, тело, сколько раз приложение читало запрос)."""
    parts = list(chunks or [b""])
    reads = 0
    sent: List[Dict[str, Any]] = []

    async def receive() -> Dict[str, Any]:
        nonlocal reads
        reads += 1
        if parts:
            body = parts.pop(0)
            return {"type": "http.request", "body": body, "more_body": bool(parts)}
        await asyncio.sleep(3600)  # клиент на связи
        return {"type": "http.disconnect"}

    async def send(message: Dict[str, Any]) -> None:
        sent.append(message)

    await app(scope, receive, send)
    start = next(m for m in sent if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    headers = {k.decode(): v.decode() for k, v in start["headers"]}
    return start["status"], headers, body, reads


class AsgiHttpTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.agents = Agents(enroll_token=TOKEN, offline_grace_ms=0, max_body=4096)
        self.app = AgentsApp(self.agents)

    async def asyncTearDown(self) -> None:
        await self.agents.close()

    async def enroll(self) -> Tuple[str, str]:
        raw = json.dumps({"token": TOKEN, "name": "a1"}).encode()
        status, _, body, _ = await call(self.app, http_scope("POST", ENROLL_PATH), [raw])
        self.assertEqual(status, 201, body)
        reply = json.loads(body)
        return reply["agentId"], reply["secret"]

    async def test_enroll_and_sync(self) -> None:
        agent_id, secret = await self.enroll()
        auth = {"Authorization": f"Agent {agent_id}.{secret}", "Host": "app:8080"}
        msg = json.dumps({"sessionId": None, "messages": [hello("a1")], "waitSeconds": 0}).encode()
        # Тело частями — собирается целиком.
        status, headers, body, _ = await call(self.app, http_scope("POST", SYNC_PATH, auth),
                                              [msg[:10], msg[10:]])
        self.assertEqual(status, 200, body)
        self.assertTrue(headers["content-type"].startswith("application/json"))
        self.assertEqual([m["type"] for m in json.loads(body)["messages"]][:1], ["welcome"])
        stored = await self.agents.get_agent(agent_id)
        self.assertEqual(stored.address, "10.0.0.1")

    async def test_content_length_over_limit_not_read(self) -> None:
        status, _, body, reads = await call(self.app, http_scope("POST", ENROLL_PATH, {"Content-Length": "70000"}),
                                            [b"{}"])
        self.assertEqual((status, json.loads(body)["code"], reads), (413, "MESSAGE_INVALID", 0))
        status, _, _, reads = await call(self.app, http_scope("POST", SYNC_PATH, {"Content-Length": "5000"}))
        self.assertEqual((status, reads), (413, 0))

    async def test_streamed_body_over_limit(self) -> None:
        status, _, body, _ = await call(self.app, http_scope("POST", ENROLL_PATH), [b"x" * 40000, b"x" * 40000])
        self.assertEqual((status, json.loads(body)["code"]), (413, "MESSAGE_INVALID"))
        status, _, _, _ = await call(self.app, http_scope("POST", SYNC_PATH), [b"x" * 3000, b"x" * 3000])
        self.assertEqual(status, 413)

    async def test_enroll_retry_after_header(self) -> None:
        agents = Agents(enroll_token=TOKEN, enroll_failure_limit=1)
        app = AgentsApp(agents)
        bad = json.dumps({"token": "bad", "name": "x"}).encode()
        await call(app, http_scope("POST", ENROLL_PATH), [bad])
        status, headers, _, _ = await call(app, http_scope("POST", ENROLL_PATH), [bad])
        self.assertEqual(status, 429)
        self.assertIn("retry-after", headers)
        await agents.close()

    async def test_release_files_fallback_and_not_found(self) -> None:
        status, _, body, _ = await call(self.app, http_scope("GET", INSTALL_PATH))
        self.assertEqual((status, json.loads(body)["code"]), (404, "NOT_FOUND"))
        status, _, _, _ = await call(self.app, http_scope("PUT", "/files/j1/out/r.txt"), [b"data"])
        self.assertEqual(status, 200)
        status, _, body, _ = await call(self.app, http_scope("GET", "/files/j1/out/r.txt"))
        self.assertEqual((status, body), (200, b"data"))
        status, _, _, _ = await call(self.app, http_scope("GET", "/api/other"))
        self.assertEqual(status, 404)

        async def api(scope: Dict[str, Any], receive: Any, send: Any) -> None:
            await send({"type": "http.response.start", "status": 204, "headers": []})
            await send({"type": "http.response.body", "body": b""})

        status, _, _, _ = await call(AgentsApp(self.agents, fallback=api), http_scope("GET", "/api/other"))
        self.assertEqual(status, 204)

    async def test_mounted_path(self) -> None:
        """Под ``Mount`` старого вида путь без префикса: полный путь — root_path + path."""
        raw = json.dumps({"token": TOKEN, "name": "a1"}).encode()
        scope = http_scope("POST", "/enroll", root_path="/api/v1/agent-link")
        status, _, body, _ = await call(self.app, scope, [raw])
        self.assertEqual(status, 201, body)


class AsgiWebSocketTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.agents = Agents(enroll_token=TOKEN, offline_grace_ms=0)
        self.app = AgentsApp(self.agents)
        status, body = await self.agents.handle_enroll({"token": TOKEN, "name": "a1"})
        self.auth = f"Agent {body['agentId']}.{body['secret']}"
        self.agent_id = body["agentId"]

    async def asyncTearDown(self) -> None:
        await self.agents.close()

    def scope(self, auth: Optional[str], subprotocols: List[str], deny: bool = True) -> Dict[str, Any]:
        headers = [(b"host", b"app")]
        if auth:
            headers.append((b"authorization", auth.encode()))
        return {"type": "websocket", "path": LINK_PATH, "root_path": "", "scheme": "ws", "client": ("10.0.0.2", 1),
                "headers": headers, "subprotocols": subprotocols,
                "extensions": {"websocket.http.response": {}} if deny else {}}

    async def refused(self, scope: Dict[str, Any]) -> List[Dict[str, Any]]:
        sent: List[Dict[str, Any]] = []
        inbox = [{"type": "websocket.connect"}]

        async def receive() -> Dict[str, Any]:
            return inbox.pop(0)

        async def send(message: Dict[str, Any]) -> None:
            sent.append(message)

        await asyncio.wait_for(self.app(scope, receive, send), 2)
        return sent

    async def test_refusals(self) -> None:
        sent = await self.refused(self.scope(self.auth, []))
        self.assertEqual(sent[0]["status"], 426)
        sent = await self.refused(self.scope(f"Agent {self.agent_id}.bad", [WS_CHANNEL]))
        self.assertEqual((sent[0]["type"], sent[0]["status"]), ("websocket.http.response.start", 401))
        sent = await self.refused(self.scope(None, [WS_CHANNEL], deny=False))
        self.assertEqual(sent, [{"type": "websocket.close", "code": 1008}])

    async def test_session(self) -> None:
        inbox: "asyncio.Queue[Dict[str, Any]]" = asyncio.Queue()
        sent: List[Dict[str, Any]] = []
        got = asyncio.Event()

        async def send(message: Dict[str, Any]) -> None:
            sent.append(message)
            got.set()

        inbox.put_nowait({"type": "websocket.connect"})
        inbox.put_nowait({"type": "websocket.receive", "text": json.dumps(hello("a1"))})
        task = asyncio.ensure_future(self.app(self.scope(self.auth, ["other", WS_CHANNEL]), inbox.get, send))
        while not any(m["type"] == "websocket.send" for m in sent):
            got.clear()
            await asyncio.wait_for(got.wait(), 2)
        self.assertEqual(sent[0], {"type": "websocket.accept", "subprotocol": WS_CHANNEL})
        self.assertEqual(json.loads(sent[1]["text"])["type"], "welcome")
        info = await self.agents.get_agent(self.agent_id)
        self.assertEqual((info.online, info.transport, info.address), (True, "ws", "10.0.0.2"))
        inbox.put_nowait({"type": "websocket.disconnect", "code": 1000})
        await asyncio.wait_for(task, 2)
        self.assertFalse((await self.agents.get_agent(self.agent_id)).online)


class AsgiLifespanTest(unittest.IsolatedAsyncioTestCase):
    async def test_lifespan_closes_agents(self) -> None:
        agents = Agents(enroll_token=TOKEN)
        inbox = [{"type": "lifespan.startup"}, {"type": "lifespan.shutdown"}]
        sent: List[str] = []

        async def receive() -> Dict[str, Any]:
            return inbox.pop(0)

        async def send(message: Dict[str, Any]) -> None:
            sent.append(message["type"])

        await AgentsApp(agents)({"type": "lifespan"}, receive, send)
        self.assertEqual(sent, ["lifespan.startup.complete", "lifespan.shutdown.complete"])
        self.assertTrue(agents._closed)


if __name__ == "__main__":
    unittest.main()
