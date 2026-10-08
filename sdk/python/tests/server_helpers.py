"""Фейковые агенты для тестов ``Agents``: HTTP sync (вызовы handle_sync) и WebSocket (фейковое соединение)."""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Dict, List, Optional, Tuple

from agent_sdk.server import Agents

TOKEN = "test-token"


def hello(name: str = "a1", *, queues: Optional[List[Tuple[str, int]]] = None,
          commands: Optional[List[str]] = None, domains: Optional[Dict[str, Optional[int]]] = None,
          jobs: Optional[List[Dict[str, Any]]] = None, boot_id: Optional[str] = None) -> Dict[str, Any]:
    caps: Dict[str, Any] = {}
    if queues:
        caps["jobs"] = {"queues": [{"name": q, "concurrency": n} for q, n in queues]}
    if commands is not None:
        caps["commands"] = {"names": commands}
    if domains is not None:
        caps["state"] = {"domains": domains}
    return {"type": "hello", "data": {
        "versions": [1],
        "agent": {"name": name, "version": "1.0.0", "sdk": "test/1", "bootId": boot_id or uuid.uuid4().hex,
                  "startedAt": 1},
        "host": {"hostname": name, "os": "linux", "arch": "amd64"},
        "capabilities": caps, "jobs": jobs or [],
    }}


class BaseAgent:
    """Общее: учётные данные, нумерация потока, полученные сообщения."""

    def __init__(self, agents: Agents, agent_id: str, secret: str) -> None:
        self.agents = agents
        self.id = agent_id
        self.auth = f"Agent {agent_id}.{secret}"
        self.seq = 0
        self.got: List[Dict[str, Any]] = []
        #: Адрес клиента и X-Forwarded-For, которые «транспорт» передаёт Agents.
        self.remote: Optional[str] = None
        self.forwarded_for: Optional[str] = None

    def stream(self, type: str, data: Dict[str, Any]) -> Dict[str, Any]:
        self.seq += 1
        return {"type": type, "seq": self.seq, "data": data}

    @staticmethod
    def reliable(type: str, data: Dict[str, Any], id: Optional[str] = None) -> Dict[str, Any]:
        return {"type": type, "id": id or uuid.uuid4().hex, "data": data}

    def of(self, type: str) -> List[Dict[str, Any]]:
        return [m for m in self.got if m["type"] == type]

    def status(self, slots: Dict[str, int], jobs: Optional[List[Dict[str, Any]]] = None,
               state: str = "idle") -> Dict[str, Any]:
        return self.stream("status", {"state": state, "slots": slots, "jobs": jobs or [], "workers": [],
                                      "outbox": 0})


async def enroll(agents: Agents, name: str = "a1") -> Tuple[str, str]:
    status, body = await agents.handle_enroll({"token": TOKEN, "name": name})
    assert status == 201, body
    return body["agentId"], body["secret"]


class SyncAgent(BaseAgent):
    """Агент по HTTP sync: каждый вызов — запрос ``handle_sync``."""

    session: Optional[str] = None

    @classmethod
    async def create(cls, agents: Agents, name: str = "a1", **hello_kw: Any) -> "SyncAgent":
        agent_id, secret = await enroll(agents, name)
        agent = cls(agents, agent_id, secret)
        status, body = await agent.sync([hello(name, **hello_kw)])
        assert status == 200, body
        return agent

    async def sync(self, messages: List[Dict[str, Any]], wait: float = 0,
                   is_disconnected: Any = None) -> Tuple[int, Dict[str, Any]]:
        body = {"sessionId": self.session, "messages": messages, "waitSeconds": wait}
        status, reply = await self.agents.handle_sync(self.auth, json.dumps(body).encode(), is_disconnected,
                                                   base_url="http://test", remote=self.remote,
                                                   forwarded_for=self.forwarded_for)
        if status == 200:
            self.session = reply["sessionId"]
            self.got.extend(reply["messages"])
        return status, reply

    async def send(self, *messages: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Отправить и вернуть доставки этого ответа."""
        status, reply = await self.sync(list(messages))
        assert status == 200, reply
        return reply["messages"]


class FakeConn:
    """WebSocket: receive_text/send_text/close, как у Starlette."""

    def __init__(self) -> None:
        self.inbox: "asyncio.Queue[Optional[str]]" = asyncio.Queue()
        self.sent: List[Dict[str, Any]] = []
        self.close_code: Optional[int] = None
        self._changed = asyncio.Event()

    async def receive_text(self) -> str:
        item = await self.inbox.get()
        if item is None:
            raise ConnectionError("соединение закрыто")
        return item

    async def send_text(self, text: str) -> None:
        if self.close_code is not None:
            raise ConnectionError("соединение закрыто")
        self.sent.append(json.loads(text))
        self._changed.set()

    async def close(self, code: int = 1000) -> None:
        if self.close_code is None:
            self.close_code = code
        self.inbox.put_nowait(None)
        self._changed.set()

    def push(self, env: Dict[str, Any]) -> None:
        self.inbox.put_nowait(json.dumps(env))

    async def until(self, pred: Any, timeout: float = 3.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not pred():
            left = deadline - loop.time()
            if left <= 0:
                raise AssertionError(f"не дождались; отправлено: {self.sent}, закрыто: {self.close_code}")
            self._changed.clear()
            try:
                await asyncio.wait_for(self._changed.wait(), left)
            except asyncio.TimeoutError:
                pass


class WsAgent(BaseAgent):
    """Агент по WebSocket: сессия ``serve_websocket`` в фоновой задаче."""

    conn: FakeConn
    task: "asyncio.Task[None]"

    @classmethod
    async def create(cls, agents: Agents, name: str = "a1", creds: Optional[Tuple[str, str]] = None,
                     **hello_kw: Any) -> "WsAgent":
        agent_id, secret = creds or await enroll(agents, name)
        agent = cls(agents, agent_id, secret)
        await agent.connect(name=name, **hello_kw)
        return agent

    async def connect(self, **hello_kw: Any) -> None:
        self.conn = FakeConn()
        self.got = self.conn.sent
        self.task = asyncio.ensure_future(self.agents.serve_websocket(
            self.auth, self.conn, base_url="http://test", remote=self.remote, forwarded_for=self.forwarded_for))
        self.conn.push(hello(**hello_kw))
        await self.wait("welcome")

    def push(self, *messages: Dict[str, Any]) -> None:
        for m in messages:
            self.conn.push(m)

    async def wait(self, type: str, count: int = 1, **match: Any) -> List[Dict[str, Any]]:
        def found() -> List[Dict[str, Any]]:
            return [m for m in self.conn.sent if m["type"] == type
                    and all((m.get("data") or {}).get(k) == v for k, v in match.items())]

        await self.conn.until(lambda: len(found()) >= count)
        return found()
