"""Поток /api/ws примера (examples/API.md): snapshot, дельты, метрики и логи по подписке, подписка клиента в Agents."""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, List, Tuple

import live
from agent_sdk.server import Agents
from http11 import serve
from main import App

from tests.wsclient import Client

TOKEN = "t"


class Agent:
    """Агент по HTTP sync (вызовы handle_sync напрямую)."""

    def __init__(self, agents: Agents) -> None:
        self.agents, self.seq, self.session = agents, 0, None

    async def start(self, name: str = "a1", channels: Any = None) -> "Agent":
        status, body = await self.agents.handle_enroll({"token": TOKEN, "name": name})
        assert status == 201, body
        self.id, self.auth = body["agentId"], f"Agent {body['agentId']}.{body['secret']}"
        await self.sync({"type": "hello", "data": {
            "versions": [1], "agent": {"name": name, "version": "1.0.0", "sdk": "t", "bootId": uuid.uuid4().hex,
                                       "startedAt": 1},
            "host": {"hostname": name, "os": "linux", "arch": "amd64"}, "capabilities": {"telemetry": {"channels": channels}} if channels else {}, "jobs": []}})
        return self

    async def sync(self, *messages: Dict[str, Any]) -> List[Dict[str, Any]]:
        body = {"sessionId": self.session, "messages": list(messages), "waitSeconds": 0}
        status, reply = await self.agents.handle_sync(self.auth, json.dumps(body).encode(), base_url="http://t")
        assert status == 200, reply
        self.session = reply["sessionId"]
        return reply["messages"]

    async def metrics(self, cpu: float, **extra: Any) -> None:
        self.seq += 1
        await self.sync({"type": "metrics", "seq": self.seq,
                         "data": {"collectedAt": 1000 + self.seq, "clockOffsetMs": 0, "host": {"cpuPercent": cpu},
                                  **extra}})


class LiveTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.agents = Agents(enroll_token=TOKEN, offline_grace_ms=0)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.app = App(self.agents, Path(tmp.name))
        self.server = await serve(self.app, "127.0.0.1", 0)
        self.port = self.server.sockets[0].getsockname()[1]
        #: Вызовы agents.subscribe: (агент, kwargs) и agents.unsubscribe: (агент, id).
        self.subs: List[Tuple[str, Dict[str, Any]]] = []
        self.unsubs: List[Tuple[str, str]] = []
        real_subscribe, real_unsubscribe = self.agents.subscribe, self.agents.unsubscribe

        async def subscribe(agent_id: str, **kw: Any) -> Dict[str, Any]:
            self.subs.append((agent_id, kw))
            return await real_subscribe(agent_id, **kw)

        async def unsubscribe(agent_id: str, id: str) -> None:
            self.unsubs.append((agent_id, id))
            await real_unsubscribe(agent_id, id)

        self.agents.subscribe = subscribe  # type: ignore[method-assign]
        self.agents.unsubscribe = unsubscribe  # type: ignore[method-assign]

    async def asyncTearDown(self) -> None:
        await self.app.live.close()
        await self.agents.close()
        self.server.close()

    async def test_snapshot_then_deltas(self) -> None:
        job0 = await self.agents.enqueue("example.echo", {"n": 0})
        c = await Client.connect(self.port)
        self.assertEqual(c.status, 101)
        snap = await c.message()
        self.assertEqual(snap["type"], "snapshot")
        self.assertEqual(set(snap["data"]), {"serverTime", "agents", "jobs", "commands", "states", "events", "alerts"})
        self.assertEqual(snap["data"]["jobs"][0]["id"], job0.id)
        self.assertEqual(snap["data"]["jobs"][0]["files"], [])

        agent = await Agent(self.agents).start()
        msg = await c.until(lambda m: m["type"] == "agent")
        self.assertEqual((msg["data"]["id"], msg["data"]["online"]), (agent.id, True))

        job = await self.agents.enqueue("example.echo", {"n": 1})
        msg = await c.until(lambda m: m["type"] == "job")
        self.assertEqual(msg["data"]["id"], job.id)
        self.assertIn("files", msg["data"])

        st = await self.agents.set_state("example.kv", {"a": 1})
        msg = await c.until(lambda m: m["type"] == "state")
        self.assertEqual(msg["data"], st.to_dict())

        await agent.sync({"type": "event", "id": "ev1", "data": {"type": "example.happened", "data": {"x": 1}}})
        msg = await c.until(lambda m: m["type"] == "event")
        self.assertEqual((msg["data"]["type"], msg["data"]["agentId"]), ("example.happened", agent.id))

        # Метрики без подписки не идут (только agent с последней точкой).
        await agent.metrics(5)
        msg = await c.until(lambda m: m["type"] == "agent")
        self.assertEqual(msg["data"]["metrics"]["host"]["cpuPercent"], 5)
        c.send({"type": "ping"})  # неизвестное — пропускается
        c.close()

    async def test_metrics_only_for_subscribed_and_subscription_per_client(self) -> None:
        agent = await Agent(self.agents).start(channels=["example.app", "example.kv"])
        other = await Agent(self.agents).start("a2")
        sub = await Client.connect(self.port)
        idle = await Client.connect(self.port)
        await sub.message()
        await idle.message()

        sub.send({"type": "subscribe", "agentId": agent.id})
        await self._until(lambda: len(self.subs) == 1)
        sub_client = next(c for c in self.app.live.clients if agent.id in c.subs)
        self.assertEqual(self.subs[0], (agent.id, {
            "id": f"{sub_client.id}:{agent.id}", "ttl_ms": live.SUBSCRIPTION_TTL_MS,
            "metrics": {"intervalMs": 1000, "groups": ["diskio", "sockets", "processes", "temperatures"]},
            "logs": {"level": "info"},
            "channels": {"example.app": {"intervalMs": 1000}, "example.kv": {"intervalMs": 1000}}}),
            "подписка сразу: метрики, группы, лог с уровня клиента, каналы всех воркеров")
        out = await agent.sync()
        self.assertIn({"subscription": {"metricsIntervalMs": 1000, "metrics": live.SUBSCRIPTION_METRICS,
                                        "logLevel": "info", "channels": {"example.app": 1000, "example.kv": 1000}}},
                      [m["data"] for m in out if m["type"] == "config"])
        await agent.metrics(7)
        await other.metrics(9)
        await agent.metrics(8, backfill=True)
        points = [await sub.until(lambda m: m["type"] == "metrics") for _ in range(2)]
        self.assertEqual([(m["agentId"], m["point"]["metrics"]["host"]["cpuPercent"], m["point"]["backfill"])
                          for m in points], [(agent.id, 7, False), (agent.id, 8, True)])
        self.assertEqual(set(points[0]["point"]), {"at", "backfill", "metrics"})
        self.assertEqual(points[0]["point"]["at"], 1001, "at — collectedAt + clockOffsetMs")
        # Неподписанный клиент точек не получает.
        with self.assertRaises(asyncio.TimeoutError):
            await idle.until(lambda m: m["type"] == "metrics", timeout=0.3)

        # Второй клиент на того же агента — своя подписка (свой id).
        idle.send({"type": "subscribe", "agentId": agent.id, "logLevel": "debug"})
        await self._until(lambda: len(self.subs) == 2)
        self.assertNotEqual(self.subs[1][1]["id"], self.subs[0][1]["id"])
        self.assertEqual(self.subs[1][1]["logs"], {"level": "debug"})
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual(len(info.subscriptions), 2)
        await agent.metrics(10)
        await idle.until(lambda m: m["type"] == "metrics")
        await sub.until(lambda m: m["type"] == "metrics")

        # Отписка одного — его подписка снята; закрытие второго — снята и его.
        sub.send({"type": "unsubscribe", "agentId": agent.id})
        await self._until(lambda: len(self.unsubs) == 1)
        self.assertEqual(self.unsubs[0], (agent.id, self.subs[0][1]["id"]))
        await agent.metrics(11)
        await idle.until(lambda m: m["type"] == "metrics")
        with self.assertRaises(asyncio.TimeoutError):
            await sub.until(lambda m: m["type"] == "metrics", timeout=0.3)
        idle.close()
        await self._until(lambda: len(self.unsubs) == 2 and not self.app.live.subscribers)
        self.assertEqual(self.unsubs[1], (agent.id, self.subs[1][1]["id"]))
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual(info.subscriptions, [])
        self.assertEqual(len(self.app.live.clients), 1)
        sub.close()
        await self._until(lambda: not self.app.live.clients)

    async def test_logs_by_client_level(self) -> None:
        agent = await Agent(self.agents).start()
        other = await Agent(self.agents).start("a2")
        sub = await Client.connect(self.port)
        idle = await Client.connect(self.port)
        await sub.message()
        await idle.message()

        sub.send({"type": "subscribe", "agentId": agent.id, "logLevel": "debug"})
        await self._until(lambda: len(self.subs) == 1)
        out = await agent.sync()
        self.assertIn("debug", [m["data"]["subscription"].get("logLevel") for m in out if m["type"] == "config"])

        entries = [{"at": 1, "level": "debug", "source": "agent", "msg": "подробно"},
                   {"at": 2, "level": "warn", "source": "agent", "msg": "важно"}]
        for a in (other, agent):
            a.seq += 1
            await a.sync({"type": "log", "seq": a.seq, "data": {"entries": entries}})
        msg = await sub.until(lambda m: m["type"] == "log")
        self.assertEqual(msg, {"type": "log", "agentId": agent.id, "entries": entries})
        with self.assertRaises(asyncio.TimeoutError):
            await sub.until(lambda m: m["type"] == "log", timeout=0.3)
        with self.assertRaises(asyncio.TimeoutError):
            await idle.until(lambda m: m["type"] == "log", timeout=0.3)

        # Другой уровень: подписка обновляется сразу, записи — с нового уровня.
        sub.send({"type": "logLevel", "agentId": agent.id, "level": "warn"})
        await self._until(lambda: len(self.subs) == 2)
        self.assertEqual(self.subs[1][1]["logs"], {"level": "warn"})
        self.assertEqual(self.subs[1][1]["id"], self.subs[0][1]["id"], "та же подписка")
        agent.seq += 1
        await agent.sync({"type": "log", "seq": agent.seq, "data": {"entries": entries}})
        msg = await sub.until(lambda m: m["type"] == "log")
        self.assertEqual(msg["entries"], entries[1:])
        sub.close()
        idle.close()
        await self._until(lambda: not self.app.live.subscribers and len(self.unsubs) == 1)

    async def test_subscription_renewed_while_subscribed(self) -> None:
        old = live.SUBSCRIPTION_RENEW
        live.SUBSCRIPTION_RENEW = 0.05
        self.addCleanup(setattr, live, "SUBSCRIPTION_RENEW", old)
        agent = await Agent(self.agents).start()
        c = await Client.connect(self.port)
        await c.message()
        c.send({"type": "subscribe", "agentId": agent.id})
        c.send({"type": "subscribe", "agentId": agent.id})  # повтор не плодит продлений
        await self._until(lambda: len(self.subs) >= 3)
        self.assertEqual({(a, kw["id"]) for a, kw in self.subs}, {(agent.id, self.subs[0][1]["id"])})
        client = next(iter(self.app.live.clients))
        self.assertEqual(len(client.renewers), 1)
        c.send({"type": "unsubscribe", "agentId": agent.id})
        await self._until(lambda: not client.renewers)
        count = len(self.subs)
        await asyncio.sleep(0.2)
        self.assertEqual(len(self.subs), count, "без подписки не продлевается")
        # Неизвестный агент: подписка с ошибкой — поток жив; мусор от клиента пропускается.
        c.send_frame(1, b"not json")
        c.send({"type": "subscribe"})
        c.send({"type": "subscribe", "agentId": "nope"})
        await self._until(lambda: any(a == "nope" for a, _ in self.subs))
        await self.agents.enqueue("example.echo")
        await c.until(lambda m: m["type"] == "job")
        c.close()

    async def test_subscriptions_routes(self) -> None:
        agent = await Agent(self.agents).start()
        status, body = await self._http("POST", f"/api/agents/{agent.id}/subscriptions",
                                        {"id": "ui-1", "ttlMs": 10_000, "status": {"intervalMs": 500},
                                         "channels": {"example.app": {"intervalMs": 1000}}})
        self.assertEqual(status, 200)
        self.assertEqual(body["id"], "ui-1")
        self.assertIsInstance(body["until"], int)
        out = await agent.sync()
        self.assertIn({"subscription": {"statusIntervalMs": 500, "channels": {"example.app": 1000}}},
                      [m["data"] for m in out if m["type"] == "config"])
        status, body = await self._http("POST", f"/api/agents/{agent.id}/subscriptions",
                                        {"status": {"intervalMs": 10}})
        self.assertEqual((status, body["code"]), (400, "MESSAGE_INVALID"))
        status, body = await self._http("POST", "/api/agents/nope/subscriptions", {})
        self.assertEqual((status, body["code"]), (404, "AGENT_NOT_FOUND"))
        self.assertEqual(await self._http("DELETE", f"/api/agents/{agent.id}/subscriptions/ui-1"), (200, {}))
        out = await agent.sync()
        self.assertIn({"subscription": {}}, [m["data"] for m in out if m["type"] == "config"])

    async def test_delete_state_route_and_stream(self) -> None:
        agent = await Agent(self.agents).start()
        common = await self.agents.set_state("example.kv", {"v": "common"})
        own = await self.agents.set_state("example.kv", {"v": "a1"}, agent_id=agent.id)
        c = await Client.connect(self.port)
        await c.message()

        # Личный: удаляется, общий переиздаётся с версией больше личной.
        status, body = await self._http("DELETE", f"/api/state/example.kv?agentId={agent.id}")
        self.assertEqual(status, 200)
        self.assertEqual(body["state"]["spec"], {"v": "common"})
        self.assertGreater(body["state"]["version"], own.version)
        self.assertIsNone(body["state"].get("agentId"))
        msg = await c.until(lambda m: m["type"] in ("stateDeleted", "state"))
        self.assertEqual(msg, {"type": "stateDeleted", "domain": "example.kv", "agentId": agent.id},
                         "stateDeleted — раньше переизданного общего")
        msg = await c.until(lambda m: m["type"] == "state")
        self.assertEqual(msg["data"]["version"], body["state"]["version"])
        self.assertGreater(msg["data"]["version"], common.version)

        # Общий: удаляется, ответ — null; в потоке stateDeleted без agentId.
        status, body = await self._http("DELETE", "/api/state/example.kv")
        self.assertEqual((status, body), (200, {"state": None}))
        msg = await c.until(lambda m: m["type"] == "stateDeleted")
        self.assertEqual(msg, {"type": "stateDeleted", "domain": "example.kv"})
        self.assertEqual(await self.agents.list_states(), [])
        # Снимка не было — не ошибка.
        self.assertEqual(await self._http("DELETE", "/api/state/example.kv"), (200, {"state": None}))
        c.close()

    async def _http(self, method: str, path: str, body: Any = None) -> Tuple[int, Any]:
        def call() -> Tuple[int, Any]:
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json"} if data else {})
            try:
                with urllib.request.urlopen(req, timeout=3) as resp:
                    return resp.status, json.loads(resp.read())
            except urllib.error.HTTPError as err:
                with err:
                    return err.code, json.loads(err.read())

        return await asyncio.to_thread(call)

    async def test_slow_client_closed(self) -> None:
        old = live.MAX_QUEUE
        live.MAX_QUEUE = 3
        self.addCleanup(setattr, live, "MAX_QUEUE", old)
        client = live.Client(object())
        for i in range(5):
            client.push({"n": i})
        self.assertTrue(client.slow)
        self.assertEqual([client.queue.get_nowait() for _ in range(4)][-1], None, "сигнал закрытия")

    async def _until(self, pred: Any, timeout: float = 3.0) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not pred():
            if loop.time() > deadline:
                raise AssertionError("условие не наступило")
            await asyncio.sleep(0.01)


if __name__ == "__main__":
    unittest.main()
