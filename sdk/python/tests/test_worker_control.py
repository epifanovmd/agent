"""``Agents``: управление воркером (pause/resume), alert ``workerDegraded``, адрес агента."""

from __future__ import annotations

import unittest
from typing import Any, List

from agent_sdk.server import Agents, AgentsError, Alert, AuditEntry

from tests.examples import between, message
from tests.server_helpers import TOKEN, SyncAgent, WsAgent, enroll, hello



class Case(unittest.IsolatedAsyncioTestCase):
    trust_proxy = False

    async def asyncSetUp(self) -> None:
        self.agents = Agents(enroll_token=TOKEN, offline_grace_ms=0, metrics_interval_ms=15000,
                             trust_proxy=self.trust_proxy)
        self.audit: List[AuditEntry] = []
        self.alerts: List[Alert] = []
        self.agents.on("audit", self.audit.append)
        self.agents.on("alert", self.alerts.append)

    async def asyncTearDown(self) -> None:
        await self.agents.close()


CONTROL = ["worker.pause", "worker.resume"]


class PauseWorkerTest(Case):
    async def test_pause_and_resume_commands(self) -> None:
        a = await WsAgent.create(self.agents, commands=CONTROL)
        cmd = await self.agents.by("ivan").pause_worker(a.id, "report", ["example.convert"])
        self.assertEqual((cmd.name, cmd.args, cmd.actor), ("worker.pause", {"name": "report",
                                                                             "queues": ["example.convert"]}, "ivan"))
        run = (await a.wait("cmd.run"))[0]["data"]
        self.assertEqual((run["name"], run["args"]), ("worker.pause", {"name": "report",
                                                                       "queues": ["example.convert"]}))
        resumed = await self.agents.resume_worker(a.id, "report")
        self.assertEqual((resumed.name, resumed.args), ("worker.resume", {"name": "report"}))
        await a.wait("cmd.run", 2)

        self.assertEqual([(e.actor, e.action, e.target, e.agent_id) for e in self.audit], [
            ("ivan", "worker.pause", a.id, a.id), ("", "worker.resume", a.id, a.id)])
        self.assertEqual(self.audit[0].details, {"commandId": cmd.id, "worker": "report",
                                                 "queues": ["example.convert"]})

    async def test_pause_matches_example(self) -> None:
        want = message("cmd.run.workerPause")["data"]
        a = await WsAgent.create(self.agents, commands=CONTROL)
        await self.agents.pause_worker(a.id, want["args"]["name"], want["args"].get("queues"))
        run = (await a.wait("cmd.run"))[0]["data"]
        self.assertEqual((run["name"], run["args"], run["timeoutSec"]),
                         (want["name"], want["args"], want["timeoutSec"]))

    async def test_errors(self) -> None:
        agent_id, _ = await enroll(self.agents)
        cases = {
            "нет агента": (lambda: self.agents.pause_worker("missing", "report"), "AGENT_NOT_FOUND"),
            "имя воркера": (lambda: self.agents.pause_worker(agent_id, "a b"), "MESSAGE_INVALID"),
            "имя очереди": (lambda: self.agents.resume_worker(agent_id, "report", ["a b"]), "MESSAGE_INVALID"),
        }
        for name, (call, code) in cases.items():
            with self.subTest(name), self.assertRaises(AgentsError) as err:
                await call()
            self.assertEqual(err.exception.code, code)
        # Агент без связи: команда ждёт его подключения.
        cmd = await self.agents.pause_worker(agent_id, "report")
        self.assertEqual(cmd.status, "pending")


class WorkerDegradedTest(Case):
    def of(self) -> List[Any]:
        return [(x.active, x.worker, x.message) for x in self.alerts if x.type == "workerDegraded"]

    async def test_alert_by_worker_health(self) -> None:
        a = await WsAgent.create(self.agents)

        def status(workers: List[Any], state: str = "idle") -> Any:
            return a.stream("status", {"state": state, "slots": {}, "jobs": [], "workers": workers, "outbox": 0})

        sick = {"name": "report", "state": "running", "instances": 1, "health": "degraded",
                "message": "нет связи с базой"}
        a.push(status([sick], "degraded"), status([sick], "degraded"))
        await a.wait("ack", 2)
        self.assertEqual(self.of(), [(True, "report", "нет связи с базой")])
        self.assertIn("workerDegraded", {x.type for x in await self.agents.alerts()})

        a.push(status([{**sick, "health": "ok", "message": None}]))
        await a.wait("ack", 3)
        self.assertEqual(self.of(), [(True, "report", "нет связи с базой"), (False, "report", "нет связи с базой")])

        # Воркер пропал из списка — проблема закончилась.
        a.push(status([sick], "degraded"), status([]))
        await a.wait("ack", 5)
        self.assertEqual([x[0] for x in self.of()], [True, False, True, False])
        self.assertEqual(await self.agents.alerts(), [])

        # Без сообщения воркера — текст по умолчанию, и он же в конце.
        quiet = {**sick, "message": None}
        a.push(status([quiet], "degraded"), status([]))
        await a.wait("ack", 7)
        self.assertEqual(self.of()[4:], [(True, "report", "Воркер report не в порядке"),
                                         (False, "report", "Воркер report не в порядке")])

    async def test_revoke_ends_alerts(self) -> None:
        a = await WsAgent.create(self.agents)
        a.push(a.stream("status", {"state": "degraded", "message": "плохо", "slots": {}, "jobs": [],
                                   "workers": [], "outbox": 0}))
        await a.wait("ack")
        await self.agents.revoke(a.id)
        self.assertEqual([(x.type, x.active, x.message) for x in self.alerts],
                         [("degraded", True, "плохо"), ("degraded", False, "плохо")])
        self.assertEqual(await self.agents.alerts(), [])

    async def test_example_status_accepted(self) -> None:
        for name, env in between("agent", "server"):
            if env["type"] != "status":
                continue
            data = env["data"]
            degraded = [w for w in data.get("workers") or [] if w.get("health") == "degraded"]
            if not degraded:
                continue
            with self.subTest(name):
                a = await WsAgent.create(self.agents, name=name.replace(".", "-"))
                a.push(a.stream("status", data))
                await a.wait("ack")
                got = {x.worker for x in self.alerts if x.type == "workerDegraded" and x.agent_id == a.id}
                self.assertEqual(got, {w["name"] for w in degraded})


class AddressTest(Case):
    async def test_ws_and_sync_address_without_port(self) -> None:
        agent_id, secret = await enroll(self.agents)
        ws = WsAgent(self.agents, agent_id, secret)
        ws.remote, ws.forwarded_for = "192.0.2.10:51234", "203.0.113.5"
        await ws.connect(name="a1")
        info = await self.agents.get_agent(agent_id)
        assert info is not None
        self.assertEqual(info.address, "192.0.2.10", "без trust_proxy X-Forwarded-For не берётся")
        self.assertEqual(info.to_dict()["address"], "192.0.2.10")

        sync = SyncAgent(self.agents, agent_id, secret)
        sync.remote = "[2001:db8::1]:443"
        status, _ = await sync.sync([hello("a1")])
        self.assertEqual(status, 200)
        info = await self.agents.get_agent(agent_id)
        assert info is not None
        self.assertEqual(info.address, "2001:db8::1")

        # Сообщения сессии адрес не меняют — только подключение.
        sync.remote = "192.0.2.99"
        await sync.send(sync.status({}))
        info = await self.agents.get_agent(agent_id)
        assert info is not None
        self.assertEqual(info.address, "2001:db8::1")

    async def test_no_remote_keeps_address_empty(self) -> None:
        a = await WsAgent.create(self.agents)
        info = await self.agents.get_agent(a.id)
        assert info is not None
        self.assertIsNone(info.address)
        self.assertNotIn("address", info.to_dict())


class TrustProxyTest(Case):
    trust_proxy = True

    async def test_first_forwarded_address(self) -> None:
        agent_id, secret = await enroll(self.agents)
        ws = WsAgent(self.agents, agent_id, secret)
        ws.remote, ws.forwarded_for = "10.0.0.2", " 203.0.113.5 , 10.0.0.1"
        await ws.connect(name="a1")
        info = await self.agents.get_agent(agent_id)
        assert info is not None
        self.assertEqual(info.address, "203.0.113.5")
        ws.conn.inbox.put_nowait(None)
        await ws.task

        sync = SyncAgent(self.agents, agent_id, secret)
        sync.remote = "10.0.0.2:4000"  # без заголовка — адрес соединения
        status, _ = await sync.sync([hello("a1")])
        self.assertEqual(status, 200)
        info = await self.agents.get_agent(agent_id)
        assert info is not None
        self.assertEqual(info.address, "10.0.0.2")


if __name__ == "__main__":
    unittest.main()
