"""``Agents``: смена ключа, ограничение регистраций, actor и аудит, история состояния, уведомления."""

from __future__ import annotations

import asyncio
import hashlib
import inspect
import unittest
from typing import Any, List, Optional

from agent_sdk.message import Close
from agent_sdk.server import Actor, Agents, AgentsError, Alert, AuditEntry, HttpReply, MemoryStore, Store

from tests.server_helpers import TOKEN, SyncAgent, WsAgent, enroll

ROTATE = "agent.rotateKey"
NEW_SECRET = "new-secret-value"
NEW_HASH = hashlib.sha256(NEW_SECRET.encode()).hexdigest()


class Case(unittest.IsolatedAsyncioTestCase):
    grace = 0

    async def asyncSetUp(self) -> None:
        self.agents = Agents(enroll_token=TOKEN, offline_grace_ms=self.grace)
        self.audit: List[AuditEntry] = []
        self.alerts: List[Alert] = []
        self.agents.on("audit", self.audit.append)
        self.agents.on("alert", self.alerts.append)

    async def asyncTearDown(self) -> None:
        await self.agents.close()


class RotateKeyTest(Case):
    async def test_rotate_over_websocket(self) -> None:
        a = await WsAgent.create(self.agents, commands=[ROTATE])
        cmd = await self.agents.rotate_key(a.id)
        self.assertEqual((cmd.name, cmd.timeout_sec, cmd.agent_id), (ROTATE, 60, a.id))
        run = (await a.wait("cmd.run"))[0]["data"]
        self.assertEqual(run["name"], ROTATE)

        a.push(a.reliable("cmd.done", {"commandId": cmd.id, "ok": True, "result": {"secretHash": NEW_HASH}},
                          id="done1"))
        await a.conn.until(lambda: a.conn.close_code is not None)
        self.assertEqual(a.conn.close_code, Close.RESTART)
        # ack отправлен до закрытия.
        self.assertTrue(any(m["type"] == "ack" and m["data"].get("ids") == ["done1"] for m in a.conn.sent))
        stored = await self.agents.get_agent(a.id)
        assert stored is not None
        self.assertEqual(stored.pending_secret_hash, NEW_HASH)
        self.assertNotIn("pendingSecretHash", stored.to_dict())
        self.assertEqual(stored.to_record()["pendingSecretHash"], NEW_HASH)
        old_hash = stored.secret_hash

        # Старый секрет ещё действует, пока не вошли с новым.
        self.assertIsNotNone(await self.agents.authenticate(a.auth))
        # Вход с новым — повышение: старый больше не принимается.
        new_auth = f"Agent {a.id}.{NEW_SECRET}"
        self.assertIsNotNone(await self.agents.authenticate(new_auth))
        stored = await self.agents.get_agent(a.id)
        assert stored is not None
        self.assertEqual((stored.secret_hash, stored.pending_secret_hash), (NEW_HASH, ""))
        self.assertNotEqual(old_hash, NEW_HASH)
        self.assertIsNone(await self.agents.authenticate(a.auth))

        # Новая сессия с новым секретом; прежняя при закрытии не затёрла ключ.
        a.auth = new_auth
        await a.connect(name="a1", commands=[ROTATE])
        stored = await self.agents.get_agent(a.id)
        assert stored is not None
        self.assertEqual(stored.secret_hash, NEW_HASH)
        self.assertEqual([e.action for e in self.audit], ["agent.rotateKey"])
        self.assertEqual((self.audit[0].target, self.audit[0].agent_id), (a.id, a.id))

    async def test_rotate_over_http_sync_acks_then_expires(self) -> None:
        a = await SyncAgent.create(self.agents, commands=[ROTATE])
        cmd = await self.agents.rotate_key(a.id)
        await a.send()
        self.assertEqual(a.of("cmd.run")[0]["data"]["commandId"], cmd.id)
        got = await a.send(a.reliable("cmd.done", {"commandId": cmd.id, "ok": True,
                                                   "result": {"secretHash": NEW_HASH}}, id="d1"))
        self.assertEqual([m["data"] for m in got if m["type"] == "ack"], [{"ids": ["d1"]}])
        status, body = await a.sync([])
        self.assertEqual((status, body["code"]), (409, "AGENT_SESSION_EXPIRED"))
        stored = await self.agents.get_agent(a.id)
        assert stored is not None
        self.assertEqual(stored.pending_secret_hash, NEW_HASH)

    async def test_rotate_errors_and_bad_result(self) -> None:
        with self.assertRaises(AgentsError) as err:
            await self.agents.rotate_key("nope")
        self.assertEqual(err.exception.code, "AGENT_NOT_FOUND")
        plain = await WsAgent.create(self.agents, "plain", commands=["example.x"])
        with self.assertRaises(AgentsError) as err:
            await self.agents.rotate_key(plain.id)
        self.assertEqual(err.exception.code, "COMMAND_NOT_SUPPORTED")

        a = await WsAgent.create(self.agents, "a2", commands=[ROTATE])
        cmd = await self.agents.rotate_key(a.id)
        a.push(a.reliable("cmd.done", {"commandId": cmd.id, "ok": True, "result": {"secretHash": "short"}}))
        await a.wait("ack")
        stored = await self.agents.get_agent(a.id)
        assert stored is not None
        self.assertEqual(stored.pending_secret_hash, "")
        self.assertIsNone(a.conn.close_code)

        # Отозван: ошибка, ожидающий хеш очищен.
        stored.pending_secret_hash = NEW_HASH
        await self.agents.store.update_agent(stored)
        await self.agents.revoke(a.id)
        stored = await self.agents.get_agent(a.id)
        assert stored is not None
        self.assertEqual(stored.pending_secret_hash, "")
        with self.assertRaises(AgentsError) as err:
            await self.agents.rotate_key(a.id)
        self.assertEqual(err.exception.code, "AGENT_REVOKED")
        self.assertIsNone(await self.agents.authenticate(f"Agent {a.id}.{NEW_SECRET}"))


class EnrollLimitTest(unittest.IsolatedAsyncioTestCase):
    async def test_limit_per_client_and_retry_after(self) -> None:
        agents = Agents(enroll_token=TOKEN, enroll_failure_limit=3, enroll_failure_window_ms=60000)
        for _ in range(3):
            status, body = await agents.handle_enroll({"token": "bad", "name": "x"}, remote="10.0.0.1")
            self.assertEqual(status, 401)
        reply = await agents.handle_enroll({"token": TOKEN, "name": "x"}, remote="10.0.0.1")
        self.assertIsInstance(reply, HttpReply)
        status, body = reply
        self.assertEqual((status, body["code"]), (429, "ENROLL_RATE_LIMITED"))
        self.assertTrue(1 <= int(reply.headers["Retry-After"]) <= 60)
        # Другой клиент — свой счёт; удачная регистрация в счёт не идёт.
        for _ in range(5):
            status, _ = await agents.handle_enroll({"token": TOKEN, "name": "y"}, remote="10.0.0.2")
            self.assertEqual(status, 201)
        # Окно прошло — снова можно.
        agents._enroll_failures["10.0.0.1"] = [t - 61000 for t in agents._enroll_failures["10.0.0.1"]]
        status, _ = await agents.handle_enroll({"token": TOKEN, "name": "x"}, remote="10.0.0.1")
        self.assertEqual(status, 201)
        await agents.close()

    async def test_no_remote_is_one_client_and_zero_disables(self) -> None:
        agents = Agents(enroll_token=TOKEN, enroll_failure_limit=1)
        await agents.handle_enroll({"token": "bad", "name": "x"})
        status, _ = await agents.handle_enroll({"token": TOKEN, "name": "x"})
        self.assertEqual(status, 429)
        await agents.close()

        agents = Agents(enroll_token=TOKEN, enroll_failure_limit=0)
        for _ in range(20):
            await agents.handle_enroll({"token": "bad", "name": "x"})
        reply = await agents.handle_enroll({"token": TOKEN, "name": "x"})
        self.assertEqual((reply[0], reply.headers), (201, {}))
        await agents.close()

    async def test_default_limit_is_ten(self) -> None:
        agents = Agents(enroll_token=TOKEN)
        self.assertEqual((agents.enroll_failure_limit, agents.enroll_failure_window_ms), (10, 60000))
        for _ in range(10):
            await agents.handle_enroll({"token": "bad", "name": "x"}, remote="h")
        status, _ = await agents.handle_enroll({"token": "bad", "name": "x"}, remote="h")
        self.assertEqual(status, 429)
        await agents.close()


class ActorAuditTest(Case):
    async def test_by_sets_actor_and_audit(self) -> None:
        a = await WsAgent.create(self.agents, commands=["example.x"], domains={"example.kv": None})
        ivan = self.agents.by("ivan")
        self.assertIsInstance(ivan, Actor)

        job = await ivan.enqueue("example.echo", {"x": 1})
        self.assertEqual((job.actor, job.to_dict()["actor"]), ("ivan", "ivan"))
        await ivan.stop_job(job.id)
        job2 = await self.agents.enqueue("example.echo")
        self.assertIsNone(job2.actor)
        self.assertNotIn("actor", job2.to_dict())
        await ivan.cancel_job(job2.id)

        cmd = await ivan.command("example.x", agent_id=a.id)
        self.assertEqual(cmd.actor, "ivan")
        st = await ivan.set_state("example.kv", {"v": 1})
        self.assertEqual(st.actor, "ivan")
        await ivan.delete_state("example.kv")
        await ivan.revoke(a.id)

        actions = [(e.actor, e.action, e.target) for e in self.audit]
        self.assertEqual(actions, [
            ("ivan", "job.enqueue", job.id), ("ivan", "job.stop", job.id), ("", "job.enqueue", job2.id),
            ("ivan", "job.cancel", job2.id), ("ivan", "command", cmd.id), ("ivan", "state.set", "example.kv"),
            ("ivan", "state.delete", "example.kv"), ("ivan", "agent.revoke", a.id),
        ])
        self.assertEqual(self.audit[4].agent_id, a.id)
        self.assertEqual(self.audit[4].details, {"name": "example.x"})
        entry = self.audit[0].to_dict()
        self.assertEqual(set(entry), {"at", "actor", "action", "target", "details"})

    def test_actor_has_all_mutating_methods(self) -> None:
        for name in ("enqueue", "cancel_job", "stop_job", "command", "call", "set_state", "delete_state",
                     "rollback_state", "revoke", "update_agent", "update_worker", "rotate_key", "pause_worker",
                     "resume_worker"):
            self.assertTrue(inspect.iscoroutinefunction(getattr(Actor, name)), name)

    async def test_call_audited_once_with_actor(self) -> None:
        a = await WsAgent.create(self.agents, commands=["example.x"])
        fut = asyncio.ensure_future(self.agents.by("bob").call("example.x", timeout_sec=5))
        run = (await a.wait("cmd.run"))[0]["data"]
        a.push(a.reliable("cmd.done", {"commandId": run["commandId"], "ok": True}))
        cmd = await fut
        self.assertEqual((cmd.status, cmd.actor), ("succeeded", "bob"))
        self.assertEqual([(e.actor, e.action) for e in self.audit], [("bob", "command")])


class HistoryTest(Case):
    async def test_memory_store_history_limit(self) -> None:
        """``list_state_history``: новые первыми; ``limit`` ≤ 0 — все снимки."""
        store = MemoryStore()
        for v in (1, 2, 3):
            await store.set_state("example.app", None, {"v": v})
        everything = await store.list_state_history("example.app", None, 0)
        self.assertEqual([s.spec["v"] for s in everything], [3, 2, 1])
        self.assertEqual(len(await store.list_state_history("example.app", None, 2)), 2)

    async def test_history_and_rollback(self) -> None:
        v1 = await self.agents.set_state("example.kv", {"v": 1})
        v2 = await self.agents.by("ann").set_state("example.kv", {"v": 2})
        own = await self.agents.set_state("example.kv", {"v": "own"}, agent_id=(await enroll(self.agents))[0])
        hist = await self.agents.state_history("example.kv")
        self.assertEqual([s.version for s in hist], [v2.version, v1.version])
        self.assertEqual(hist[0].actor, "ann")
        self.assertEqual(len(await self.agents.state_history("example.kv", limit=1)), 1)
        self.assertEqual([s.spec for s in await self.agents.state_history("example.kv", agent_id=own.agent_id)],
                         [{"v": "own"}])

        await self.agents.delete_state("example.kv")
        self.assertEqual(len(await self.agents.state_history("example.kv")), 2)  # история не удаляется

        back = await self.agents.by("ann").rollback_state("example.kv", v1.version)
        self.assertEqual(back.spec, {"v": 1})
        self.assertGreater(back.version, own.version)
        self.assertEqual(back.actor, "ann")
        cur = await self.agents.store.get_state("example.kv", None)
        assert cur is not None
        self.assertEqual(cur.version, back.version)
        self.assertEqual(self.audit[-1].action, "state.rollback")
        self.assertEqual(self.audit[-1].details, {"fromVersion": v1.version})

        with self.assertRaises(AgentsError) as err:
            await self.agents.rollback_state("example.kv", 12345)
        self.assertEqual((err.exception.code, err.exception.status), ("STATE_VERSION_NOT_FOUND", 404))

    async def test_memory_store_keeps_last_50(self) -> None:
        store = MemoryStore()
        for i in range(55):
            await store.set_state("example.kv", None, i)
        hist = await store.list_state_history("example.kv", None, 100)
        self.assertEqual([s.spec for s in hist[:2]], [54, 53])
        self.assertEqual(len(hist), 50)
        self.assertEqual(await store.list_state_history("example.kv", "other", 10), [])

    def test_store_methods_abstract(self) -> None:
        self.assertIn("prune_metrics", Store.__abstractmethods__)
        self.assertIn("list_state_history", Store.__abstractmethods__)


class AlertsTest(Case):
    def of(self, type: str) -> List[Any]:
        return [(x.active, x.domain or x.worker, x.message) for x in self.alerts if x.type == type]

    async def test_status_alerts(self) -> None:
        a = await WsAgent.create(self.agents)

        def status(state: str, workers: List[Any], message: Optional[str] = None) -> Any:
            d = {"state": state, "slots": {}, "jobs": [], "workers": workers, "outbox": 0}
            if message:
                d["message"] = message
            return a.stream("status", d)

        down = [{"name": "echo", "state": "backoff", "instances": 0}]
        a.push(status("degraded", down, "воркеры перезапускаются"))
        a.push(status("degraded", down, "воркеры перезапускаются"))  # повтор — без события
        await a.wait("ack", 2)
        self.assertEqual(self.of("degraded"), [(True, None, "воркеры перезапускаются")])
        self.assertEqual(self.of("workerDown"), [(True, "echo", "Воркер echo: backoff")])
        self.assertEqual({x.type for x in await self.agents.alerts()}, {"degraded", "workerDown"})
        self.assertEqual(self.alerts[0].agent_name, "a1")

        a.push(status("idle", [{"name": "echo", "state": "starting", "instances": 0}]))
        await a.wait("ack", 3)
        # Конец — с текстом начала.
        self.assertEqual(self.of("degraded"), [(True, None, "воркеры перезапускаются"),
                                               (False, None, "воркеры перезапускаются")])
        self.assertEqual(self.of("workerDown"), [(True, "echo", "Воркер echo: backoff"),
                                                 (False, "echo", "Воркер echo: backoff")])
        self.assertEqual(await self.agents.alerts(), [])

        # degraded без status.message — текст по умолчанию.
        a.push(status("degraded", []))
        a.push(status("idle", []))
        await a.wait("ack", 5)
        self.assertEqual(self.of("degraded")[2:], [(True, None, "Агент не в порядке"),
                                                   (False, None, "Агент не в порядке")])

        # Воркер пропал из списка — сбой закончился.
        a.push(status("idle", down))
        a.push(status("idle", []))
        await a.wait("ack", 7)
        self.assertEqual([x[0] for x in self.of("workerDown")], [True, False, True, False])

    async def test_state_failed_and_offline(self) -> None:
        a = await WsAgent.create(self.agents, domains={"example.kv": None})
        a.push(a.reliable("state.applied", {"domain": "example.kv", "version": 5, "ok": False, "error": "boom"}))
        a.push(a.reliable("state.applied", {"domain": "example.kv", "version": 5, "ok": False, "error": "boom"}))
        a.push(a.reliable("state.applied", {"domain": "example.kv", "version": 6, "ok": True}))
        await a.wait("ack", 3)
        self.assertEqual(self.of("stateFailed"), [(True, "example.kv", "boom"), (False, "example.kv", "boom")])

        a.conn.inbox.put_nowait(None)  # обрыв: offline_grace_ms=0 — сразу offline
        await a.task
        self.assertEqual(self.of("offline"), [(True, None, "Агент без связи")])
        self.assertEqual((await self.agents.alerts())[0].type, "offline")
        await a.connect(name="a1")
        self.assertEqual(self.of("offline"), [(True, None, "Агент без связи"), (False, None, "Агент без связи")])
        self.assertEqual(await self.agents.alerts(), [])
        self.assertEqual(set(self.alerts[0].to_dict()) >= {"type", "agentId", "agentName", "active", "at"}, True)


if __name__ == "__main__":
    unittest.main()
