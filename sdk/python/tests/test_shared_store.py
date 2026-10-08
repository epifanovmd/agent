"""Условная запись Store (``rev``), несколько ``Agents`` на одном Store, уборка, постраничность, удаление агента."""

from __future__ import annotations

import asyncio
import unittest
from typing import Any, List

from agent_sdk.message import now_ms
from agent_sdk.server import Agent, AgentsError, AuditEntry, Agents, Command, Job, MemoryStore, MetricsPoint
from agent_sdk.server.model import AgentEvent

from tests.server_helpers import TOKEN, SyncAgent, hello

Q = "example.echo"


class YieldingStore(MemoryStore):
    """MemoryStore, который уступает цикл событий перед каждым обращением: как настоящая БД —
    операции разных ``Agents`` перемежаются."""

    async def get_agent(self, agent_id: str) -> Any:
        await asyncio.sleep(0)
        return await super().get_agent(agent_id)

    async def update_agent(self, agent: Agent) -> bool:
        await asyncio.sleep(0)
        return await super().update_agent(agent)

    async def get_job(self, job_id: str) -> Any:
        await asyncio.sleep(0)
        return await super().get_job(job_id)

    async def update_job(self, job: Job) -> bool:
        await asyncio.sleep(0)
        return await super().update_job(job)

    async def list_jobs(self, **kw: Any) -> List[Job]:
        await asyncio.sleep(0)
        return await super().list_jobs(**kw)


class MemoryStoreRevTest(unittest.IsolatedAsyncioTestCase):
    async def test_conditional_update(self) -> None:
        store = MemoryStore()
        await store.create_agent(Agent(id="a", name="a"))
        await store.create_job(Job(id="j", queue=Q))
        await store.create_command(Command(id="c", agent_id="a", name="example.run"))
        for get, update in ((store.get_agent, store.update_agent), (store.get_job, store.update_job),
                            (store.get_command, store.update_command)):
            rid = {store.get_agent: "a", store.get_job: "j", store.get_command: "c"}[get]
            first, second = await get(rid), await get(rid)
            self.assertEqual(first.rev, 0)
            self.assertTrue(await update(first))
            self.assertEqual(first.rev, 1, "при успехе rev записи растёт")
            self.assertEqual((await get(rid)).rev, 1)
            self.assertFalse(await update(second), "запись изменилась с чтения — не пишется")
            self.assertEqual(second.rev, 0)
            second.rev = 1
            self.assertTrue(await update(second))
        self.assertFalse(await store.update_job(Job(id="nope", queue=Q)), "записи нет — False")
        self.assertNotIn("rev", (await store.get_job("j")).to_dict())
        self.assertEqual((await store.get_job("j")).to_record()["rev"], 2)
        self.assertEqual(Job.from_record((await store.get_job("j")).to_record()).rev, 2)

    async def test_pages(self) -> None:
        store = MemoryStore()
        for i in range(5):
            await store.create_job(Job(id=f"j{i}", queue=Q if i % 2 == 0 else "example.other"))
            await store.create_command(Command(id=f"c{i}", agent_id="a", name="example.run"))
        self.assertEqual([j.id for j in await store.list_jobs(limit=2)], ["j4", "j3"])
        self.assertEqual([j.id for j in await store.list_jobs(limit=2, after="j3")], ["j2", "j1"])
        self.assertEqual([j.id for j in await store.list_jobs(after="j1")], ["j0"])
        self.assertEqual([j.id for j in await store.list_jobs(queue=Q, after="j3")], ["j2", "j0"],
                         "after — по общему порядку, даже если сама запись не под фильтром")
        self.assertEqual(await store.list_jobs(after="nope"), [])
        self.assertEqual([c.id for c in await store.list_commands(limit=3, after="c4")], ["c3", "c2", "c1"])
        self.assertEqual(len(await store.list_commands(limit=0)), 5)

    async def test_prune(self) -> None:
        store = MemoryStore()
        await store.create_job(Job(id="old", queue=Q, status="completed", finished_at=100))
        await store.create_job(Job(id="new", queue=Q, status="failed", finished_at=300))
        await store.create_job(Job(id="active", queue=Q, status="running"))
        await store.create_command(Command(id="c-old", agent_id="a", name="x", status="succeeded", finished_at=100))
        await store.create_command(Command(id="c-run", agent_id="a", name="x", status="running"))
        await store.add_event(AgentEvent(agent_id="a", agent_name="a", source="agent", type="t", at=100))
        await store.add_event(AgentEvent(agent_id="a", agent_name="a", source="agent", type="t", at=300))
        self.assertEqual(await store.prune(), 0, "без сроков — ничего")
        self.assertEqual(await store.prune(jobs_before=200, commands_before=200, events_before=200), 3)
        self.assertEqual({j.id for j in await store.list_jobs()}, {"new", "active"})
        self.assertEqual([c.id for c in await store.list_commands()], ["c-run"])
        self.assertEqual([e.at for e in await store.list_events(0)], [300])


class SharedStoreTest(unittest.IsolatedAsyncioTestCase):
    """Два ``Agents`` (процесса бэкенда) на одном хранилище."""

    async def asyncSetUp(self) -> None:
        self.store = YieldingStore()
        self.a1 = Agents(enroll_token=TOKEN, store=self.store, offline_grace_ms=0)
        self.a2 = Agents(enroll_token=TOKEN, store=self.store, offline_grace_ms=0)

    async def asyncTearDown(self) -> None:
        await self.a1.close()
        await self.a2.close()

    async def test_queued_job_goes_to_one_agent(self) -> None:
        x = await SyncAgent.create(self.a1, "x", queues=[(Q, 1)])
        y = await SyncAgent.create(self.a2, "y", queues=[(Q, 1)])
        job = await self.a1.enqueue(Q, {"n": 1})  # у x ещё нет status: задача ждёт
        self.assertEqual(job.status, "queued")
        await asyncio.gather(x.send(x.status({Q: 1})), y.send(y.status({Q: 1})))
        assigned = x.of("job.assign") + y.of("job.assign")
        self.assertEqual(len(assigned), 1, "задачу выдали одному агенту")
        stored = await self.store.get_job(job.id)
        self.assertEqual(stored.status, "running")
        self.assertIn(stored.agent_id, (x.id, y.id))

    async def test_revoke_not_overwritten_by_other_process(self) -> None:
        agent = await SyncAgent.create(self.a2, "x")
        await asyncio.gather(self.a1.revoke(agent.id),
                             agent.sync([agent.stream("status", {"state": "idle", "slots": {}, "jobs": []})]))
        stored = await self.store.get_agent(agent.id)
        self.assertTrue(stored.revoked, "сообщение в другом процессе не затёрло отзыв")
        self.assertFalse(stored.online)
        status, body = await agent.sync([])
        self.assertEqual((status, body["code"]), (401, "AGENT_CREDENTIALS_INVALID"))

    async def test_subscription_survives_messages_of_other_process(self) -> None:
        agent = await SyncAgent.create(self.a2, "x")
        await asyncio.gather(self.a1.subscribe(agent.id, id="s1", status={"intervalMs": 1000}),
                             agent.send(agent.stream("inventory", {"os": "linux"})))
        stored = await self.store.get_agent(agent.id)
        self.assertEqual([s["id"] for s in stored.subscriptions], ["s1"])
        self.assertEqual(stored.inventory, {"os": "linux"})

    async def test_stream_repeat_after_reconnect_not_processed_twice(self) -> None:
        points: List[Any] = []
        self.a2.on("metrics", lambda agent_id, point: points.append(point))
        agent = await SyncAgent.create(self.a1, "x", boot_id="boot-1")
        metrics = agent.stream("metrics", {"collectedAt": now_ms(), "clockOffsetMs": 0})
        await agent.send(metrics)
        self.assertEqual(len(await self.store.list_metrics(agent.id)), 1)
        # Ответ потерян, агент переподключился к другому процессу и повторил то же сообщение.
        agent.agents, agent.session = self.a2, None
        await agent.send(hello("x", boot_id="boot-1"))
        got = await agent.send(metrics)
        self.assertEqual([m["data"] for m in got if m["type"] == "ack"], [{"seq": metrics["seq"]}])
        self.assertEqual(len(await self.store.list_metrics(agent.id)), 1, "повтор не обработан")
        self.assertEqual(points, [])
        # Новый запуск агента (другой bootId) — нумерация заново.
        agent.session, agent.seq = None, 0
        await agent.send(hello("x", boot_id="boot-2"))
        await agent.send(agent.stream("metrics", {"collectedAt": now_ms() + 20_000, "clockOffsetMs": 0}))
        self.assertEqual(len(points), 1)

    async def test_sweep_keeps_lease_extended_elsewhere(self) -> None:
        agent = await SyncAgent.create(self.a1, "x", queues=[(Q, 1)])
        await agent.send(agent.status({Q: 1}))
        job = await self.a1.enqueue(Q, {})
        stale = await self.store.get_job(job.id)
        self.assertEqual(stale.status, "running")
        # Аренду продлил status в процессе a1; сверка a2 видит старую запись.
        fresh = await self.store.get_job(job.id)
        fresh.lease_until = stale.lease_until + 60_000
        self.assertTrue(await self.store.update_job(fresh))
        self.assertFalse(await self.a2._fail_attempt(stale, "LEASE_EXPIRED", "истекла", True,
                                                     expired_at=stale.lease_until + 1, cancel=True))
        await self.a2.sweep(now=stale.lease_until + 1)
        self.assertEqual((await self.store.get_job(job.id)).status, "running")
        await self.a2.sweep(now=fresh.lease_until + 1)
        after = await self.store.get_job(job.id)
        self.assertEqual((after.status, after.error["code"]), ("failed", "LEASE_EXPIRED"))

    async def test_alerts_visible_to_other_process(self) -> None:
        events: List[Any] = []
        self.a2.on("alert", events.append)
        agent = await SyncAgent.create(self.a1, "x")
        await agent.send(agent.status({}, state="degraded"))
        self.assertEqual([a.type for a in await self.a2.alerts()], ["degraded"])
        self.assertEqual(events, [], "событие alert — у процесса, который записал изменение")
        await self.a2.revoke(agent.id)
        self.assertEqual(await self.a1.alerts(), [], "отзыв заканчивает все проблемы")
        await asyncio.sleep(0)
        self.assertEqual([(a.type, a.active) for a in events], [("degraded", False)])


class DeleteAgentTest(unittest.IsolatedAsyncioTestCase):
    async def test_delete_only_revoked(self) -> None:
        agents = Agents(enroll_token=TOKEN, offline_grace_ms=0)
        audits: List[AuditEntry] = []
        agents.on("audit", audits.append)
        agent = await SyncAgent.create(agents, "x", queues=[(Q, 1)])
        await agents.store.add_metrics(agent.id, MetricsPoint(at=now_ms()))
        job = await agents.enqueue(Q, {})
        with self.assertRaises(AgentsError) as err:
            await agents.delete_agent(agent.id)
        self.assertEqual((err.exception.code, err.exception.status), ("AGENT_NOT_REVOKED", 409))
        with self.assertRaises(AgentsError) as err:
            await agents.delete_agent("nope")
        self.assertEqual((err.exception.code, err.exception.status), ("AGENT_NOT_FOUND", 404))
        await agents.revoke(agent.id)
        await agents.by("ivan").delete_agent(agent.id)
        self.assertIsNone(await agents.get_agent(agent.id))
        self.assertEqual(await agents.list_metrics(agent.id), [])
        self.assertIsNotNone(await agents.get_job(job.id), "задачи остаются")
        self.assertEqual([(a.action, a.actor, a.target) for a in audits if a.action == "agent.delete"],
                         [("agent.delete", "ivan", agent.id)])
        await agents.close()


class AgentsPruneAndPagesTest(unittest.IsolatedAsyncioTestCase):
    async def test_prune_and_pages(self) -> None:
        agents = Agents(enroll_token=TOKEN)
        ids = [(await agents.enqueue(Q, {"n": i})).id for i in range(3)]
        await agents.cancel_job(ids[0])
        page = await agents.list_jobs(limit=2)
        self.assertEqual([j.id for j in page], ids[::-1][:2])
        self.assertEqual([j.id for j in await agents.list_jobs(limit=2, after=page[-1].id)], [ids[0]])
        self.assertEqual(await agents.prune(jobs_older_than_ms=60_000), 0, "отменена только что")
        old = await agents.store.get_job(ids[0])
        old.finished_at = now_ms() - 120_000
        self.assertTrue(await agents.store.update_job(old))
        self.assertEqual(await agents.prune(jobs_older_than_ms=60_000, commands_older_than_ms=0), 1)
        self.assertEqual({j.id for j in await agents.list_jobs()}, set(ids[1:]))
        await agents.close()


class JobRetryTest(unittest.IsolatedAsyncioTestCase):
    async def test_retry_resets_attempt_fields(self) -> None:
        agents = Agents(enroll_token=TOKEN, offline_grace_ms=0)
        agent = await SyncAgent.create(agents, "x", queues=[(Q, 1)])
        await agent.send(agent.status({Q: 1}))
        job = await agents.enqueue(Q, {}, max_attempts=2)
        ref = {"jobId": job.id, "attempt": 0}
        await agent.send(agent.reliable("job.accept", ref))
        await agent.send(agent.status({Q: 0}, jobs=[ref]))  # слотов нет: повтор останется ждать
        await agent.send(agent.reliable("job.progress", {**ref, "progress": 0.5}))
        await agents.stop_job(job.id)
        before = await agents.get_job(job.id)
        self.assertTrue(before.stop_requested)
        self.assertGreater(before.lease_until, 0)
        await agent.send(agent.reliable("job.fail", {**ref, "code": "BOOM", "message": "x", "retryable": True}))
        after = await agents.get_job(job.id)
        self.assertEqual((after.attempt, after.agent_id, after.accepted), (1, None, False))
        self.assertEqual((after.stop_requested, after.progress, after.lease_until, after.event_seq),
                         (False, 0, 0, 0))
        self.assertEqual(after.status, "queued")
        await agents.close()


if __name__ == "__main__":
    unittest.main()
