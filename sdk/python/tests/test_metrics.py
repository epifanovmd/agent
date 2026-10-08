"""``Agents``: история метрик, inventory, отсрочка offline, отзыв, выпуск агента."""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from agent_sdk.message import INSTALL_PATH, RELEASES_PATH, Close, now_ms
from agent_sdk.server import Agents, AgentsError, MemoryStore, MetricsPoint

from tests.examples import message
from tests.server_helpers import TOKEN, FakeConn, SyncAgent, WsAgent, enroll, hello



class Case(unittest.IsolatedAsyncioTestCase):
    agents_options: Dict[str, Any] = {}

    async def asyncSetUp(self) -> None:
        self.agents = Agents(enroll_token=TOKEN, status_interval_ms=1000, metrics_interval_ms=2000,
                       **{"offline_grace_ms": 0, "metrics_store_interval_ms": 0, "metrics_retention_ms": 0,
                          **self.agents_options})

    async def asyncTearDown(self) -> None:
        await self.agents.close()


class MetricsHistoryTest(Case):
    async def test_time_correction_backfill_and_since(self) -> None:
        agent = await SyncAgent.create(self.agents)
        before = now_ms()
        # Часы агента сильно расходятся с сервером, смещения нет — время точки = момент получения.
        live = {"collectedAt": 10_000, "host": {"cpuPercent": 1}}
        await agent.send(agent.stream("metrics", live))
        after = now_ms()
        points = await self.agents.list_metrics(agent.id)
        self.assertEqual(len(points), 1)
        self.assertTrue(before <= points[0].at <= after, "без clockOffsetMs — момент получения")
        self.assertFalse(points[0].backfill)
        self.assertEqual(points[0].metrics, live)

        # Досланная точка (образец): старше живой на минуту — в истории раньше неё.
        backfill = message("metrics.backfill")
        backfill["seq"] = agent.seq + 1
        agent.seq += 1
        out = await agent.send(backfill)
        self.assertIn({"seq": backfill["seq"]}, [m["data"] for m in out if m["type"] == "ack"])
        points = await self.agents.list_metrics(agent.id)
        self.assertEqual([p.backfill for p in points], [True, False], "по возрастанию at")
        self.assertLess(points[0].at, points[1].at - 50_000)
        self.assertEqual(points[0].metrics, backfill["data"], "сообщение целиком")
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual(info.metrics, live, "agent.metrics — последняя не-backfill точка")

        # Без collectedAt — тоже момент получения.
        await agent.send(agent.stream("metrics", {"host": {}}))
        points = await self.agents.list_metrics(agent.id)
        self.assertGreaterEqual(points[-1].at, after)
        self.assertEqual(await self.agents.list_metrics(agent.id, since=points[-1].at), [], "since — строго позже")
        self.assertEqual(len(await self.agents.list_metrics(agent.id, since=points[0].at)), 2)
        self.assertEqual(points[0].to_dict().keys(), {"at", "backfill", "metrics"})

    async def test_time_by_clock_offset(self) -> None:
        agent = await SyncAgent.create(self.agents)
        # Часы агента отстают на 100 000 мс: время точки — collectedAt + clockOffsetMs.
        now = now_ms()
        collected = now - 100_000 - 3000
        await agent.send(agent.stream("metrics", {"collectedAt": collected, "clockOffsetMs": 100_000,
                                                  "host": {}}))
        points = await self.agents.list_metrics(agent.id)
        self.assertEqual(points[-1].at, now - 3000)
        # Переотправленная после обрыва точка (застряла на минуту) — тоже по collectedAt + clockOffsetMs.
        old = now - 100_000 - 60_000
        await agent.send(agent.stream("metrics", {"collectedAt": old, "clockOffsetMs": 100_000,
                                                  "host": {}}))
        points = await self.agents.list_metrics(agent.id)
        self.assertEqual(points[0].at, now - 60_000, "переотправленная — по collectedAt + clockOffsetMs")
        # Отрицательное смещение и смещение 0 тоже учитываются.
        await agent.send(agent.stream("metrics", {"collectedAt": 5_000, "clockOffsetMs": -1000, "host": {}}))
        await agent.send(agent.stream("metrics", {"collectedAt": 7_000, "clockOffsetMs": 0, "host": {}}))
        self.assertEqual([p.at for p in (await self.agents.list_metrics(agent.id))[:2]], [4_000, 7_000])
        # Смещение не число — момент получения.
        before = now_ms()
        await agent.send(agent.stream("metrics", {"collectedAt": 10, "clockOffsetMs": "x", "host": {}}))
        last = (await self.agents.list_metrics(agent.id))[-1]
        self.assertTrue(before <= last.at <= now_ms())

    async def test_metrics_event_for_every_point(self) -> None:
        got: List[Tuple[str, MetricsPoint]] = []
        async_got: List[bool] = []

        async def on_async(agent_id: str, point: MetricsPoint) -> None:
            async_got.append(point.backfill)

        def broken(agent_id: str, point: MetricsPoint) -> None:
            raise RuntimeError("сбой подписчика")

        unsubscribe = self.agents.on("metrics", lambda agent_id, point: got.append((agent_id, point)))
        self.agents.on("metrics", on_async)
        self.agents.on("metrics", broken)
        agent = await SyncAgent.create(self.agents)
        live = {"collectedAt": 1000, "clockOffsetMs": 0, "host": {"cpuPercent": 1}}
        late = {"collectedAt": 500, "clockOffsetMs": 0, "backfill": True, "host": {}}
        await agent.send(agent.stream("metrics", live), agent.stream("metrics", late))
        await asyncio.sleep(0)
        self.assertEqual([(a, p.at, p.backfill, p.metrics) for a, p in got],
                         [(agent.id, 1000, False, live), (agent.id, 500, True, late)], "и досланные")
        self.assertEqual(async_got, [False, True])
        # Повтор по seq не сохраняется — и события нет; статус — не метрики.
        dup = agent.stream("metrics", live)
        dup["seq"] = 1
        await agent.send(dup, agent.status({}))
        self.assertEqual(len(got), 2)
        unsubscribe()
        await agent.send(agent.stream("metrics", live))
        await asyncio.sleep(0)
        self.assertEqual(len(got), 2, "отписка")
        self.assertEqual(len(async_got), 3)
        self.agents.off("metrics", on_async)
        with self.assertRaises(ValueError):
            self.agents.on("point", on_async)

    async def test_example_metrics_with_interfaces_and_channels(self) -> None:
        agent = await SyncAgent.create(self.agents)
        msg = message("metrics")
        msg["seq"] = 1
        await agent.send(msg)
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual(info.metrics, msg["data"])
        self.assertEqual(info.to_dict()["metrics"]["host"]["interfaces"][1]["name"], "tun0")
        self.assertEqual(info.metrics["host"]["conntrack"], 1234)

    async def test_memory_store_keeps_last_points(self) -> None:
        store = MemoryStore(keep_metrics=3)
        self.assertEqual(MemoryStore().keep_metrics, 4320)
        for at in (5, 1, 3, 7, 6):
            await store.add_metrics("a", MetricsPoint(at=at, metrics={"n": at}))
        self.assertEqual([p.at for p in await store.list_metrics("a")], [5, 6, 7])
        self.assertEqual(await store.list_metrics("other"), [])

    async def test_inventory(self) -> None:
        agent = await SyncAgent.create(self.agents)
        msg = message("inventory")
        out = await agent.send(msg)
        self.assertIn({"seq": msg["seq"]}, [m["data"] for m in out if m["type"] == "ack"])
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual(info.inventory, msg["data"])
        self.assertEqual(info.to_dict()["inventory"]["ports"], {"tcp": [22, 443], "udp": [53]})


class AgentChangeTest(Case):
    async def test_agent_change_only_on_meaningful_updates(self) -> None:
        """«Агент изменился» — только когда есть что обновить: повторный status (пульс),
        досланная точка метрик и прогресс задач уведомлений не дают."""
        agent = await SyncAgent.create(self.agents)
        changes: List[str] = []
        self.agents.on("change", lambda c: changes.append(c.id) if c.kind == "agent" and c.id == agent.id else None)

        async def count() -> int:
            await asyncio.sleep(0.05)  # уведомления выдаются после операции ``Agents``
            return len(changes)

        base = await count()
        idle = {"state": "idle", "slots": {"q": 1}, "jobs": [], "workers": [], "outbox": 0}
        await agent.send(agent.stream("status", idle))
        await agent.send(agent.stream("status", idle))
        self.assertEqual(await count() - base, 1, "два одинаковых status — одно уведомление")
        await agent.send(agent.stream("metrics", {"collectedAt": 1, "clockOffsetMs": 0, "backfill": True}))
        await agent.send(agent.stream("job.progress", {"jobId": "нет", "attempt": 0, "progress": 0.5}))
        self.assertEqual(await count() - base, 1, "backfill и прогресс задачи не меняют агента")
        await agent.send(agent.stream("status", dict(idle, state="busy")))
        await agent.send(agent.stream("metrics", {"collectedAt": 2, "clockOffsetMs": 0}))
        self.assertEqual(await count() - base, 3, "изменившийся status и обычная точка — по уведомлению")
        self.assertTrue((await self.agents.get_agent(agent.id)).last_seen_at, "lastSeenAt сохраняется")


class RefreshTest(Case):
    async def test_refresh_delivers_changes_made_by_another_instance(self) -> None:
        """Несколько процессов с общим Store: команда и снимок, созданные другим ``Agents``,
        доходят до агента после refresh того ``Agents``, где его сессия."""
        other = Agents(enroll_token=TOKEN, store=self.agents.store)
        try:
            agent = await SyncAgent.create(self.agents, commands=["x.do"], domains={"x.refresh": None})
            await other.command("x.do", agent_id=agent.id)
            await other.set_state("x.refresh", {"n": 1})
            got = await agent.send()
            self.assertFalse([m for m in got if m["type"] in ("cmd.run", "state.put")], "без refresh не знает")
            await self.agents.refresh(agent.id)
            types = [m["type"] for m in await agent.send()]
            self.assertIn("cmd.run", types)
            self.assertIn("state.put", types)
            await self.agents.refresh()
            self.assertFalse([m for m in await agent.send() if m["type"] in ("cmd.run", "state.put")],
                             "повторный refresh — без повторной доставки")
        finally:
            await other.close()


class CallAcrossProcessesTest(Case):
    async def test_call_finds_result_saved_by_another_process(self) -> None:
        """call в процессе без сессии агента: итог сохраняет другой процесс (cmd.done
        приходит туда) — call находит его в store, а не ждёт до срока."""
        other = Agents(enroll_token=TOKEN, store=self.agents.store)
        try:
            agent = await SyncAgent.create(self.agents, commands=["x.do"])
            call = asyncio.ensure_future(other.call("x.do", agent_id=agent.id, timeout_sec=30))
            await asyncio.sleep(0.05)
            await self.agents.refresh(agent.id)  # NOTIFY → refresh в процессе сессии
            run = next(m["data"] for m in await agent.send() if m["type"] == "cmd.run")
            started = asyncio.get_running_loop().time()
            await agent.send(agent.reliable("cmd.done", {"commandId": run["commandId"], "ok": True}))
            cmd = await asyncio.wait_for(call, 5)
            self.assertEqual(cmd.status, "succeeded")
            self.assertLess(asyncio.get_running_loop().time() - started, 2.5, "по проверке store, а не по сроку")
        finally:
            await other.close()


class SharedStoreTest(Case):
    """Два ``Agents`` (процесса) на общем Store: отзыв другого процесса доходит до
    сессии этого через запись агента."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.other = Agents(enroll_token=TOKEN, store=self.agents.store, metrics_interval_ms=2000)

    async def asyncTearDown(self) -> None:
        await self.other.close()
        await super().asyncTearDown()

    async def test_refresh_closes_session_revoked_elsewhere(self) -> None:
        agent = await WsAgent.create(self.agents)
        await self.other.revoke(agent.id)
        self.assertIsNone(agent.conn.close_code, "без refresh сессия жива")
        await self.agents.refresh(agent.id)
        await agent.conn.until(lambda: agent.conn.close_code is not None)
        self.assertEqual(agent.conn.close_code, Close.UNAUTHORIZED)
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual((info.revoked, info.online), (True, False))

    async def test_message_does_not_undo_revoke_elsewhere(self) -> None:
        agent = await SyncAgent.create(self.agents)
        await self.other.revoke(agent.id)
        # Сообщение до refresh: запись сохраняется с отзывом, сессия закрывается.
        status, _ = await agent.sync([agent.status({})])
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual((info.revoked, info.online), (True, False), "отзыв не затёрт")
        status, _ = await agent.sync([])
        self.assertEqual(status, 401)

class MetricsStoreIntervalTest(Case):
    agents_options = {"metrics_store_interval_ms": 10_000}

    async def test_thinning_saves_sparse_points_but_emits_every(self) -> None:
        self.assertEqual(Agents(enroll_token=TOKEN).metrics_store_interval_ms, 15_000)
        got: List[int] = []
        self.agents.on("metrics", lambda agent_id, point: got.append(point.at))
        agent = await SyncAgent.create(self.agents)
        base = now_ms()  # точки свежие: срок хранения их не удалит
        ats = [base + d for d in (0, 5000, 10_000, 14_000, 20_000, 19_999)]
        for at in ats:
            await agent.send(agent.stream("metrics", {"collectedAt": at, "clockOffsetMs": 0, "host": {"n": at}}))
        await asyncio.sleep(0)
        self.assertEqual([p.at for p in await self.agents.list_metrics(agent.id)],
                         [base, base + 10_000, base + 20_000])
        self.assertEqual(got, ats, "событие metrics — каждая точка")
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual(info.metrics["host"], {"n": base + 20_000}, "agent.metrics — последняя по времени точка")


class MetricsBackfillThinningTest(Case):
    agents_options = {"metrics_store_interval_ms": 15_000}

    async def test_backfill_thinned_separately(self) -> None:
        got: List[Tuple[int, bool]] = []
        self.agents.on("metrics", lambda agent_id, point: got.append((point.at, point.backfill)))
        agent = await SyncAgent.create(self.agents)
        base = now_ms()
        live = [0, 5_000, 15_000, 30_000]
        late = [-40_000, -30_000, -25_000, -10_000]
        for d in live:
            await agent.send(agent.stream("metrics", {"collectedAt": base + d, "clockOffsetMs": 0}))
        for d in late:
            await agent.send(agent.stream("metrics", {"collectedAt": base + d, "clockOffsetMs": 0,
                                                      "backfill": True}))
        await asyncio.sleep(0)
        points = await self.agents.list_metrics(agent.id)
        self.assertEqual([(p.at - base, p.backfill) for p in points],
                         [(-40_000, True), (-25_000, True), (-10_000, True),
                          (0, False), (15_000, False), (30_000, False)])
        self.assertEqual(got, [(base + d, False) for d in live] + [(base + d, True) for d in late],
                         "событие metrics — на каждую точку")


class MetricsRetentionTest(Case):
    agents_options = {"metrics_retention_ms": 7 * 24 * 3600 * 1000}

    async def test_memory_store_prune(self) -> None:
        store = MemoryStore()
        for agent_id, at in (("a", 1), ("a", 5), ("a", 9), ("b", 2)):
            await store.add_metrics(agent_id, MetricsPoint(at=at))
        self.assertEqual(await store.prune_metrics(5), 2)
        self.assertEqual([p.at for p in await store.list_metrics("a")], [5, 9])
        self.assertEqual(await store.list_metrics("b"), [])
        self.assertEqual(await store.prune_metrics(0), 0)

    async def test_retention_prunes_by_option(self) -> None:
        day = 24 * 3600 * 1000
        self.assertEqual(Agents(enroll_token=TOKEN).metrics_retention_ms, 7 * day)
        store = self.agents.store
        now = now_ms()
        for at in (now - 8 * day, now - 6 * day, now):
            await store.add_metrics("a", MetricsPoint(at=at))
        self.assertEqual(await self.agents.prune_metrics(now), 1)
        self.assertEqual(len(await store.list_metrics("a")), 2)
        keep = Agents(enroll_token=TOKEN, store=store, metrics_retention_ms=0)
        self.assertEqual(await keep.prune_metrics(now + 100 * day), 0, "0 — хранить всегда")
        self.assertEqual(len(await store.list_metrics("a")), 2)

    async def test_prune_at_start(self) -> None:
        store = MemoryStore()
        await store.add_metrics("a", MetricsPoint(at=1))
        await store.add_metrics("a", MetricsPoint(at=now_ms()))
        agents = Agents(enroll_token=TOKEN, store=store)
        try:
            await agents.sweep()  # первая операция запускает фоновую сверку
            for _ in range(100):
                if len(await store.list_metrics("a")) == 1:
                    break
                await asyncio.sleep(0.01)
            self.assertEqual(len(await store.list_metrics("a")), 1, "старое удалено при старте")
        finally:
            await agents.close()


class OfflineGraceTest(Case):
    agents_options = {"offline_grace_ms": 60_000}

    async def test_offline_after_grace_only(self) -> None:
        agent = await WsAgent.create(self.agents)
        await agent.conn.close()
        await asyncio.wait_for(agent.task, 2)
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertTrue(info.online, "в пределах отсрочки — на связи")

        # Переподключился — изменений нет; снова ушёл — offline только по истечении.
        await agent.connect(name="a1")
        await agent.conn.close()
        await asyncio.wait_for(agent.task, 2)
        await self.agents.sweep(now=now_ms() + 30_000)
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertTrue(info.online)
        await self.agents.sweep(now=now_ms() + 61_000)
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertFalse(info.online)

    async def test_reconnect_cancels_grace(self) -> None:
        agent = await WsAgent.create(self.agents)
        await agent.conn.close()
        await asyncio.wait_for(agent.task, 2)
        await agent.connect(name="a1")
        await self.agents.sweep(now=now_ms() + 61_000)
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertTrue(info.online, "агент на связи: отсрочка снята")


class StaleOfflineTest(Case):
    """Процесс с сессией агента упал, не сняв ``online``: снимает сверка другого процесса по
    ``last_seen_at`` (``offline_after_ms``)."""

    async def test_new_process_keeps_neighbours_online(self) -> None:
        """Процесс, запущенный позже, не снимает ``online`` с агентов соседнего процесса."""
        agent = await WsAgent.create(self.agents, "neighbour")
        late = Agents(enroll_token=TOKEN, store=self.agents.store)
        self.addAsyncCleanup(late.close)
        late._start()  # фоновая проверка нового процесса — как при первом запросе
        await asyncio.sleep(0.1)
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertTrue(info.online, "агент на связи с соседним процессом")

    async def test_other_process_marks_stale_agent_offline(self) -> None:
        crashed = Agents(enroll_token=TOKEN, store=self.agents.store)
        self.addAsyncCleanup(crashed.close)
        agent = await SyncAgent.create(crashed)  # сессия — в «упавшем» процессе
        alerts: List[Any] = []
        changes: List[str] = []
        self.agents.on("alert", alerts.append)
        self.agents.on("change", lambda c: changes.append(c.id))
        info = await self.agents.get_agent(agent.id)
        assert info is not None and info.online and info.last_seen_at
        seen = info.last_seen_at
        self.assertEqual(self.agents.offline_after_ms, 30_000, "по умолчанию max(3 × 1000, 30000) + 0")

        await self.agents.sweep(now=seen + 29_000)
        self.assertTrue((await self.agents.get_agent(agent.id)).online, "вести свежие")
        await self.agents.sweep(now=seen + 31_000)
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertFalse(info.online)
        self.assertEqual([(a.type, a.agent_id, a.active) for a in alerts], [("offline", agent.id, True)])
        self.assertEqual(changes, [agent.id])
        await self.agents.sweep(now=seen + 60_000)
        self.assertEqual(len(alerts), 1, "повторно не поднимается")

        # Агент на связи с этим процессом — не трогается, сколько бы ни прошло.
        here = await WsAgent.create(self.agents, "b1")
        await self.agents.sweep(now=now_ms() + 10 ** 9)
        self.assertTrue((await self.agents.get_agent(here.id)).online)

    async def test_option_and_fresh_record_reread(self) -> None:
        agents = Agents(enroll_token=TOKEN, store=self.agents.store, offline_after_ms=5000, offline_grace_ms=0)
        try:
            agent = await SyncAgent.create(self.agents)
            info = await agents.get_agent(agent.id)
            assert info is not None and info.last_seen_at
            await agents.sweep(now=info.last_seen_at + 4000)
            self.assertTrue((await agents.get_agent(agent.id)).online)
            await agents.sweep(now=info.last_seen_at + 6000)
            self.assertFalse((await agents.get_agent(agent.id)).online)
        finally:
            await agents.close()


class RevokeTest(Case):
    async def test_revoke_closes_4401_and_rejects_credentials(self) -> None:
        agent = await WsAgent.create(self.agents)
        revoked = await self.agents.revoke(agent.id)
        self.assertTrue(revoked.revoked)
        await agent.conn.until(lambda: agent.conn.close_code is not None)
        self.assertEqual(agent.conn.close_code, Close.UNAUTHORIZED)
        await asyncio.wait_for(agent.task, 2)
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual((info.revoked, info.online), (True, False))
        self.assertTrue(info.to_dict()["revoked"])

        self.assertIsNone(await self.agents.authenticate(agent.auth))
        conn = FakeConn()
        conn.push(hello("a1"))
        await asyncio.wait_for(self.agents.serve_websocket(agent.auth, conn), 2)
        self.assertEqual(conn.close_code, Close.UNAUTHORIZED)
        status, body = await self.agents.handle_sync(agent.auth, {"sessionId": None, "messages": [hello("a1")]})
        self.assertEqual(status, 401)
        with self.assertRaises(AgentsError):
            await self.agents.revoke("nope")
        with self.assertRaises(AgentsError) as err:
            await self.agents.subscribe(agent.id, status={"intervalMs": 1000})
        self.assertEqual(err.exception.code, "AGENT_REVOKED")

    async def test_revoke_http_session_long_poll_gets_401(self) -> None:
        agent = await SyncAgent.create(self.agents)
        poll = asyncio.ensure_future(agent.sync([], wait=5))
        await asyncio.sleep(0.05)
        await self.agents.revoke(agent.id)
        status, body = await asyncio.wait_for(poll, 2)
        self.assertEqual(status, 401)
        status, _ = await agent.sync([])
        self.assertEqual(status, 401)


INSTALL_SH = '#!/bin/sh\nset -eu\nDEFAULT_SERVER=""\nDEFAULT_PUBLIC_KEY=""\nSERVER="$DEFAULT_SERVER"\n'


def update_hello(name: str, *, version: str = "1.0.0", os: str = "linux", arch: str = "amd64",
                 mode: str = "self") -> Dict[str, Any]:
    msg = hello(name, commands=["agent.logs", "agent.update"])
    msg["data"]["agent"]["version"] = version
    msg["data"]["host"].update({"os": os, "arch": arch})
    msg["data"]["capabilities"]["update"] = {"mode": mode}
    return msg


class ReleaseTest(Case):
    async def asyncSetUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name) / "release"
        self.dir.mkdir()
        (self.dir / "agent-linux-amd64").write_bytes(b"ELF-linux")
        (self.dir / "agent-darwin-arm64").write_bytes(b"MACHO")
        (self.dir / "secret.txt").write_text("не из манифеста")
        (Path(tmp.name) / "outside").write_text("вне каталога")
        (self.dir / "install.sh").write_text(INSTALL_SH)
        self.manifest = {"version": "1.1.0", "artifacts": [
            {"os": "linux", "arch": "amd64", "file": "agent-linux-amd64", "sha256": "aa", "signature": "c2ln"},
            {"os": "darwin", "arch": "arm64", "file": "agent-darwin-arm64", "sha256": "bb"},
        ]}
        (self.dir / "manifest.json").write_text(json.dumps(self.manifest))
        self.agents_options = {"releases_dir": str(self.dir), "public_key": "S2V5S2V5+/="}
        await super().asyncSetUp()

    async def test_serves_manifest_files_and_install_sh(self) -> None:
        status, body, kind = await self.agents.handle_release(f"{RELEASES_PATH}/manifest.json")
        self.assertEqual((status, json.loads(body), kind), (200, self.manifest, "application/json"))
        self.assertEqual(await self.agents.release(), self.manifest)
        status, body, kind = await self.agents.handle_release(f"{RELEASES_PATH}/agent-linux-amd64?x=1")
        self.assertEqual((status, body, kind), (200, b"ELF-linux", "application/octet-stream"))
        for bad in ("secret.txt", "install.sh", "../outside", "..%2Foutside", "", "agent-linux-amd64/x"):
            with self.subTest(bad):
                status, _, _ = await self.agents.handle_release(f"{RELEASES_PATH}/{bad}")
                self.assertEqual(status, 404)

        status, body, kind = await self.agents.handle_release(INSTALL_PATH, "https://fleet.example:8443/")
        self.assertEqual(status, 200)
        self.assertTrue(kind.startswith("text/x-shellscript"))
        text = body.decode()
        self.assertIn('DEFAULT_SERVER="https://fleet.example:8443"\n', text)
        self.assertIn('DEFAULT_PUBLIC_KEY="S2V5S2V5+/="\n', text)
        self.assertIn('SERVER="$DEFAULT_SERVER"', text, "остальное не тронуто")
        _, body, _ = await self.agents.handle_release(INSTALL_PATH, "http://[::1]:18090/prefix")
        self.assertIn('DEFAULT_SERVER="http://[::1]:18090/prefix"\n', body.decode())
        # Адрес с кавычками и подстановками shell (заголовок Host) — 400, а не вырезание.
        for bad in ('http://x"$(reboot)`id`', "http://x/$(id)", "http://x/a\\b", "http://x\nrm -rf /",
                    "http://x/\n", "ftp://x", "x", "", "http://x y", "http://x/`id`", 'http://x/"'):
            with self.subTest(bad):
                status, body, kind = await self.agents.handle_release(INSTALL_PATH, bad)
                self.assertEqual(status, 400)
                self.assertEqual(json.loads(body)["code"], "MESSAGE_INVALID")
                self.assertTrue(kind.startswith("application/json"))

    async def test_install_sh_base_url_option_wins_and_bad_key_skipped(self) -> None:
        logs: List[str] = []
        agents = Agents(enroll_token=TOKEN, releases_dir=str(self.dir), base_url="https://public.example",
                  public_key='abc"$(id)', log=lambda msg, extra: logs.append(msg))
        self.addAsyncCleanup(agents.close)
        status, body, _ = await agents.handle_release(INSTALL_PATH, "http://internal:8080")
        self.assertEqual(status, 200)
        text = body.decode()
        self.assertIn('DEFAULT_SERVER="https://public.example"\n', text, "опция base_url — первой")
        self.assertIn('DEFAULT_PUBLIC_KEY=""\n', text, "ключ не base64 — не подставлен")
        self.assertTrue(any("public_key" in m for m in logs), "предупреждение в лог")
        # Опция задана — адрес запроса не важен (даже некорректный).
        status, _, _ = await agents.handle_release(INSTALL_PATH, 'http://x"')
        self.assertEqual(status, 200)

    async def test_without_releases_dir(self) -> None:
        agents = Agents(enroll_token=TOKEN)
        self.assertIsNone(await agents.release())
        self.assertEqual(await agents.update_candidates(), [])
        for path in (f"{RELEASES_PATH}/manifest.json", INSTALL_PATH):
            status, _, _ = await agents.handle_release(path)
            self.assertEqual(status, 404)
        await agents.close()

    async def test_update_candidates_and_update_agent(self) -> None:
        async def connect(name: str, **kw: Any) -> SyncAgent:
            agent = SyncAgent(self.agents, *await enroll(self.agents, name))
            await agent.send(update_hello(name, **kw))
            return agent

        linux = await connect("linux")
        await connect("fresh", version="1.1.0")
        docker = await connect("docker", mode="external")
        riscv = await connect("riscv", arch="riscv64")

        candidates = await self.agents.update_candidates()
        self.assertEqual([c.to_dict() for c in candidates], [{
            "agentId": linux.id, "name": "linux", "online": True, "current": "1.0.0", "target": "1.1.0",
            "os": "linux", "arch": "amd64"}])

        cmd = await self.agents.update_agent(linux.id)
        self.assertEqual((cmd.name, cmd.timeout_sec, cmd.agent_id), ("agent.update", 300, linux.id))
        self.assertEqual(cmd.args, {"version": "1.1.0", "url": f"{RELEASES_PATH}/agent-linux-amd64",
                                    "sha256": "aa", "signature": "c2ln"})
        run = [m for m in await linux.send() if m["type"] == "cmd.run"][-1]["data"]
        want = message("cmd.run.update")["data"]
        self.assertEqual(set(run), set(want))
        self.assertEqual(set(run["args"]), set(want["args"]))

        for agent in (docker, riscv):
            with self.assertRaises(AgentsError) as err:
                await self.agents.update_agent(agent.id)
            self.assertEqual(err.exception.code, "UPDATE_NOT_AVAILABLE")
        with self.assertRaises(AgentsError) as err:
            await self.agents.update_agent("nope")
        self.assertEqual(err.exception.code, "AGENT_NOT_FOUND")

        await self.agents.revoke(linux.id)
        self.assertEqual(await self.agents.update_candidates(), [], "отозванный — не кандидат")


class WorkerReleaseTest(Case):
    async def asyncSetUp(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.dir = Path(tmp.name)
        (self.dir / "agent-linux-amd64").write_bytes(b"ELF-linux")
        (self.dir / "sysinfo-1.3.0-linux-amd64").write_bytes(b"sysinfo-1.3")
        (self.dir / "sysinfo-1.10.0-linux-amd64").write_bytes(b"sysinfo-1.10")
        (self.dir / "report-2.0.0-linux-amd64").write_bytes(b"report")
        (self.dir / "other").write_bytes(b"not in manifest")
        self.manifest = {"version": "1.1.0", "artifacts": [
            {"os": "linux", "arch": "amd64", "file": "agent-linux-amd64", "sha256": "aa", "signature": "c2ln"},
        ], "workers": [
            {"name": "sysinfo", "version": "1.10.0", "os": "linux", "arch": "amd64",
             "file": "sysinfo-1.10.0-linux-amd64", "sha256": "s10", "signature": "c2l4",
             "restart": "stop-first", "stopTimeout": "30s"},
            {"name": "sysinfo", "version": "1.3.0", "os": "linux", "arch": "amd64",
             "file": "sysinfo-1.3.0-linux-amd64", "sha256": "s13", "signature": "c2kx"},
            {"name": "report", "version": "2.0.0", "os": "linux", "arch": "amd64",
             "file": "report-2.0.0-linux-amd64", "sha256": "w2", "signature": "d2c="},
        ]}
        (self.dir / "manifest.json").write_text(json.dumps(self.manifest))
        self.agents_options = {"releases_dir": str(self.dir)}
        await super().asyncSetUp()
        self.audit: List[Any] = []
        self.agents.on("audit", self.audit.append)

    async def connect(self, name: str, workers: List[Dict[str, Any]], *, commands: Optional[List[str]] = None,
                      **kw: Any) -> SyncAgent:
        agent = SyncAgent(self.agents, *await enroll(self.agents, name))
        msg = update_hello(name, **kw)
        msg["data"]["capabilities"]["commands"] = {
            "names": ["agent.update", "worker.update"] if commands is None else commands}
        await agent.send(msg, agent.stream("status", {"state": "ready", "jobs": [], "workers": workers}))
        return agent

    async def test_release_and_files(self) -> None:
        self.assertEqual(await self.agents.release(), self.manifest)
        for file, data in (("sysinfo-1.3.0-linux-amd64", b"sysinfo-1.3"), ("report-2.0.0-linux-amd64", b"report"),
                           ("agent-linux-amd64", b"ELF-linux")):
            with self.subTest(file):
                status, body, kind = await self.agents.handle_release(f"{RELEASES_PATH}/{file}")
                self.assertEqual((status, body, kind), (200, data, "application/octet-stream"))
        status, _, _ = await self.agents.handle_release(f"{RELEASES_PATH}/other")
        self.assertEqual(status, 404, "не из манифеста — не раздаётся")

    async def test_candidates_and_update_worker(self) -> None:
        a = await self.connect("a", [
            {"name": "sysinfo", "state": "running", "instances": 1, "version": "1.3.0", "release": True},
            {"name": "report", "state": "running", "instances": 1, "version": "2.0.0", "release": True},
            {"name": "local", "state": "running", "instances": 1, "version": "0.1.0"},
        ])
        # Не объявил worker.update — не кандидат и 409.
        no_cmd = await self.connect("old-agent", [
            {"name": "sysinfo", "state": "running", "instances": 1, "version": "1.0.0", "release": True}],
            commands=["agent.update"])
        # update.mode = external (агент в контейнере), worker.update объявлен — кандидат и обновляется.
        docker = await self.connect("docker", [
            {"name": "sysinfo", "state": "running", "instances": 1, "version": "1.0.0", "release": True}],
            mode="external")
        await self.connect("arm", [
            {"name": "sysinfo", "state": "running", "instances": 1, "version": "1.0.0", "release": True}],
            arch="arm64")
        not_release = await self.connect("plain", [
            {"name": "sysinfo", "state": "running", "instances": 1, "version": "1.0.0"}])

        candidates = [c.to_dict() for c in await self.agents.worker_update_candidates()]
        self.assertEqual(candidates, [
            {"agentId": a.id, "agentName": "a", "online": True, "worker": "sysinfo", "current": "1.3.0",
             "target": "1.10.0", "os": "linux", "arch": "amd64"},
            {"agentId": docker.id, "agentName": "docker", "online": True, "worker": "sysinfo",
             "current": "1.0.0", "target": "1.10.0", "os": "linux", "arch": "amd64"},
        ])

        cmd = await self.agents.by("ivan").update_worker(a.id, "sysinfo")
        self.assertEqual((cmd.name, cmd.timeout_sec, cmd.agent_id, cmd.actor),
                         ("worker.update", 300, a.id, "ivan"))
        self.assertEqual(cmd.args, {"name": "sysinfo", "version": "1.10.0",
                                    "url": f"{RELEASES_PATH}/sysinfo-1.10.0-linux-amd64",
                                    "sha256": "s10", "signature": "c2l4"})
        run = [m for m in await a.send() if m["type"] == "cmd.run"][-1]["data"]
        self.assertEqual((run["name"], run["args"]), ("worker.update", cmd.args))
        entry = self.audit[-1]
        self.assertEqual((entry.actor, entry.action, entry.target, entry.agent_id), ("ivan", "worker.update", a.id, a.id))
        self.assertEqual(entry.details, {"worker": "sysinfo", "version": "1.10.0", "commandId": cmd.id})

        # Та же версия — команду можно послать (переустановка), кандидатом не была.
        await self.agents.update_worker(a.id, "report")
        cmd = await self.agents.update_worker(docker.id, "sysinfo")
        self.assertEqual((cmd.name, cmd.args["version"]), ("worker.update", "1.10.0"))
        for agent_id, name in ((a.id, "local"), (a.id, "missing"), (no_cmd.id, "sysinfo"),
                               (not_release.id, "sysinfo")):
            with self.subTest(name=name), self.assertRaises(AgentsError) as err:
                await self.agents.update_worker(agent_id, name)
            self.assertEqual((err.exception.code, err.exception.status), ("UPDATE_NOT_AVAILABLE", 409))
        with self.assertRaises(AgentsError) as err:
            await self.agents.update_worker("nope", "sysinfo")
        self.assertEqual(err.exception.code, "AGENT_NOT_FOUND")
        with self.assertRaises(AgentsError) as err:
            await self.agents.update_worker(a.id, "bad name")
        self.assertEqual(err.exception.code, "MESSAGE_INVALID")

        await self.agents.revoke(a.id)
        self.assertEqual([c.agent_name for c in await self.agents.worker_update_candidates()], ["docker"],
                         "отозванный — не кандидат")

    async def test_manifest_without_workers(self) -> None:
        del self.manifest["workers"]
        (self.dir / "manifest.json").write_text(json.dumps(self.manifest))
        self.assertNotIn("workers", await self.agents.release())
        self.assertEqual(await self.agents.worker_update_candidates(), [])
        status, _, _ = await self.agents.handle_release(f"{RELEASES_PATH}/report-2.0.0-linux-amd64")
        self.assertEqual(status, 404)


if __name__ == "__main__":
    unittest.main()
