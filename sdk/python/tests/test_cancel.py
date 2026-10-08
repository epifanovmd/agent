"""Отмена команды: ``cancel_command`` (§6.4 ``cmd.cancel``)."""

from __future__ import annotations

import asyncio
import unittest
from typing import List

from agent_sdk.server import AgentsError, AuditEntry, Agents, Command, MemoryStore

from tests.examples import message
from tests.server_helpers import TOKEN, SyncAgent, WsAgent, hello

CMD = "example.run"


class CancelCommandTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.agents = Agents(enroll_token=TOKEN, offline_grace_ms=0)
        self.audits: List[AuditEntry] = []
        self.agents.on("audit", self.audits.append)

    async def asyncTearDown(self) -> None:
        await self.agents.close()

    async def test_pending_not_sent_cancelled_silently(self) -> None:
        agent = await SyncAgent.create(self.agents, commands=[])  # команду не объявил: не отправляется
        cmd = await self.agents.command(CMD, agent_id=agent.id)
        cancelled = await self.agents.by("ivan").cancel_command(cmd.id)
        self.assertEqual(cancelled.status, "cancelled")
        self.assertEqual(cancelled.error, {"code": "CANCELLED", "message": "Команду отменили"})
        self.assertIsNotNone(cancelled.finished_at)
        self.assertTrue(cancelled.finished())
        got = await agent.send()
        self.assertEqual([m for m in got if m["type"] in ("cmd.cancel", "cmd.run")], [])
        self.assertEqual([(a.action, a.actor, a.target, a.agent_id) for a in self.audits
                          if a.action == "command.cancel"], [("command.cancel", "ivan", cmd.id, agent.id)])

    async def test_sent_command_gets_cmd_cancel_and_late_done_ignored(self) -> None:
        agent = await WsAgent.create(self.agents, commands=[CMD])
        cmd = await self.agents.command(CMD, agent_id=agent.id)
        await agent.wait("cmd.run", commandId=cmd.id)
        waiter = asyncio.ensure_future(self.agents.call(CMD, agent_id=agent.id))
        run2 = (await agent.wait("cmd.run", 2))[-1]["data"]["commandId"]
        await self.agents.cancel_command(cmd.id)
        cancel = (await agent.wait("cmd.cancel", commandId=cmd.id))[0]
        sample = message("cmd.cancel@server")
        self.assertEqual((cancel["type"], set(cancel["data"])), (sample["type"], set(sample["data"])))
        # Поздний итог агента (прерванная команда) — ack без ошибки, итог не меняется.
        done = message("cmd.done.cancelled")
        done["data"]["commandId"] = cmd.id
        agent.push(done)
        await agent.wait("ack")
        self.assertEqual(agent.of("error"), [])
        stored = await self.agents.get_command(cmd.id)
        self.assertEqual((stored.status, stored.error["code"]), ("cancelled", "CANCELLED"))
        # Ждущий call отменённой команды возвращается сразу.
        await self.agents.cancel_command(run2)
        result = await asyncio.wait_for(waiter, 2)
        self.assertEqual(result.status, "cancelled")

    async def test_running_command_gets_cmd_cancel(self) -> None:
        agent = await SyncAgent.create(self.agents, commands=[CMD])
        cmd = await self.agents.command(CMD, agent_id=agent.id)
        await agent.send()
        await agent.send(agent.reliable("cmd.accept", {"commandId": cmd.id}))
        self.assertEqual((await self.agents.get_command(cmd.id)).status, "running")
        await self.agents.cancel_command(cmd.id)
        got = await agent.send()
        self.assertEqual([m["data"] for m in got if m["type"] == "cmd.cancel"], [{"commandId": cmd.id}])
        status, reply = await agent.sync([agent.reliable("cmd.output", {"commandId": cmd.id, "chunk": "late"})])
        self.assertEqual(status, 200, reply)
        self.assertEqual((await self.agents.get_command(cmd.id)).output, "", "вывод после отмены не пишется")

    async def test_errors(self) -> None:
        agent = await SyncAgent.create(self.agents, commands=[CMD])
        with self.assertRaises(AgentsError) as err:
            await self.agents.cancel_command("nope")
        self.assertEqual((err.exception.code, err.exception.status), ("COMMAND_NOT_FOUND", 404))
        cmd = await self.agents.command(CMD, agent_id=agent.id)
        await agent.send()
        await agent.send(agent.reliable("cmd.done", {"commandId": cmd.id, "ok": True}))
        with self.assertRaises(AgentsError) as err:
            await self.agents.cancel_command(cmd.id)
        self.assertEqual((err.exception.code, err.exception.status), ("COMMAND_NOT_ACTIVE", 409))
        self.assertEqual((await self.agents.get_command(cmd.id)).status, "succeeded")

    async def test_cancel_from_other_process_sent_on_refresh_once(self) -> None:
        store = MemoryStore()
        a1 = Agents(enroll_token=TOKEN, store=store, offline_grace_ms=0)
        a2 = Agents(enroll_token=TOKEN, store=store, offline_grace_ms=0)
        try:
            agent = await SyncAgent.create(a1, commands=[CMD])
            cmd = await a1.command(CMD, agent_id=agent.id)
            await agent.send()
            await a2.cancel_command(cmd.id)  # сессии агента в a2 нет: сообщения нет
            self.assertEqual([m for m in await agent.send() if m["type"] == "cmd.cancel"], [])
            await a1.refresh()
            self.assertEqual([m["data"] for m in await agent.send() if m["type"] == "cmd.cancel"],
                             [{"commandId": cmd.id}])
            await a1.refresh()
            self.assertEqual([m for m in await agent.send() if m["type"] == "cmd.cancel"], [], "один раз")
        finally:
            await a1.close()
            await a2.close()

    async def test_running_from_previous_session_cancelled_on_refresh(self) -> None:
        store = MemoryStore()
        a1 = Agents(enroll_token=TOKEN, store=store, offline_grace_ms=0)
        a2 = Agents(enroll_token=TOKEN, store=store, offline_grace_ms=0)
        try:
            agent = await SyncAgent.create(a1, commands=[CMD])
            cmd = await a1.command(CMD, agent_id=agent.id)
            await agent.send()
            await agent.send(agent.reliable("cmd.accept", {"commandId": cmd.id}))
            # Агент переподключился к другому процессу; команда выполняется с прошлой сессии.
            agent.agents, agent.session = a2, None
            await agent.send(hello("a1", commands=[CMD]))
            await a1.cancel_command(cmd.id)  # сессии агента в a1 нет: сообщения нет
            await a2.refresh()
            self.assertEqual([m["data"] for m in await agent.send() if m["type"] == "cmd.cancel"],
                             [{"commandId": cmd.id}])
            await a2.refresh()
            self.assertEqual([m for m in await agent.send() if m["type"] == "cmd.cancel"], [], "один раз")
        finally:
            await a1.close()
            await a2.close()

    async def test_store_conflict(self) -> None:
        class ConflictStore(MemoryStore):
            """Условная запись команды всегда не проходит: запись всё время меняют другие."""

            async def update_command(self, command: Command) -> bool:
                return False

        agents = Agents(enroll_token=TOKEN, store=ConflictStore(), offline_grace_ms=0)
        try:
            agent = await SyncAgent.create(agents, commands=[])
            cmd = await agents.command(CMD, agent_id=agent.id)
            with self.assertRaises(AgentsError) as err:
                await agents.cancel_command(cmd.id)
            self.assertEqual((err.exception.code, err.exception.status), ("STORE_CONFLICT", 409))
        finally:
            await agents.close()


if __name__ == "__main__":
    unittest.main()
