"""``Agents``: подписки (subscribe/unsubscribe) — сводная в welcome и config, продление, истечение,
другой процесс, отзыв, проверки."""

from __future__ import annotations

import asyncio
import unittest
from typing import Any, Dict, List

from agent_sdk.message import now_ms
from agent_sdk.server import Agents, AgentsError

from tests.examples import message
from tests.server_helpers import TOKEN, SyncAgent, WsAgent, enroll



def configs(messages: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [m["data"] for m in messages if m["type"] == "config"]


class Case(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.agents = Agents(enroll_token=TOKEN, status_interval_ms=1000, metrics_interval_ms=2000,
                             offline_grace_ms=0)

    async def asyncTearDown(self) -> None:
        await self.agents.close()


class SubscribeTest(Case):
    async def test_config_extend_replace_and_welcome(self) -> None:
        agent = await WsAgent.create(self.agents)
        welcome = agent.of("welcome")[0]["data"]["config"]
        self.assertEqual(welcome, {"statusIntervalMs": 1000, "metricsIntervalMs": 2000}, "подписок нет — поля нет")

        sub = await self.agents.subscribe(agent.id, ttl_ms=10_000, metrics={"intervalMs": 1000,
                                                                            "groups": ["sockets", "diskio", "sockets"]})
        self.assertEqual(set(sub), {"id", "until"})
        self.assertGreater(sub["until"], now_ms())
        await agent.wait("config")
        self.assertEqual(configs(agent.got), [{"subscription": {"metricsIntervalMs": 1000,
                                                                "metrics": ["sockets", "diskio"]}}])
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual(info.to_dict()["subscriptions"], [{
            "id": sub["id"], "until": sub["until"],
            "metrics": {"intervalMs": 1000, "groups": ["sockets", "diskio"]}}])

        # Тот же id и то же содержимое — продление без config.
        later = await self.agents.subscribe(agent.id, id=sub["id"], ttl_ms=20_000,
                                            metrics={"intervalMs": 1000, "groups": ["sockets", "diskio"]})
        self.assertEqual(later["id"], sub["id"])
        self.assertGreater(later["until"], sub["until"])
        await asyncio.sleep(0.05)
        self.assertEqual(len(agent.of("config")), 1, "сводная та же — config не повторяется")
        # Тот же id, другое содержимое — заменяет.
        await self.agents.subscribe(agent.id, id=sub["id"], logs={"level": "info"})
        await agent.wait("config", 2)
        self.assertEqual(configs(agent.got)[1], {"subscription": {"logLevel": "info"}})
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual(len(info.subscriptions), 1)

        # Переподключение — сводная в welcome.
        creds = (agent.id, agent.auth.split(".", 1)[1])
        again = await WsAgent.create(self.agents, "a1", creds=creds)
        self.assertEqual(again.of("welcome")[0]["data"]["config"]["subscription"], {"logLevel": "info"})

        await self.agents.unsubscribe(again.id, sub["id"])
        await again.wait("config")
        self.assertEqual(configs(again.got), [{"subscription": {}}])
        await self.agents.unsubscribe(again.id, sub["id"])  # нет такой — ничего
        await asyncio.sleep(0.05)
        self.assertEqual(len(again.of("config")), 1)
        third = await WsAgent.create(self.agents, "a1", creds=creds)
        self.assertNotIn("subscription", third.of("welcome")[0]["data"]["config"])

    async def test_summary_of_several(self) -> None:
        agent = await WsAgent.create(self.agents)
        await self.agents.subscribe(agent.id, id="a", status={"intervalMs": 1000},
                                    metrics={"intervalMs": 1000, "groups": ["diskio", "sockets"]},
                                    logs={"level": "warn"}, channels={"example.app": {"intervalMs": 2000}})
        await self.agents.subscribe(agent.id, id="b", status={"intervalMs": 500},
                                    metrics={"groups": ["sockets", "processes"]}, logs={"level": "debug"},
                                    channels={"example.app": {"intervalMs": 1000}, "example.kv": {"intervalMs": 3000}})
        await agent.wait("config", 2)
        self.assertEqual(configs(agent.got)[-1], {"subscription": {
            "statusIntervalMs": 500, "metricsIntervalMs": 1000, "metrics": ["diskio", "sockets", "processes"],
            "logLevel": "debug", "channels": {"example.app": 1000, "example.kv": 3000}}})
        await self.agents.unsubscribe(agent.id, "b")
        await agent.wait("config", 3)
        self.assertEqual(configs(agent.got)[-1], {"subscription": {
            "statusIntervalMs": 1000, "metricsIntervalMs": 1000, "metrics": ["diskio", "sockets"],
            "logLevel": "warn", "channels": {"example.app": 2000}}})

    async def test_expiry_by_sweep(self) -> None:
        agent = await WsAgent.create(self.agents)
        short = await self.agents.subscribe(agent.id, ttl_ms=1000, logs={"level": "debug"})
        await self.agents.subscribe(agent.id, ttl_ms=60_000, logs={"level": "warn"})
        await agent.wait("config")
        self.assertEqual(configs(agent.got), [{"subscription": {"logLevel": "debug"}}])
        await self.agents.sweep(now=short["until"] + 1)
        await agent.wait("config", 2)
        self.assertEqual(configs(agent.got)[1], {"subscription": {"logLevel": "warn"}})
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual([s["logs"]["level"] for s in info.subscriptions], ["warn"], "истёкшая удалена")

    async def test_expiry_by_background_sweep(self) -> None:
        agent = await WsAgent.create(self.agents)
        await self.agents.subscribe(agent.id, ttl_ms=50, status={"intervalMs": 200})
        await agent.wait("config")
        await agent.conn.until(lambda: len(agent.of("config")) >= 2, timeout=3)
        self.assertEqual(agent.of("config")[1]["data"], {"subscription": {}})

    async def test_errors(self) -> None:
        agent_id, _ = await enroll(self.agents)
        bad: List[Dict[str, Any]] = [
            {"status": {"intervalMs": 199}},
            {"status": {}},
            {"status": {"intervalMs": "1000"}},
            {"metrics": {"intervalMs": 100}},
            {"metrics": {"groups": ["Sockets"]}},
            {"metrics": {"groups": [""]}},
            {"metrics": {"groups": "sockets"}},
            {"logs": {"level": "trace"}},
            {"logs": {}},
            {"channels": {"bad name": {"intervalMs": 1000}}},
            {"channels": {"example.app": {"intervalMs": 10}}},
            {"channels": {"example.app": 1000}},
            {"ttl_ms": 0},
            {"id": ""},
        ]
        for kw in bad:
            with self.subTest(kw), self.assertRaises(AgentsError) as err:
                await self.agents.subscribe(agent_id, **kw)
            self.assertEqual(err.exception.code, "MESSAGE_INVALID")
        info = await self.agents.get_agent(agent_id)
        assert info is not None
        self.assertEqual(info.subscriptions, [])
        for call in (self.agents.subscribe("nope"), self.agents.unsubscribe("nope", "x")):
            with self.assertRaises(AgentsError) as err:
                await call
            self.assertEqual(err.exception.code, "AGENT_NOT_FOUND")

    async def test_revoke_removes_subscriptions(self) -> None:
        agent_id, _ = await enroll(self.agents)
        await self.agents.subscribe(agent_id, status={"intervalMs": 1000})
        await self.agents.revoke(agent_id)
        info = await self.agents.get_agent(agent_id)
        assert info is not None
        self.assertEqual(info.subscriptions, [])
        with self.assertRaises(AgentsError) as err:
            await self.agents.subscribe(agent_id, status={"intervalMs": 1000})
        self.assertEqual(err.exception.code, "AGENT_REVOKED")

    async def test_example_shapes(self) -> None:
        want = message("config.subscription")["data"]["subscription"]
        agent = await WsAgent.create(self.agents)
        await self.agents.subscribe(agent.id, metrics={"intervalMs": want["metricsIntervalMs"],
                                                       "groups": want["metrics"]},
                                    logs={"level": want["logLevel"]},
                                    channels={k: {"intervalMs": v} for k, v in want["channels"].items()})
        self.assertEqual((await agent.wait("config"))[0]["data"], {"subscription": want})
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        await self.agents.unsubscribe(agent.id, info.subscriptions[0]["id"])
        self.assertEqual((await agent.wait("config", 2))[1]["data"], message("config.subscription.empty")["data"])

        want = message("welcome.subscription")["data"]["config"]["subscription"]
        other = await WsAgent.create(self.agents, "a2")
        await self.agents.subscribe(other.id, status={"intervalMs": want["statusIntervalMs"]},
                                    metrics={"intervalMs": want["metricsIntervalMs"], "groups": want["metrics"]},
                                    channels={k: {"intervalMs": v} for k, v in want["channels"].items()})
        await other.conn.close()
        await asyncio.wait_for(other.task, 2)
        await other.connect(name="a2")
        self.assertEqual(other.of("welcome")[0]["data"]["config"]["subscription"], want)


class SharedStoreTest(Case):
    """Два ``Agents`` (процесса) на общем Store: подписку другого процесса доставляет процесс
    с сессией агента; у агента без связи истёкшие удаляет сверка любого процесса."""

    async def asyncSetUp(self) -> None:
        await super().asyncSetUp()
        self.other = Agents(enroll_token=TOKEN, store=self.agents.store, metrics_interval_ms=2000)

    async def asyncTearDown(self) -> None:
        await self.other.close()
        await super().asyncTearDown()

    async def test_other_process_refresh_and_message(self) -> None:
        agent = await SyncAgent.create(self.agents)
        sub = await self.other.subscribe(agent.id, ttl_ms=10_000, status={"intervalMs": 500})
        self.assertFalse(configs(await agent.send()), "без refresh — не знает")
        await self.agents.refresh()
        self.assertEqual(configs(await agent.send()), [{"subscription": {"statusIntervalMs": 500}}])
        # Повторный refresh и сообщения агента — без повторного config; подписка не затирается.
        await self.agents.refresh(agent.id)
        self.assertFalse(configs(await agent.send(agent.status({}))))
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual([s["id"] for s in info.subscriptions], [sub["id"]])

        # Изменение от другого процесса: сообщение агента применяет запись и без refresh.
        await self.other.subscribe(agent.id, id=sub["id"], ttl_ms=10_000, status={"intervalMs": 700})
        self.assertEqual(configs(await agent.send(agent.status({}))), [{"subscription": {"statusIntervalMs": 700}}])

        # Срок истёк: сверка процесса с сессией — пустая сводная, запись очищена.
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        await self.other.sweep(now=info.subscriptions[0]["until"] + 1)
        self.assertEqual(len((await self.agents.get_agent(agent.id)).subscriptions), 1,
                         "агент на связи с другим процессом — не трогает")
        await self.agents.sweep(now=info.subscriptions[0]["until"] + 1)
        self.assertEqual(configs(await agent.send()), [{"subscription": {}}])
        self.assertEqual((await self.agents.get_agent(agent.id)).subscriptions, [])

    async def test_offline_expiry_by_any_process(self) -> None:
        agent_id, _ = await enroll(self.agents, "a2")
        sub = await self.other.subscribe(agent_id, ttl_ms=1000, status={"intervalMs": 1000})
        await self.agents.sweep(now=sub["until"] - 1)
        self.assertEqual(len((await self.agents.get_agent(agent_id)).subscriptions), 1)
        await self.agents.sweep(now=sub["until"] + 1)
        self.assertEqual((await self.agents.get_agent(agent_id)).subscriptions, [])


if __name__ == "__main__":
    unittest.main()
