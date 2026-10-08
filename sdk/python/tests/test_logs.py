"""``Agents``: событие ``log``."""

from __future__ import annotations

import unittest
from typing import Any, Dict, List, Tuple

from agent_sdk.server import Agents

from tests.server_helpers import TOKEN, SyncAgent

ENTRIES = [
    {"at": 1, "level": "warn", "source": "agent", "msg": "нет связи"},
    {"at": 2, "level": "error", "source": "report", "msg": "упал", "attrs": {"code": 1}},
]


class Case(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.agents = Agents(enroll_token=TOKEN, status_interval_ms=1000, metrics_interval_ms=2000,
                             offline_grace_ms=0)

    async def asyncTearDown(self) -> None:
        await self.agents.close()


class LogEventTest(Case):
    async def test_log_event_and_ack(self) -> None:
        got: List[Tuple[str, List[Dict[str, Any]]]] = []
        self.agents.on("log", lambda agent_id, entries: got.append((agent_id, entries)))
        changes: List[str] = []
        self.agents.on("change", lambda c: changes.append(c.kind))
        agent = await SyncAgent.create(self.agents)
        changes.clear()
        msg = agent.stream("log", {"entries": ENTRIES})
        out = await agent.send(msg)
        self.assertIn({"seq": msg["seq"]}, [m["data"] for m in out if m["type"] == "ack"])
        self.assertEqual(got, [(agent.id, ENTRIES)])
        self.assertEqual(changes, [], "лог не меняет агента")
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertNotIn("log", info.to_dict(), "SDK логи не хранит")

        out = await agent.send(agent.stream("log", {"entries": "x"}))
        self.assertEqual([m["data"]["code"] for m in out if m["type"] == "error"], ["MESSAGE_INVALID"])
        with self.assertRaises(ValueError):
            self.agents.on("logs", lambda *a: None)


if __name__ == "__main__":
    unittest.main()
