"""Образцы sdk/spec/examples: Agents принимает каждый агент → сервер, конверты разбираются без потерь."""

from __future__ import annotations

import json
import unittest
import uuid

from agent_sdk.message import RELIABLE, STREAM, decode, encode, envelope, merge_capabilities
from agent_sdk.server import Agents
from agent_sdk.server.model import Command, Job

from tests.examples import between, every, message
from tests.server_helpers import TOKEN, SyncAgent, enroll

JOB_ID = "11111111-1111-4111-8111-111111111111"
COMMAND_ID = "22222222-2222-4222-8222-222222222222"


class EnvelopeTest(unittest.TestCase):
    def test_every_example_roundtrips(self) -> None:
        found = every()
        self.assertGreater(len(found), 30)
        for name, want in found:
            with self.subTest(name):
                env = decode(json.dumps(want))
                self.assertEqual(decode(encode(env)), want, "без потери полей")

    def test_envelope_and_decode_errors(self) -> None:
        env = envelope("ack", {"ids": ["x"]}, re="r")
        self.assertEqual((env["type"], env["re"], env["data"]), ("ack", "r", {"ids": ["x"]}))
        self.assertNotIn("id", env)
        for bad in ("{", "[]", '{"data": {}}', '{"type": "x", "data": 1}', '{"type": "x", "seq": "1"}'):
            with self.assertRaises(Exception):
                decode(bad)

    def test_merge_capabilities(self) -> None:
        hello = message("hello")["data"]["capabilities"]
        later = message("capabilities")["data"]
        merged = merge_capabilities(hello, later)
        self.assertIn("agent.update", merged["commands"]["names"], "сужение — только новым hello")
        self.assertIn("example.app.reload", merged["commands"]["names"])
        self.assertEqual(merged["state"]["domains"], {"dns": None, "example.app": 12})
        self.assertEqual(sorted(merged["telemetry"]["channels"]), ["example.app", "gpu", "host"])
        self.assertEqual(merge_capabilities({"state": {"domains": {"d": 5}}}, {"state": {"domains": {"d": 3}}}),
                         {"state": {"domains": {"d": 5}}}, "у домена — большая версия")
        self.assertIn("agent.update", hello["commands"]["names"], "исходные не меняются")


class ServerAcceptsAgentMessagesTest(unittest.IsolatedAsyncioTestCase):
    """Сервер принимает каждое сообщение агент → сервер своей схемой (§9)."""

    async def test_every_agent_message(self) -> None:
        for name, env in between("agent", "server"):
            with self.subTest(name):
                await self._accept(env)

    async def _accept(self, message: dict) -> None:  # type: ignore[type-arg]
        agents = Agents(enroll_token=TOKEN)
        try:
            agent_id, secret = await enroll(agents, "node-01")
            agent = SyncAgent(agents, agent_id, secret)
            if message["type"] == "hello":
                out = await agent.send(message)
                self.assertEqual(out[0]["type"], "welcome")
                info = await agents.get_agent(agent_id)
                assert info is not None
                self.assertEqual(info.hello, message["data"])
                # Задача из hello.jobs на сервере не числится — job.cancel (§6.3).
                if message["data"]["jobs"]:
                    self.assertEqual([m["data"]["jobId"] for m in out if m["type"] == "job.cancel"], [JOB_ID])
                return
            # Задача и команда образцов — за этим агентом.
            await agents.store.create_job(Job(id=JOB_ID, queue="example.echo", status="running", agent_id=agent_id,
                                           lease_until=2 ** 62, outputs=["best"]))
            await agents.store.create_command(Command(id=COMMAND_ID, agent_id=agent_id, name="agent.logs",
                                                   created_at=2 ** 62))
            hello = {"type": "hello", "data": {
                "versions": [1], "agent": {"name": "node-01", "version": "1", "bootId": uuid.uuid4().hex,
                                            "startedAt": 1},
                "host": {"hostname": "node-01", "os": "linux", "arch": "amd64"}, "capabilities": {},
                "jobs": [{"jobId": JOB_ID, "attempt": 0}]}}
            await agent.send(hello)
            out = await agent.send(message)
            errors = [m["data"] for m in out if m["type"] == "error"]
            self.assertEqual(errors, [], f"{message['type']} отклонено")
            kind = message["type"]
            if kind in RELIABLE:
                self.assertIn({"ids": [message["id"]]}, [m["data"] for m in out if m["type"] == "ack"])
            elif kind in STREAM:
                self.assertIn({"seq": message["seq"]}, [m["data"] for m in out if m["type"] == "ack"])
            else:
                self.assertEqual([m["re"] for m in out if m["type"] == kind], [message["id"]], "ответ на запрос")
        finally:
            await agents.close()


if __name__ == "__main__":
    unittest.main()
