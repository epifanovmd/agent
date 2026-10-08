"""Маршруты примера: смена ключа, история и откат состояния, X-Actor, alert и audit в потоке, 429 регистрации."""

from __future__ import annotations

import asyncio
import json
import tempfile
import unittest
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from agent_sdk.message import ENROLL_PATH, SYNC_PATH
from agent_sdk.server import Agents
from http11 import serve
from main import App

from tests.wsclient import Client

TOKEN = "t"


class Agent:
    """Агент по HTTP sync (вызовы handle_sync напрямую), объявляет agent.rotateKey."""

    def __init__(self, agents: Agents) -> None:
        self.agents, self.seq, self.session = agents, 0, None

    async def start(self, name: str = "a1", commands: Optional[List[str]] = None,
                    update: Optional[Dict[str, Any]] = None) -> "Agent":
        status, body = await self.agents.handle_enroll({"token": TOKEN, "name": name})
        assert status == 201, body
        self.id, self.auth = body["agentId"], f"Agent {body['agentId']}.{body['secret']}"
        await self.sync({"type": "hello", "data": {
            "versions": [1], "agent": {"name": name, "version": "1.0.0", "sdk": "t", "bootId": uuid.uuid4().hex,
                                       "startedAt": 1},
            "host": {"hostname": name, "os": "linux", "arch": "amd64"},
            "capabilities": {"commands": {"names": ["agent.rotateKey", *(commands or [])]},
                             **({"update": update} if update else {})}, "jobs": []}})
        return self

    async def sync(self, *messages: Dict[str, Any]) -> List[Dict[str, Any]]:
        body = {"sessionId": self.session, "messages": list(messages), "waitSeconds": 0}
        status, reply = await self.agents.handle_sync(self.auth, json.dumps(body).encode(), base_url="http://t")
        assert status == 200, reply
        self.session = reply["sessionId"]
        return reply["messages"]


class AdminTest(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.agents = Agents(enroll_token=TOKEN, offline_grace_ms=0, enroll_failure_limit=2)
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        self.app = App(self.agents, Path(tmp.name))
        self.server = await serve(self.app, "127.0.0.1", 0, body_limit=self.agents.body_limit)
        self.port = self.server.sockets[0].getsockname()[1]

    async def asyncTearDown(self) -> None:
        await self.app.live.close()
        await self.agents.close()
        self.server.close()

    async def http(self, method: str, path: str, body: Any = None,
                   headers: Optional[Dict[str, str]] = None) -> Tuple[int, Any, Dict[str, str]]:
        def call() -> Tuple[int, Any, Dict[str, str]]:
            data = json.dumps(body).encode() if body is not None else None
            req = urllib.request.Request(f"http://127.0.0.1:{self.port}{path}", data=data, method=method,
                                         headers={"Content-Type": "application/json", **(headers or {})})
            try:
                with urllib.request.urlopen(req, timeout=3) as resp:
                    return resp.status, json.loads(resp.read()), dict(resp.headers)
            except urllib.error.HTTPError as err:
                with err:
                    return err.code, json.loads(err.read()), dict(err.headers)

        return await asyncio.to_thread(call)

    async def test_rotate_key_route_actor_and_audit_stream(self) -> None:
        agent = await Agent(self.agents).start()
        c = await Client.connect(self.port)
        snap = await c.message()
        self.assertEqual(snap["data"]["alerts"], [])

        status, cmd, _ = await self.http("POST", f"/api/agents/{agent.id}/rotate-key", headers={"X-Actor": "ivan"})
        self.assertEqual(status, 201)
        self.assertEqual((cmd["name"], cmd["actor"], cmd["agentId"]), ("agent.rotateKey", "ivan", agent.id))
        msg = await c.until(lambda m: m["type"] == "audit")
        self.assertEqual((msg["data"]["actor"], msg["data"]["action"], msg["data"]["target"]),
                         ("ivan", "agent.rotateKey", agent.id))

        status, body, _ = await self.http("POST", "/api/agents/nope/rotate-key")
        self.assertEqual((status, body["code"]), (404, "AGENT_NOT_FOUND"))
        c.close()

    async def test_state_history_rollback_and_default_actor(self) -> None:
        status, v1, _ = await self.http("PUT", "/api/state/example.kv", {"v": 1})
        self.assertEqual((status, v1["actor"]), (200, "web"))
        status, v2, _ = await self.http("PUT", "/api/state/example.kv", {"v": 2}, {"X-Actor": "ann"})
        self.assertEqual(v2["actor"], "ann")

        status, hist, _ = await self.http("GET", "/api/state/example.kv/history?limit=10")
        self.assertEqual(status, 200)
        self.assertEqual([s["version"] for s in hist], [v2["version"], v1["version"]])
        status, hist, _ = await self.http("GET", "/api/state/example.kv/history?limit=1")
        self.assertEqual(len(hist), 1)

        c = await Client.connect(self.port)
        await c.message()
        status, back, _ = await self.http("POST", "/api/state/example.kv/rollback", {"version": v1["version"]},
                                          {"X-Actor": "bob"})
        self.assertEqual(status, 200)
        self.assertEqual((back["spec"], back["actor"]), ({"v": 1}, "bob"))
        self.assertGreater(back["version"], v2["version"])
        msg = await c.until(lambda m: m["type"] == "audit")
        self.assertEqual((msg["data"]["action"], msg["data"]["details"]), ("state.rollback",
                                                                          {"fromVersion": v1["version"]}))
        c.close()

        status, body, _ = await self.http("POST", "/api/state/example.kv/rollback", {"version": 1})
        self.assertEqual((status, body["code"]), (404, "STATE_VERSION_NOT_FOUND"))
        status, body, _ = await self.http("POST", "/api/state/example.kv/rollback", {})
        self.assertEqual(status, 400)

    async def test_alerts_in_snapshot_and_stream(self) -> None:
        agent = await Agent(self.agents).start()
        c = await Client.connect(self.port)
        await c.message()
        agent.seq += 1
        await agent.sync({"type": "status", "seq": agent.seq, "data": {
            "state": "degraded", "message": "плохо", "slots": {}, "jobs": [], "workers": [], "outbox": 0}})
        msg = await c.until(lambda m: m["type"] == "alert")
        self.assertEqual((msg["data"]["type"], msg["data"]["active"], msg["data"]["agentId"]),
                         ("degraded", True, agent.id))
        status, snap, _ = await self.http("GET", "/api/snapshot")
        self.assertEqual([a["type"] for a in snap["alerts"]], ["degraded"])
        c.close()

    async def test_releases_install_command(self) -> None:
        status, body, _ = await self.http("GET", "/api/releases")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["installCommand"],
                         f"curl -fsSL 'http://127.0.0.1:{self.port}/api/v1/agent-link/install.sh'"
                         " | sudo sh -s -- --token '<ENROLL_TOKEN>'")

    async def test_worker_candidates_and_update_route(self) -> None:
        tmp = tempfile.TemporaryDirectory()
        self.addCleanup(tmp.cleanup)
        (Path(tmp.name) / "manifest.json").write_text(json.dumps({"version": "1.0.0", "artifacts": [], "workers": [
            {"name": "sysinfo", "version": "1.3.0", "os": "linux", "arch": "amd64",
             "file": "sysinfo-1.3.0-linux-amd64", "sha256": "s", "signature": "c2ln"}]}))
        self.agents.releases_dir = tmp.name
        agent = Agent(self.agents)
        await agent.start(commands=["worker.update"], update={"mode": "self"})
        await agent.sync({"type": "status", "seq": 1, "data": {
            "state": "ready", "jobs": [],
            "workers": [{"name": "sysinfo", "state": "running", "instances": 1, "version": "1.2.0",
                         "release": True}]}})

        status, body, _ = await self.http("GET", "/api/releases")
        self.assertEqual(status, 200, body)
        self.assertEqual(body["workerCandidates"], [
            {"agentId": agent.id, "agentName": "a1", "online": True, "worker": "sysinfo", "current": "1.2.0",
             "target": "1.3.0", "os": "linux", "arch": "amd64"}])

        status, cmd, _ = await self.http("POST", f"/api/agents/{agent.id}/workers/sysinfo/update",
                                         headers={"X-Actor": "ivan"})
        self.assertEqual(status, 201, cmd)
        self.assertEqual((cmd["name"], cmd["actor"], cmd["args"]["name"], cmd["args"]["version"]),
                         ("worker.update", "ivan", "sysinfo", "1.3.0"))
        status, body, _ = await self.http("POST", f"/api/agents/{agent.id}/workers/other/update")
        self.assertEqual((status, body["code"]), (409, "UPDATE_NOT_AVAILABLE"))

    async def test_worker_pause_resume_routes(self) -> None:
        agent = Agent(self.agents)
        await agent.start(commands=["worker.pause", "worker.resume"])
        status, cmd, _ = await self.http("POST", f"/api/agents/{agent.id}/workers/report/pause",
                                         {"queues": ["example.echo"]}, {"X-Actor": "ivan"})
        self.assertEqual(status, 201, cmd)
        self.assertEqual((cmd["name"], cmd["actor"], cmd["args"]),
                         ("worker.pause", "ivan", {"name": "report", "queues": ["example.echo"]}))
        status, cmd, _ = await self.http("POST", f"/api/agents/{agent.id}/workers/report/resume")
        self.assertEqual(status, 201, cmd)
        self.assertEqual((cmd["name"], cmd["args"]), ("worker.resume", {"name": "report"}))
        runs = [m["data"]["name"] for m in await agent.sync() if m["type"] == "cmd.run"]
        self.assertEqual(runs, ["worker.pause", "worker.resume"])
        status, body, _ = await self.http("POST", "/api/agents/nope/workers/report/pause")
        self.assertEqual((status, body["code"]), (404, "AGENT_NOT_FOUND"))
        status, body, _ = await self.http("POST", f"/api/agents/{agent.id}/workers/a%20b/pause")
        self.assertEqual(status, 400)

    async def test_agent_address_from_sync(self) -> None:
        status, body = await self.agents.handle_enroll({"token": TOKEN, "name": "a1"})
        auth = f"Agent {body['agentId']}.{body['secret']}"
        hello = {"type": "hello", "data": {
            "versions": [1], "agent": {"name": "a1", "version": "1.0.0", "bootId": uuid.uuid4().hex, "startedAt": 1},
            "host": {"hostname": "a1", "os": "linux", "arch": "amd64"}, "capabilities": {}, "jobs": []}}
        status, reply, _ = await self.http("POST", SYNC_PATH, {"messages": [hello], "waitSeconds": 0},
                                           {"Authorization": auth, "X-Forwarded-For": "203.0.113.5"})
        self.assertEqual(status, 200, reply)
        info = await self.agents.get_agent(body["agentId"])
        assert info is not None
        self.assertEqual(info.address, "127.0.0.1", "без TRUST_PROXY заголовок не берётся")

    async def test_enroll_rate_limited_with_retry_after(self) -> None:
        for _ in range(2):
            status, body, _ = await self.http("POST", ENROLL_PATH, {"token": "bad", "name": "x"})
            self.assertEqual(status, 401)
        status, body, headers = await self.http("POST", ENROLL_PATH, {"token": TOKEN, "name": "x"})
        self.assertEqual((status, body["code"]), (429, "ENROLL_RATE_LIMITED"))
        self.assertTrue(int(headers["Retry-After"]) >= 1)
        # Счёт — по адресу клиента (127.0.0.1), не общий.
        self.assertEqual(list(self.agents._enroll_failures), ["127.0.0.1"])


if __name__ == "__main__":
    unittest.main()
