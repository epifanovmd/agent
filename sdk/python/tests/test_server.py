"""``Agents`` (agent_sdk.server): фейковые агенты по HTTP sync и WebSocket."""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time
import unittest
from typing import Any, Dict, List

from agent_sdk.message import Close
from agent_sdk.server import Change, Agents, AgentsError, MemoryStore

from tests.examples import message
from tests.server_helpers import TOKEN, SyncAgent, WsAgent, enroll, hello

Q = "example.echo"


class AgentsCase(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        # offline_grace_ms=0: без отсрочки offline (её проверяет test_metrics.py).
        self.agents = Agents(enroll_token=TOKEN, status_interval_ms=1000, metrics_interval_ms=2000, offline_grace_ms=0)

    async def asyncTearDown(self) -> None:
        await self.agents.close()


class EnrollTest(AgentsCase):
    async def test_enroll_and_authenticate(self) -> None:
        status, body = await self.agents.handle_enroll(b'{"token": "wrong", "name": "a1"}')
        self.assertEqual((status, body["code"]), (401, "AGENT_ENROLLMENT_TOKEN_INVALID"))
        status, body = await self.agents.handle_enroll({"token": TOKEN})
        self.assertEqual(status, 400)

        agent_id, secret = await enroll(self.agents, "a1")
        stored = await self.agents.store.get_agent(agent_id)
        assert stored is not None
        self.assertEqual(stored.secret_hash, hashlib.sha256(secret.encode()).hexdigest())
        self.assertNotIn("secretHash", stored.to_dict())
        self.assertNotIn("secret", json.dumps(stored.to_dict()))

        self.assertIsNotNone(await self.agents.authenticate(f"Agent {agent_id}.{secret}"))
        self.assertIsNone(await self.agents.authenticate(f"Agent {agent_id}.bad"))
        self.assertIsNone(await self.agents.authenticate("Bearer x"))
        status, body = await self.agents.handle_sync(f"Agent {agent_id}.bad", {"sessionId": None, "messages": []})
        self.assertEqual(status, 401)

    async def test_enroll_callback(self) -> None:
        seen: List[Dict[str, Any]] = []

        async def check(token: str, info: Dict[str, Any]) -> Any:
            seen.append(info)
            return {"labels": {"zone": "eu"}} if token == "fleet" else None

        agents = Agents(enroll=check)
        status, _ = await agents.handle_enroll({"token": "nope", "name": "x"})
        self.assertEqual(status, 401)
        status, body = await agents.handle_enroll({"token": "fleet", "name": "x", "labels": {"disk": "ssd"}})
        self.assertEqual(status, 201)
        agent = await agents.get_agent(body["agentId"])
        assert agent is not None
        self.assertEqual(agent.labels, {"disk": "ssd", "zone": "eu"})
        self.assertEqual(seen[-1], {"name": "x", "labels": {"disk": "ssd"}, "host": None})
        await agents.close()

    async def test_hello_labels(self) -> None:
        """Метки следуют за hello; выданные при регистрации переписать нельзя."""
        agents = Agents(enroll=lambda token, info: {"labels": {"nodeId": "n1"}})
        changes: List[Change] = []
        agents.on("change", changes.append)
        status, body = await agents.handle_enroll({"token": "t", "name": "x", "labels": {"zone": "eu"}})
        self.assertEqual(status, 201)
        agent = SyncAgent(agents, body["agentId"], body["secret"])

        async def say_hello(labels: Dict[str, str]) -> Dict[str, str]:
            msg = hello("x")
            msg["data"]["labels"] = labels
            agent.session = None
            status, reply = await agent.sync([msg])
            self.assertEqual(status, 200, reply)
            info = await agents.get_agent(agent.id)
            assert info is not None
            return info.labels

        changes.clear()
        self.assertEqual(await say_hello({"zone": "us", "disk": "ssd", "nodeId": "n2"}),
                         {"zone": "us", "disk": "ssd", "nodeId": "n1"})
        self.assertIn("agent", {c.kind for c in changes})
        self.assertEqual(await say_hello({"zone": "us"}), {"zone": "us", "nodeId": "n1"})
        stored = await agents.store.get_agent(agent.id)
        assert stored is not None
        self.assertNotIn("grantedLabels", stored.to_dict())

        # Запись без granted_labels — выданных меток нет: метки из hello.
        stored.granted_labels = None
        await agents.store.update_agent(stored)
        self.assertEqual(await say_hello({"nodeId": "n3", "rack": "r1"}), {"nodeId": "n3", "rack": "r1"})
        await agents.close()


class SyncTest(AgentsCase):
    async def test_welcome_and_hello_required(self) -> None:
        agent_id, secret = await enroll(self.agents)
        status, body = await self.agents.handle_sync(f"Agent {agent_id}.{secret}",
                                                  {"sessionId": None, "messages": [{"type": "status", "seq": 1}]})
        self.assertEqual((status, body["code"]), (400, "AGENT_HELLO_REQUIRED"))
        agent = await SyncAgent.create(self.agents)
        welcome = agent.of("welcome")[0]["data"]
        self.assertEqual(set(welcome), set(message("welcome")["data"]))
        self.assertEqual(welcome["config"], {"statusIntervalMs": 1000, "metricsIntervalMs": 2000})
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertTrue(info.online)
        self.assertEqual(info.transport, "http")
        # Забытая сессия — 409 AGENT_SESSION_EXPIRED.
        status, body = await self.agents.handle_sync(agent.auth, {"sessionId": "other", "messages": []})
        self.assertEqual((status, body["code"]), (409, "AGENT_SESSION_REPLACED"))

    async def test_job_by_slots_lifecycle(self) -> None:
        agent = await SyncAgent.create(self.agents, queues=[(Q, 1)])
        job = await self.agents.enqueue(Q, {"text": "hi"}, inputs={"src": "вход"}, outputs=["out"])
        self.assertEqual(job.status, "queued", "без status.slots не раздаётся")
        second = await self.agents.enqueue(Q, {"text": "2"})

        out = await agent.send(agent.status({Q: 1}))
        assigns = [m for m in out if m["type"] == "job.assign"]
        self.assertEqual(len(assigns), 1, "один слот — одна задача")
        assign = assigns[0]["data"]
        self.assertEqual(set(assign), set(message("job.assign")["data"]))
        self.assertEqual(assign["inputs"], {"src": f"http://test/files/{job.id}/in/src"})
        self.assertEqual(assign["outputs"]["out"]["url"], f"http://test/files/{job.id}/out/out")
        self.assertIn({"type": "ack", "data": {"seq": agent.seq}}, [{"type": m["type"], "data": m["data"]}
                                                                  for m in out])
        # Выданная занимает слот до job.accept: повторный status с тем же слотом не даёт вторую.
        out = await agent.send(agent.status({Q: 1}))
        self.assertFalse([m for m in out if m["type"] == "job.assign"])

        ref = {"jobId": job.id, "attempt": 0}
        complete = agent.reliable("job.complete", {**ref, "result": {"echo": "hi"}})
        out = await agent.send(
            agent.stream("job.accept", ref),
            agent.stream("job.progress", {**ref, "progress": 0.5, "text": "половина", "log": ["строка"]}),
            agent.reliable("job.event", {**ref, "seq": 1, "type": "epoch", "data": {"loss": 0.4}}),
            agent.reliable("job.event", {**ref, "seq": 1, "type": "epoch", "data": {"loss": 0.4}}),
            complete,
        )
        self.assertIn({"ids": [complete["id"]]}, [m["data"] for m in out if m["type"] == "ack"])
        done = await self.agents.get_job(job.id)
        assert done is not None
        self.assertEqual((done.status, done.result, done.text, done.log), ("completed", {"echo": "hi"}, "половина",
                                                                          ["строка"]))
        self.assertEqual(len(done.events), 1, "повтор job.event отброшен по seq")
        self.assertTrue(done.accepted)
        # Повтор итога (потерянный ack) — подтверждается, не ошибка.
        out = await agent.send(complete)
        self.assertEqual([m["data"] for m in out if m["type"] == "ack"], [{"ids": [complete["id"]]}])

        # Итог освободил слот — следующая задача выдана сразу.
        self.assertEqual([m["data"]["jobId"] for m in agent.of("job.assign")], [job.id, second.id])

        # Файлы задачи: вход по ссылке, выход — PUT.
        self.assertEqual(await self.agents.handle_file("GET", f"{job.id}/in/src"), (200, "вход".encode()))
        self.assertEqual((await self.agents.handle_file("PUT", f"{job.id}/out/out", b"res"))[0], 200)
        self.assertEqual(await self.agents.files.outputs(done), ["out"])
        self.assertEqual((await self.agents.handle_file("GET", "../etc/passwd"))[0], 404)

    async def test_job_fail_retry_and_reject(self) -> None:
        agent = await SyncAgent.create(self.agents, queues=[(Q, 2)])
        job = await self.agents.enqueue(Q, max_attempts=2)
        await agent.send(agent.status({Q: 2}))
        out = await agent.send(agent.reliable("job.fail", {"jobId": job.id, "attempt": 0, "code": "FLAKY",
                                                          "message": "сбой", "retryable": True}))
        again = [m["data"] for m in out if m["type"] == "job.assign"]
        self.assertEqual([(a["jobId"], a["attempt"]) for a in again], [(job.id, 1)], "повтор — новая попытка")
        # Итог старой попытки — JOB_LEASE_LOST без повтора.
        out = await agent.send(agent.reliable("job.complete", {"jobId": job.id, "attempt": 0}))
        err = [m for m in out if m["type"] == "error"][0]
        self.assertEqual(set(err["data"]), set(message("error")["data"]))
        self.assertEqual((err["data"]["code"], err["data"]["retryable"]), ("JOB_LEASE_LOST", False))
        await agent.send(agent.reliable("job.reject", {"jobId": job.id, "attempt": 1, "code": "QUEUE_NOT_SERVED",
                                                      "message": "нет"}))
        rejected = await self.agents.get_job(job.id)
        assert rejected is not None
        self.assertEqual((rejected.status, rejected.agent_id), ("queued", None))

        fatal = await self.agents.enqueue("example.other", max_attempts=3)
        await agent.send(agent.status({"example.other": 1}))
        await agent.send(agent.reliable("job.fail", {"jobId": fatal.id, "attempt": 0, "code": "BAD_INPUT",
                                                    "message": "нет", "retryable": False}))
        failed = await self.agents.get_job(fatal.id)
        assert failed is not None
        self.assertEqual((failed.status, failed.error), ("failed", {"code": "BAD_INPUT", "message": "нет"}))

    async def test_long_poll_wakes_and_keeps_deliveries_for_gone_client(self) -> None:
        agent = await SyncAgent.create(self.agents, queues=[(Q, 1)])
        await agent.send(agent.status({Q: 1}))
        started = time.monotonic()
        poll = asyncio.ensure_future(agent.sync([], wait=5))
        await asyncio.sleep(0.05)
        job = await self.agents.enqueue(Q)
        status, reply = await asyncio.wait_for(poll, 2)
        self.assertEqual(status, 200)
        self.assertLess(time.monotonic() - started, 2)
        self.assertEqual([m["data"]["jobId"] for m in reply["messages"] if m["type"] == "job.assign"], [job.id])

        # Клиент ушёл, пока ждал: доставки остаются для следующего запроса.
        gone = {"value": False}
        poll = asyncio.ensure_future(agent.sync([], wait=5, is_disconnected=lambda: gone["value"]))
        await asyncio.sleep(0.05)
        gone["value"] = True
        cmd_job = await self.agents.cancel_job(job.id)
        self.assertEqual(cmd_job.status, "cancelled")
        status, _ = await asyncio.wait_for(poll, 2)
        self.assertEqual(status, 499)
        out = await agent.send()
        self.assertEqual([m["data"] for m in out if m["type"] == "job.cancel"], [{"jobId": job.id, "attempt": 0}])

    async def test_stream_duplicate_only_acked_and_unknown_type(self) -> None:
        agent = await SyncAgent.create(self.agents)
        first = agent.stream("metrics", {"collectedAt": 1, "host": {"cpuPercent": 1}})
        await agent.send(first)
        out = await agent.send({**first, "data": {"collectedAt": 2}})
        self.assertEqual([m["data"] for m in out], [{"seq": first["seq"]}])
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual(info.metrics, {"collectedAt": 1, "host": {"cpuPercent": 1}})
        out = await agent.send({"type": "example.unknown", "id": "u1", "data": {}})
        self.assertEqual(out[0]["data"]["code"], "UNKNOWN_TYPE")
        self.assertEqual(out[0]["re"], "u1")

    async def test_idle_http_session_goes_offline(self) -> None:
        import agent_sdk.server.agents as agents_module

        agent = await SyncAgent.create(self.agents)
        old = agents_module.SYNC_IDLE
        agents_module.SYNC_IDLE = 0.0
        try:
            await asyncio.sleep(0.01)
            await self.agents.sweep()
        finally:
            agents_module.SYNC_IDLE = old
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertFalse(info.online)
        status, body = await agent.sync([])
        self.assertEqual((status, body["code"]), (409, "AGENT_SESSION_EXPIRED"))


class CapabilitiesCommandsStateTest(AgentsCase):
    async def test_capabilities_merge_delivers_state_and_commands(self) -> None:
        agent = await SyncAgent.create(self.agents, commands=["agent.logs"], domains={"dns": None})
        with self.assertRaises(AgentsError) as ctx:
            await self.agents.command("example.kv.get")
        self.assertEqual((ctx.exception.code, ctx.exception.status), ("COMMAND_NOT_SUPPORTED", 404))
        st = await self.agents.set_state("example.kv", {"a": 1})
        self.assertFalse(agent.of("state.put"))

        out = await agent.send(agent.stream("capabilities", {
            "commands": {"names": ["example.kv.get"]}, "state": {"domains": {"example.kv": None}},
        }))
        puts = [m["data"] for m in out if m["type"] == "state.put"]
        self.assertEqual(puts, [{"domain": "example.kv", "version": st.version, "spec": {"a": 1}}])
        self.assertEqual(set(puts[0]), set(message("state.put@server")["data"]))
        info = await self.agents.get_agent(agent.id)
        assert info is not None and info.capabilities is not None
        self.assertEqual(info.capabilities["commands"]["names"], ["agent.logs", "example.kv.get"])
        self.assertEqual(set(info.capabilities["state"]["domains"]), {"dns", "example.kv"})

        changes: List[Change] = []
        unsubscribe = self.agents.on("change", changes.append)
        await agent.send(agent.reliable("state.applied", {"domain": "example.kv", "version": st.version, "ok": True,
                                                         "report": {"keys": 1}}))
        await asyncio.sleep(0)
        unsubscribe()
        self.assertEqual({(c.kind, c.id) for c in changes}, {("agent", agent.id), ("state", "example.kv")},
                         "state.applied — change agent и state (id — раздел)")
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertEqual(info.state_applied["example.kv"]["report"], {"keys": 1})
        # Та же версия больше не доставляется (агент её применил).
        out = await agent.send(agent.stream("capabilities", {"state": {"domains": {"example.kv": st.version}}}))
        self.assertFalse([m for m in out if m["type"] == "state.put"])

        cmd = await self.agents.command("example.kv.get", {"key": "a"}, timeout_sec=5)
        out = await agent.send()
        runs = [m["data"] for m in out if m["type"] == "cmd.run"]
        self.assertEqual(runs, [{"commandId": cmd.id, "name": "example.kv.get", "args": {"key": "a"},
                                 "timeoutSec": 5}])
        self.assertEqual(set(runs[0]), set(message("cmd.run")["data"]))
        ref = {"commandId": cmd.id}
        await agent.send(agent.stream("cmd.accept", ref), agent.stream("cmd.output", {**ref, "chunk": "a="}),
                         agent.stream("cmd.output", {**ref, "chunk": "1\n"}))
        running = await self.agents.get_command(cmd.id)
        assert running is not None
        self.assertEqual((running.status, running.output), ("running", "a=1\n"))
        await agent.send(agent.reliable("cmd.done", {**ref, "ok": True, "result": {"value": 1}, "exitCode": 0}))
        done = await self.agents.get_command(cmd.id)
        assert done is not None
        self.assertEqual((done.status, done.result, done.exit_code), ("succeeded", {"value": 1}, 0))
        self.assertEqual(done.to_dict()["exitCode"], 0)

    async def test_call_waits_for_result_and_timeout(self) -> None:
        agent = await WsAgent.create(self.agents, commands=["agent.logs"])
        call = asyncio.ensure_future(self.agents.call("agent.logs", {"lines": 10}))
        run = (await agent.wait("cmd.run"))[0]["data"]
        agent.push(agent.reliable("cmd.done", {"commandId": run["commandId"], "ok": False,
                                               "error": {"code": "NOPE", "message": "нет"}}))
        done = await asyncio.wait_for(call, 2)
        self.assertEqual((done.status, done.error), ("failed", {"code": "NOPE", "message": "нет"}))

        # Агент пропал: срок + 15 с — TIMEOUT на сервере.
        cmd = await self.agents.command("agent.logs", timeout_sec=1)
        await self.agents.sweep(now=cmd.created_at + 16_001)
        timed = await self.agents.get_command(cmd.id)
        assert timed is not None
        self.assertEqual((timed.status, timed.error["code"] if timed.error else None), ("failed", "TIMEOUT"))

    async def test_pending_command_delivered_on_connect(self) -> None:
        creds = await enroll(self.agents, "a1")
        first = await WsAgent.create(self.agents, "a1", creds=creds, commands=["agent.logs"])
        self.assertTrue(first.got)
        await first.conn.close()
        await asyncio.wait_for(first.task, 2)
        cmd = await self.agents.command("agent.logs", agent_id=creds[0])
        again = await WsAgent.create(self.agents, "a1", creds=creds, commands=["agent.logs"])
        runs = await again.wait("cmd.run")
        self.assertEqual(runs[0]["data"]["commandId"], cmd.id)

    async def test_lists_newest_first_dispatch_oldest_first(self) -> None:
        creds = await enroll(self.agents, "a1")
        job_ids, cmd_ids = [], []
        for _ in range(3):
            job_ids.append((await self.agents.enqueue(Q)).id)
            cmd_ids.append((await self.agents.command("agent.logs", agent_id=creds[0])).id)
        self.assertEqual([j.id for j in await self.agents.list_jobs()], job_ids[::-1])
        self.assertEqual([c.id for c in await self.agents.list_commands()], cmd_ids[::-1])
        self.assertEqual([j.id for j in await self.agents.store.list_jobs(queue=Q)], job_ids[::-1])
        self.assertEqual([c.id for c in await self.agents.store.list_commands(agent_id=creds[0])], cmd_ids[::-1])

        # Доставка команд и раздача задач — старые первыми.
        agent = await WsAgent.create(self.agents, "a1", creds=creds, commands=["agent.logs"], queues=[(Q, 1)])
        runs = await agent.wait("cmd.run", 3)
        self.assertEqual([r["data"]["commandId"] for r in runs], cmd_ids)
        agent.push(agent.status({Q: 1}))
        assign = (await agent.wait("job.assign"))[0]["data"]
        self.assertEqual(assign["jobId"], job_ids[0])

    def test_memory_store_defaults(self) -> None:
        s = MemoryStore()
        self.assertEqual((s.keep_jobs, s.keep_commands, s.keep_events, s.keep_metrics, s.keep_state_history),
                         (1000, 500, 1000, 4320, 50))

    async def test_per_agent_state_and_monotonic_version(self) -> None:
        a1 = await WsAgent.create(self.agents, "a1", domains={"example.kv": None})
        a2 = await WsAgent.create(self.agents, "a2", domains={"example.kv": None})
        common = await self.agents.set_state("example.kv", {"v": "common"})
        await a1.wait("state.put", version=common.version)
        await a2.wait("state.put", version=common.version)

        own = await self.agents.set_state("example.kv", {"v": "a1"}, agent_id=a1.id)
        self.assertGreater(own.version, common.version)
        self.assertEqual(own.agent_id, a1.id)
        await a1.wait("state.put", version=own.version)
        await asyncio.sleep(0.05)
        self.assertEqual([m["data"]["version"] for m in a2.of("state.put")], [common.version])

        # Версия растёт и при постановке в ту же миллисекунду; не меньше now_ms.
        versions = [(await self.agents.set_state("example.fast", i)).version for i in range(5)]
        self.assertEqual(versions, sorted(set(versions)))
        self.assertGreaterEqual(versions[0], int(time.time() * 1000) - 5000)
        states = {(s.domain, s.agent_id) for s in await self.agents.list_states()}
        self.assertEqual(states, {("example.kv", None), ("example.kv", a1.id), ("example.fast", None)})

        with self.assertRaises(AgentsError):
            await self.agents.set_state("example.kv", {}, agent_id="nobody")

    async def test_delete_personal_state_republishes_common(self) -> None:
        a1 = await WsAgent.create(self.agents, "a1", domains={"example.kv": None})
        a2 = await WsAgent.create(self.agents, "a2", domains={"example.kv": None})
        common = await self.agents.set_state("example.kv", {"v": "common"})
        own = await self.agents.set_state("example.kv", {"v": "a1"}, agent_id=a1.id)
        await a1.wait("state.put", version=own.version)
        events: List[Any] = []
        self.agents.on("change", lambda ch: events.append(("change", ch.kind, ch.id)))
        self.agents.on("stateDeleted", lambda d, a: events.append(("stateDeleted", d, a)))

        again = await self.agents.delete_state("example.kv", a1.id)
        assert again is not None
        self.assertIsNone(again.agent_id)
        self.assertEqual(again.spec, {"v": "common"})
        self.assertGreater(again.version, own.version)
        put = (await a1.wait("state.put", version=again.version))[-1]["data"]
        self.assertEqual(put["spec"], {"v": "common"})
        await a2.wait("state.put", version=again.version)
        self.assertGreater(again.version, common.version)
        self.assertIsNone(await self.agents.store.get_state("example.kv", a1.id))
        await asyncio.sleep(0.05)
        self.assertEqual(events, [("stateDeleted", "example.kv", a1.id), ("change", "state", "example.kv")])

        # Снимка (и агента) не было — не ошибка: текущий общий без новой версии, уведомлений нет;
        # пустой домен — ошибка.
        events.clear()
        same = await self.agents.delete_state("example.kv", "nobody")
        assert same is not None
        self.assertEqual(same.version, again.version)
        await asyncio.sleep(0.05)
        self.assertEqual(events, [])
        with self.assertRaises(AgentsError) as err:
            await self.agents.delete_state("")
        self.assertEqual(err.exception.code, "MESSAGE_INVALID")

    async def test_delete_personal_state_without_common_sends_nothing(self) -> None:
        a1 = await WsAgent.create(self.agents, "a1", domains={"example.kv": None})
        own = await self.agents.set_state("example.kv", {"v": "a1"}, agent_id=a1.id)
        await a1.wait("state.put", version=own.version)
        self.assertIsNone(await self.agents.delete_state("example.kv", a1.id))
        await asyncio.sleep(0.05)
        self.assertEqual([m["data"]["version"] for m in a1.of("state.put")], [own.version])
        self.assertEqual(await self.agents.list_states(), [])

    async def test_delete_common_state_keeps_personal(self) -> None:
        a1 = await WsAgent.create(self.agents, "a1", domains={"example.kv": None})
        a2 = await WsAgent.create(self.agents, "a2", domains={"example.kv": None})
        common = await self.agents.set_state("example.kv", {"v": "common"})
        own = await self.agents.set_state("example.kv", {"v": "a1"}, agent_id=a1.id)
        await a1.wait("state.put", version=own.version)
        await a2.wait("state.put", version=common.version)
        deleted: List[Any] = []
        self.agents.on("stateDeleted", lambda d, a: deleted.append((d, a)))
        self.assertIsNone(await self.agents.delete_state("example.kv"))
        self.assertIsNone(await self.agents.delete_state("example.kv"), "повторно — не ошибка")
        self.assertEqual(deleted, [("example.kv", None)])
        await asyncio.sleep(0.05)
        self.assertEqual([m["data"]["version"] for m in a1.of("state.put")], [common.version, own.version])
        self.assertEqual([m["data"]["version"] for m in a2.of("state.put")], [common.version])
        states = {(s.domain, s.agent_id) for s in await self.agents.list_states()}
        self.assertEqual(states, {("example.kv", a1.id)})

    async def test_version_not_rolled_back_after_delete(self) -> None:
        # Подряд — версии уходят вперёд now_ms: после удаления не должны откатиться к now_ms.
        v1 = (await self.agents.set_state("d", 1)).version
        for i in range(20):
            v2 = (await self.agents.set_state("d", i)).version
        self.assertIsNone(await self.agents.delete_state("d"))
        self.assertEqual(await self.agents.list_states(), [])
        v3 = (await self.agents.set_state("d", 3)).version
        self.assertGreater(v3, v2)
        self.assertGreater(v2, v1)

    async def test_version_survives_restart(self) -> None:
        store = MemoryStore()
        first = Agents(enroll_token=TOKEN, store=store)
        v1 = (await first.set_state("d", 1)).version
        await first.close()
        second = Agents(enroll_token=TOKEN, store=MemoryStore())
        v2 = (await second.set_state("d", 2)).version
        self.assertGreaterEqual(v2, v1, "без хранилища — не меньше now_ms")
        await second.close()


class NamesAndWarningsTest(AgentsCase):
    async def test_bad_names_rejected(self) -> None:
        for bad in [".x", "a b", "a/b", "a" * 65]:
            for call in (lambda: self.agents.set_state(bad, {}), lambda: self.agents.delete_state(bad),
                         lambda: self.agents.enqueue(bad), lambda: self.agents.command(bad),
                         lambda: self.agents.call(bad)):
                with self.subTest(name=bad):
                    with self.assertRaises(AgentsError) as ctx:
                        await call()
                    self.assertEqual(ctx.exception.code, "MESSAGE_INVALID")
        self.assertEqual(await self.agents.store.list_states(), [])

    async def test_set_state_warns_on_undeclared_domain(self) -> None:
        a1 = await SyncAgent.create(self.agents, "a1", domains={"example.kv": None})
        a2 = await SyncAgent.create(self.agents, "a2", domains={})
        with self.assertLogs("agent_sdk.server", "WARNING") as logs:
            await self.agents.set_state("example.none", {"a": 1})
            await self.agents.set_state("example.kv", {"a": 1}, agent_id=a2.id)
        self.assertEqual(len(logs.output), 2)
        self.assertIn("раздел состояния ещё никто не объявлял", logs.output[0])
        self.assertIn("example.none", logs.output[0])
        self.assertIn("example.kv", logs.output[1])
        self.assertIsNotNone(await self.agents.store.get_state("example.none", None))

        with self.assertLogs("agent_sdk.server", "WARNING") as logs:
            await self.agents.set_state("example.kv", {"a": 2})
            await self.agents.set_state("example.kv", {"a": 3}, agent_id=a1.id)
            logging.getLogger("agent_sdk.server").warning("маркер")
        self.assertEqual(len(logs.output), 1, logs.output)

        # Без сессий (новый экземпляр на том же хранилище) — объявление берётся из хранилища.
        other = Agents(enroll_token=TOKEN, store=self.agents.store)
        try:
            with self.assertLogs("agent_sdk.server", "WARNING") as logs:
                await other.set_state("example.kv", {"a": 4})
                await other.set_state("example.kv", {"a": 5}, agent_id=a1.id)
                await other.set_state("example.kv", {"a": 6}, agent_id=a2.id)
            self.assertEqual(len(logs.output), 1, logs.output)
            self.assertIn("example.kv", logs.output[0])
        finally:
            await other.close()


class SchedulingTest(AgentsCase):
    async def test_less_loaded_first_and_pinned(self) -> None:
        busy = await SyncAgent.create(self.agents, "busy", queues=[(Q, 4)])
        free = await SyncAgent.create(self.agents, "free", queues=[(Q, 4)])
        running = await self.agents.enqueue(Q, agent_id=busy.id)
        await busy.send(busy.status({Q: 4}))
        await busy.send(busy.status({Q: 3}, jobs=[{"jobId": running.id, "attempt": 0, "queue": Q}]))
        await free.send(free.status({Q: 4}))

        job = await self.agents.enqueue(Q)
        got = await self.agents.get_job(job.id)
        assert got is not None
        self.assertEqual(got.agent_id, free.id, "сначала менее загруженный")

        pinned = await self.agents.enqueue(Q, agent_id=busy.id)
        got = await self.agents.get_job(pinned.id)
        assert got is not None
        self.assertEqual((got.agent_id, got.pinned_agent_id), (busy.id, busy.id))
        with self.assertRaises(AgentsError):
            await self.agents.enqueue(Q, agent_id="nobody")

    async def test_paused_agent_gets_nothing(self) -> None:
        agent = await SyncAgent.create(self.agents, queues=[(Q, 1)])
        job = await self.agents.enqueue(Q)
        out = await agent.send(agent.status({Q: 1}, state="draining"))
        self.assertFalse([m for m in out if m["type"] == "job.assign"])
        got = await self.agents.get_job(job.id)
        assert got is not None
        self.assertEqual(got.status, "queued")

    async def test_lease_renewed_by_status_and_expires(self) -> None:
        agent = await SyncAgent.create(self.agents, queues=[(Q, 1)])
        job = await self.agents.enqueue(Q, lease_seconds=10, max_attempts=2)
        await agent.send(agent.status({Q: 1}))
        before = await self.agents.get_job(job.id)
        await asyncio.sleep(0.01)
        await agent.send(agent.status({Q: 0}, jobs=[{"jobId": job.id, "attempt": 0, "queue": Q}]))
        after = await self.agents.get_job(job.id)
        assert before is not None and after is not None
        self.assertGreater(after.lease_until, before.lease_until)
        await self.agents.sweep(now=after.lease_until + 1)
        expired = await self.agents.get_job(job.id)
        assert expired is not None
        self.assertEqual((expired.status, expired.attempt, expired.error["code"] if expired.error else None),
                         ("queued", 1, "LEASE_EXPIRED"))

    async def test_reconcile_on_hello(self) -> None:
        creds = await enroll(self.agents, "a1")
        agent = await WsAgent.create(self.agents, "a1", creds=creds, queues=[(Q, 3)])
        accepted = await self.agents.enqueue(Q, max_attempts=2)
        unaccepted = await self.agents.enqueue(Q)
        kept = await self.agents.enqueue(Q)
        agent.push(agent.status({Q: 3}))
        await agent.wait("job.assign", 3)
        agent.push(agent.stream("job.accept", {"jobId": accepted.id, "attempt": 0}),
                   agent.stream("job.accept", {"jobId": kept.id, "attempt": 0}))
        await agent.wait("ack", seq=agent.seq)
        await self.agents.stop_job(kept.id)
        await agent.wait("job.stop", jobId=kept.id)

        # Рестарт агента: держит только kept и чужую задачу.
        await agent.conn.close()
        await asyncio.wait_for(agent.task, 2)
        again = await WsAgent.create(self.agents, "a1", creds=creds, queues=[(Q, 3)],
                                     jobs=[{"jobId": kept.id, "attempt": 0}, {"jobId": "stranger", "attempt": 0}])
        await again.wait("job.assign", jobId=unaccepted.id)
        await again.wait("job.stop", jobId=kept.id)
        await again.wait("job.cancel", jobId="stranger")
        lost = await self.agents.get_job(accepted.id)
        assert lost is not None
        self.assertEqual((lost.status, lost.attempt, lost.error["code"] if lost.error else None),
                         ("queued", 1, "AGENT_LOST"))


class WebSocketTest(AgentsCase):
    async def test_replacement_closes_previous_with_4410(self) -> None:
        creds = await enroll(self.agents, "a1")
        first = await WsAgent.create(self.agents, "a1", creds=creds)
        second = await WsAgent.create(self.agents, "a1", creds=creds)
        await first.conn.until(lambda: first.conn.close_code is not None)
        self.assertEqual(first.conn.close_code, Close.REPLACED)
        await asyncio.wait_for(first.task, 2)
        info = await self.agents.get_agent(creds[0])
        assert info is not None
        self.assertTrue(info.online, "вытесненная сессия не снимает агента со связи")
        self.assertEqual(info.transport, "ws")
        # HTTP-сессия вытесняет WebSocket так же.
        sync = SyncAgent(self.agents, *creds)
        await sync.send(hello("a1"))
        await second.conn.until(lambda: second.conn.close_code == Close.REPLACED)

    async def test_bad_credentials_hello_and_version(self) -> None:
        agent_id, _ = await enroll(self.agents, "a1")
        conn = await self._serve(f"Agent {agent_id}.bad", None)
        self.assertEqual(conn.close_code, Close.UNAUTHORIZED)

        creds = await enroll(self.agents, "a3")
        auth = f"Agent {creds[0]}.{creds[1]}"
        conn = await self._serve(auth, {"type": "status", "seq": 1, "data": {}})
        self.assertEqual(conn.close_code, Close.INVALID)
        bad_version = hello("a3")
        bad_version["data"]["versions"] = [99]
        conn = await self._serve(auth, bad_version)
        self.assertEqual(conn.close_code, Close.UNSUPPORTED)

    async def _serve(self, auth: str, first: Any) -> Any:
        from tests.server_helpers import FakeConn

        conn = FakeConn()
        if first is not None:
            conn.push(first)
        await asyncio.wait_for(self.agents.serve_websocket(auth, conn), 2)
        return conn

    async def test_disconnect_goes_offline_and_close_sends_1012(self) -> None:
        agent = await WsAgent.create(self.agents)
        await agent.conn.close()
        await asyncio.wait_for(agent.task, 2)
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        self.assertFalse(info.online)

        other = await WsAgent.create(self.agents, "a2")
        await self.agents.close()
        await other.conn.until(lambda: other.conn.close_code == Close.RESTART)


class EventsAndChangesTest(AgentsCase):
    async def test_events_deduplicated_and_change_notifications(self) -> None:
        changes: List[Change] = []
        async_changes: List[str] = []

        async def on_async(change: Change) -> None:
            async_changes.append(change.kind)

        unsubscribe = self.agents.on("change", changes.append)
        self.agents.on("change", on_async)
        agent = await WsAgent.create(self.agents)
        event = message("event@agent")
        agent.push(event, event)
        await agent.wait("ack", 2, ids=[event["id"]])
        events = await self.agents.list_events()
        self.assertEqual(len(events), 1, "повтор события по id отброшен")
        self.assertEqual(events[0].to_dict(), {"agentId": agent.id, "agentName": "a1", "source": "report",
                                               "type": "listener.changed", "data": {"listener": "http", "status": "up"},
                                               "at": event["ts"]})
        await asyncio.sleep(0)
        kinds = {c.kind for c in changes}
        self.assertTrue({"agent", "event"} <= kinds)
        self.assertIn(Change(kind="event", id=event["id"]), changes, "change event — id сообщения")
        second = agent.reliable("event", {"source": "report", "type": "second"})
        agent.push(second)
        await agent.wait("ack", 1, ids=[second["id"]])
        self.assertEqual([e.type for e in await self.agents.list_events()], ["second", "listener.changed"],
                         "новые первыми")
        self.assertEqual([e.type for e in await self.agents.list_events(1)], ["second"])
        self.assertEqual(len(await self.agents.list_events(0)), 2, "limit ≤ 0 — все")
        self.assertIn("event", async_changes)
        unsubscribe()
        before = len(changes)
        await self.agents.enqueue(Q)
        self.assertEqual(len(changes), before)
        with self.assertRaises(ValueError):
            self.agents.on("job", changes.append)

    async def test_job_urls_request(self) -> None:
        agent = await SyncAgent.create(self.agents, queues=[(Q, 1)])
        job = await self.agents.enqueue(Q, inputs={"a": "1"}, outputs=["best", "other"])
        await agent.send(agent.status({Q: 1}))
        out = await agent.send({"type": "job.urls", "id": "r1", "data": {"jobId": job.id, "attempt": 0,
                                                                        "outputs": ["best"]}})
        reply = [m for m in out if m["type"] == "job.urls"][0]
        self.assertEqual(reply["re"], "r1")
        self.assertEqual(set(reply["data"]), set(message("job.urls@server")["data"]))
        self.assertEqual(list(reply["data"]["outputs"]), ["best"])
        self.assertEqual(reply["data"]["inputs"], {"a": f"http://test/files/{job.id}/in/a"})

    async def test_snapshot_shapes(self) -> None:
        agent = await SyncAgent.create(self.agents, queues=[(Q, 1)])
        job = await self.agents.enqueue(Q, {"x": 1})
        data = job.to_dict()
        for key in ("id", "queue", "data", "status", "attempt", "maxAttempts", "leaseSeconds", "accepted",
                    "progress", "log", "events", "stopRequested", "inputs", "outputs", "createdAt"):
            self.assertIn(key, data)
        self.assertNotIn("leaseUntil", data)
        info = await self.agents.get_agent(agent.id)
        assert info is not None
        # Без подписок поля subscriptions нет (как в Go и Node).
        self.assertEqual(set(info.to_dict()) - {"hello", "capabilities", "transport", "lastSeenAt"},
                         {"id", "name", "labels", "online", "revoked", "enrolledAt", "stateApplied"})


if __name__ == "__main__":
    unittest.main()
