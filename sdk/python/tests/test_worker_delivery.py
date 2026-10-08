"""agent_sdk.worker: доставка итогов и устойчивость — ping, повторная доставка задачи, предел
одновременных задач, итоги не в JSON и больше 16 МБ, вызовы до run, ожидание отмены, файлы."""

from __future__ import annotations

import json
import socket
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Dict, List

from agent_sdk.worker import AgentError, Cancelled, Channel, Command, CommandFailed, Job, JobFailed, Worker
from agent_sdk.worker import channel as channel_module
from agent_sdk.worker import job as job_module

from tests.examples import message
from tests.test_worker import FakeAgent, assign

BIG = "x" * channel_module.MAX_LINE


def _until(pred: Any, timeout: float = 5.0) -> None:
    deadline = time.monotonic() + timeout
    while not pred():
        if time.monotonic() > deadline:
            raise AssertionError("не дождались")
        time.sleep(0.01)


class _Base(unittest.TestCase):
    def start(self, worker: Worker, agent: FakeAgent, ready: bool = True) -> None:
        thread = threading.Thread(target=worker.run, kwargs={"install_signals": False}, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(worker.drain)
        agent.wait("worker.register")
        if ready:
            agent.send("worker.ready", {"agentVersion": "1"})

    def idle_channel(self) -> Channel:
        """Канал без агента (для задачи вне воркера); сокеты закрываются после теста."""
        ours, theirs = socket.socketpair()
        self.addCleanup(theirs.close)
        channel = Channel(ours)
        self.addCleanup(channel.close)
        return channel

    def pair(self, name: str = "report") -> "tuple[Worker, FakeAgent]":
        ours, theirs = socket.socketpair()
        self.addCleanup(theirs.close)
        return Worker(name, channel=Channel(ours)), FakeAgent(theirs)


class PingTest(_Base):
    def test_ping_pong_matches_examples(self) -> None:
        worker, agent = self.pair()
        worker.channel("example.app")
        self.start(worker, agent)
        self.assertIs(agent.wait("worker.register")[0]["data"]["ping"], True)
        ping = message("worker.ping")
        agent.sock.sendall(json.dumps(ping).encode() + b"\n")
        pong = agent.wait("worker.pong")[0]
        want = message("worker.pong")
        self.assertEqual((pong["re"], pong["data"]), (want["re"], want["data"]))
        self.assertEqual(set(pong) - {"ts"}, set(want) - {"ts"})

    def test_agent_error_logged_with_code(self) -> None:
        worker, agent = self.pair()
        worker.channel("example.app")
        with self.assertLogs("agent_sdk.worker", "WARNING") as logs:
            self.start(worker, agent)
            agent.send("error", {"code": "BAD_MESSAGE", "message": "непонятно"})
            _until(lambda: any("BAD_MESSAGE" in line for line in logs.output))
        self.assertTrue(any("непонятно" in line for line in logs.output))


class JobsTest(_Base):
    def test_redelivery_same_attempt_ignored_new_attempt_replaces(self) -> None:
        worker, agent = self.pair()
        runs: List[int] = []
        cancelled: List[int] = []

        @worker.job("example.q", concurrency=2)
        def handle(job: Job) -> Any:
            runs.append(job.attempt)
            if job.attempt == 0:
                if job.wait(5):
                    cancelled.append(job.attempt)
                raise Cancelled(job.id)
            return {"attempt": job.attempt}

        self.start(worker, agent)
        agent.send("job.assign", assign("j", queue="example.q"))
        _until(lambda: runs == [0])
        agent.send("job.assign", assign("j", queue="example.q"))
        time.sleep(0.15)
        self.assertEqual(runs, [0], "та же попытка — без последствий")
        agent.send("job.assign", assign("j", queue="example.q", attempt=1))
        complete = agent.wait("job.complete")[0]["data"]
        self.assertEqual(complete, {"jobId": "j", "attempt": 1, "result": {"attempt": 1}})
        _until(lambda: cancelled == [0])
        time.sleep(0.1)
        self.assertEqual(len(agent.of("job.complete")), 1)
        self.assertEqual([m["data"] for m in agent.of("job.fail")],
                         [{"jobId": "j", "attempt": 0, "code": "CANCELLED", "message": "задача отменена",
                           "retryable": False}])

    def test_cancel_confirmed_once_whatever_handler_returns(self) -> None:
        """После job.cancel итог обработчика не отправляется: CANCELLED ровно один раз —
        и когда обработчик вернул результат, и для ждущей задачи."""
        worker, agent = self.pair()
        release = threading.Event()
        runs: List[str] = []

        @worker.job("example.q", concurrency=1)
        def handle(job: Job) -> Any:
            runs.append(job.id)
            release.wait(5)  # отмену обработчик не замечает и возвращает результат
            return "ok"

        self.start(worker, agent)
        agent.send("job.assign", assign("run", queue="example.q"))
        agent.send("job.assign", assign("wait", queue="example.q"))
        _until(lambda: runs == ["run"])
        agent.send("job.cancel", {"jobId": "wait", "attempt": 0})
        self.assertEqual(agent.wait("job.fail")[0]["data"]["jobId"], "wait")
        agent.send("job.cancel", {"jobId": "run", "attempt": 0})
        agent.send("job.cancel", {"jobId": "run", "attempt": 0})
        time.sleep(0.1)
        self.assertEqual(len(agent.of("job.fail")), 1, "до возврата обработчика подтверждения нет")
        release.set()
        agent.wait("job.fail", 2)
        time.sleep(0.2)
        results = [(m["type"], m["data"]["jobId"], m["data"].get("code")) for m in agent.got
                   if m["type"] in ("job.complete", "job.fail")]
        self.assertEqual(results, [("job.fail", "wait", "CANCELLED"), ("job.fail", "run", "CANCELLED")])
        self.assertEqual(runs, ["run"])

    def test_cancel_not_confirmed_after_channel_closed(self) -> None:
        worker, agent = self.pair()
        release = threading.Event()
        returned = threading.Event()

        @worker.job("example.q")
        def handle(job: Job) -> Any:
            release.wait(5)
            returned.set()
            return "ok"

        self.start(worker, agent)
        agent.send("job.assign", assign("j", queue="example.q"))
        time.sleep(0.05)
        agent.send("job.cancel", {"jobId": "j", "attempt": 0})
        time.sleep(0.05)
        agent.sock.shutdown(socket.SHUT_RDWR)  # агент ушёл: канал закрыт
        release.set()
        self.assertTrue(returned.wait(5))
        time.sleep(0.1)
        self.assertEqual(agent.of("job.fail"), [])

    def test_queue_concurrency_and_cancel_while_waiting(self) -> None:
        worker, agent = self.pair()
        release = threading.Event()
        started: List[str] = []
        lock = threading.Lock()
        active = {"example.one": 0, "example.two": 0}
        peak = {"example.one": 0, "example.two": 0}

        def handler(job: Job) -> Any:
            with lock:
                started.append(job.id)
                active[job.queue] += 1
                peak[job.queue] = max(peak[job.queue], active[job.queue])
            release.wait(5)
            with lock:
                active[job.queue] -= 1
            return job.id

        worker.register("example.one", handler, concurrency=1)
        worker.register("example.two", handler, concurrency=1)
        self.start(worker, agent)
        for job_id in ("a1", "a2", "a3"):
            agent.send("job.assign", assign(job_id, queue="example.one"))
        agent.send("job.assign", assign("b1", queue="example.two"))
        _until(lambda: set(started) == {"a1", "b1"})
        time.sleep(0.1)
        self.assertEqual(set(started), {"a1", "b1"}, "очередь example.one — по одной, другая не ждёт")
        agent.send("job.cancel", {"jobId": "a2", "attempt": 0})
        time.sleep(0.05)
        release.set()
        done = agent.wait("job.complete", 3)
        self.assertEqual({m["data"]["jobId"] for m in done}, {"a1", "a3", "b1"})
        self.assertEqual([(m["data"]["jobId"], m["data"]["code"]) for m in agent.of("job.fail")],
                         [("a2", "CANCELLED")])
        self.assertNotIn("a2", started, "отменённая до запуска — обработчик не вызван")
        self.assertEqual(peak, {"example.one": 1, "example.two": 1})

    def test_input_path_base_name_and_lazy_tmp(self) -> None:
        closed = self.idle_channel()
        closed.close()
        job = Job(closed, assign("t1", inputs={"../../etc/x": "http://127.0.0.1:1/x"}))
        self.assertIsNone(job._tmp, "каталог — только когда нужен")
        with self.assertRaises(ValueError):
            job_module._base_name("..")
        self.assertEqual(job_module._base_name("a/b\\c.txt"), "c.txt")
        with self.assertRaises((OSError, AgentError)):
            job.input_path("../../etc/x")  # скачать нечего, но путь — внутри каталога задачи
        self.assertIsNotNone(job._tmp)
        tmp = job._tmp
        job.close()
        self.assertFalse(tmp.exists())

    def test_wait_and_cancel_event(self) -> None:
        job = Job(self.idle_channel(), assign("w1"))
        self.assertFalse(job.wait(0.01))
        self.assertFalse(job.cancel_event.is_set())
        threading.Timer(0.05, job.cancel).start()
        began = time.monotonic()
        self.assertTrue(job.wait(5))
        self.assertLess(time.monotonic() - began, 2)
        self.assertTrue(job.cancel_event.is_set())

    def test_upload_retry_pause_interrupted_by_cancel(self) -> None:
        class Forbidden(BaseHTTPRequestHandler):
            def do_PUT(self) -> None:  # noqa: N802
                self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(403)
                self.end_headers()

            def log_message(self, *args: Any) -> None:
                pass

        server = HTTPServer(("127.0.0.1", 0), Forbidden)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        prev = job_module.UPLOAD_RETRY_SECONDS
        job_module.UPLOAD_RETRY_SECONDS = 30
        self.addCleanup(setattr, job_module, "UPLOAD_RETRY_SECONDS", prev)
        job = Job(self.idle_channel(),
                  assign("u", outputs={"out": {"url": f"http://127.0.0.1:{server.server_port}/o"}}))
        threading.Timer(0.2, job.cancel).start()
        began = time.monotonic()
        with self.assertRaises(Cancelled):
            job.upload("out", b"data")
        self.assertLess(time.monotonic() - began, 5)

    def test_request_on_closed_channel(self) -> None:
        channel = self.idle_channel()
        channel.close()
        with self.assertRaises(AgentError) as ctx:
            channel.request("job.urls", {"jobId": "x", "attempt": 0}, timeout=1)
        self.assertEqual(ctx.exception.code, "CHANNEL_CLOSED")


class ResultsTest(_Base):
    """Итоги не в JSON и больше 16 МБ — ответ с ошибкой, канал цел; коды — по схеме."""

    def setUp(self) -> None:
        self.worker, self.agent = self.pair()
        w = self.worker

        @w.job("example.q", concurrency=4)
        def handle(job: Job) -> Any:
            kind = job.data.get("kind")
            if kind == "big":
                return BIG
            if kind == "object":
                return object()
            if kind == "code":
                raise JobFailed("bad code", "плохо", retryable=False)
            return {"ok": True}

        @w.command("example.cmd")
        def cmd(c: Command) -> Any:
            kind = c.args.get("kind")
            if kind == "big":
                return BIG
            if kind == "code":
                raise CommandFailed("not-a-code", "нет")
            return {1, 2}

        @w.state("example.app")
        def apply(version: int, spec: Any) -> Any:
            return BIG if spec.get("big") else {"bad": float("nan")}

        @w.cleanup
        def cleanup() -> None:
            return None

        w.channel("example.app")
        self.start(w, self.agent)

    def test_job_results(self) -> None:
        for job_id, kind in (("big", "big"), ("obj", "object"), ("code", "code")):
            self.agent.send("job.assign", assign(job_id, queue="example.q", data={"kind": kind}))
        fails = {m["data"]["jobId"]: m["data"] for m in self.agent.wait("job.fail", 3)}
        self.assertEqual((fails["big"]["code"], fails["big"]["retryable"]), ("RESULT_TOO_LARGE", False))
        self.assertEqual((fails["obj"]["code"], fails["obj"]["retryable"]), ("WORKER_ERROR", True))
        self.assertEqual(fails["code"]["code"], "WORKER_ERROR")
        self.assertIn("bad code", fails["code"]["message"])
        self.assertFalse(self.agent.of("job.complete"))

    def test_command_results(self) -> None:
        for cid, kind in (("c-big", "big"), ("c-set", "set"), ("c-code", "code")):
            self.agent.send("cmd.run", {"commandId": cid, "name": "example.cmd", "args": {"kind": kind}})
        done = {m["data"]["commandId"]: m["data"] for m in self.agent.wait("cmd.done", 3)}
        self.assertEqual(done["c-big"]["error"]["code"], "RESULT_TOO_LARGE")
        self.assertEqual(done["c-set"]["error"]["code"], "COMMAND_FAILED")
        self.assertEqual(done["c-code"]["error"]["code"], "COMMAND_FAILED")
        self.assertIn("not-a-code", done["c-code"]["error"]["message"])
        for d in done.values():
            self.assertFalse(d["ok"])
            self.assertNotIn("result", d)

    def test_state_and_cleanup_always_answer(self) -> None:
        self.agent.send("state.put", {"domain": "example.app", "version": 1, "spec": {"big": True}}, id="s1")
        self.agent.send("state.put", {"domain": "example.app", "version": 2, "spec": {}}, id="s2")
        self.agent.send("worker.cleanup", {}, id="c1")
        replies = {m["re"]: m["data"] for m in self.agent.wait("state.applied", 2)}
        self.assertFalse(replies["s1"]["ok"])
        self.assertTrue(replies["s1"]["error"].startswith("RESULT_TOO_LARGE"))
        self.assertFalse(replies["s2"]["ok"])
        self.assertEqual(self.agent.wait("worker.cleaned")[0]["data"], {"ok": True})

    def test_big_event_and_telemetry_dropped(self) -> None:
        self.worker.event("example.big", BIG)
        self.worker.report("example.app", BIG)
        with self.assertRaises(TypeError):
            self.worker.event("example.bad", object())
        self.worker.event("example.small", 1)
        events = self.agent.wait("event")
        self.assertEqual([e["data"]["type"] for e in events], ["example.small"])
        self.assertFalse(self.agent.of("telemetry"))


class EarlyTest(_Base):
    def test_calls_before_run_sent_after_ready(self) -> None:
        worker, agent = self.pair()
        worker.register("example.echo", lambda job: None)
        worker.channel("example.app")
        worker.set_health(False, "example.db недоступна")
        worker.pause(["example.echo"])
        worker.resume(["example.echo"])
        worker.request_restart("новые настройки example.app")
        worker.event("app.started", {"port": 8080})
        worker.report("example.app", {"n": 1})
        with self.assertRaises(TypeError):
            worker.event("example.bad", object())
        self.start(worker, agent, ready=False)
        time.sleep(0.1)
        self.assertEqual([m["type"] for m in agent.got], ["worker.register"], "до worker.ready — ничего")
        agent.send("worker.ready", {"agentVersion": "1"})
        agent.wait("telemetry")
        got = agent.got[1:]
        self.assertEqual([m["type"] for m in got], [
            "worker.health", "worker.pause", "worker.resume", "worker.restart", "event", "telemetry"])
        self.assertEqual(got[0]["data"], message("worker.health.degraded")["data"])
        self.assertEqual(got[1]["data"], message("worker.pause")["data"])
        self.assertEqual(got[3]["data"], message("worker.restart")["data"])

    def test_limit_drops_oldest(self) -> None:
        from agent_sdk.worker.worker import EARLY_LIMIT

        worker, agent = self.pair()
        worker.channel("example.app")
        for i in range(EARLY_LIMIT + 5):
            worker.event("example.n", i)
        self.start(worker, agent)
        events = agent.wait("event", EARLY_LIMIT)
        self.assertEqual(events[0]["data"]["data"], 5)
        time.sleep(0.1)
        self.assertEqual(len(agent.of("event")), EARLY_LIMIT)


class TelemetryTest(_Base):
    def test_none_not_sent(self) -> None:
        worker, agent = self.pair()
        calls: List[int] = []

        @worker.telemetry("example.empty", interval=0.02)
        def empty() -> Dict[str, Any]:
            calls.append(1)
            return None  # type: ignore[return-value]

        self.start(worker, agent)
        _until(lambda: len(calls) >= 3)
        self.assertFalse(agent.of("telemetry"))


if __name__ == "__main__":
    unittest.main()
