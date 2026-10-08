"""agent_sdk.worker: обмен с агентом по socketpair (фейковый агент); образцы sdk/spec/examples (воркер ↔ агент)."""

from __future__ import annotations

import json
import logging
import os
import shutil
import socket
import ssl
import subprocess
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Dict, List, Optional

from agent_sdk.worker import (
    AUTO_INTERVAL, Cancelled, Channel, Command, CommandFailed, Job, JobFailed, StateFailed, Worker, WorkerContext,
)
from agent_sdk.worker import files as files_module
from agent_sdk.worker import job as job_module

from tests.examples import message

# Ожидаемые ошибки обработчиков в тестах — без трассировок в выводе.
logging.getLogger("agent_sdk").setLevel(logging.CRITICAL)


class FakeAgent:
    """Сторона агента: читает сообщения воркера, отвечает на запросы."""

    def __init__(self, sock: socket.socket) -> None:
        self.sock = sock
        self.reader = sock.makefile("rb")
        self.got: List[Dict[str, Any]] = []
        self.cond = threading.Condition()
        self.urls_reply: Optional[Dict[str, Any]] = None
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        for raw in self.reader:
            message = json.loads(raw)
            if message["type"] == "job.urls" and message.get("id"):
                self.send("job.urls", self.urls_reply or {"inputs": {}, "outputs": {}, "expiresAt": 0},
                          re=message["id"])
            with self.cond:
                self.got.append(message)
                self.cond.notify_all()

    def send(self, type: str, data: Any = None, re: Optional[str] = None, id: Optional[str] = None) -> None:
        envelope: Dict[str, Any] = {"type": type, "data": data or {}}
        if re:
            envelope["re"] = re
        if id:
            envelope["id"] = id
        self.sock.sendall(json.dumps(envelope).encode() + b"\n")

    def of(self, type: str) -> List[Dict[str, Any]]:
        with self.cond:
            return [m for m in self.got if m["type"] == type]

    def wait(self, type: str, count: int = 1, timeout: float = 5.0) -> List[Dict[str, Any]]:
        deadline = time.monotonic() + timeout
        with self.cond:
            while True:
                found = [m for m in self.got if m["type"] == type]
                if len(found) >= count:
                    return found
                left = deadline - time.monotonic()
                if left <= 0:
                    raise AssertionError(f"нет {count}× {type}: {self.got}")
                self.cond.wait(left)


def assign(job_id: str, queue: str = "example.echo", **extra: Any) -> Dict[str, Any]:
    return {"jobId": job_id, "attempt": 0, "queue": queue, "data": {"text": "hi"},
            "leaseSeconds": 60, "inputs": {}, "outputs": {}, **extra}


class WorkerMessagesTest(unittest.TestCase):
    def setUp(self) -> None:
        job_module.PROGRESS_MIN_INTERVAL = 0.05
        job_module.UPLOAD_RETRY_SECONDS = 0
        ours, theirs = socket.socketpair()
        self.agent = FakeAgent(theirs)
        self.worker = Worker("test", version="9.9", channel=Channel(ours))
        self.release = threading.Event()
        self.started = threading.Event()

        @self.worker.job("example.echo", concurrency=2)
        def echo(job: Job) -> dict:
            job.progress(0.5, "половина")
            job.log("строка")
            job.event("step", {"n": 1})
            if job.data.get("fail") == "fatal":
                raise JobFailed("BAD_INPUT", "нет", retryable=False)
            if job.data.get("fail") == "boom":
                raise ValueError("сломалось")
            if job.data.get("wait"):
                self.started.set()
                while not self.release.wait(0.01):
                    job.check_cancelled()
                    if job.stop_requested:
                        return {"stopped": True}
            return {"echo": job.data["text"]}

        self.thread = threading.Thread(target=self.worker.run, kwargs={"install_signals": False}, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.worker.drain()
        self.release.set()
        self.thread.join(5)

    def test_register_complete_progress_event(self) -> None:
        register = self.agent.wait("worker.register")[0]["data"]
        self.assertEqual(register["queues"], [{"name": "example.echo", "concurrency": 2}])
        self.assertEqual(register["version"], "9.9")
        self.assertTrue(register["sdk"].startswith("python/"))

        self.agent.send("worker.ready", {"agentVersion": "1"})
        self.agent.send("job.assign", assign("j1"))
        complete = self.agent.wait("job.complete")[0]["data"]
        self.assertEqual(complete, {"jobId": "j1", "attempt": 0, "result": {"echo": "hi"}})

        event = self.agent.wait("job.event")[0]["data"]
        self.assertEqual((event["seq"], event["type"]), (1, "step"))
        progress = self.agent.wait("job.progress")
        self.assertTrue(any(p["data"].get("progress") == 0.5 for p in progress))
        self.assertTrue(any("строка" in p["data"].get("log", []) for p in progress))
        # Прогресс до события — порядок сохранён.
        types = [m["type"] for m in self.agent.got if m["type"] in ("job.progress", "job.event")]
        self.assertEqual(types[0], "job.progress")

    def test_failures(self) -> None:
        self.agent.send("job.assign", assign("f1", data={"text": "x", "fail": "fatal"}))
        self.agent.send("job.assign", assign("f2", data={"text": "x", "fail": "boom"}))
        self.agent.send("job.assign", assign("f3", queue="unknown"))
        fails = {m["data"]["jobId"]: m["data"] for m in self.agent.wait("job.fail", 3)}
        self.assertEqual((fails["f1"]["code"], fails["f1"]["retryable"]), ("BAD_INPUT", False))
        self.assertEqual((fails["f2"]["code"], fails["f2"]["retryable"]), ("WORKER_ERROR", True))
        self.assertIn("ValueError", fails["f2"]["message"])
        self.assertEqual(fails["f3"]["code"], "WORKER_STOPPING")

    def test_cancel_sends_nothing_and_stop_completes(self) -> None:
        self.agent.send("job.assign", assign("c1", data={"text": "x", "wait": True}))
        self.assertTrue(self.started.wait(5))
        self.agent.send("job.cancel", {"jobId": "c1", "attempt": 0})
        time.sleep(0.2)
        self.assertFalse([m for m in self.agent.got if m["type"] in ("job.complete", "job.fail")])

        self.started.clear()
        self.agent.send("job.assign", assign("s1", data={"text": "x", "wait": True}))
        self.assertTrue(self.started.wait(5))
        self.agent.send("job.stop", {"jobId": "s1", "attempt": 0})
        complete = self.agent.wait("job.complete")[0]["data"]
        self.assertEqual(complete["result"], {"stopped": True})

    def test_drain_finishes_current_and_exits(self) -> None:
        self.agent.send("job.assign", assign("d1", data={"text": "x", "wait": True}))
        self.assertTrue(self.started.wait(5))
        self.agent.send("worker.drain")
        time.sleep(0.2)
        self.assertTrue(self.thread.is_alive(), "текущая задача дорабатывается")
        self.agent.send("job.assign", assign("d2"))
        self.assertEqual(self.agent.wait("job.fail")[0]["data"]["code"], "WORKER_STOPPING")
        self.release.set()
        self.agent.wait("job.complete")
        self.thread.join(5)
        self.assertFalse(self.thread.is_alive(), "после текущих задач воркер завершился")



class MinimalRegisterTest(unittest.TestCase):
    def test_minimal_register_matches_example(self) -> None:
        ours, theirs = socket.socketpair()
        agent = FakeAgent(theirs)
        worker = Worker("echo", channel=Channel(ours))
        worker.register("example.echo", lambda job: None)
        thread = threading.Thread(target=worker.run, kwargs={"install_signals": False}, daemon=True)
        thread.start()
        register = agent.wait("worker.register")[0]["data"]
        want = message("worker.register.minimal")["data"]
        self.assertEqual(set(register) - {"version", "sdk"}, set(want), "пустые списки не отправляются")
        self.assertEqual(register["queues"], want["queues"])
        drain = message("worker.drain")
        agent.sock.sendall(json.dumps(drain).encode() + b"\n")
        thread.join(5)
        self.assertFalse(thread.is_alive(), "worker.drain — выход")


class PrimitivesTest(unittest.TestCase):
    """Команды, желаемое состояние, телеметрия и события воркера (§10)."""

    def setUp(self) -> None:
        ours, theirs = socket.socketpair()
        self.agent = FakeAgent(theirs)
        self.worker = Worker("report", version="2.0", channel=Channel(ours))
        self.applied: List[Any] = []
        self.release = threading.Event()

        @self.worker.command("example.app.reload")
        def reload(cmd: Command) -> dict:
            cmd.write("перечитываю\n")
            if cmd.args.get("fail"):
                raise CommandFailed("RELOAD_FAILED", "конфигурация битая")
            if cmd.args.get("boom"):
                raise ValueError("сломалось")
            if cmd.args.get("wait"):
                while not self.release.wait(0.01):
                    cmd.check_cancelled()
            return {"reloaded": True}

        @self.worker.state("example.app")
        def apply(version: int, spec: Any) -> dict:
            if spec.get("bad"):
                raise ValueError("порт занят")
            if spec.get("busy"):
                raise StateFailed("порт занят", report={"listeners": 0, "port": spec["busy"]})
            self.applied.append((version, spec))
            return {"listeners": 1}

        @self.worker.telemetry("example.app", interval=0.05)
        def stats() -> dict:
            return {"connections": 3}

        self.worker.channel("example.push")
        self.thread = threading.Thread(target=self.worker.run, kwargs={"install_signals": False}, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.release.set()
        self.worker.drain()
        self.thread.join(5)

    def test_register_matches_example(self) -> None:
        register = self.agent.wait("worker.register")[0]["data"]
        want = message("worker.register")["data"]
        self.assertEqual(set(register), set(want), "поля регистрации — как в образце")
        self.assertEqual(register["commands"], ["example.app.reload"])
        self.assertEqual(register["domains"], ["example.app"])
        self.assertEqual(register["channels"], ["example.app", "example.push"])
        self.assertEqual(register["queues"], [])

        ready = message("worker.ready")
        self.agent.send(ready["type"], ready["data"])
        deadline = time.monotonic() + 5
        while not self.worker.rejected and time.monotonic() < deadline:
            time.sleep(0.01)
        self.assertEqual(self.worker.rejected, {"agent.reboot"})

    def test_command_output_result_and_errors(self) -> None:
        self.agent.send("cmd.run", {"commandId": "c1", "name": "example.app.reload", "timeoutSec": 5})
        self.agent.send("cmd.run", {"commandId": "c2", "name": "example.app.reload", "args": {"fail": True}})
        self.agent.send("cmd.run", {"commandId": "c3", "name": "example.app.reload", "args": {"boom": True}})
        self.agent.send("cmd.run", {"commandId": "c4", "name": "nope"})
        done = {m["data"]["commandId"]: m["data"] for m in self.agent.wait("cmd.done", 4)}
        self.assertEqual(done["c1"], {"commandId": "c1", "ok": True, "result": {"reloaded": True}})
        self.assertEqual(done["c2"]["error"]["code"], "RELOAD_FAILED")
        self.assertEqual(done["c3"]["error"]["code"], "COMMAND_FAILED")
        self.assertIn("ValueError", done["c3"]["error"]["message"])
        self.assertEqual(done["c4"]["error"]["code"], "COMMAND_UNKNOWN")
        outputs = [m["data"] for m in self.agent.wait("cmd.output", 3) if m["data"]["commandId"] == "c1"]
        self.assertEqual(outputs, [{"commandId": "c1", "chunk": "перечитываю\n"}])

    def test_cancelled_command_sends_no_result(self) -> None:
        self.agent.send("cmd.run", {"commandId": "w1", "name": "example.app.reload", "args": {"wait": True}})
        self.agent.wait("cmd.output")
        cancel = message("cmd.cancel")
        self.agent.send("cmd.cancel", {"commandId": "w1"})
        self.assertEqual(set(cancel["data"]), {"commandId"})
        time.sleep(0.2)
        self.assertFalse([m for m in self.agent.got if m["type"] == "cmd.done"])

    def test_state_apply_replies_by_request_id(self) -> None:
        put = message("state.put@agent")
        self.agent.sock.sendall(json.dumps(put).encode() + b"\n")
        self.agent.send("state.put", {"domain": "example.app", "version": 13, "spec": {"bad": True}}, id="r2")
        replies = {m["re"]: m["data"] for m in self.agent.wait("state.applied", 2)}
        self.assertEqual(replies[put["id"]], {"domain": "example.app", "version": 12, "ok": True,
                                              "report": {"listeners": 1}})
        self.assertFalse(replies["r2"]["ok"])
        self.assertIn("порт занят", replies["r2"]["error"])
        self.assertEqual(self.applied, [(12, {"listen": ":8443"})])
        want = message("state.applied@worker")
        sent = [m for m in self.agent.got if m.get("re") == put["id"]][0]
        self.assertEqual(set(sent) - {"ts"}, set(want) - {"ts"})

    def test_state_failed_with_report(self) -> None:
        self.agent.send("state.put", {"domain": "example.app", "version": 14, "spec": {"busy": 8080}}, id="r3")
        self.agent.send("state.put", {"domain": "example.app", "version": 15, "spec": {"bad": True}}, id="r4")
        replies = {m["re"]: m["data"] for m in self.agent.wait("state.applied", 2)}
        self.assertEqual(replies["r3"], {"domain": "example.app", "version": 14, "ok": False,
                                         "error": "порт занят", "report": {"listeners": 0, "port": 8080}})
        self.assertNotIn("report", replies["r4"], "обычное исключение — без отчёта")

    def test_telemetry_after_ready_and_events(self) -> None:
        self.agent.wait("worker.register")
        time.sleep(0.15)
        self.assertFalse(self.agent.of("telemetry"), "опрос — после worker.ready")
        self.agent.send("worker.ready", {"agentVersion": "1"})
        telemetry = self.agent.wait("telemetry", 2)
        self.assertEqual(telemetry[0]["data"], {"channel": "example.app", "data": {"connections": 3}})
        self.worker.report("example.push", {"n": 1})
        self.worker.event("listener.changed", {"status": "up"})
        pushed = [m["data"] for m in self.agent.wait("telemetry", 3) if m["data"]["channel"] == "example.push"]
        self.assertEqual(pushed, [{"channel": "example.push", "data": {"n": 1}}])
        self.assertEqual(self.agent.wait("event")[0]["data"], {"type": "listener.changed", "data": {"status": "up"}})
        with self.assertRaises(ValueError):
            self.worker.report("undeclared", {})
        # Форма — как в образцах воркер → агент.
        for kind in ("telemetry", "event"):
            sent = self.agent.wait(kind)[0]
            want = message("event@worker" if kind == "event" else kind)
            self.assertEqual(set(sent["data"]), set(want["data"]), kind)
            self.assertEqual(set(sent) - {"ts"}, set(want) - {"ts"}, kind)

    def test_rejected_channel_not_polled(self) -> None:
        self.agent.wait("worker.register")
        self.agent.send("worker.ready", {"agentVersion": "1", "rejected": ["example.app"]})
        time.sleep(0.2)
        self.assertEqual(self.worker.rejected, {"example.app"})
        self.assertFalse(self.agent.of("telemetry"))


class ContextAndControlTest(unittest.TestCase):
    """Контекст воркера (``worker.context``) и самоуправление: health, pause, resume, restart."""

    def setUp(self) -> None:
        ours, theirs = socket.socketpair()
        self.agent = FakeAgent(theirs)
        self.worker = Worker("report", channel=Channel(ours))
        self.seen: List[WorkerContext] = []
        self.polls: List[float] = []

        @self.worker.telemetry("example.app", interval=AUTO_INTERVAL)
        def stats() -> dict:
            self.polls.append(time.monotonic())
            return {"n": len(self.polls)}

        @self.worker.on_context
        def changed(ctx: WorkerContext) -> None:
            self.seen.append(ctx)

        self.direct: List[str] = []
        self.assertIsNotNone(self.worker.on_context(lambda ctx: self.direct.append(ctx.mode)))
        self.worker.on_context(lambda ctx: 1 / 0)  # сбой подписчика не мешает остальным
        # До run: уйдёт сразу после регистрации.
        self.worker.set_health(False, "example.db недоступна")
        self.thread = threading.Thread(target=self.worker.run, kwargs={"install_signals": False}, daemon=True)
        self.thread.start()

    def tearDown(self) -> None:
        self.worker.drain()
        self.thread.join(5)

    def _context(self, name: str, **override: Any) -> None:
        env = message(name)
        env["data"] = {**env["data"], **override}
        self.agent.sock.sendall(json.dumps(env).encode() + b"\n")

    def _wait(self, pred: Any, timeout: float = 5.0) -> None:
        deadline = time.monotonic() + timeout
        while not pred():
            if time.monotonic() > deadline:
                raise AssertionError("не дождались")
            time.sleep(0.01)

    def test_defaults_and_context(self) -> None:
        ctx = self.worker.context
        self.assertEqual((ctx.mode, ctx.online, ctx.metrics_interval_ms, ctx.channels, ctx.agent.id),
                         ("run", False, 0, {}, ""))
        self.agent.wait("worker.register")
        self._context("worker.context")
        self._wait(lambda: len(self.seen) == 1)
        ctx = self.worker.context
        self.assertEqual(self.seen, [ctx])
        want = message("worker.context")["data"]
        self.assertEqual((ctx.mode, ctx.online, ctx.metrics_interval_ms, ctx.log_level),
                         ("run", True, want["metricsIntervalMs"], want["logLevel"]))
        self.assertEqual(ctx.channels, want.get("channels", {}))
        self.assertEqual((ctx.agent.id, ctx.agent.name, ctx.agent.version, ctx.agent.labels),
                         ("33333333-3333-4333-8333-333333333333", "node-01", "1.1.0", {"zone": "eu"}))
        self._context("worker.context.cleanup")
        self._wait(lambda: len(self.seen) == 2)
        self.assertEqual((self.worker.context.mode, self.worker.context.agent.labels), ("cleanup", {}))
        self.assertEqual(self.direct, ["run", "cleanup"])

    def test_control_messages_match_examples(self) -> None:
        register_at = self.agent.got.index(self.agent.wait("worker.register")[0])
        health = self.agent.wait("worker.health")
        self.assertGreater(self.agent.got.index(health[0]), register_at, "после регистрации")
        self.assertEqual(health[0]["data"], message("worker.health.degraded")["data"])
        self.worker.set_health(True)
        self.worker.pause(["example.echo"])
        self.worker.resume(["example.echo"])
        self.worker.pause()
        self.worker.request_restart("новые настройки example.app")
        self.worker.request_restart()
        self.assertEqual(self.agent.wait("worker.health", 2)[1]["data"], message("worker.health")["data"])
        pauses = self.agent.wait("worker.pause", 2)
        self.assertEqual(pauses[0]["data"], message("worker.pause")["data"])
        self.assertEqual(pauses[1]["data"], {}, "без очередей — все")
        self.assertEqual(self.agent.wait("worker.resume")[0]["data"], message("worker.resume")["data"])
        restarts = self.agent.wait("worker.restart", 2)
        self.assertEqual(restarts[0]["data"], message("worker.restart")["data"])
        self.assertEqual(restarts[1]["data"], {})
        for m in pauses + restarts:
            self.assertNotIn("id", m, "без ответа")
        with self.assertRaises(ValueError):
            self.worker.pause(["a b"])

    def test_telemetry_follows_metrics_interval(self) -> None:
        self.agent.wait("worker.register")
        self.agent.send("worker.ready", {"agentVersion": "1"})
        self._wait(lambda: len(self.polls) >= 1)
        time.sleep(0.2)
        self.assertEqual(len(self.polls), 1, "частота неизвестна — 15 с")
        # Частоту сообщили — следующий опрос по ней, без ожидания прежних 15 с.
        self._context("worker.context", metricsIntervalMs=50, channels={})
        self._wait(lambda: len(self.polls) >= 4)
        self._context("worker.context", metricsIntervalMs=60_000, channels={})
        time.sleep(0.1)
        count = len(self.polls)
        time.sleep(0.3)
        self.assertLessEqual(len(self.polls), count + 1)

    def test_telemetry_follows_channel_subscription(self) -> None:
        self.agent.wait("worker.register")
        self.agent.send("worker.ready", {"agentVersion": "1"})
        self._context("worker.context", metricsIntervalMs=60_000, channels={"example.other": 50})
        self._wait(lambda: len(self.polls) >= 1)
        time.sleep(0.2)
        self.assertEqual(len(self.polls), 1, "подписка на чужой канал — по частоте метрик")
        # Подписка на свой канал — чаще частоты метрик.
        self._context("worker.context", metricsIntervalMs=60_000, channels={"example.app": 50})
        self._wait(lambda: len(self.polls) >= 4)
        self.assertEqual(self.worker.context.channels, {"example.app": 50})
        # Подписка снята — снова частота метрик.
        self._context("worker.context", metricsIntervalMs=60_000, channels={})
        time.sleep(0.1)
        count = len(self.polls)
        time.sleep(0.3)
        self.assertLessEqual(len(self.polls), count + 1)

    def test_context_channels_parsing(self) -> None:
        ctx = WorkerContext.from_message({"channels": {"example.app": 1000, "bad": "x", "zero": 0, "no": True}})
        self.assertEqual(ctx.channels, {"example.app": 1000})
        self.assertEqual(WorkerContext.from_message({"channels": [1]}).channels, {})

    def test_invalid_interval(self) -> None:
        for bad in (0, -1, "fast", "metrics", True):
            with self.subTest(bad), self.assertRaises(ValueError):
                self.worker.telemetry("example.x", interval=bad)  # type: ignore[arg-type]


class _Uploads(BaseHTTPRequestHandler):
    received: List[bytes] = []

    def do_PUT(self) -> None:  # noqa: N802
        body = self.rfile.read(int(self.headers["Content-Length"]))
        if self.path.startswith("/expired"):
            self.send_response(403)
        else:
            _Uploads.received.append(body)
            self.send_response(200)
        self.end_headers()

    def do_GET(self) -> None:  # noqa: N802
        self.send_response(200)
        self.end_headers()
        self.wfile.write(b"input-bytes")

    def log_message(self, *args: Any) -> None:
        pass


class FilesTest(unittest.TestCase):
    def test_upload_retries_with_fresh_url_and_download(self) -> None:
        job_module.UPLOAD_RETRY_SECONDS = 0
        server = HTTPServer(("127.0.0.1", 0), _Uploads)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        base = f"http://127.0.0.1:{server.server_port}"
        ours, theirs = socket.socketpair()
        agent = FakeAgent(theirs)
        agent.urls_reply = {"inputs": {}, "outputs": {"out": {"url": f"{base}/fresh", "contentType": "text/plain"}},
                            "expiresAt": int(time.time() * 1000) + 3_600_000}
        channel = Channel(ours)
        threading.Thread(target=lambda: list(channel.messages()), daemon=True).start()
        job = Job(channel, assign("u1", inputs={"src": f"{base}/in"},
                                  outputs={"out": {"url": f"{base}/expired", "contentType": "text/plain"}}))

        job.upload("out", b"result")
        self.assertEqual(_Uploads.received, [b"result"])
        self.assertEqual(len(agent.wait("job.urls")), 1)
        self.assertEqual(job.input_path("src").read_bytes(), b"input-bytes")
        with self.assertRaises(Cancelled):
            job.cancel()
            job.check_cancelled()
        job.close()
        server.shutdown()
        server.server_close()



class ServerCATest(unittest.TestCase):
    """AGENT_SERVER_CA_FILE: файлы задач с сервера на своём корневом сертификате."""

    def setUp(self) -> None:
        openssl = shutil.which("openssl")
        if not openssl:
            self.skipTest("нет openssl: самоподписанный сертификат не сгенерировать")
        self.dir = Path(tempfile.mkdtemp())
        self.addCleanup(shutil.rmtree, self.dir, True)
        self.cert, key = self.dir / "cert.pem", self.dir / "key.pem"
        done = subprocess.run(
            [openssl, "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1", "-subj", "/CN=localhost",
             "-addext", "subjectAltName=DNS:localhost,IP:127.0.0.1",
             "-keyout", str(key), "-out", str(self.cert)],
            stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, check=False)
        if done.returncode != 0:
            self.skipTest(f"openssl не создал сертификат: {done.stderr.decode(errors='replace').strip()}")
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        context.load_cert_chain(str(self.cert), str(key))
        server = HTTPServer(("127.0.0.1", 0), _Uploads)
        server.socket = context.wrap_socket(server.socket, server_side=True)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        self.addCleanup(server.server_close)
        self.addCleanup(server.shutdown)
        self.base = f"https://127.0.0.1:{server.server_port}"
        prev = os.environ.pop("AGENT_SERVER_CA_FILE", None)
        self.addCleanup(lambda: os.environ.pop("AGENT_SERVER_CA_FILE", None) if prev is None
                        else os.environ.__setitem__("AGENT_SERVER_CA_FILE", prev))

    def test_download_and_upload_with_server_ca(self) -> None:
        with self.assertRaises(Exception, msg="без CA сервера сертификат не принят"):
            files_module.download(f"{self.base}/in", self.dir / "none")
        os.environ["AGENT_SERVER_CA_FILE"] = str(self.cert)
        target = files_module.download(f"{self.base}/in", self.dir / "in")
        self.assertEqual(target.read_bytes(), b"input-bytes")
        _Uploads.received.clear()
        files_module.upload(f"{self.base}/out", b"result", "text/plain")
        self.assertEqual(_Uploads.received, [b"result"])


if __name__ == "__main__":
    unittest.main()


class CleanupTest(unittest.TestCase):
    """``worker.cleanup`` → ``worker.cleaned {ok, error?}`` с ``re`` = id запроса."""

    def _start(self, handler: Optional[Any], queue: bool = True) -> FakeAgent:
        ours, theirs = socket.socketpair()
        agent = FakeAgent(theirs)
        worker = Worker("kv", channel=Channel(ours))
        if queue:
            worker.register("example.kv", lambda job: None)
        if handler is not None:
            worker.cleanup(handler)
        thread = threading.Thread(target=worker.run, kwargs={"install_signals": False}, daemon=True)
        thread.start()
        self.addCleanup(thread.join, 5)
        self.addCleanup(worker.drain)
        agent.wait("worker.register")
        return agent

    def _request(self, agent: FakeAgent) -> Dict[str, Any]:
        request = message("worker.cleanup")
        self.assertEqual((request["type"], request["data"]), ("worker.cleanup", {}))
        agent.sock.sendall(json.dumps(request).encode() + b"\n")
        reply = agent.wait("worker.cleaned")[0]
        self.assertEqual(reply["re"], request["id"])
        return reply

    def _check_example(self, reply: Dict[str, Any], name: str) -> None:
        want = message(name)
        self.assertEqual(set(reply) - {"ts"}, set(want) - {"ts"})
        self.assertEqual(set(reply["data"]), set(want["data"]))

    def test_handler_called(self) -> None:
        calls: List[int] = []
        agent = self._start(lambda: calls.append(1))
        reply = self._request(agent)
        self.assertEqual(reply["data"], {"ok": True})
        self.assertEqual(calls, [1])
        self._check_example(reply, "worker.cleaned")

    def test_cleanup_only_worker_runs(self) -> None:
        calls: List[int] = []
        reply = self._request(self._start(lambda: calls.append(1), queue=False))
        self.assertEqual((reply["data"], calls), ({"ok": True}, [1]))

    def test_nothing_declared_fails(self) -> None:
        ours, theirs = socket.socketpair()
        self.addCleanup(theirs.close)
        with self.assertRaisesRegex(RuntimeError, "нечего объявить"):
            Worker("kv", channel=Channel(ours)).run(install_signals=False)

    def test_no_handler_ok(self) -> None:
        reply = self._request(self._start(None))
        self.assertEqual(reply["data"], {"ok": True})

    def test_handler_error(self) -> None:
        def fail() -> None:
            raise OSError("каталог занят")

        reply = self._request(self._start(fail))
        self.assertFalse(reply["data"]["ok"])
        self.assertIsInstance(reply["data"]["error"], str)
        self.assertIn("каталог занят", reply["data"]["error"])
        self._check_example(reply, "worker.cleaned.failed")


class StoppingTest(unittest.TestCase):
    def test_stopping_signal_after_drain(self) -> None:
        w = Worker("s", version="1")
        self.assertFalse(w.stopping.is_set())
        w.drain()
        self.assertTrue(w.stopping.wait(0))


class NamesTest(unittest.TestCase):
    """Имена объявлений — по ``NAME_PATTERN``; неверное — ``ValueError`` при объявлении."""

    BAD = ["", ".x", "-x", "a b", "a/b", "имя", "a" * 65]

    def test_valid_name(self) -> None:
        from agent_sdk.message import NAME_PATTERN, valid_name
        self.assertTrue(NAME_PATTERN.startswith("^"))
        for name in ["a", "example.kv", "Q_1-x.y", "9", "a" * 64]:
            self.assertTrue(valid_name(name), name)
        for name in self.BAD + [None, 1]:
            self.assertFalse(valid_name(name), name)

    def test_bad_names_rejected_at_declaration(self) -> None:
        w = Worker("w")
        for name in self.BAD:
            with self.subTest(name=name):
                with self.assertRaisesRegex(ValueError, "неверное имя"):
                    w.job(name)
                with self.assertRaisesRegex(ValueError, "неверное имя"):
                    w.register(name, lambda j: None)
                with self.assertRaisesRegex(ValueError, "неверное имя"):
                    w.command(name)
                with self.assertRaisesRegex(ValueError, "неверное имя"):
                    w.state(name)
                with self.assertRaisesRegex(ValueError, "неверное имя"):
                    w.telemetry(name)
                with self.assertRaisesRegex(ValueError, "неверное имя"):
                    w.channel(name)
        self.assertFalse(w._handlers or w._commands or w._domains or w._channels)
        w.job("example.ok")(lambda j: None)
        w.channel("example.ok")
