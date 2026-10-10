"""База воркера (agent worker new) против поддельного агента — контракт sdk/spec §12.

Заготовка worker.py: health, manifest, настройки, маршрут, события, задачи (ход, повтор jobId,
busy, отмена, отказ), метрики. Сверх заготовки: ответ по частям, проверка задачи до 202, действие
при запуске, продолжение задачи после перезапуска, уборка сохранённого.

    python3 -m unittest discover -s test/worker-base
"""

import http.client
import json
import os
import shutil
import socket
import socketserver
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from http.server import BaseHTTPRequestHandler

ROOT = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
TEMPLATES = os.path.join(ROOT, "internal", "scaffold", "worker")


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, path):
        super().__init__("localhost", timeout=10)
        self.path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self.path)


def call(sock, method, path, body=None):
    """Запрос к воркеру: (статус, тело JSON или None, заголовки, сырое тело)."""
    conn = UnixConnection(sock)
    try:
        conn.request(method, path, None if body is None else json.dumps(body).encode(),
                     {"content-type": "application/json"})
        res = conn.getresponse()
        raw = res.read()
        try:
            data = json.loads(raw) if raw else None
        except ValueError:
            data = None
        return res.status, data, dict(res.getheaders()), raw
    finally:
        conn.close()


def wait_for(check, timeout=10.0, what=""):
    deadline = time.monotonic() + timeout
    while True:
        value = check()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError("не дождались " + what)
        time.sleep(0.05)


class FakeAgent:
    """Сокет агента: принимает POST /events (202) и запоминает их."""

    def __init__(self, path):
        self.events = []
        events = self.events

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                raw = self.rfile.read(int(self.headers.get("content-length") or 0))
                assert self.headers.get("authorization") == "Bearer tok"
                if self.path == "/events":
                    events.append(json.loads(raw))
                self.send_response(202)
                self.send_header("content-length", "2")
                self.end_headers()
                self.wfile.write(b"{}")

            def log_message(self, *args):
                pass

            def address_string(self):
                return "worker"

        class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
            daemon_threads = True

        self.server = Server(path, Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def find(self, type, job_id=None):
        return next((e for e in list(self.events) if e["type"] == type
                     and (job_id is None or e.get("data", {}).get("jobId") == job_id)), None)

    def close(self):
        self.server.shutdown()
        self.server.server_close()


class WorkerCase(unittest.TestCase):
    """Каталог воркера, поддельный агент и запуск воркера."""

    def setUp(self):
        self.dir = tempfile.mkdtemp(prefix="worker-base-")
        self.agent_sock = os.path.join(self.dir, "agent.sock")
        self.worker_sock = os.path.join(self.dir, "worker.sock")
        self.agent = FakeAgent(self.agent_sock)
        self.proc = None
        shutil.copy(os.path.join(TEMPLATES, "agent_worker.py"), self.dir)
        with open(os.path.join(self.dir, "VERSION"), "w") as f:
            f.write("2.3.4\n")

    def tearDown(self):
        if self.proc:
            self.proc.kill()
            self.proc.wait()
        self.agent.close()
        shutil.rmtree(self.dir, ignore_errors=True)

    def start(self, main):
        env = {**os.environ, "AGENT_WORKER": "demo", "AGENT_WORKER_SOCKET": self.worker_sock,
               "AGENT_SOCKET": self.agent_sock, "AGENT_WORKER_TOKEN": "tok",
               "WORKER_STATE_DIR": os.path.join(self.dir, "state")}
        self.proc = subprocess.Popen([sys.executable, os.path.join(self.dir, main)], cwd=self.dir, env=env)

        def up():
            try:
                return call(self.worker_sock, "GET", "/health")[0] == 200
            except OSError:
                return False
        wait_for(up, what="запуска воркера")


class TestSkeleton(WorkerCase):
    """Заготовка worker.py из agent worker new."""

    def setUp(self):
        super().setUp()
        with open(os.path.join(TEMPLATES, "worker.py"), encoding="utf-8") as f:
            code = f.read().replace("{{.Name}}", "demo").replace("{{.Class}}", "Demo")
        with open(os.path.join(self.dir, "worker.py"), "w", encoding="utf-8") as f:
            f.write(code)
        self.start("worker.py")

    def test_contract(self):
        sock, events = self.worker_sock, self.agent
        self.assertEqual(call(sock, "GET", "/health")[1], {"ok": True})
        m = call(sock, "GET", "/manifest")[1]
        self.assertEqual(m["version"], "2.3.4")
        self.assertEqual([c["key"] for c in m["configs"]], ["settings"])
        self.assertEqual(["%s %s" % (r["method"], r["path"]) for r in m["routes"]], ["POST /hello"])
        self.assertEqual([e["type"] for e in m["events"]], ["demo.greeted"])
        self.assertEqual([j["type"] for j in m["jobs"]], ["demo.count"])

        self.assertEqual(call(sock, "PUT", "/config/settings", {"version": 3, "data": {"greeting": "хай"}})[0], 204)
        status, body, _, _ = call(sock, "POST", "/hello", {"name": "Аня"})
        self.assertEqual((status, body), (200, {"text": "хай, Аня"}))
        wait_for(lambda: events.find("demo.greeted"), what="demo.greeted")
        self.assertEqual(call(sock, "DELETE", "/config/settings")[0], 204)
        self.assertEqual(call(sock, "POST", "/nope")[0], 404)

    def test_jobs(self):
        sock, events = self.worker_sock, self.agent
        status, started, _, _ = call(sock, "POST", "/jobs", {"type": "demo.count", "jobId": "j1", "data": {"to": 2}})
        self.assertEqual(status, 202)
        again = call(sock, "POST", "/jobs", {"type": "demo.count", "jobId": "j1", "data": {"to": 2}})[1]
        self.assertEqual(again["id"], started["id"], "повтор jobId — та же задача")
        self.assertTrue(call(sock, "GET", "/health")[1]["busy"])
        done = wait_for(lambda: events.find("job.done", "j1"), what="job.done")
        self.assertEqual(done["data"]["result"], {"counted": 2})
        self.assertTrue(events.find("job.progress", "j1"))
        self.assertEqual(call(sock, "GET", "/jobs/" + started["id"])[1]["state"], "done")

        long = call(sock, "POST", "/jobs", {"type": "demo.count", "jobId": "j2", "data": {"to": 30}})[1]
        wait_for(lambda: events.find("job.progress", "j2"), what="хода j2")
        status, body, _, _ = call(sock, "POST", "/jobs/%s/cancel" % long["id"])
        self.assertEqual((status, body["state"]), (200, "cancelled"))
        wait_for(lambda: events.find("job.cancelled", "j2"), what="job.cancelled")

        call(sock, "POST", "/jobs", {"type": "demo.count", "jobId": "j3", "data": {"to": 0}})
        failed = wait_for(lambda: events.find("job.failed", "j3"), what="job.failed")
        self.assertEqual(failed["data"]["error"]["code"], "BAD_INPUT")
        self.assertEqual(call(sock, "POST", "/jobs", {"type": "other"})[0], 400)

        time.sleep(0.3)
        metrics = call(sock, "GET", "/metrics")[1]
        self.assertEqual((metrics["jobsDone"], metrics["jobsCancelled"], metrics["jobsFailed"]), (1, 1, 1))
        self.assertEqual(len([e for e in events.events if e["type"] == "job.cancelled"]), 1, "итог у задачи один")


EXTRA = '''from agent_worker import HTTPError, Response, Worker, job, route


class Extra(Worker):
    events = {"x.started": None}

    @route("GET", "/stream")
    def stream(self, req):
        n = int(req.query.get("n", "3"))
        return Response(200, ("строка %d\\n" % i for i in range(n)))

    @job("x.steps", resumable=True)
    def steps(self, job):
        for i in range(job.saved.get("step", 0), int(job.data["steps"])):
            job.save(step=i + 1)
            job.progress((i + 1) / job.data["steps"])
            job.sleep(0.3)
        return {"steps": job.data["steps"]}

    def prepare_job(self, type, data, files):
        if not isinstance((data or {}).get("steps"), int):
            raise HTTPError(400, "steps — число")

    def on_start(self):
        self.emit("x.started", {"ok": True})


if __name__ == "__main__":
    Extra().run()
'''


class TestExtra(WorkerCase):
    """Возможности базы сверх заготовки."""

    def setUp(self):
        super().setUp()
        with open(os.path.join(self.dir, "extra.py"), "w", encoding="utf-8") as f:
            f.write(EXTRA)
        self.start("extra.py")

    def test_stream_prepare_start(self):
        wait_for(lambda: self.agent.find("x.started"), what="on_start")
        status, _, headers, raw = call(self.worker_sock, "GET", "/stream?n=3")
        self.assertEqual(status, 200)
        self.assertEqual(headers.get("transfer-encoding", headers.get("Transfer-Encoding")), "chunked")
        self.assertEqual(raw.decode(), "строка 0\nстрока 1\nстрока 2\n")
        self.assertEqual(call(self.worker_sock, "POST", "/jobs", {"type": "x.steps", "data": {"steps": "много"}})[0], 400)

    def test_resume_and_cleanup(self):
        sock, events = self.worker_sock, self.agent
        call(sock, "POST", "/jobs", {"type": "x.steps", "jobId": "r1", "data": {"steps": 20}})
        wait_for(lambda: len([e for e in events.events if e["type"] == "job.progress"]) >= 2, what="хода")
        self.proc.kill()  # воркер упал посреди задачи
        self.proc.wait()
        self.start("extra.py")
        wait_for(lambda: events.find("job.done", "r1"), timeout=20, what="job.done после перезапуска")
        progress = [e for e in events.events if e["type"] == "job.progress"]
        self.assertLess(len(progress), 22, "задача продолжилась с сохранённого шага, а не с начала")

        call(sock, "POST", "/jobs", {"type": "x.steps", "jobId": "r2", "data": {"steps": 50}})
        self.assertEqual(call(sock, "POST", "/cleanup")[0], 204)
        self.assertEqual(os.listdir(os.path.join(self.dir, "state", "jobs")), [])


if __name__ == "__main__":
    unittest.main()
