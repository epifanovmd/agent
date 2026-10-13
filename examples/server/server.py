"""Сервер стенда — минимальный бэкенд агентов на Python (стандартная библиотека, Python ≥ 3.10).

Реализует сторону бэкенда протокола (docs/PROTOCOL.md, часть I) и небольшой HTTP API для
человека и сквозных тестов. Состояние — в памяти и в файле DATA_FILE (переживает перезапуск).

Переменные окружения:
  PORT          порт (по умолчанию 8080)
  ENROLL_TOKEN  токен регистрации агентов (по умолчанию demo-token)
  DATA_FILE     файл состояния (по умолчанию .dev/server.json)
  TLS_CERT, TLS_KEY  сертификат и ключ — HTTPS вместо HTTP

API (JSON):
  GET    /api/agents                                 агенты: id, имя, связь, версия
  GET    /api/agents/{id}                            всё об агенте: hello, state, desired
  PUT    /api/agents/{id}/configs/{worker}/{key}     тело — значение ключа; новая версия и rev
  DELETE /api/agents/{id}/configs/{worker}/{key}     удалить ключ
  PUT    /api/agents/{id}/watch                      {metricsIntervalMs, logLevel, until}; DELETE — снять
  POST   /api/agents/{id}/commands?wait=30           {type, args, deadline?} → команда агенту (§8);
                                                     wait — ждать итога столько секунд
  POST   /api/agents/{id}/workers/{worker}/command   {type, args, timeoutMs?} → итог worker.command
  GET    /api/agents/{id}/commands/{cid}             команда и её итог
  GET    /api/agents/{id}/events?limit=100           события воркеров, новые первыми
  GET    /api/agents/{id}/metrics?limit=10           точки метрик, новые первыми
  GET    /api/agents/{id}/logs?limit=100             записи журнала, новые первыми
  POST   /api/agents/{id}/revoke                     отозвать агента

Запрос воркера echo.lookup сервер обслуживает сам: {"prefix": "<имя агента>: "}.
"""

import hashlib
import hmac
import json
import os
import secrets
import ssl
import sys
import threading
import time
import uuid
from collections import deque
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlsplit

PORT = int(os.environ.get("PORT", "8080"))
ENROLL_TOKEN = os.environ.get("ENROLL_TOKEN", "demo-token")
DATA_FILE = os.environ.get("DATA_FILE", ".dev/server.json")
PING = 15
KEEP = {"events": 1000, "metrics": 200, "logs": 1000}


def now_ms():
    return int(time.time() * 1000)


def sha256(text):
    return hashlib.sha256(text.encode()).hexdigest()


class Store:
    """Агенты, их желаемое, команды и итоги; изменения будят потоки агентов."""

    def __init__(self, path):
        self.path = path
        self.cond = threading.Condition()
        self.seq = 0
        self.agents = {}
        self.load()

    def load(self):
        try:
            with open(self.path, encoding="utf-8") as f:
                data = json.load(f)
        except (OSError, ValueError):
            return
        self.seq = data.get("seq", 0)
        for a in data.get("agents", {}).values():
            for k in KEEP:
                a[k] = deque(a.get(k, []), maxlen=KEEP[k])
            a["online"] = 0
            self.agents[a["id"]] = a

    def save(self):
        data = {"seq": self.seq, "agents": {}}
        for aid, a in self.agents.items():
            data["agents"][aid] = {k: (list(v) if isinstance(v, deque) else v) for k, v in a.items()}
        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f)
        os.replace(tmp, self.path)

    def changed(self):
        self.save()
        self.cond.notify_all()

    def add_agent(self, name, labels, host):
        aid, secret = uuid.uuid4().hex, secrets.token_hex(24)
        self.agents[aid] = {
            "id": aid, "name": name, "labels": labels, "host": host, "secretHash": sha256(secret),
            "pendingSecretHash": None, "revoked": False, "hello": None, "state": None, "online": 0,
            "desired": {"rev": 1, "workers": {}, "watch": None}, "commands": [],
            "events": deque(maxlen=KEEP["events"]), "metrics": deque(maxlen=KEEP["metrics"]),
            "logs": deque(maxlen=KEEP["logs"]),
        }
        self.changed()
        return aid, secret

    def command(self, a, typ, args, deadline=None):
        self.seq += 1
        cmd = {"seq": self.seq, "id": uuid.uuid4().hex, "type": typ, "args": args or {}}
        if deadline:
            cmd["deadline"] = deadline
        a["commands"].append({"command": cmd, "result": None, "at": now_ms()})
        a["commands"] = a["commands"][-500:]
        self.changed()
        return cmd

    @staticmethod
    def find_command(a, cid):
        for c in a["commands"]:
            if c["command"]["id"] == cid:
                return c
        return None


store = Store(DATA_FILE)


class HTTPError(Exception):
    def __init__(self, status, code, message):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *args):
        if not self.path.startswith("/agent/v1/"):
            sys.stderr.write("%s %s\n" % (self.command, fmt % args))

    # ─── общее ───

    def body(self):
        n = int(self.headers.get("Content-Length") or 0)
        raw = self.rfile.read(n) if n else b""
        if not raw:
            return None
        try:
            return json.loads(raw)
        except ValueError as e:
            raise HTTPError(400, "MESSAGE_INVALID", "тело не JSON: %s" % e) from e

    def send(self, status, value):
        raw = json.dumps(value).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def route(self, method):
        url = urlsplit(self.path)
        self.query = {k: v[-1] for k, v in parse_qs(url.query).items()}
        parts = [p for p in url.path.split("/") if p]
        try:
            if parts[:2] == ["agent", "v1"]:
                self.agent_route(method, parts[2:])
            elif parts[:1] == ["api"]:
                self.api_route(method, parts[1:])
            else:
                raise HTTPError(404, "NOT_FOUND", "нет такого адреса")
        except HTTPError as e:
            self.send(e.status, {"code": e.code, "message": e.message})
        except (BrokenPipeError, ConnectionResetError):
            pass

    def do_GET(self):
        self.route("GET")

    def do_POST(self):
        self.route("POST")

    def do_PUT(self):
        self.route("PUT")

    def do_DELETE(self):
        self.route("DELETE")

    # ─── агент (§3–§9) ───

    def authorize(self):
        """Агент по заголовку Authorization: Agent <id>.<secret> (§5)."""
        scheme, _, cred = (self.headers.get("Authorization") or "").partition(" ")
        aid, _, secret = cred.partition(".")
        a = store.agents.get(aid)
        if scheme != "Agent" or a is None:
            raise HTTPError(401, "AGENT_UNKNOWN", "ключ агента не известен")
        h = sha256(secret)
        if a["pendingSecretHash"] and hmac.compare_digest(h, a["pendingSecretHash"]):
            a["secretHash"], a["pendingSecretHash"] = h, None
            store.save()
        elif not hmac.compare_digest(h, a["secretHash"]):
            raise HTTPError(401, "AGENT_UNKNOWN", "ключ агента не известен")
        if a["revoked"]:
            raise HTTPError(403, "AGENT_REVOKED", "агент отозван")
        a["online"] = now_ms()
        return a

    def agent_route(self, method, parts):
        name = parts[0] if parts else ""
        if (method, name) == ("POST", "enroll"):
            req = self.body() or {}
            if not hmac.compare_digest(str(req.get("token", "")), ENROLL_TOKEN):
                raise HTTPError(401, "ENROLL_DENIED", "токен не принят")
            with store.cond:
                aid, secret = store.add_agent(req.get("name", ""), req.get("labels") or {}, req.get("host") or {})
            print("агент зарегистрирован:", req.get("name"), aid, flush=True)
            return self.send(200, {"agentId": aid, "secret": secret})
        with store.cond:
            a = self.authorize()
        if (method, name) == ("GET", "stream"):
            return self.stream(a)
        if (method, name) == ("GET", "poll"):
            return self.poll(a)
        body = self.body()
        with store.cond:
            if name == "hello":
                a["hello"] = body
                a["name"] = body.get("name", a["name"])
                store.save()
                return self.send(200, {"status": "active", "serverTime": now_ms(), "stateMs": 30000, "metricsMs": 10000})
            if name == "results":
                accepted = []
                for r in body or []:
                    c = store.find_command(a, r.get("re"))
                    if c is not None:
                        c["result"] = r
                        if c["command"]["type"] == "agent.rotateKey" and r.get("ok"):
                            a["pendingSecretHash"] = (r.get("result") or {}).get("secretHash")
                    accepted.append(r.get("re"))
                store.changed()
                return self.send(200, {"accepted": accepted})
            if name == "events":
                ids = set(e["id"] for e in a["events"])
                for e in body or []:
                    if e.get("id") not in ids:
                        a["events"].append(e)
                store.changed()
                return self.send(200, {"accepted": [e.get("id") for e in body or []]})
            if name in ("state", "metrics", "logs"):
                if name == "state":
                    a["state"] = body
                else:
                    a[name].extend(body or [])
                store.save()
                return self.send(200, {})
            if name == "requests":
                return self.send(200, self.worker_request(a, body or {}))
        raise HTTPError(404, "NOT_FOUND", "нет такого адреса")

    def worker_request(self, a, req):
        """Запрос воркера к бэкенду (§17)."""
        if req.get("type") == "echo.lookup":
            return {"ok": True, "data": {"prefix": a["name"] + ": "}}
        return {"ok": False, "error": {"code": "UNKNOWN_REQUEST", "message": "запрос %s не известен" % req.get("type")}}

    def pending(self, a, after):
        return [c["command"] for c in a["commands"] if c["result"] is None and c["command"]["seq"] > after]

    def stream(self, a):
        """Поток вниз (§6): желаемое при подключении и изменении, незавершённые команды."""
        after = int(self.query.get("after", 0))
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.close_connection = True
        rev = None
        try:
            self.wfile.write(b": ok\n\n")
            self.wfile.flush()
            while True:
                with store.cond:
                    if a["revoked"]:
                        return
                    out = []
                    if a["desired"]["rev"] != rev:
                        rev = a["desired"]["rev"]
                        out.append("id: %d\nevent: desired\ndata: %s\n\n" % (rev, json.dumps(a["desired"])))
                    for cmd in self.pending(a, after):
                        out.append("id: c-%d\nevent: command\ndata: %s\n\n" % (cmd["seq"], json.dumps(cmd)))
                        after = cmd["seq"]
                    if not out:
                        store.cond.wait(PING)
                        a["online"] = now_ms()
                        out.append(": ping\n\n")
                self.wfile.write("".join(out).encode())
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            return

    def poll(self, a):
        """Длинный опрос (§6)."""
        after, rev = int(self.query.get("after", 0)), int(self.query.get("rev", 0))
        deadline = time.time() + min(int(self.query.get("wait", 30)), 60)
        with store.cond:
            while True:
                cmds = self.pending(a, after)
                if cmds or a["desired"]["rev"] != rev or time.time() >= deadline:
                    break
                store.cond.wait(max(0, deadline - time.time()))
            reply = {"commands": cmds}
            if a["desired"]["rev"] != rev:
                reply["desired"] = a["desired"]
        self.send(200, reply)

    # ─── API ───

    def agent(self, aid):
        a = store.agents.get(aid)
        if a is None:
            raise HTTPError(404, "AGENT_NOT_FOUND", "агента %s нет" % aid)
        return a

    def summary(self, a):
        hello = a["hello"] or {}
        return {"id": a["id"], "name": a["name"], "labels": a["labels"], "revoked": a["revoked"],
                "online": now_ms() - a["online"] < 90000, "version": (hello.get("agent") or {}).get("version")}

    def wait_result(self, a, cid, seconds):
        deadline = time.time() + seconds
        with store.cond:
            while True:
                c = store.find_command(a, cid)
                if c["result"] is not None or time.time() >= deadline:
                    return c
                store.cond.wait(deadline - time.time())

    def api_route(self, method, parts):
        if parts == ["agents"] and method == "GET":
            with store.cond:
                return self.send(200, [self.summary(a) for a in store.agents.values()])
        if len(parts) < 2 or parts[0] != "agents":
            raise HTTPError(404, "NOT_FOUND", "нет такого адреса")
        a, rest = self.agent(parts[1]), parts[2:]
        limit = int(self.query.get("limit", 100))
        if not rest and method == "GET":
            with store.cond:
                return self.send(200, {**self.summary(a), "host": a["host"], "hello": a["hello"], "state": a["state"],
                                       "desired": a["desired"]})
        if len(rest) == 3 and rest[0] == "configs" and method in ("PUT", "DELETE"):
            worker, key = rest[1], rest[2]
            body = self.body() if method == "PUT" else None
            with store.cond:
                configs = a["desired"]["workers"].setdefault(worker, {"configs": {}})["configs"]
                if method == "PUT":
                    version = configs.get(key, {}).get("version", 0) + 1
                    configs[key] = {"version": version, "data": body}
                else:
                    configs.pop(key, None)
                a["desired"]["rev"] += 1
                store.changed()
                return self.send(200, a["desired"])
        if rest == ["watch"] and method in ("PUT", "DELETE"):
            body = self.body() if method == "PUT" else None
            with store.cond:
                a["desired"]["watch"] = body
                a["desired"]["rev"] += 1
                store.changed()
                return self.send(200, a["desired"])
        if rest == ["commands"] and method == "POST":
            body = self.body() or {}
            if not body.get("type"):
                raise HTTPError(400, "MESSAGE_INVALID", "нужен type")
            with store.cond:
                cmd = store.command(a, body["type"], body.get("args"), body.get("deadline"))
            wait = float(self.query.get("wait", 0))
            if wait > 0:
                return self.send(200, self.wait_result(a, cmd["id"], wait))
            return self.send(200, cmd)
        if len(rest) == 3 and rest[0] == "workers" and rest[2] == "command" and method == "POST":
            body = self.body() or {}
            timeout = int(body.get("timeoutMs") or 30000)
            with store.cond:
                cmd = store.command(a, "worker.command", {"worker": rest[1], "type": body.get("type"),
                                                          "args": body.get("args"), "timeoutMs": timeout})
            c = self.wait_result(a, cmd["id"], timeout / 1000 + 5)
            if c["result"] is None:
                raise HTTPError(504, "TIMEOUT", "агент не прислал итог")
            return self.send(200, c["result"])
        if len(rest) == 2 and rest[0] == "commands" and method == "GET":
            with store.cond:
                c = store.find_command(a, rest[1])
            if c is None:
                raise HTTPError(404, "NOT_FOUND", "команды нет")
            return self.send(200, c)
        if len(rest) == 1 and rest[0] in KEEP and method == "GET":
            with store.cond:
                return self.send(200, list(a[rest[0]])[::-1][:limit])
        if rest == ["revoke"] and method == "POST":
            with store.cond:
                a["revoked"] = True
                store.changed()
            return self.send(200, self.summary(a))
        raise HTTPError(404, "NOT_FOUND", "нет такого адреса")


def main():
    server = ThreadingHTTPServer(("", PORT), Handler)
    server.daemon_threads = True
    scheme = "http"
    if os.environ.get("TLS_CERT"):
        ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
        ctx.load_cert_chain(os.environ["TLS_CERT"], os.environ.get("TLS_KEY"))
        server.socket = ctx.wrap_socket(server.socket, server_side=True)
        scheme = "https"
    print("сервер стенда: %s://localhost:%d, токен регистрации — ENROLL_TOKEN" % (scheme, PORT), flush=True)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
