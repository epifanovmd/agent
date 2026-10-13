"""Воркер echo — пример воркера на Python без библиотек (стандартная библиотека, Python ≥ 3.10).

Воркер — HTTP-клиент сокета агента (docs/PROTOCOL.md, §11–§17): регистрируется, держит поток
GET /stream (желаемое и команды), присылает итоги, ход, события и пульс.

Настройка settings {"prefix": "…", "upper": true} — как echo.say меняет текст.

Команды:
  echo.say  {"text", "lookup"?}           → {"text"}; lookup: true — префикс спросить у бэкенда
                                            (запрос echo.lookup); событие echo.said
  echo.long {"steps", "delayMs", "text"?} → {"text", "steps"}; ход — после каждого шага, пока идёт —
                                            пульс busy: true (агент не заменяет воркер); отмена —
                                            итог CANCELLED
  echo.hang {}                            → {} и воркер перестаёт присылать пульс (агент его
                                            перезапустит)

Запускает воркер агент (agent.yaml → workers).
"""

import http.client
import json
import os
import signal
import socket
import sys
import threading
import time
import uuid

VERSION = "1.0.0"
SOCKET = os.environ.get("AGENT_SOCKET", "")
TOKEN = os.environ.get("AGENT_WORKER_TOKEN", "")

MANIFEST = {
    "description": "Эхо: текст с префиксом, долгая работа с ходом и отменой",
    "configs": [
        {
            "key": "settings",
            "description": "Префикс и регистр текста echo.say",
            "schema": {
                "type": "object",
                "properties": {"prefix": {"type": "string", "maxLength": 100}, "upper": {"type": "boolean"}},
                "additionalProperties": False,
            },
        }
    ],
    "commands": [
        {
            "type": "echo.say",
            "description": "Текст с префиксом",
            "schema": {"type": "object", "properties": {"text": {"type": "string"}, "lookup": {"type": "boolean"}},
                       "required": ["text"]},
            "result": {"type": "object", "properties": {"text": {"type": "string"}}},
        },
        {
            "type": "echo.long",
            "description": "Долгая работа по шагам: ход, отмена, busy",
            "schema": {"type": "object", "properties": {"steps": {"type": "integer", "minimum": 1},
                                                        "delayMs": {"type": "integer", "minimum": 0},
                                                        "text": {"type": "string"}}},
        },
        {"type": "echo.hang", "description": "Перестать присылать пульс"},
    ],
    "events": [{"type": "echo.said", "description": "echo.say выполнена",
                "schema": {"type": "object", "properties": {"text": {"type": "string"}}}}],
    "requests": [{"type": "echo.lookup", "description": "Префикс от бэкенда",
                  "response": {"type": "object", "properties": {"prefix": {"type": "string"}}}}],
}


class Error(Exception):
    """Ошибка с кодом — итог ok: false."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message


class UnixConnection(http.client.HTTPConnection):
    def __init__(self, timeout=30):
        super().__init__("agent", timeout=timeout)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(SOCKET)


def call(method, path, body=None, timeout=30):
    """Запрос к сокету агента: (код, JSON ответа)."""
    conn = UnixConnection(timeout)
    try:
        headers = {"Authorization": "Worker " + TOKEN}
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        conn.request(method, path, data, headers)
        resp = conn.getresponse()
        raw = resp.read()
        return resp.status, json.loads(raw) if raw else {}
    finally:
        conn.close()


def log(*args):
    print(*args, flush=True)


def warn(*args):
    print(*args, file=sys.stderr, flush=True)


class Echo:
    def __init__(self):
        self.lock = threading.Lock()
        self.prefix, self.upper = "", False
        self.applied = {}  # ключ → версия
        self.running = {}  # id команды → threading.Event (отмена)
        self.done = {}  # id → итог (последние 10 000)
        self.unsent = []  # итоги, которые агент ещё не принял
        self.counters = {"said": 0, "long": 0}
        self.busy = 0
        self.hung = False
        self.stop = threading.Event()

    # ─── настройки (§14) ───

    def on_desired(self, configs):
        results = []
        for key, value in configs.items():
            if self.applied.get(key) == value["version"]:
                continue
            item = {"key": key, "version": value["version"], "ok": True}
            try:
                self.configure(key, value.get("data"))
                self.applied[key] = value["version"]
            except Error as e:
                item.update(ok=False, error={"code": e.code, "message": e.message})
            results.append(item)
        for key in list(self.applied):
            if key not in configs:
                self.configure(key, None)
                del self.applied[key]
        if results:
            call("POST", "/applied", results)

    def configure(self, key, data):
        if key != "settings":
            return
        data = data or {}
        if not isinstance(data, dict) or not isinstance(data.get("prefix", ""), str):
            raise Error("CONFIG_REJECTED", "settings: нужен объект {prefix, upper}")
        with self.lock:
            self.prefix, self.upper = data.get("prefix", ""), bool(data.get("upper", False))
        log("настройка settings:", data)

    # ─── команды (§15) ───

    def on_command(self, cmd):
        cid = cmd["id"]
        with self.lock:
            if cid in self.running:
                return
            if cid in self.done:
                self.unsent.append(self.done[cid])
                return
            cancel = threading.Event()
            self.running[cid] = cancel
        threading.Thread(target=self.execute, args=(cmd, cancel), daemon=True).start()

    def on_cancel(self, re):
        with self.lock:
            cancel = self.running.get(re)
        if cancel:
            cancel.set()

    def execute(self, cmd, cancel):
        cid, args = cmd["id"], cmd.get("args") or {}
        result = {"re": cid, "ok": True}
        try:
            handler = {"echo.say": self.say, "echo.long": self.long, "echo.hang": self.hang}.get(cmd["type"])
            if handler is None:
                raise Error("UNKNOWN_COMMAND", "команда %s не поддерживается" % cmd["type"])
            result["result"] = handler(cid, args, cancel)
        except Error as e:
            result = {"re": cid, "ok": False, "error": {"code": e.code, "message": e.message}}
        except Exception as e:  # noqa: BLE001 — итог с ошибкой вместо падения воркера
            result = {"re": cid, "ok": False, "error": {"code": "COMMAND_FAILED", "message": str(e)}}
        with self.lock:
            del self.running[cid]
            self.done[cid] = result
            if len(self.done) > 10000:
                self.done.pop(next(iter(self.done)))
            self.unsent.append(result)

    def say(self, _cid, args, _cancel):
        text = str(args.get("text", ""))
        with self.lock:
            prefix, upper = self.prefix, self.upper
        if args.get("lookup"):
            status, reply = call("POST", "/requests", {"type": "echo.lookup", "data": {"text": text}, "timeoutMs": 10000})
            if status != 200 or not reply.get("ok"):
                err = reply.get("error") or {}
                raise Error(err.get("code", "LOOKUP_FAILED"), err.get("message", "бэкенд не ответил"))
            prefix = (reply.get("data") or {}).get("prefix", "")
        text = prefix + (text.upper() if upper else text)
        with self.lock:
            self.counters["said"] += 1
        self.emit("echo.said", {"text": text})
        return {"text": text}

    def emit(self, typ, data):
        """Событие агенту: повтор, пока агент недоступен (перезапускается) или очередь полна."""
        event = {"id": str(uuid.uuid4()), "type": typ, "data": data}
        for _ in range(60):
            try:
                status, reply = call("POST", "/events", [event])
                if status == 202:
                    return
                if status != 503:
                    raise Error(reply.get("code", "EVENT_FAILED"), reply.get("message", "событие не принято"))
            except OSError:
                pass
            if self.stop.wait(1):
                return
        raise Error("EVENT_FAILED", "агент не принял событие")

    def long(self, cid, args, cancel):
        steps, delay = max(1, int(args.get("steps", 5))), int(args.get("delayMs", 500)) / 1000
        with self.lock:
            self.busy += 1
        try:
            for step in range(1, steps + 1):
                if cancel.wait(delay):
                    raise Error("CANCELLED", "команда отменена на шаге %d" % step)
                try:
                    call("POST", "/progress", {"re": cid, "progress": step / steps, "message": "шаг %d из %d" % (step, steps)})
                except OSError:
                    pass  # ход необязателен: агент перезапускается
            with self.lock:
                self.counters["long"] += 1
            return {"text": str(args.get("text", "готово")), "steps": steps}
        finally:
            with self.lock:
                self.busy -= 1

    def hang(self, _cid, _args, _cancel):
        log("echo.hang: пульса больше не будет")
        self.hung = True
        return {}

    # ─── связь ───

    def beat(self):
        """Пульс: самочувствие, занятость, метрики."""
        if self.hung:
            return
        with self.lock:
            health = {"ok": True, "busy": self.busy > 0, "metrics": dict(self.counters)}
        try:
            call("POST", "/health", health)
        except OSError as e:
            warn("пульс не отправлен:", e)

    def heartbeat(self):
        while not self.stop.wait(10):
            self.beat()

    def deliver(self):
        while not self.stop.wait(0.2):
            with self.lock:
                batch = list(self.unsent)
            if not batch:
                continue
            try:
                status, _ = call("POST", "/results", batch)
            except OSError:
                continue
            if status == 200:
                with self.lock:
                    del self.unsent[: len(batch)]

    def session(self):
        status, reply = call("POST", "/register", {"version": VERSION, "manifest": MANIFEST})
        if status != 200:
            raise OSError("регистрация: HTTP %d %s" % (status, reply))
        self.beat()  # пульс до регистрации агент не принимает: первый — сразу после неё
        conn = UnixConnection(timeout=60)
        conn.request("GET", "/stream", headers={"Authorization": "Worker " + TOKEN})
        resp = conn.getresponse()
        if resp.status != 200:
            raise OSError("поток: HTTP %d" % resp.status)
        log("echo", VERSION, "на связи с агентом")
        event, data = "", []
        while not self.stop.is_set():
            line = resp.readline()
            if not line:
                raise OSError("агент закрыл поток")
            line = line.decode().rstrip("\r\n")
            if line.startswith(":"):
                continue
            if line:
                field, _, value = line.partition(":")
                value = value[1:] if value.startswith(" ") else value
                if field == "event":
                    event = value
                elif field == "data":
                    data.append(value)
                continue
            if data:
                payload = json.loads("\n".join(data))
                if event == "desired":
                    self.on_desired(payload.get("configs") or {})
                elif event == "command":
                    self.on_command(payload)
                elif event == "cancel":
                    self.on_cancel(payload.get("re"))
            event, data = "", []

    def run(self):
        threading.Thread(target=self.heartbeat, daemon=True).start()
        threading.Thread(target=self.deliver, daemon=True).start()
        delay = 1
        while not self.stop.is_set():
            try:
                self.session()
            except (OSError, ValueError, http.client.HTTPException) as e:
                if self.stop.is_set():
                    break
                warn("связь с агентом:", e, "— повтор через", delay, "с")
                self.stop.wait(delay)
                delay = min(delay * 2, 10)


def main():
    if not SOCKET or not TOKEN:
        sys.exit("echo: нет AGENT_SOCKET или AGENT_WORKER_TOKEN — воркер запускает агент")
    echo = Echo()

    def stop(*_):
        echo.stop.set()
        sys.exit(0)

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    print("echo", VERSION, "запущен", flush=True)
    echo.run()


if __name__ == "__main__":
    main()
