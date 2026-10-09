"""Воркер echo — пример воркера без SDK на Python (только стандартная библиотека, Python ≥ 3.10).

HTTP-сервис на unix-сокете AGENT_WORKER_SOCKET (sdk/spec §12):

  POST /echo            тело {"text": "…"} или текст → {"text": "<префикс>ТЕКСТ"}
  GET  /stream?n=5      ответ по частям: n строк с паузой (потоковый ответ fetch)
  GET  /bytes?n=256     двоичный ответ: n байт 0, 1, …, 255, 0, …
  POST /hang            «зависнуть»: GET /health больше не отвечает (агент перезапустит воркер)
  POST /jobs            задачи (§12): {"type", "jobId", "data"}
                          echo.quick {"text", "lookup"?} → 200 {"result": {"text"}} — итог сразу;
                          lookup: true — префикс спросить у бэкенда (запрос echo.lookup);
                          echo.long {"steps": 5, "delayMs": 500, "text"?} → 202 {"id"}; дальше события
                          job.progress {jobId, id, progress, message} и job.done {jobId, id, result}
                          через агента; пока задача идёт, GET /health отвечает busy: true (агент не
                          заменяет воркер до её окончания)
  GET  /jobs/{id}       состояние: {id, state, progress, result?}; state — running | done | cancelled
  POST /jobs/{id}/cancel  прервать задачу (событие job.cancelled)
  PUT  /config/settings {version, data: {"prefix": "…", "upper": true}} → 200 {"applied": {"prefix",
                        "upper"}} — подробный итог: значения, которые теперь действуют (бэкенд видит
                        его в ConfigStatus.result); неверное — 400 {message}
  DELETE /config/settings  вернуть значения по умолчанию
  GET  /metrics         счётчики
  GET  /health          {ok, message, info}
  GET  /manifest        что воркер умеет: версия, ключ настроек settings, маршруты, события, задачи
                        и запросы к бэкенду — со схемами
  POST /cleanup         убрать созданное на узле: файл ECHO_STATE_FILE, настройку и счётчики

ECHO_STATE_FILE (необязательно) — файл на узле, куда echo записывает применённую настройку:
пример того, что воркер создаёт на узле и убирает при POST /cleanup.

ECHO_JOBS_DIR (необязательно) — каталог, где echo хранит ход каждой долгой задачи (<id>.json)
после каждого шага: запущенный заново воркер продолжает незаконченные задачи с сохранённого шага.

События уходят агенту: POST /events на AGENT_SOCKET с заголовком
Authorization: Bearer $AGENT_WORKER_TOKEN; запрос к бэкенду — POST /requests туда же, ответ —
когда бэкенд ответит. Запускает воркер агент (agent.yaml → workers).
"""

import http.client
import json
import os
import signal
import socket
import socketserver
import sys
import threading
import time
import uuid
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

VERSION = "1.0.0"
DEFAULTS = {"prefix": "", "upper": True}
MAX_PREFIX = 64

# Манифест (sdk/spec §12): по нему агент пропускает к воркеру только объявленные маршруты, задачи,
# события и запросы к бэкенду, а сервер может проверить настройку, тело запроса и data события по
# схемам.
MANIFEST = {
    "version": VERSION,
    "description": "Эхо: текст, потоковый и двоичный ответ, быстрые и долгие задачи",
    "configs": [
        {
            "key": "settings",
            "description": "Как отвечать: префикс и верхний регистр",
            "schema": {
                "type": "object",
                "properties": {
                    "prefix": {"type": "string", "maxLength": MAX_PREFIX},
                    "upper": {"type": "boolean"},
                },
                "additionalProperties": False,
            },
        }
    ],
    "routes": [
        {
            "method": "POST",
            "path": "/echo",
            "description": "Текст с префиксом",
            "request": {"type": "object", "properties": {"text": {"type": "string"}}},
            "response": {"type": "object", "properties": {"text": {"type": "string"}}, "required": ["text"]},
        },
        {"method": "GET", "path": "/stream", "description": "Ответ по частям, ?n= строк"},
        {"method": "GET", "path": "/bytes", "description": "Двоичный ответ, ?n= байт"},
        {"method": "POST", "path": "/hang", "description": "Зависнуть: GET /health больше не отвечает"},
    ],
    "events": [
        {
            "type": "echo.started",
            "description": "Воркер запущен",
            "schema": {
                "type": "object",
                "properties": {"version": {"type": "string"}, "pid": {"type": "integer"}},
                "required": ["version", "pid"],
            },
        },
    ],
    "jobs": [
        {
            "type": "echo.quick",
            "description": "Текст с префиксом — итог сразу; lookup: true — префикс от бэкенда",
            "schema": {"type": "object", "properties": {"text": {"type": "string"}, "lookup": {"type": "boolean"}}},
        },
        {
            "type": "echo.long",
            "description": "Долгая задача: шаги с паузой, ход — job.progress, итог — job.done",
            "schema": {
                "type": "object",
                "properties": {
                    "steps": {"type": "integer", "minimum": 1, "maximum": 100},
                    "delayMs": {"type": "integer", "minimum": 0, "maximum": 10000},
                    "text": {"type": "string"},
                },
            },
        },
    ],
    "requests": [
        {
            "type": "echo.lookup",
            "description": "Спросить у бэкенда префикс для текста",
            "schema": {"type": "object", "properties": {"text": {"type": "string"}}},
            "response": {"type": "object", "properties": {"prefix": {"type": "string"}}, "required": ["prefix"]},
        },
    ],
}

lock = threading.Lock()
settings = dict(DEFAULTS)
settings_version = 0
counters = {"requests": 0, "echoed": 0, "streamed": 0, "jobs": 0, "jobsRunning": 0, "events": 0, "eventErrors": 0}
last_event_error = ""
hung = threading.Event()
STATE_FILE = os.environ.get("ECHO_STATE_FILE", "")
JOBS_DIR = os.environ.get("ECHO_JOBS_DIR", "")
# Долгие задачи: id → {id, jobId, steps, delay, text, step, state}; ход — в JOBS_DIR.
jobs = {}


def count(name, delta=1):
    with lock:
        counters[name] += delta


class AgentConnection(http.client.HTTPConnection):
    """HTTP к агенту по unix-сокету AGENT_SOCKET."""

    def __init__(self):
        super().__init__("agent", timeout=10)

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(os.environ["AGENT_SOCKET"])


def agent(method, path, body=None):
    """Запрос к агенту: (статус, тело JSON или None)."""
    conn = AgentConnection()
    try:
        headers = {"authorization": "Bearer " + os.environ.get("AGENT_WORKER_TOKEN", "")}
        raw = None
        if body is not None:
            raw = json.dumps(body).encode()
            headers["content-type"] = "application/json"
        conn.request(method, path, raw, headers)
        res = conn.getresponse()
        data = res.read()
        return res.status, (json.loads(data) if data else None)
    finally:
        conn.close()


def event(type_, data, wait=30):
    """Событие серверу: агент хранит его на диске, пока сервер не подтвердит. Агент недоступен
    (перезапускается — воркер при этом работает дальше) — повтор до wait секунд."""
    global last_event_error
    deadline = time.monotonic() + wait
    while True:
        try:
            status, body = agent("POST", "/events", {"type": type_, "data": data})
            if status != 202:
                raise RuntimeError(f"HTTP {status}: {(body or {}).get('message', '')}")
            count("events")
            with lock:
                last_event_error = ""
            return
        except OSError as e:  # сокет агента не отвечает: агент перезапускается
            if time.monotonic() < deadline:
                time.sleep(0.5)
                continue
            err = e
        except Exception as e:  # очередь полна или ответ с ошибкой: событие теряется, это видно в health
            err = e
        count("eventErrors")
        with lock:
            last_event_error = str(err)
        print(f"событие {type_} не отправлено: {err}", file=sys.stderr, flush=True)
        return


def lookup(text):
    """Запрос к бэкенду echo.lookup: (префикс, None) или (None, текст ошибки)."""
    try:
        status, body = agent("POST", "/requests", {"type": "echo.lookup", "data": {"text": text}, "timeoutMs": 5000})
    except OSError as e:
        return None, f"агент недоступен: {e}"
    if status != 200:
        return None, f"HTTP {status} {(body or {}).get('code', '')}: {(body or {}).get('message', '')}"
    prefix = ((body or {}).get("data") or {}).get("prefix")
    if not isinstance(prefix, str):
        return None, "бэкенд не прислал prefix"
    return prefix, None


def save_state():
    """Применённая настройка — в ECHO_STATE_FILE (если задан)."""
    if not STATE_FILE:
        return
    with lock:
        state = {"version": settings_version, "settings": settings}
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, ensure_ascii=False)


def remove_state():
    if STATE_FILE and os.path.exists(STATE_FILE):
        os.unlink(STATE_FILE)


def transform(text):
    with lock:
        prefix, upper = settings["prefix"], settings["upper"]
    return prefix + (text.upper() if upper else text)


def validate(data):
    """Проверка настроек settings: текст ошибки или None."""
    if not isinstance(data, dict):
        return "data: нужен объект {prefix, upper}"
    unknown = set(data) - set(DEFAULTS)
    if unknown:
        return "неизвестные поля: " + ", ".join(sorted(unknown))
    prefix = data.get("prefix", "")
    if not isinstance(prefix, str) or len(prefix) > MAX_PREFIX:
        return f"prefix: строка до {MAX_PREFIX} символов"
    if not isinstance(data.get("upper", True), bool):
        return "upper: true или false"
    return None


def save_job(job):
    """Ход задачи — на диск (ECHO_JOBS_DIR) атомарно: после перезапуска задача продолжится."""
    if not JOBS_DIR:
        return
    os.makedirs(JOBS_DIR, exist_ok=True)
    path = os.path.join(JOBS_DIR, job["id"] + ".json")
    with lock:
        raw = json.dumps(job)
    with open(path + ".tmp", "w", encoding="utf-8") as f:
        f.write(raw)
    os.replace(path + ".tmp", path)


def load_jobs():
    """Незаконченные задачи прошлого запуска — продолжить с сохранённого шага."""
    if not JOBS_DIR or not os.path.isdir(JOBS_DIR):
        return
    for name in sorted(os.listdir(JOBS_DIR)):
        if not name.endswith(".json"):
            continue
        try:
            with open(os.path.join(JOBS_DIR, name), encoding="utf-8") as f:
                job = json.load(f)
        except (OSError, ValueError):
            continue
        with lock:
            jobs[job["id"]] = job
        if job.get("state") == "running":
            print(f"задача {job['id']}: продолжаю с шага {job['step']} из {job['steps']}", flush=True)
            start_job(job)


def job_status(job):
    """Состояние задачи для GET /jobs/{id} (под lock)."""
    body = {"id": job["id"], "state": job["state"], "progress": job["step"] / job["steps"]}
    if job["state"] == "done":
        body["result"] = {"text": job["result"]}
    return body


def start_job(job):
    count("jobsRunning")
    threading.Thread(target=run_job, args=(job,), daemon=True).start()


def run_job(job):
    """Долгая задача: событие job.progress на каждый шаг (ход — на диск) и итог job.done."""
    ids = {"jobId": job["jobId"], "id": job["id"]}
    try:
        while True:
            with lock:
                if job["state"] != "running" or job["step"] >= job["steps"]:
                    break
            time.sleep(job["delay"])
            with lock:
                if job["state"] != "running":
                    break
                job["step"] += 1
                step, steps = job["step"], job["steps"]
            save_job(job)
            event("job.progress", {**ids, "progress": step / steps, "message": f"шаг {step} из {steps}"})
        text = transform(job["text"] or f"готово: {job['steps']} шагов")
        with lock:
            finished = job["state"] == "running"
            if finished:
                job["state"] = "done"
                job["result"] = text
        result = {"text": text}
        if finished:
            save_job(job)
            event("job.done", {**ids, "result": result})
    finally:
        count("jobsRunning", -1)


class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def reply(self, status, body=None):
        raw = b"" if body is None else json.dumps(body, ensure_ascii=False).encode()
        self.send_response(status)
        if body is not None:
            self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def raw_body(self):
        n = int(self.headers.get("content-length") or 0)
        return self.rfile.read(n) if n else b""

    def json_body(self):
        raw = self.raw_body()
        return json.loads(raw) if raw else None

    def route(self):
        url = urlparse(self.path)
        return url.path, parse_qs(url.query)

    def do_GET(self):
        path, query = self.route()
        if path == "/health":
            if hung.is_set():
                while True:  # «завис»: ответа не будет, пока агент не перезапустит воркер
                    time.sleep(60)
            with lock:
                err, s, v = last_event_error, dict(settings), settings_version
                running = [j for j in jobs.values() if j["state"] == "running"]
            info = {"version": VERSION, "pid": os.getpid(), "settingsVersion": v, "prefix": s["prefix"]}
            message = "работаю"
            if running:
                message = "идёт задача: " + ", ".join(f"{j['id']} — шаг {j['step']} из {j['steps']}" for j in running)
            if err:
                return self.reply(200, {"ok": False, "busy": bool(running), "message": "события не доходят: " + err, "info": info})
            return self.reply(200, {"ok": True, "busy": bool(running), "message": message, "info": info})
        if path == "/manifest":
            return self.reply(200, MANIFEST)
        if path == "/metrics":
            with lock:
                metrics = {**counters, "settingsVersion": settings_version}
            return self.reply(200, metrics)
        if path.startswith("/jobs/"):
            with lock:
                job = jobs.get(path[len("/jobs/"):])
                body = None if job is None else job_status(job)
            if body is None:
                return self.reply(404, {"message": "нет такой задачи"})
            return self.reply(200, body)
        if path == "/stream":
            return self.stream(query)
        if path == "/bytes":
            return self.binary(query)
        self.reply(404, {"message": "нет маршрута"})

    def stream(self, query):
        count("requests")
        try:
            n = max(1, min(int(query.get("n", ["5"])[0]), 100))
        except ValueError:
            return self.reply(400, {"message": "n — число"})
        count("streamed")
        self.send_response(200)
        self.send_header("content-type", "text/plain; charset=utf-8")
        self.send_header("transfer-encoding", "chunked")
        self.end_headers()
        try:
            for i in range(1, n + 1):
                data = (transform(f"строка {i} из {n}") + "\n").encode()
                self.wfile.write(b"%x\r\n%s\r\n" % (len(data), data))
                self.wfile.flush()
                time.sleep(0.3)
            self.wfile.write(b"0\r\n\r\n")
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True  # запрос отменили (fetch.cancel)

    def binary(self, query):
        count("requests")
        try:
            n = max(0, min(int(query.get("n", ["256"])[0]), 1 << 20))
        except ValueError:
            return self.reply(400, {"message": "n — число"})
        raw = bytes(i % 256 for i in range(n))
        self.send_response(200)
        self.send_header("content-type", "application/octet-stream")
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_POST(self):
        global settings, settings_version
        path, _ = self.route()
        if path == "/cleanup":
            with lock:
                settings, settings_version = dict(DEFAULTS), 0
                for k in counters:
                    if k != "jobsRunning":
                        counters[k] = 0
            remove_state()
            if JOBS_DIR and os.path.isdir(JOBS_DIR):
                for name in os.listdir(JOBS_DIR):
                    os.unlink(os.path.join(JOBS_DIR, name))
            return self.reply(204)
        if path == "/hang":
            hung.set()
            print("завис: GET /health больше не отвечает", flush=True)
            return self.reply(202, {"hung": True})
        if path == "/echo":
            count("requests")
            raw = self.raw_body()
            try:
                body = json.loads(raw) if raw else {}
                text = body.get("text", "") if isinstance(body, dict) else str(body)
            except ValueError:
                text = raw.decode(errors="replace")
            count("echoed")
            return self.reply(200, {"text": transform(str(text))})
        if path == "/jobs":
            return self.post_job()
        if path.startswith("/jobs/") and path.endswith("/cancel"):
            job_id = path[len("/jobs/"):-len("/cancel")]
            with lock:
                job = jobs.get(job_id)
                cancelled = job is not None and job["state"] == "running"
                if cancelled:
                    job["state"] = "cancelled"
            if job is None:
                return self.reply(404, {"message": "нет такой задачи"})
            if cancelled:
                save_job(job)
                event("job.cancelled", {"jobId": job["jobId"], "id": job_id})
            with lock:
                body = job_status(job)
            return self.reply(200, body)
        self.reply(404, {"message": "нет маршрута"})

    def post_job(self):
        """POST /jobs: echo.quick — итог сразу (200), echo.long — 202 {id} и события job.*."""
        count("requests")
        try:
            body = self.json_body() or {}
            type_, job_id, data = body.get("type"), str(body.get("jobId") or ""), body.get("data") or {}
            if not isinstance(data, dict):
                raise TypeError
        except (ValueError, TypeError, AttributeError):
            return self.reply(400, {"message": "тело: {type, jobId, data}, data — объект"})
        text = str(data.get("text", ""))
        if type_ == "echo.quick":
            count("echoed")
            if not data.get("lookup"):
                return self.reply(200, {"result": {"text": transform(text)}})
            prefix, err = lookup(text)
            if err:
                return self.reply(503, {"message": "префикс от бэкенда не получен: " + err})
            return self.reply(200, {"result": {"text": prefix + transform(text), "prefix": prefix}})
        if type_ != "echo.long":
            return self.reply(400, {"message": f"задачи {type_} нет: есть echo.quick и echo.long"})
        try:
            steps = int(data.get("steps", 5))
            delay = int(data.get("delayMs", 500)) / 1000
        except (ValueError, TypeError):
            return self.reply(400, {"message": "steps и delayMs — числа"})
        if not 1 <= steps <= 100 or not 0 <= delay <= 10:
            return self.reply(400, {"message": "steps — от 1 до 100, delayMs — до 10000"})
        with lock:
            # Повтор с тем же jobId — та же задача.
            same = next((j for j in jobs.values() if job_id and j["jobId"] == job_id), None)
            if same is None:
                job = {"id": uuid.uuid4().hex[:12], "jobId": job_id, "steps": steps, "delay": delay,
                       "text": text, "step": 0, "state": "running"}
                jobs[job["id"]] = job
        if same is not None:
            return self.reply(202, {"id": same["id"]})
        count("jobs")
        save_job(job)
        start_job(job)
        return self.reply(202, {"id": job["id"]})

    def do_PUT(self):
        global settings, settings_version
        path, _ = self.route()
        if not path.startswith("/config/"):
            return self.reply(404, {"message": "нет маршрута"})
        key = path[len("/config/"):]
        if key != "settings":
            return self.reply(400, {"message": f"ключ {key} не поддерживается: есть только settings"})
        try:
            body = self.json_body()
        except ValueError:
            return self.reply(400, {"message": "тело — не JSON"})
        data = (body or {}).get("data")
        err = validate(data)
        if err:
            return self.reply(400, {"message": err})
        with lock:
            settings = {**DEFAULTS, **data}
            settings_version = int(body.get("version") or 0)
        save_state()
        # В вывод — только версия: значения настроек могут быть секретными, а вывод воркера агент
        # передаёт в журнал как есть.
        print(f"настройки применены: версия {settings_version}", flush=True)
        self.reply(200, {"applied": {"prefix": settings["prefix"], "upper": settings["upper"]}})

    def do_DELETE(self):
        global settings, settings_version
        path, _ = self.route()
        if path == "/config/settings":
            with lock:
                settings, settings_version = dict(DEFAULTS), 0
            save_state()
            return self.reply(204)
        if path.startswith("/config/"):
            return self.reply(204)
        self.reply(404, {"message": "нет маршрута"})

    def log_message(self, fmt, *args):
        # Служебные запросы агента (health, metrics) идут постоянно — в журнал только остальные.
        if self.path not in ("/health", "/metrics", "/manifest"):
            print(fmt % args, flush=True)


class Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True

    def get_request(self):
        sock, _ = super().get_request()
        return sock, ("agent", 0)  # у unix-сокета нет адреса клиента, а BaseHTTPRequestHandler его ждёт


def hello():
    """После запуска: кто мы на этом узле (GET /context) и событие echo.started."""
    try:
        _, ctx = agent("GET", "/context")
        name = ((ctx or {}).get("agent") or {}).get("name", "?")
        online = "есть" if (ctx or {}).get("online") else "нет"
        print(f"echo {VERSION}: агент {name}, связь с сервером: {online}", flush=True)
    except Exception as e:
        print(f"агент не ответил на /context: {e}", file=sys.stderr, flush=True)
    event("echo.started", {"version": VERSION, "pid": os.getpid()})


def main():
    path = os.environ.get("AGENT_WORKER_SOCKET")
    if not path:
        print("нет AGENT_WORKER_SOCKET — воркер запускает агент", file=sys.stderr)
        sys.exit(2)
    if os.path.exists(path):
        os.unlink(path)
    server = Server(path, Handler)
    threading.Thread(target=hello, daemon=True).start()
    load_jobs()
    # SIGTERM — остановка: созданное на узле не убирается (это делает только POST /cleanup).
    signal.signal(signal.SIGTERM, lambda *_: threading.Thread(target=server.shutdown).start())
    server.serve_forever()


if __name__ == "__main__":
    main()
