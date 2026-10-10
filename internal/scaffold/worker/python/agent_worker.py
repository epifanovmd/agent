# agent-worker-base: 1
"""База воркера агента на Python (≥ 3.8, только стандартная библиотека).

Не правьте этот файл: его создаёт и обновляет `agent worker new` / `agent worker sync`.
Свой код — в классе-наследнике (worker.py):

    from agent_worker import Worker, Config, route, job

    class Report(Worker):
        description = "Отчёты"
        settings = Config("settings", schema={"type": "object"}, default={"limit": 10})
        events = {"report.sent": {"type": "object"}}

        @route("POST", "/reports/{id}/send", request={"type": "object"})
        def send(self, req):
            self.emit("report.sent", {"id": req.params["id"]})
            return {"ok": True}

        @job("report.build", schema={"type": "object"})
        def build(self, job):
            job.progress(0.5, "половина")
            return {"rows": self.settings.value["limit"]}

    if __name__ == "__main__":
        Report().run()

Маршрут возвращает значение — тело ответа 200 (None — 204), кортеж (статус, тело) или Response;
тело Response может быть итератором строк или байтов — ответ уходит по частям (chunked).
Ошибка — raise HTTPError(статус, текст).

База сама обслуживает GET /health (busy, пока идут долгие задачи), GET /manifest (из объявлений
и файла VERSION рядом с воркером), PUT и DELETE /config/{key}, GET /metrics, POST /cleanup,
POST /jobs, GET /jobs/{id}, POST /jobs/{id}/cancel; события задач (job.progress, job.done,
job.failed, job.cancelled) уходят агенту сами. Контракт — sdk/spec §12 агента.
"""

import http.client
import inspect
import json
import os
import re
import signal
import socket
import socketserver
import sys
import threading
import time
import traceback
import urllib.request
import uuid
from http.server import BaseHTTPRequestHandler
from urllib.parse import parse_qs, urlparse

__all__ = ["Worker", "Config", "route", "job", "HTTPError", "JobError", "Cancelled",
           "RequestRejected", "AgentUnavailable", "Request", "Response"]


# ─── Ошибки ────────────────────────────────────────────────────────────────

class HTTPError(Exception):
    """Ответ маршрута с ошибкой: статус и { message, code? }."""

    def __init__(self, status, message, code=None):
        super().__init__(message)
        self.status, self.message, self.code = status, message, code


class JobError(Exception):
    """Задача не выполнена: job.failed { error: { code, message } }."""

    def __init__(self, message, code="JOB_FAILED"):
        super().__init__(message)
        self.code, self.message = code, message


class Cancelled(Exception):
    """Задачу отменили (POST /jobs/{id}/cancel)."""


class RequestRejected(Exception):
    """Бэкенд отказал в запросе воркера (422 { code, message })."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message


class AgentUnavailable(Exception):
    """Запрос к бэкенду не дошёл: нет связи, срок вышел, агент недоступен (повторить можно)."""

    def __init__(self, code, message):
        super().__init__(message)
        self.code, self.message = code, message


# ─── Объявления ────────────────────────────────────────────────────────────

def route(method, path, request=None, response=None, description=None):
    """Маршрут для запросов бэкенда (fetch): {name} в path — один сегмент, req.params[name]."""

    def mark(fn):
        fn._agent_route = {"method": method.upper(), "path": path, "request": request,
                           "response": response, "description": description or _doc(fn)}
        return fn
    return mark


def job(type, schema=None, description=None, quick=False, resumable=False):
    """Тип задачи. quick — итог сразу (200), иначе долгая (202, ход — событиями). resumable —
    после перезапуска воркера задача продолжается с job.saved (нужен каталог состояния)."""

    def mark(fn):
        fn._agent_job = {"type": type, "schema": schema, "description": description or _doc(fn),
                         "quick": quick, "resumable": resumable}
        return fn
    return mark


class Config:
    """Ключ настроек: значение — .value (до первой настройки — default), версия — .version."""

    def __init__(self, key, schema=None, default=None, description=None):
        self.key, self.schema, self.default, self.description = key, schema, default, description
        self.value, self.version = default, 0


def _doc(fn):
    return (inspect.getdoc(fn) or "").split("\n")[0] or None


def _clean(d):
    return {k: v for k, v in d.items() if v is not None}


# ─── Запрос и ответ ────────────────────────────────────────────────────────

class Request:
    """Запрос к маршруту: method, path, params ({name} пути), query, headers, body (байты)."""

    def __init__(self, method, path, params, query, headers, body):
        self.method, self.path, self.params = method, path, params
        self.query, self.headers, self.body = query, headers, body

    def json(self):
        """Тело как JSON (пусто — {})."""
        try:
            return json.loads(self.body or b"{}")
        except ValueError:
            raise HTTPError(400, "тело — не JSON")


class Response:
    """Ответ маршрута как есть: статус, тело (bytes, str, JSON-значение или итератор частей
    str/bytes — по частям), заголовки."""

    def __init__(self, status=200, body=None, headers=None):
        self.status, self.body, self.headers = status, body, headers or {}


# ─── Задача ────────────────────────────────────────────────────────────────

class Job:
    """Долгая (или быстрая) задача: data, files, ход, отмена, сохранённое состояние."""

    def __init__(self, worker, id, job_id, type, data, files, saved=None):
        self.worker, self.id, self.job_id, self.type = worker, id, job_id, type
        self.data, self.files = data, files or {}
        self.state, self.progress_value, self.result, self.error = "running", 0.0, None, None
        self.saved = saved or {}
        self._cancel = threading.Event()

    @property
    def cancelled(self):
        return self._cancel.is_set()

    def check(self):
        """Отменили — Cancelled (задача завершится job.cancelled)."""
        if self.cancelled:
            raise Cancelled()

    def sleep(self, seconds):
        """Пауза, которую прерывает отмена (Cancelled)."""
        if self._cancel.wait(seconds):
            raise Cancelled()

    def progress(self, value, message=None):
        """Ход: доля 0…1 и пояснение → событие job.progress."""
        self.check()
        self.progress_value = max(0.0, min(1.0, float(value)))
        self.worker._job_event("job.progress", self, progress=self.progress_value, message=message)

    def save(self, **state):
        """Запомнить состояние задачи на диске: перезапущенный воркер продолжит с job.saved."""
        self.saved.update(state)
        self.worker._persist(self)

    def download(self, name, path):
        """Входной файл files.inputs[name] → path."""
        urllib.request.urlretrieve(self.files["inputs"][name], path)

    def upload(self, name, path, content_type="application/octet-stream"):
        """path → выходной файл files.outputs[name] (PUT)."""
        with open(path, "rb") as f:
            req = urllib.request.Request(self.files["outputs"][name], data=f.read(), method="PUT",
                                         headers={"content-type": content_type})
        urllib.request.urlopen(req).close()

    def view(self):
        return _clean({"id": self.id, "state": self.state, "progress": self.progress_value,
                       "result": self.result, "error": self.error})


# ─── Связь с агентом ───────────────────────────────────────────────────────

class _UnixConnection(http.client.HTTPConnection):
    def __init__(self, path, timeout):
        super().__init__("localhost", timeout=timeout)
        self._path = path

    def connect(self):
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.settimeout(self.timeout)
        self.sock.connect(self._path)


# ─── Воркер ────────────────────────────────────────────────────────────────

class Worker:
    """База воркера: наследник объявляет маршруты (@route), задачи (@job), настройки (Config),
    события (events) и запросы к бэкенду (requests) и при желании переопределяет health(),
    metrics(), validate_config(), on_config(), cleanup()."""

    description = None
    #: типы событий: {"тип": схема data или None}
    events = {}
    #: запросы к бэкенду: {"тип": {"schema": …, "response": …, "description": …}} или {"тип": None}
    requests = {}
    #: каталог состояния долгих задач (resumable); по умолчанию — WORKER_STATE_DIR
    state_dir = None
    #: True — строка в журнал на каждый запрос (кроме служебных: health, metrics, manifest)
    log_requests = False

    def __init__(self):
        cls = type(self)
        self.name = os.environ.get("AGENT_WORKER", cls.__name__.lower())
        self.version = self._read_version()
        self._routes, self._jobs, self._configs = [], {}, {}
        # Объявления — в порядке записи в классе (база → наследник; переопределённое — на месте прежнего).
        attrs = {}
        for klass in reversed(cls.__mro__):
            for attr in vars(klass):
                attrs.setdefault(attr, None)
        for attr in attrs:
            value = getattr(cls, attr, None)
            if isinstance(value, Config):
                cfg = Config(value.key, value.schema, value.default, value.description)
                setattr(self, attr, cfg)
                self._configs[cfg.key] = cfg
            elif callable(value) and hasattr(value, "_agent_route"):
                r = value._agent_route
                pattern = "^" + re.sub(r"\\\{(\w+)\\\}", r"(?P<\1>[^/]+)", re.escape(r["path"])) + "$"
                self._routes.append((r, re.compile(pattern), getattr(self, attr)))
            elif callable(value) and hasattr(value, "_agent_job"):
                self._jobs[value._agent_job["type"]] = (value._agent_job, getattr(self, attr))
        self._live = {}
        self._by_job_id = {}
        self._lock = threading.Lock()
        self._counters = {"requests": 0, "jobsDone": 0, "jobsFailed": 0, "jobsCancelled": 0}
        self._state_dir = self.state_dir or os.environ.get("WORKER_STATE_DIR")

    # ── Что можно переопределить ──────────────────────────────────────────

    def health(self):
        """Самочувствие: {ok, message?, info?}; busy база добавит сама."""
        return {"ok": True}

    def metrics(self):
        """Свои метрики (добавляются к счётчикам базы)."""
        return {}

    def validate_config(self, key, data):
        """Проверить значение настройки; неверное — ValueError (агент получит отказ)."""

    def on_config(self, key, data):
        """Настройка применена (или сброшена к default после DELETE)."""

    def cleanup(self):
        """Убрать за собой при удалении агента; False — убирать нечего (сохранённые задачи
        база убирает сама)."""
        return False

    def prepare_job(self, type, data, files):
        """Проверить задачу до ответа 202 (долгую) или до запуска (быструю); неверная —
        raise HTTPError(400, текст)."""

    def on_start(self):
        """После запуска, когда сокет уже слушает (в своём потоке): например, событие «запущен»."""

    # ── Возможности базы ──────────────────────────────────────────────────

    def emit(self, type, data=None):
        """Событие бэкенду (тип — из events); агент хранит его, пока бэкенд не подтвердит."""
        self._agent_retry("POST", "/events", _clean({"type": type, "data": data}))

    def ask(self, type, data=None, timeout_ms=None):
        """Запрос к бэкенду (тип — из requests) → data ответа; отказ — RequestRejected, нет
        связи или срок вышел — AgentUnavailable."""
        timeout = (timeout_ms or 30000) / 1000 + 5
        status, body = self._agent("POST", "/requests",
                                   _clean({"type": type, "data": data, "timeoutMs": timeout_ms}), timeout)
        if status == 200:
            return body.get("data")
        if status == 422:
            raise RequestRejected(body.get("code", "REJECTED"), body.get("message", ""))
        raise AgentUnavailable(body.get("code", str(status)), body.get("message", ""))

    def context(self):
        """{agent: {id, name, version, labels?}, online} — кто агент и есть ли связь."""
        return self._agent("GET", "/context")[1]

    def agent_call(self, method, path, body=None, timeout=10):
        """Запрос к сокету агента как есть → (статус, тело): без повторов и разбора ошибок."""
        return self._agent(method, path, body, timeout)

    def log(self, message, **fields):
        """Строка в журнал воркера (stdout → журнал агента, уровень info)."""
        print(message + ("" if not fields else " " + json.dumps(fields, ensure_ascii=False)), flush=True)

    def run(self):
        """Слушать AGENT_WORKER_SOCKET до SIGTERM."""
        path = os.environ["AGENT_WORKER_SOCKET"]
        if os.path.exists(path):
            os.unlink(path)
        worker = self

        class Handler(_Handler):
            pass
        Handler.worker = worker
        server = _Server(path, Handler)
        signal.signal(signal.SIGTERM, lambda *_: threading.Thread(target=server.shutdown, daemon=True).start())
        threading.Thread(target=self._resume, daemon=True).start()
        threading.Thread(target=self.on_start, daemon=True).start()
        self.log("воркер %s %s слушает %s" % (self.name, self.version, path))
        try:
            server.serve_forever()
        finally:
            server.server_close()

    # ── Устройство ────────────────────────────────────────────────────────

    def _read_version(self):
        if os.environ.get("AGENT_WORKER_VERSION"):
            return os.environ["AGENT_WORKER_VERSION"]
        module = sys.modules.get(type(self).__module__)
        base = os.path.dirname(os.path.abspath(getattr(module, "__file__", None) or sys.argv[0]))
        try:
            with open(os.path.join(base, "VERSION"), encoding="utf-8") as f:
                return f.read().strip() or "0.0.0"
        except OSError:
            return "0.0.0"

    def manifest(self):
        m = {"version": self.version, "description": self.description}
        if self._configs:
            m["configs"] = [_clean({"key": c.key, "schema": c.schema, "description": c.description})
                            for c in self._configs.values()]
        if self._routes:
            m["routes"] = [_clean({k: r[k] for k in ("method", "path", "description", "request", "response")})
                           for r, _, _ in self._routes]
        if self.events:
            m["events"] = [_clean({"type": t, "schema": s}) for t, s in self.events.items()]
        if self._jobs:
            m["jobs"] = [_clean({"type": t, "schema": j["schema"], "description": j["description"]})
                         for t, (j, _) in self._jobs.items()]
        if self.requests:
            m["requests"] = [_clean({"type": t, **(r or {})}) for t, r in self.requests.items()]
        return _clean(m)

    def _agent(self, method, path, body=None, timeout=10):
        conn = _UnixConnection(os.environ["AGENT_SOCKET"], timeout)
        try:
            raw = None if body is None else json.dumps(body).encode()
            conn.request(method, path, body=raw, headers={
                "authorization": "Bearer " + os.environ.get("AGENT_WORKER_TOKEN", ""),
                "content-type": "application/json"})
            resp = conn.getresponse()
            data = resp.read()
            try:
                return resp.status, json.loads(data or b"{}")
            except ValueError:
                return resp.status, {"message": data.decode(errors="replace")}
        finally:
            conn.close()

    def _agent_retry(self, method, path, body, attempts=60):
        """Важное агенту: повторять, пока агент перезапускается или очередь полна."""
        delay = 0.5
        for i in range(attempts):
            try:
                status, resp = self._agent(method, path, body)
                if status < 300:
                    return resp
                if status not in (503, 502):
                    raise RuntimeError("%s %s: %s %s" % (method, path, status, resp.get("message")))
            except OSError:
                pass
            time.sleep(delay)
            delay = min(delay * 2, 10)
        raise RuntimeError("%s %s: агент недоступен" % (method, path))

    def _job_event(self, type, j, **data):
        self._agent_retry("POST", "/events", {"type": type, "data": _clean({"jobId": j.job_id, "id": j.id, **data})})

    def _persist(self, j):
        if not self._state_dir:
            return
        with self._lock:  # не вперемешку с уборкой (POST /cleanup)
            if getattr(j, "forgotten", False):
                return
            os.makedirs(os.path.join(self._state_dir, "jobs"), exist_ok=True)
            path = os.path.join(self._state_dir, "jobs", j.id + ".json")
            with open(path + ".new", "w", encoding="utf-8") as f:
                json.dump({"id": j.id, "jobId": j.job_id, "type": j.type, "data": j.data, "files": j.files,
                           "saved": j.saved}, f)
            os.replace(path + ".new", path)

    def _forget(self, j):
        if self._state_dir:
            try:
                os.remove(os.path.join(self._state_dir, "jobs", j.id + ".json"))
            except OSError:
                pass

    def _resume(self):
        """Незаконченные задачи прежнего запуска: resumable — продолжить, иначе — job.failed."""
        folder = os.path.join(self._state_dir or "", "jobs")
        if not self._state_dir or not os.path.isdir(folder):
            return
        for name in sorted(os.listdir(folder)):
            try:
                with open(os.path.join(folder, name), encoding="utf-8") as f:
                    s = json.load(f)
            except (OSError, ValueError):
                continue
            j = Job(self, s["id"], s["jobId"], s["type"], s.get("data"), s.get("files"), s.get("saved"))
            spec = self._jobs.get(j.type)
            if spec and spec[0]["resumable"]:
                self.log("задача продолжается после перезапуска", id=j.id, type=j.type)
                self._start(j, spec[1])
            else:
                self._finish(j, "failed", error={"code": "JOB_LOST", "message": "воркер перезапустился"})

    def _start(self, j, handler):
        with self._lock:
            self._live[j.id] = j
            self._by_job_id[j.job_id] = j.id
        threading.Thread(target=self._work, args=(j, handler), daemon=True).start()

    def _work(self, j, handler):
        try:
            result = handler(j)
            self._finish(j, "done", result=result)
        except Cancelled:
            self._finish(j, "cancelled")
        except JobError as e:
            self._finish(j, "failed", error={"code": e.code, "message": e.message})
        except Exception as e:  # noqa: BLE001 — любая ошибка обработчика — провал задачи
            traceback.print_exc()
            self._finish(j, "failed", error={"code": "JOB_FAILED", "message": str(e) or type(e).__name__})

    def _finish(self, j, state, result=None, error=None):
        with self._lock:
            if j.state != "running":
                return  # уже завершена (отмена): итог у задачи один
            j.state, j.result, j.error = state, result, error
        try:
            if state == "done":
                self._counters["jobsDone"] += 1
                self._job_event("job.done", j, result=result)
            elif state == "cancelled":
                self._counters["jobsCancelled"] += 1
                self._job_event("job.cancelled", j)
            else:
                self._counters["jobsFailed"] += 1
                self._job_event("job.failed", j, error=error)
        finally:
            self._forget(j)

    def _busy(self):
        return any(j.state == "running" for j in list(self._live.values()))

    def _dispatch(self, method, raw_path, headers, body):
        url = urlparse(raw_path)
        path = url.path
        self._counters["requests"] += 1
        if method == "GET" and path == "/health":
            h = dict(self.health() or {"ok": True})
            if self._busy():
                h.setdefault("busy", True)
            return 200, h
        if method == "GET" and path == "/manifest":
            return 200, self.manifest()
        if method == "GET" and path == "/metrics":
            running = sum(1 for j in self._live.values() if j.state == "running")
            return 200, {**self._counters, "jobsRunning": running, **(self.metrics() or {})}
        if method == "POST" and path == "/cleanup":
            # Агента удаляют: идущие задачи останавливаются и больше ничего не сохраняют.
            cleaned = self.cleanup() is not False
            folder = os.path.join(self._state_dir or "", "jobs")
            with self._lock:
                for j in list(self._live.values()):
                    j.forgotten = True
                    j._cancel.set()
                if self._state_dir and os.path.isdir(folder):
                    for name in os.listdir(folder):
                        try:
                            os.remove(os.path.join(folder, name))
                        except FileNotFoundError:
                            pass
                    cleaned = True
            return (204, None) if cleaned else (404, {"message": "убирать нечего"})
        m = re.match(r"^/config/([^/]+)$", path)
        if m:
            return self._config(method, m.group(1), body)
        if path == "/jobs" or path.startswith("/jobs/"):
            return self._job_route(method, path, body)
        for r, pattern, handler in self._routes:
            pm = pattern.match(path)
            if pm and r["method"] == method:
                req = Request(method, path, pm.groupdict(), {k: v[0] for k, v in parse_qs(url.query).items()},
                              headers, body)
                return handler(req)
        return 404, {"message": "нет маршрута %s %s" % (method, path)}

    def _config(self, method, key, body):
        cfg = self._configs.get(key)
        if cfg is None:
            return 404, {"message": "ключ настроек %r не объявлен" % key}
        if method == "DELETE":
            cfg.value, cfg.version = cfg.default, 0
            self.on_config(key, cfg.value)
            return 204, None
        if method != "PUT":
            return 405, {"message": "PUT или DELETE"}
        try:
            payload = json.loads(body or b"{}")
            data = payload.get("data")
            self.validate_config(key, data)
        except ValueError as e:
            return 400, {"message": str(e) or "неверное значение"}
        cfg.value, cfg.version = data, payload.get("version", 0)
        self.on_config(key, data)
        return 204, None

    def _job_route(self, method, path, body):
        if method == "POST" and path == "/jobs":
            try:
                payload = json.loads(body or b"{}")
            except ValueError:
                return 400, {"message": "тело — не JSON"}
            spec = self._jobs.get(payload.get("type"))
            if spec is None:
                return 400, {"message": "неизвестный тип задачи %r" % payload.get("type")}
            job_id = str(payload.get("jobId") or uuid.uuid4().hex)
            with self._lock:
                known = self._by_job_id.get(job_id)
            if known:
                return 202, {"id": known}
            self.prepare_job(payload["type"], payload.get("data"), payload.get("files"))
            j = Job(self, uuid.uuid4().hex, job_id, payload["type"], payload.get("data"), payload.get("files"))
            meta, handler = spec
            if meta["quick"]:
                try:
                    return 200, _clean({"result": handler(j)})
                except JobError as e:
                    return 422, {"code": e.code, "message": e.message}
            if meta["resumable"]:
                self._persist(j)
            self._start(j, handler)
            return 202, {"id": j.id}
        m = re.match(r"^/jobs/([^/]+)(/cancel)?$", path)
        j = self._live.get(m.group(1)) if m else None
        if j is None:
            return 404, {"message": "нет задачи"}
        if m.group(2):
            if method != "POST":
                return 405, {"message": "POST"}
            if j.state == "running":
                j._cancel.set()
                self._finish(j, "cancelled")
            return 200, {"id": j.id, "state": j.state}
        return 200, j.view()


class _Server(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True


class _Handler(BaseHTTPRequestHandler):
    worker = None
    protocol_version = "HTTP/1.1"

    def _handle(self):
        length = int(self.headers.get("content-length") or 0)
        body = self.rfile.read(length) if length else b""
        try:
            out = self.worker._dispatch(self.command, self.path, dict(self.headers), body)
        except HTTPError as e:
            out = (e.status, _clean({"message": e.message, "code": e.code}))
        except Exception as e:  # noqa: BLE001 — ошибка обработчика — 500 с причиной
            traceback.print_exc()
            out = (500, {"message": str(e) or type(e).__name__})
        if isinstance(out, Response):
            status, payload, headers = out.status, out.body, out.headers
        elif isinstance(out, tuple):
            status, payload, headers = out[0], out[1], {}
        elif out is None:
            status, payload, headers = 204, None, {}
        else:
            status, payload, headers = 200, out, {}
        if payload is not None and not isinstance(payload, (bytes, str, dict, list, int, float, bool)):
            return self._stream(status, payload, headers)
        if payload is None:
            raw = b""
        elif isinstance(payload, bytes):
            raw = payload
        elif isinstance(payload, str):
            raw = payload.encode()
            headers.setdefault("content-type", "text/plain; charset=utf-8")
        else:
            raw = json.dumps(payload, ensure_ascii=False).encode()
            headers.setdefault("content-type", "application/json")
        self._log_request(status)
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("content-length", str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def _log_request(self, status):
        if self.worker.log_requests and self.path.split("?")[0] not in ("/health", "/metrics", "/manifest"):
            print("%s %s %d" % (self.command, self.path, status), flush=True)

    def _stream(self, status, parts, headers):
        """Ответ по частям (Transfer-Encoding: chunked); клиент ушёл — перестать."""
        self._log_request(status)
        headers.setdefault("content-type", "text/plain; charset=utf-8")
        self.send_response(status)
        for k, v in headers.items():
            self.send_header(k, v)
        self.send_header("transfer-encoding", "chunked")
        self.end_headers()
        try:
            for part in parts:
                data = part.encode() if isinstance(part, str) else bytes(part)
                if data:
                    self.wfile.write(b"%x\r\n%s\r\n" % (len(data), data))
                    self.wfile.flush()
            self.wfile.write(b"0\r\n\r\n")
        except (BrokenPipeError, ConnectionResetError):
            self.close_connection = True

    do_GET = do_POST = do_PUT = do_DELETE = do_PATCH = _handle

    def address_string(self):
        return "agent"

    def log_message(self, fmt, *args):
        pass
