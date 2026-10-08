"""Воркер агента: объявляет очереди, команды, домены состояния и каналы
телеметрии; выполняет задачи, команды и применения в отдельных потоках
(задач очереди одновременно — не больше её ``concurrency``)."""

from __future__ import annotations

import json
import logging
import os
import signal
import sys
import threading
import time
from collections import deque
from typing import Any, Callable, Deque, Dict, List, Optional, Set, Tuple, Union

from .. import __version__
from ..message import ENV_WORKER, NAME_PATTERN, valid_name
from .channel import Channel
from .command import Command
from .context import WorkerContext
from .errors import (
    CANCELLED, COMMAND_FAILED, COMMAND_UNKNOWN, RESULT_TOO_LARGE, WORKER_ERROR, WORKER_STOPPING, Cancelled, CommandFailed,
    JobFailed, MessageTooLarge, StateFailed, normalize_code,
)
from .job import Job

log = logging.getLogger("agent_sdk.worker")

Handler = Callable[[Job], Any]
CommandHandler = Callable[[Command], Any]
StateHandler = Callable[[int, Any], Any]
TelemetryProvider = Callable[[], Any]
CleanupHandler = Callable[[], Any]
ContextHandler = Callable[[WorkerContext], Any]

#: ``interval="auto"`` у ``telemetry``: с частотой подписки на канал (``channels`` контекста), без неё —
#: с частотой метрик агента (``metricsIntervalMs`` контекста), неизвестна — ``DEFAULT_METRICS_INTERVAL``.
AUTO_INTERVAL = "auto"
#: Частота метрик, пока агент её не сообщил, с.
DEFAULT_METRICS_INTERVAL = 15.0
#: Сколько сообщений копится до ``worker.ready``; лишние вытесняют самые старые.
EARLY_LIMIT = 1000


def _check_name(kind: str, name: Any) -> None:
    """Неверное имя — ``ValueError`` сразу при объявлении."""
    if not valid_name(name):
        raise ValueError(f"{kind} {name!r}: неверное имя — нужно {NAME_PATTERN}")


class Worker:
    """Воркер: обработчики и цикл приёма сообщений агента.

    Запускает его агент (``workers`` в конфигурации агента) и передаёт канал
    IPC. Сеть, повторы, досылка итогов после обрыва связи, учётные данные и
    обновление — забота агента; воркер знает только свою предметную область::

        worker = Worker("report", version="1.0.0")

        @worker.job("example.convert", concurrency=2)       # задачи очереди
        def convert(job: Job) -> dict: ...

        @worker.command("example.app.reload")               # команда агенту
        def reload(cmd: Command) -> dict: ...

        @worker.state("example.app")                        # желаемое состояние
        def apply(version: int, spec: dict) -> dict: ...

        @worker.telemetry("example.app", interval=15)       # канал в metrics
        def stats() -> dict: ...                            # interval="auto" — с частотой подписки или метрик

        @worker.on_context                                  # контекст агента: связь, частота метрик…
        def changed(ctx: WorkerContext) -> None: ...

        @worker.cleanup                                     # удаление агента с узла
        def cleanup() -> None: ...

        worker.event("app.started", {"port": 8080})         # событие серверу
        worker.set_health(False, "нет связи с базой")      # самочувствие: status.workers[].health
        worker.pause(["example.convert"])                   # не брать новые задачи; resume — снова брать
        worker.request_restart("утечка памяти")             # попросить агента заменить воркер
        worker.run()

    ``set_health``, ``pause``, ``resume``, ``request_restart``, ``report`` и ``event`` можно
    звать и до ``run``: сообщения копятся и уходят сразу после ``worker.ready``.

    Остановка — SIGTERM (агент): новые задачи не берутся, текущие задачи и
    команды дорабатываются. ``worker.drain`` (замена без простоя) — то же.
    """

    def __init__(self, name: Optional[str] = None, *, version: str = "0.0.0",
                 channel: Optional[Channel] = None) -> None:
        self.name = name or os.environ.get(ENV_WORKER) or "worker"
        self.version = version
        self._channel = channel
        self._handlers: Dict[str, Handler] = {}
        self._concurrency: Dict[str, int] = {}
        self._commands: Dict[str, CommandHandler] = {}
        self._domains: Dict[str, StateHandler] = {}
        self._channels: List[str] = []
        self._providers: Dict[str, tuple] = {}
        self._cleanup: Optional[CleanupHandler] = None
        self._context = WorkerContext()
        self._context_handlers: List[ContextHandler] = []
        #: Сообщения воркера до ``worker.ready`` — уйдут сразу после него.
        self._early: Deque[Tuple[str, Any]] = deque(maxlen=EARLY_LIMIT)
        self._early_lock = threading.Lock()
        self._early_dropped = False
        self._ready = False
        self._jobs: Dict[str, Job] = {}
        #: Очередь → задач в работе и ждущие места (не больше ``concurrency`` одновременно).
        self._running: Dict[str, int] = {}
        self._waiting: Dict[str, Deque[Job]] = {}
        self._threads: Set[threading.Thread] = set()
        self._running_commands: Dict[str, Command] = {}
        self._busy = 0  # команды и применения состояния в работе
        self._telemetry_started = False
        self._lock = threading.Lock()
        self._draining = threading.Event()
        self._done = threading.Event()
        #: Будит источники телеметрии при смене контекста (частота метрик).
        self._wake = threading.Condition(self._lock)
        #: Имена, которые агент отклонил (зарезервированы или заняты).
        self.rejected: Set[str] = set()

    # ── объявления ──────────────────────────────────────────────────────

    def job(self, queue: str, concurrency: int = 1) -> Callable[[Handler], Handler]:
        """Декоратор обработчика очереди; ``concurrency`` — задач одновременно."""
        _check_name("очередь", queue)

        def decorator(fn: Handler) -> Handler:
            self.register(queue, fn, concurrency)
            return fn

        return decorator

    def register(self, queue: str, fn: Handler, concurrency: int = 1) -> None:
        _check_name("очередь", queue)
        if concurrency < 1:
            raise ValueError("concurrency — не меньше 1")
        self._handlers[queue] = fn
        self._concurrency[queue] = concurrency

    def command(self, name: str) -> Callable[[CommandHandler], CommandHandler]:
        """Декоратор команды: ``fn(cmd) -> результат``; ``CommandFailed`` — ошибка с кодом.

        Имена ``agent.*`` и ``worker.*`` принадлежат агенту.
        """
        _check_name("команда", name)

        def decorator(fn: CommandHandler) -> CommandHandler:
            self._commands[name] = fn
            return fn

        return decorator

    def state(self, domain: str) -> Callable[[StateHandler], StateHandler]:
        """Декоратор домена желаемого состояния: ``fn(version, spec) -> отчёт``.

        Применение идемпотентно: агент присылает последний снимок после каждой
        регистрации воркера и при новой версии; исключение — снимок не
        применён, агент повторит.
        """
        _check_name("раздел состояния", domain)

        def decorator(fn: StateHandler) -> StateHandler:
            self._domains[domain] = fn
            return fn

        return decorator

    def channel(self, name: str) -> None:
        """Объявить канал телеметрии: данные — ``report(name, data)``."""
        _check_name("канал", name)
        if name not in self._channels:
            self._channels.append(name)

    def telemetry(self, name: str, interval: Union[float, str] = 15.0) -> Callable[[TelemetryProvider], TelemetryProvider]:
        """Декоратор источника канала телеметрии: вызывается раз в ``interval`` секунд.

        ``interval="auto"`` (``AUTO_INTERVAL``) — с частотой подписки сервера на этот канал
        (``context.channels``), без подписки — с частотой метрик агента (``context.metrics_interval_ms``);
        меняется на ходу; пока обе неизвестны — раз в 15 с.
        """
        _check_name("канал", name)
        if interval != AUTO_INTERVAL and (isinstance(interval, bool) or not isinstance(interval, (int, float))
                                          or interval <= 0):
            raise ValueError(f"interval — число секунд больше 0 или {AUTO_INTERVAL!r}")

        def decorator(fn: TelemetryProvider) -> TelemetryProvider:
            self.channel(name)
            self._providers[name] = (fn, interval)
            return fn

        return decorator

    def cleanup(self, fn: CleanupHandler) -> CleanupHandler:
        """Декоратор уборки: ``fn()`` убирает всё, что воркер создал на узле.

        Вызывается по ``worker.cleanup`` — его шлёт только ``agent cleanup`` при
        удалении агента с узла. Нет обработчика — ``{ok: true}`` сразу;
        исключение — ``{ok: false, error: текст}``.
        """
        self._cleanup = fn
        return fn

    def on_context(self, fn: ContextHandler) -> ContextHandler:
        """Подписка на контекст агента: ``fn(WorkerContext)`` — после ``worker.ready`` и при каждом
        изменении (связь, частота метрик, наблюдение, уровень логов). Можно декоратором.
        Вызывается в потоке приёма сообщений: долгую работу — в свой поток; исключение
        пишется в лог и не роняет воркер."""
        self._context_handlers.append(fn)
        return fn

    @property
    def context(self) -> WorkerContext:
        """Текущий контекст агента; до первого ``worker.context`` — значения по умолчанию
        (``mode`` run, ``online`` False, ``metrics_interval_ms`` 0)."""
        with self._lock:
            return self._context

    # ── из обработчиков ─────────────────────────────────────────────────
    #
    # set_health, pause, resume, request_restart, report и event можно звать и до ``run``:
    # до ``worker.ready`` сообщения копятся (не больше ``EARLY_LIMIT``, лишние вытесняют
    # самые старые) и уходят сразу после него по порядку.

    def report(self, channel: str, data: Any) -> None:
        """Последние данные канала телеметрии: уйдут в ближайший ``metrics`` агента.

        Данные не превращаются в JSON — ``TypeError``/``ValueError``; больше 16 МБ — не
        отправляются, запись в лог."""
        if channel not in self._channels:
            raise ValueError(f"канал {channel!r} не объявлен: worker.channel({channel!r})")
        self._control("telemetry", {"channel": channel, "data": data})

    def event(self, type: str, data: Any = None) -> None:
        """Событие воркера серверу; доставка надёжная (агент хранит до подтверждения).

        Данные не превращаются в JSON — ``TypeError``/``ValueError``; больше 16 МБ — не
        отправляется, запись в лог."""
        message: Dict[str, Any] = {"type": str(type)[:50]}
        if data is not None:
            message["data"] = data
        self._control("event", message)

    def set_health(self, ok: bool, message: Optional[str] = None) -> None:
        """Самочувствие воркера: ``ok=False`` — не в порядке (``message`` — причина). Агент
        показывает его в ``status.workers[].health`` и переводит узел в ``degraded``; на сервере —
        alert ``workerDegraded``."""
        data: Dict[str, Any] = {"ok": bool(ok)}
        if message:
            data["message"] = str(message)[:2000]
        self._control("worker.health", data)

    def pause(self, queues: Optional[List[str]] = None) -> None:
        """Не брать новые задачи очередей ``queues`` (без них — всех своих); выданные
        доделываются. Пауза с сервера (``pause_worker``) — отдельно, снимается только им."""
        self._control("worker.pause", _queues(queues))

    def resume(self, queues: Optional[List[str]] = None) -> None:
        """Снова брать задачи очередей ``queues`` (без них — всех), снять свою ``pause``."""
        self._control("worker.resume", _queues(queues))

    def request_restart(self, reason: Optional[str] = None) -> None:
        """Попросить агента заменить воркер штатно (как команда ``worker.restart``): без
        статуса сбоя и без alert. Этот процесс получит ``worker.drain`` или SIGTERM."""
        self._control("worker.restart", {"reason": str(reason)[:2000]} if reason else {})

    # ── работа ──────────────────────────────────────────────────────────

    def run(self, install_signals: bool = True) -> None:
        """Зарегистрироваться у агента и работать до остановки."""
        if not (self._handlers or self._commands or self._domains or self._channels or self._cleanup):
            raise RuntimeError("нечего объявить: нужны очередь, команда, домен, канал телеметрии или уборка")
        if self._channel is None:
            # Запущен агентом: лог SDK — в stderr (агент пишет его в свой журнал).
            _log_to_stderr()
            self._channel = Channel.from_env()
        channel = self._channel
        if install_signals:
            signal.signal(signal.SIGTERM, lambda *_: self.drain())
            signal.signal(signal.SIGINT, lambda *_: self.drain())

        register: Dict[str, Any] = {
            "name": self.name,
            "version": self.version,
            "sdk": f"python/{__version__}",
            "queues": [{"name": q, "concurrency": n} for q, n in self._concurrency.items()],
        }
        if self._commands:
            register["commands"] = list(self._commands)
        if self._domains:
            register["domains"] = list(self._domains)
        if self._channels:
            register["channels"] = list(self._channels)
        register["ping"] = True
        with self._lock:
            self._running = {q: 0 for q in self._handlers}
            self._waiting = {q: deque() for q in self._handlers}
        channel.send("worker.register", register)
        reader = threading.Thread(target=self._read, args=(channel,), name="agent-ipc", daemon=True)
        reader.start()
        try:
            while not self._done.wait(0.5):
                if self._draining.is_set() and self._idle():
                    break
        finally:
            for thread in list(self._threads):
                thread.join()
            channel.close()
        log.info("воркер %s остановлен", self.name)

    @property
    def stopping(self) -> threading.Event:
        """Устанавливается, когда воркер уходит (``worker.drain``, SIGTERM, ``drain()``):
        свои фоновые циклы по нему останавливаются (``while not w.stopping.wait(10): …``)."""
        return self._draining

    def drain(self) -> None:
        """Не брать новых задач; завершиться после текущих задач и команд."""
        if not self._draining.is_set():
            log.info("воркер %s дорабатывает задачи и завершается", self.name)
        self._draining.set()

    # ── внутреннее ──────────────────────────────────────────────────────

    def _control(self, type: str, data: Dict[str, Any]) -> None:
        """Сообщение воркера от его имени: до ``worker.ready`` — в очередь, после — сразу."""
        with self._early_lock:
            if not self._ready:
                json.dumps(data, allow_nan=False)  # не JSON — ошибка сразу, а не при отправке
                if len(self._early) == EARLY_LIMIT and not self._early_dropped:
                    self._early_dropped = True
                    log.warning("до worker.ready накоплено больше %d сообщений: старые отброшены", EARLY_LIMIT)
                self._early.append((type, data))
                return
        self._send(type, data)

    def _flush_early(self) -> None:
        """``worker.ready``: накопленные сообщения агенту по порядку."""
        with self._early_lock:
            if self._ready:
                return
            for type, data in self._early:
                try:
                    self._send(type, data)
                except (TypeError, ValueError) as err:
                    log.warning("%s не отправлено агенту: %s", type, err)
            self._early.clear()
            self._ready = True

    def _send(self, type: str, data: Any) -> None:
        """Отправка от имени воркера: больше 16 МБ или канал закрыт — запись в лог."""
        if self._channel is None:
            raise RuntimeError("воркер ещё не запущен (worker.run)")
        try:
            self._channel.send(type, data)
        except MessageTooLarge:
            log.warning("%s больше 16 МБ — не отправлено агенту", type)
        except OSError as err:
            log.warning("%s не отправлено агенту: %s", type, err)

    def _deliver(self, channel: Channel, type: str, data: Dict[str, Any], re: Optional[str] = None,
                 fallback: Optional[Callable[[Optional[str], str], Dict[str, Any]]] = None,
                 fallback_type: Optional[str] = None) -> None:
        """Итог агенту. Данные не превращаются в JSON или больше 16 МБ — вместо них
        ``fallback(код, текст)`` типа ``fallback_type`` (по умолчанию — тот же; код
        ``RESULT_TOO_LARGE`` или ``None``); канал закрыт — запись в лог."""
        try:
            channel.send(type, data, re=re)
            return
        except MessageTooLarge:
            code: Optional[str] = RESULT_TOO_LARGE
            text = "больше 16 МБ"
        except (TypeError, ValueError) as err:
            code, text = None, f"не превращается в JSON: {err}"
        except OSError as err:
            log.warning("%s не отправлено агенту: %s", type, err)
            return
        log.warning("%s: итог %s", type, text)
        if fallback is not None:
            self._deliver(channel, fallback_type or type, fallback(code, text), re=re)

    def _set_context(self, data: Dict[str, Any]) -> None:
        ctx = WorkerContext.from_message(data)
        with self._lock:
            self._context = ctx
            self._wake.notify_all()
        for fn in list(self._context_handlers):
            try:
                fn(ctx)
            except Exception:  # noqa: BLE001 — сбой подписчика не роняет воркер
                log.exception("обработчик контекста упал")

    def _idle(self) -> bool:
        with self._lock:
            return not self._jobs and self._busy == 0

    def _read(self, channel: Channel) -> None:
        for message in channel.messages():
            kind, data = message.get("type"), message.get("data") or {}
            if not isinstance(data, dict):
                data = {}
            if kind == "worker.ready":
                self.rejected = set(data.get("rejected") or [])
                if self.rejected:
                    log.warning("агент отклонил имена (зарезервированы или заняты): %s", sorted(self.rejected))
                log.info("воркер %s зарегистрирован у агента %s", self.name, data.get("agentVersion"))
                self._flush_early()
                self._start_telemetry()
            elif kind == "worker.ping":
                self._deliver(channel, "worker.pong", {}, re=message.get("id"))
            elif kind == "worker.context":
                self._set_context(data)
            elif kind == "job.assign":
                self._assign(channel, data)
            elif kind in ("job.cancel", "job.stop"):
                with self._lock:
                    job = self._jobs.get(data.get("jobId"))
                if job and job.attempt == data.get("attempt"):
                    if kind == "job.cancel":
                        job.cancel(by_agent=True)
                        self._drop_waiting(channel, job)
                    else:
                        job.request_stop()
            elif kind == "cmd.run":
                self._spawn(self._run_command, channel, data)
            elif kind == "cmd.cancel":
                with self._lock:
                    cmd = self._running_commands.get(data.get("commandId"))
                if cmd:
                    cmd.cancel()
            elif kind == "state.put":
                self._spawn(self._apply_state, channel, data, message.get("id"))
            elif kind == "worker.drain":
                self.drain()
            elif kind == "worker.cleanup":
                self._spawn(self._run_cleanup, channel, message.get("id"))
            elif kind == "error":
                log.warning("ошибка от агента: %s %s", data.get("code"), data.get("message"))
            else:
                log.debug("сообщение агента %s пропущено", kind)
        # Канал закрыт: агента нет — текущие задачи прервать, ждущие убрать: итоги некому отдать.
        with self._lock:
            jobs = list(self._jobs.values())
            commands = list(self._running_commands.values())
            waiting = [job for q in self._waiting.values() for job in q]
            for q in self._waiting.values():
                q.clear()
            for job in waiting:
                if self._jobs.get(job.id) is job:
                    del self._jobs[job.id]
        for job in jobs:
            job.cancel()
        for job in waiting:
            job.close()
        for cmd in commands:
            cmd.cancel()
        self._done.set()

    def _spawn(self, target: Callable[..., None], *args: Any) -> None:
        with self._lock:
            self._busy += 1

        def run() -> None:
            try:
                target(*args)
            finally:
                with self._lock:
                    self._busy -= 1

        threading.Thread(target=run, daemon=True).start()

    # ── задачи ──────────────────────────────────────────────────────────

    def _assign(self, channel: Channel, data: Dict[str, Any]) -> None:
        job_id, queue, attempt = data.get("jobId"), data.get("queue"), data.get("attempt", 0)
        if not isinstance(job_id, str) or not job_id:
            log.warning("job.assign без jobId пропущен")
            return
        if self._draining.is_set() or queue not in self._handlers:
            self._deliver(channel, "job.fail", {
                "jobId": job_id, "attempt": attempt, "code": WORKER_STOPPING, "retryable": True,
                "message": f"Воркер {self.name} не берёт задачу очереди {queue}",
            })
            return
        job = Job(channel, data)
        start = False
        with self._lock:
            prev = self._jobs.get(job.id)
            if prev is not None and prev.attempt >= job.attempt:
                return  # повторная доставка той же (или прежней) попытки
            self._jobs[job.id] = job
            if self._running[job.queue] < self._concurrency[job.queue]:
                self._running[job.queue] += 1
                start = True
            else:
                self._waiting[job.queue].append(job)
        if prev is not None:
            prev.cancel(by_agent=True)  # новая попытка той же задачи: прежняя уже не нужна
            self._drop_waiting(channel, prev)
        if start:
            self._start_job(channel, job)

    def _drop_waiting(self, channel: Channel, job: Job) -> None:
        """Отменённая задача из ожидания — прочь: обработчик не вызывается."""
        with self._lock:
            waiting = self._waiting.get(job.queue)
            if waiting is None or job not in waiting:
                return
            waiting.remove(job)
            if self._jobs.get(job.id) is job:
                del self._jobs[job.id]
        self._confirm_cancel(channel, job)
        job.close()

    def _confirm_cancel(self, channel: Channel, job: Job) -> None:
        """Обработчик отменённой задачи завершился (или не вызывался): ``job.fail`` с кодом
        CANCELLED подтверждает агенту, что место свободно. Ровно один раз; агент серверу его
        не передаёт. Канал закрыт — не отправляется."""
        with self._lock:
            if not job._cancel_confirm or job._cancel_confirmed:
                return
            job._cancel_confirmed = True
        if channel.closed:
            return
        self._deliver(channel, "job.fail", {**job.ref, "code": CANCELLED, "message": "задача отменена",
                                            "retryable": False})

    def _start_job(self, channel: Channel, job: Job) -> None:
        thread = threading.Thread(target=self._execute, args=(channel, job),
                                  name=f"{self.name}-job-{job.queue}", daemon=True)
        with self._lock:
            self._threads.add(thread)
        thread.start()

    def _execute(self, channel: Channel, job: Job) -> None:
        try:
            if not job.cancelled:
                self._handle(channel, job)
        finally:
            job.close()
            following: Optional[Job] = None
            skipped: List[Job] = []
            with self._lock:
                if self._jobs.get(job.id) is job:
                    del self._jobs[job.id]
                waiting = self._waiting[job.queue]
                while waiting and following is None:
                    candidate = waiting.popleft()
                    if not candidate.cancelled:
                        following = candidate
                    else:
                        skipped.append(candidate)
                if following is None:
                    self._running[job.queue] -= 1
                self._threads.discard(threading.current_thread())
            self._confirm_cancel(channel, job)
            for candidate in skipped:
                self._confirm_cancel(channel, candidate)
            if following is not None:
                self._start_job(channel, following)

    def _handle(self, channel: Channel, job: Job) -> None:
        handler = self._handlers[job.queue]
        try:
            result = handler(job)
        except Cancelled:
            return
        except JobFailed as err:
            if not job.cancelled:
                job.flush()
                self._fail(channel, job, err.code, err.message, err.retryable)
            return
        except Exception as err:  # noqa: BLE001 — любая ошибка обработчика — провал попытки
            log.exception("задача %s упала", job.id)
            if not job.cancelled:
                job.flush()
                self._fail(channel, job, WORKER_ERROR, f"{type(err).__name__}: {err}", True)
            return
        if job.cancelled:
            return
        job.flush()

        def fallback(code: Optional[str], text: str) -> Dict[str, Any]:
            return self._fail_data(job, code or WORKER_ERROR, f"результат задачи {text}", code is None)

        self._deliver(channel, "job.complete", {**job.ref, "result": result}, fallback=fallback,
                      fallback_type="job.fail")

    def _fail_data(self, job: Job, code: Any, message: Any, retryable: bool) -> Dict[str, Any]:
        code, text = normalize_code(code, str(message), WORKER_ERROR)
        return {**job.ref, "code": code, "message": text[:2000], "retryable": bool(retryable)}

    def _fail(self, channel: Channel, job: Job, code: Any, message: Any, retryable: bool) -> None:
        self._deliver(channel, "job.fail", self._fail_data(job, code, message, retryable))

    # ── команды, состояние, уборка ──────────────────────────────────────

    def _run_command(self, channel: Channel, data: Dict[str, Any]) -> None:
        cmd = Command(channel, data)
        handler = self._commands.get(cmd.name)
        done: Dict[str, Any] = {"commandId": cmd.id, "ok": False}
        with self._lock:
            self._running_commands[cmd.id] = cmd
        try:
            if handler is None:
                done["error"] = {"code": COMMAND_UNKNOWN, "message": f"Команда {cmd.name} не поддерживается"}
            else:
                result = handler(cmd)
                done["ok"] = True
                if result is not None:
                    done["result"] = result
        except Cancelled:
            return
        except CommandFailed as err:
            code, text = normalize_code(err.code, str(err.message), COMMAND_FAILED)
            done["error"] = {"code": code, "message": text[:2000]}
        except Exception as err:  # noqa: BLE001 — любая ошибка обработчика — провал команды
            if not cmd.cancelled:
                log.exception("команда %s упала", cmd.name)
            done["error"] = {"code": COMMAND_FAILED, "message": f"{type(err).__name__}: {err}"[:2000]}
        finally:
            with self._lock:
                self._running_commands.pop(cmd.id, None)
        if cmd.cancelled:
            return  # срок истёк: итог уже не нужен

        def fallback(code: Optional[str], text: str) -> Dict[str, Any]:
            return {"commandId": cmd.id, "ok": False,
                    "error": {"code": code or COMMAND_FAILED, "message": f"итог команды {text}"[:2000]}}

        self._deliver(channel, "cmd.done", done, fallback=fallback)

    def _apply_state(self, channel: Channel, data: Dict[str, Any], request_id: Optional[str]) -> None:
        domain, version = data.get("domain"), data.get("version")
        applied: Dict[str, Any] = {"domain": domain, "version": version, "ok": False}
        handler = self._domains.get(domain)  # type: ignore[arg-type]
        try:
            if handler is None:
                applied["error"] = f"домен {domain} не обслуживается воркером {self.name}"
            else:
                report = handler(version, data.get("spec"))  # type: ignore[arg-type]
                applied["ok"] = True
                if report is not None:
                    applied["report"] = report
        except StateFailed as err:
            log.warning("домен %s версии %s не применён: %s", domain, version, err.message)
            applied["error"] = str(err.message)[:2000]
            if err.report is not None:
                applied["report"] = err.report
        except Exception as err:  # noqa: BLE001 — любая ошибка — снимок не применён, агент повторит
            log.exception("домен %s версии %s не применён", domain, version)
            applied["error"] = f"{type(err).__name__}: {err}"[:2000]

        def fallback(code: Optional[str], text: str) -> Dict[str, Any]:
            error = f"отчёт состояния {text}"
            return {"domain": domain, "version": version, "ok": False,
                    "error": (f"{code}: {error}" if code else error)[:2000]}

        self._deliver(channel, "state.applied", applied, re=request_id, fallback=fallback)

    def _run_cleanup(self, channel: Channel, request_id: Optional[str]) -> None:
        cleaned: Dict[str, Any] = {"ok": True}
        if self._cleanup is not None:
            try:
                self._cleanup()
            except Exception as err:  # noqa: BLE001 — сбой уборки — ответ с ошибкой, не падение
                log.exception("уборка воркера %s не удалась", self.name)
                cleaned = {"ok": False, "error": f"{type(err).__name__}: {err}"[:2000]}

        def fallback(code: Optional[str], text: str) -> Dict[str, Any]:
            error = f"итог уборки {text}"
            return {"ok": False, "error": (f"{code}: {error}" if code else error)[:2000]}

        self._deliver(channel, "worker.cleaned", cleaned, re=request_id, fallback=fallback)

    # ── телеметрия ──────────────────────────────────────────────────────

    def _start_telemetry(self) -> None:
        """Опрос источников телеметрии — после ``worker.ready`` (один раз); каналы, которые
        агент отклонил, не опрашиваются."""
        if self._telemetry_started:
            return
        self._telemetry_started = True
        for name, (fn, interval) in self._providers.items():
            if name in self.rejected:
                log.warning("канал телеметрии %s отклонён агентом — не опрашивается", name)
                continue
            threading.Thread(target=self._provide, args=(name, fn, interval),
                             name=f"worker-telemetry-{name}", daemon=True).start()

    def _interval(self, name: str, interval: Union[float, str]) -> float:
        """Период опроса, с: ``"auto"`` — по контексту агента (вызывать под ``_lock``)."""
        if interval == AUTO_INTERVAL:
            ms = self._context.channels.get(name) or self._context.metrics_interval_ms
            return ms / 1000 if ms > 0 else DEFAULT_METRICS_INTERVAL
        return float(interval)

    def _provide(self, name: str, fn: TelemetryProvider, interval: Union[float, str]) -> None:
        while not self._done.is_set() and not self._draining.is_set():
            started = time.monotonic()
            try:
                data = fn()
                if data is not None:  # None — нет данных: точку не отправлять
                    self.report(name, data)
            except Exception:  # noqa: BLE001 — сбой источника не роняет воркер
                log.exception("телеметрия %s не собрана", name)
            # Ждать до следующего опроса; смена частоты (контекст) пересчитывает срок.
            with self._lock:
                while not self._done.is_set() and not self._draining.is_set():
                    left = started + self._interval(name, interval) - time.monotonic()
                    if left <= 0:
                        break
                    self._wake.wait(min(left, 0.5))


def _queues(queues: Optional[List[str]]) -> Dict[str, Any]:
    """``{queues}`` для pause/resume; ``None`` или пусто — все очереди воркера."""
    if queues is None:
        return {}
    if isinstance(queues, str):
        queues = [queues]
    names = list(queues)
    for q in names:
        _check_name("очередь", q)
    return {"queues": names} if names else {}


def _log_to_stderr() -> None:
    """Лог SDK в stderr, если приложение не настроило ``logging`` само."""
    sdk_log = logging.getLogger("agent_sdk")
    if logging.getLogger().handlers or sdk_log.handlers:
        return
    handler = logging.StreamHandler(sys.stderr)
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    sdk_log.addHandler(handler)
    sdk_log.setLevel(logging.INFO)
