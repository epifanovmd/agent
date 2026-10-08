"""Воркер агента: объявляет очереди, команды, домены состояния и каналы
телеметрии; выполняет задачи в пуле потоков, команды и применения — в
отдельных потоках."""

from __future__ import annotations

import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from typing import Any, Callable, Dict, List, Optional, Set, Union

from .. import __version__
from ..message import ENV_WORKER, NAME_PATTERN, valid_name
from .channel import Channel
from .command import Command
from .context import WorkerContext
from .errors import Cancelled, CommandFailed, JobFailed, StateFailed
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
        #: Сообщения самоуправления до ``run`` — уйдут сразу после регистрации.
        self._early: List[tuple] = []
        self._started = False
        self._jobs: Dict[str, Job] = {}
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

    def report(self, channel: str, data: Any) -> None:
        """Последние данные канала телеметрии: уйдут в ближайший ``metrics`` агента."""
        if channel not in self._channels:
            raise ValueError(f"канал {channel!r} не объявлен: worker.channel({channel!r})")
        self._send("telemetry", {"channel": channel, "data": data})

    def event(self, type: str, data: Any = None) -> None:
        """Событие воркера серверу; доставка надёжная (агент хранит до подтверждения)."""
        message: Dict[str, Any] = {"type": type[:50]}
        if data is not None:
            message["data"] = data
        self._send("event", message)

    def set_health(self, ok: bool, message: Optional[str] = None) -> None:
        """Самочувствие воркера: ``ok=False`` — не в порядке (``message`` — причина). Агент
        показывает его в ``status.workers[].health`` и переводит узел в ``degraded``; на сервере —
        alert ``workerDegraded``. Можно звать и до ``run``."""
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

        pool = ThreadPoolExecutor(
            max_workers=max(1, sum(self._concurrency.values())), thread_name_prefix=f"{self.name}-job"
        )
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
        channel.send("worker.register", register)
        with self._lock:
            early, self._early = self._early, []
            self._started = True
        for kind, data in early:
            self._send(kind, data)
        reader = threading.Thread(target=self._read, args=(channel, pool), name="agent-ipc", daemon=True)
        reader.start()
        try:
            while not self._done.wait(0.5):
                if self._draining.is_set() and self._idle():
                    break
        finally:
            pool.shutdown(wait=True)
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

    def _send(self, type: str, data: Any) -> None:
        if self._channel is None:
            raise RuntimeError("воркер ещё не запущен (worker.run)")
        try:
            self._channel.send(type, data)
        except OSError as err:
            log.warning("%s не отправлено агенту: %s", type, err)

    def _control(self, type: str, data: Dict[str, Any]) -> None:
        """Сообщение самоуправления: до ``run`` — копится до регистрации."""
        with self._lock:
            if not self._started:
                self._early.append((type, data))
                return
        self._send(type, data)

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

    def _read(self, channel: Channel, pool: ThreadPoolExecutor) -> None:
        for message in channel.messages():
            kind, data = message.get("type"), message.get("data") or {}
            if kind == "worker.ready":
                self.rejected = set(data.get("rejected") or [])
                if self.rejected:
                    log.warning("агент отклонил имена (зарезервированы или заняты): %s", sorted(self.rejected))
                log.info("воркер %s зарегистрирован у агента %s", self.name, data.get("agentVersion"))
                self._start_telemetry()
            elif kind == "worker.context":
                self._set_context(data)
            elif kind == "job.assign":
                self._assign(channel, pool, data)
            elif kind in ("job.cancel", "job.stop"):
                with self._lock:
                    job = self._jobs.get(data.get("jobId"))
                if job and job.attempt == data.get("attempt"):
                    if kind == "job.cancel":
                        job.cancel()
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
        # Канал закрыт: агента нет — текущие задачи прервать, итоги некому отдать.
        with self._lock:
            jobs = list(self._jobs.values())
            commands = list(self._running_commands.values())
        for job in jobs:
            job.cancel()
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

    def _assign(self, channel: Channel, pool: ThreadPoolExecutor, data: Dict[str, Any]) -> None:
        job = Job(channel, data)
        if self._draining.is_set() or job.queue not in self._handlers:
            channel.send("job.fail", {
                **job.ref, "code": "WORKER_STOPPING", "retryable": True,
                "message": f"Воркер {self.name} не берёт задачу очереди {job.queue}",
            })
            job.close()
            return
        with self._lock:
            self._jobs[job.id] = job
        pool.submit(self._execute, channel, job)

    def _execute(self, channel: Channel, job: Job) -> None:
        handler = self._handlers[job.queue]
        try:
            result = handler(job)
            if job.cancelled:
                return
            job.flush()
            channel.send("job.complete", {**job.ref, "result": result})
        except Cancelled:
            pass
        except JobFailed as err:
            if not job.cancelled:
                job.flush()
                channel.send("job.fail", {
                    **job.ref, "code": err.code, "message": err.message[:2000], "retryable": err.retryable,
                })
        except Exception as err:  # noqa: BLE001 — любая ошибка обработчика — провал попытки
            log.exception("задача %s упала", job.id)
            if not job.cancelled:
                job.flush()
                channel.send("job.fail", {
                    **job.ref, "code": "WORKER_ERROR",
                    "message": f"{type(err).__name__}: {err}"[:2000], "retryable": True,
                })
        finally:
            job.close()
            with self._lock:
                self._jobs.pop(job.id, None)

    def _run_command(self, channel: Channel, data: Dict[str, Any]) -> None:
        cmd = Command(channel, data)
        handler = self._commands.get(cmd.name)
        done: Dict[str, Any] = {"commandId": cmd.id, "ok": False}
        with self._lock:
            self._running_commands[cmd.id] = cmd
        try:
            if handler is None:
                done["error"] = {"code": "COMMAND_UNKNOWN", "message": f"Команда {cmd.name} не поддерживается"}
            else:
                result = handler(cmd)
                done["ok"] = True
                if result is not None:
                    done["result"] = result
        except Cancelled:
            return
        except CommandFailed as err:
            done["error"] = {"code": err.code, "message": err.message[:2000]}
        except Exception as err:  # noqa: BLE001 — любая ошибка обработчика — провал команды
            log.exception("команда %s упала", cmd.name)
            done["error"] = {"code": "COMMAND_FAILED", "message": f"{type(err).__name__}: {err}"[:2000]}
        finally:
            with self._lock:
                self._running_commands.pop(cmd.id, None)
        if not cmd.cancelled:
            self._reply(channel, "cmd.done", done)

    def _apply_state(self, channel: Channel, data: Dict[str, Any], request_id: Optional[str]) -> None:
        domain, version = data.get("domain"), data.get("version")
        applied: Dict[str, Any] = {"domain": domain, "version": version, "ok": False}
        handler = self._domains.get(domain)
        try:
            if handler is None:
                applied["error"] = f"домен {domain} не обслуживается воркером {self.name}"
            else:
                report = handler(version, data.get("spec"))
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
        self._reply(channel, "state.applied", applied, re=request_id)

    def _run_cleanup(self, channel: Channel, request_id: Optional[str]) -> None:
        cleaned: Dict[str, Any] = {"ok": True}
        if self._cleanup is not None:
            try:
                self._cleanup()
            except Exception as err:  # noqa: BLE001 — сбой уборки — ответ с ошибкой, не падение
                log.exception("уборка воркера %s не удалась", self.name)
                cleaned = {"ok": False, "error": f"{type(err).__name__}: {err}"[:2000]}
        self._reply(channel, "worker.cleaned", cleaned, re=request_id)

    def _reply(self, channel: Channel, type: str, data: Dict[str, Any], re: Optional[str] = None) -> None:
        try:
            channel.send(type, data, re=re)
        except OSError as err:
            log.warning("%s не отправлено агенту: %s", type, err)

    def _start_telemetry(self) -> None:
        """Опрос источников телеметрии — после ``worker.ready`` (один раз)."""
        if self._telemetry_started:
            return
        self._telemetry_started = True
        for name, (fn, interval) in self._providers.items():
            if name in self.rejected:
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
                self.report(name, fn())
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
