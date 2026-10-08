"""``Agents`` — все подключённые агенты: серверная часть (§2–§7) на asyncio, без привязки к веб-фреймворку.

``Agents`` делает механику связи: регистрацию агентов, сессии (WebSocket и HTTP
sync), классы доставки и подтверждения, сверку задач при ``hello``, раздачу
задач по слотам с арендой, сроки команд, версии желаемого состояния, события.
Бэкенду остаются его данные и решения (API приложения) и HTTP-сервер, который
передаёт ``Agents`` запросы транспорта:

- ``POST /api/v1/agent-link/enroll`` → ``await agents.handle_enroll(body, remote)`` (``reply.headers`` — ``Retry-After``);
- ``POST /api/v1/agent-link/sync`` → ``await agents.handle_sync(authorization, body, is_disconnected)``;
- WebSocket ``/api/v1/agent-link`` → ``await agents.authenticate(authorization)`` до upgrade,
  затем ``await agents.serve_websocket(authorization, conn)``;
- ``GET``/``PUT /files/<key>`` → ``await agents.handle_file(method, key, body)`` (``MemoryFiles``);
- ``GET /api/v1/agent-link/releases/…`` и ``GET /api/v1/agent-link/install.sh`` →
  ``await agents.handle_release(path, base_url)`` (``releases_dir``).

Все операции ``Agents`` выполняются по одной (``asyncio.Lock``): сессии агентов живут
в этом процессе.
"""

from __future__ import annotations

import asyncio
import contextlib
import contextvars
import inspect
import json
import logging
import os
import re
import time
from collections import OrderedDict
from urllib.parse import quote
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional, Set, Tuple, Union

from ..message import (
    LOG_LEVELS, MESSAGE_VERSION, NAME_PATTERN, RELEASES_PATH, RELIABLE, STREAM, Close, merge_capabilities, new_id,
    now_ms, valid_name,
)
from .files import Files, MemoryFiles
from .install import install_command
from .seal import seal_value
from .model import (
    CMD_ACTIVE, CMD_FAILED, CMD_PENDING, CMD_RUNNING, CMD_SUCCEEDED, JOB_ACTIVE, JOB_CANCELLED, JOB_COMPLETED,
    JOB_FAILED, JOB_QUEUED, JOB_RUNNING, ALERT_DEGRADED, ALERT_OFFLINE, ALERT_STATE_FAILED, ALERT_WORKER_DEGRADED,
    ALERT_WORKER_DOWN,
    Agent, AgentEvent, Alert, AuditEntry, Change, Command, DesiredState, Job, MetricsPoint, UpdateCandidate,
    WorkerUpdateCandidate, hello_labels,
)
from .session import AgentsError, Session
from .store import MemoryStore, Store
from .transport import Transport

#: Сессия HTTP без запросов дольше этого — агент без связи, с.
SYNC_IDLE = 60.0
#: Период проверки аренд, сроков команд и сессий, с.
SWEEP_INTERVAL = 1.0
#: Запас к сроку команды: итог агента с TIMEOUT приходит сам, этот — если агент пропал, мс.
COMMAND_GRACE_MS = 15_000
#: call: как часто проверять итог в store, с; запас к сроку команды и COMMAND_GRACE_MS, с.
CALL_POLL_INTERVAL = 1.0
CALL_SLACK = 5.0
#: Аренда задачи — не меньше, с.
MIN_LEASE_SECONDS = 5
KEEP_LOG = 500
KEEP_OUTPUT = 256 * 1024
#: Сколько id событий помнить для отбрасывания повторов.
KEEP_EVENT_IDS = 2000
#: Состояния агента, в которых задачи не раздаются.
PAUSED = ("draining", "updating", "starting")
#: Срок команды agent.update, с.
UPDATE_TIMEOUT_SEC = 300
#: Встроенная команда агента: обновить воркер из выпуска (``release: true``).
WORKER_UPDATE_COMMAND = "worker.update"
#: Встроенные команды агента: не брать / снова брать новые задачи воркера (``{name, queues?}``).
WORKER_PAUSE_COMMAND = "worker.pause"
WORKER_RESUME_COMMAND = "worker.resume"
#: Срок команд worker.pause и worker.resume, с.
WORKER_CONTROL_TIMEOUT_SEC = 30
#: Интервал в подписке (``subscribe``) — не меньше, мс.
MIN_SUBSCRIPTION_INTERVAL_MS = 200
#: Срок подписки по умолчанию, мс.
SUBSCRIPTION_TTL_MS = 30_000
#: Прореживание истории метрик по умолчанию: точка в Store — не чаще, мс.
METRICS_STORE_INTERVAL_MS = 15_000
#: Срок хранения истории метрик по умолчанию, мс (7 суток).
METRICS_RETENTION_MS = 7 * 24 * 3600 * 1000
#: Период удаления старых точек метрик, с.
PRUNE_INTERVAL = 3600.0
#: Встроенная команда агента: сменить секрет (итог — ``{secretHash}``).
ROTATE_KEY_COMMAND = "agent.rotateKey"
#: Срок команды agent.rotateKey, с.
ROTATE_KEY_TIMEOUT_SEC = 60
#: Сколько версий истории просматривает ``rollback_state``.
ROLLBACK_SEARCH_LIMIT = 1000
#: Состояния воркера в ``status.workers``, означающие сбой (падение, перезапуск с паузой, ошибка).
WORKER_DOWN_STATES = frozenset({"backoff", "crashed", "failed", "error"})
_SECRET_HASH = re.compile(r"[0-9a-f]{64}")
#: Имя группы метрик узла (``subscribe(metrics={"groups": …})``): ``sockets``, ``cpu.cores``.
_METRICS_GROUP = re.compile(r"[a-z][a-z0-9]*(\.[a-z][a-z0-9]*)*")
#: Кто выполняет действие: ставит ``Actor`` (``agents.by``) на время вызова.
_ACTOR: "contextvars.ContextVar[str]" = contextvars.ContextVar("agent_sdk_actor", default="")

Listener = Callable[[Change], Union[None, Awaitable[None]]]
#: Подписчик ``metrics``: ``fn(agent_id, point)`` — функция или корутина.
MetricsListener = Callable[[str, MetricsPoint], Union[None, Awaitable[None]]]
#: Подписчик ``log``: ``fn(agent_id, entries)`` — функция или корутина.
LogListener = Callable[[str, List[Dict[str, Any]]], Union[None, Awaitable[None]]]
EnrollCheck = Callable[[str], Union[Optional[Dict[str, Any]], Awaitable[Optional[Dict[str, Any]]]]]


class Agents(Transport):
    """Движок связи с агентами для бэкенда.

    ``enroll_token`` — общий токен регистрации или ``enroll(token) -> {labels?} | None``
    (своя проверка; функция или корутина). ``store`` — хранилище (``MemoryStore``),
    ``files`` — провайдер ссылок на файлы задач (``MemoryFiles``). ``base_url`` —
    адрес сервера для ссылок, если транспорт его не передал. ``log`` — ``logging.Logger``
    или ``fn(msg, extra)``.

    ``offline_grace_ms`` — сколько агент после закрытия сессии ещё считается на связи
    (переподключение без мигания ``online``). ``offline_after_ms`` — агент на связи по записи, но
    без сессии в этом процессе и без вестей (``last_seen_at``) дольше этого — без связи: так сверка
    любого процесса снимает ``online`` агента, чей процесс бэкенда упал (по умолчанию
    ``max(3 × status_interval_ms, 30000) + offline_grace_ms``). ``releases_dir`` — каталог выпуска агента
    (``manifest.json``, сборки ``agent-<os>-<arch>``, ``install.sh``), ``public_key`` — ключ
    проверки релизов (base64), подставляется в ``install.sh``.

    ``metrics_store_interval_ms`` — прореживание истории: точка сохраняется в Store, только
    если её ``at`` не раньше последней сохранённой у агента + интервал (0 — каждая; живые и
    досланные точки прореживаются отдельно);
    событие ``metrics`` и ``agent.metrics`` — по каждой точке. ``metrics_retention_ms`` —
    срок хранения истории: при старте и раз в час ``store.prune_metrics(now − срок)``
    (0 — хранить всегда).

    ``enroll_failure_limit`` — сколько неудачных регистраций (неверный токен или запрос) одного клиента
    (адрес ``remote`` в ``handle_enroll``) допускается за ``enroll_failure_window_ms``; дальше —
    ``429 ENROLL_RATE_LIMITED`` до конца окна (0 — без ограничения).

    ``trust_proxy`` — бэкенд за прокси: адрес агента (``Agent.address``) — первый адрес
    ``X-Forwarded-For`` (параметр ``forwarded_for`` у ``handle_sync`` и ``serve_websocket``), а не
    ``remote``. Без прокси не включать: заголовок подделывает любой клиент.
    """

    def __init__(self, *, enroll_token: Optional[str] = None, enroll: Optional[EnrollCheck] = None,
                 store: Optional[Store] = None, files: Optional[Files] = None,
                 status_interval_ms: int = 5000, metrics_interval_ms: int = 15000,
                 offline_grace_ms: int = 20000, releases_dir: Optional[str] = None, public_key: str = "",
                 log: Any = None, base_url: str = "",
                 metrics_store_interval_ms: int = METRICS_STORE_INTERVAL_MS,
                 metrics_retention_ms: int = METRICS_RETENTION_MS,
                 enroll_failure_limit: int = 10, enroll_failure_window_ms: int = 60000,
                 offline_after_ms: Optional[int] = None, trust_proxy: bool = False) -> None:
        if not enroll_token and enroll is None:
            raise ValueError("нужен enroll_token или enroll")
        self.store: Store = store if store is not None else MemoryStore()
        self.files: Files = files if files is not None else MemoryFiles()
        self.status_interval_ms = status_interval_ms
        self.metrics_interval_ms = metrics_interval_ms
        self.offline_grace_ms = offline_grace_ms
        self.offline_after_ms = int(offline_after_ms) if offline_after_ms and offline_after_ms > 0 \
            else max(3 * status_interval_ms, 30000) + max(0, offline_grace_ms)
        self.metrics_store_interval_ms = max(0, int(metrics_store_interval_ms or 0))
        self.metrics_retention_ms = max(0, int(metrics_retention_ms or 0))
        self.enroll_failure_limit = max(0, int(enroll_failure_limit or 0))
        self.enroll_failure_window_ms = max(1, int(enroll_failure_window_ms or 60000))
        #: Клиент → времена неудачных регистраций в окне, мс.
        self._enroll_failures: Dict[str, List[int]] = {}
        self.releases_dir = os.fspath(releases_dir) if releases_dir else None
        self.public_key = public_key or ""
        self.base_url = base_url
        self.trust_proxy = bool(trust_proxy)
        self._enroll_token = enroll_token
        self._enroll = enroll
        self._logger = log if log is not None else logging.getLogger("agent_sdk.server")
        self._sessions: Dict[str, Session] = {}
        self._listeners: List[Listener] = []
        self._changes: List[Change] = []
        self._metrics_listeners: List[MetricsListener] = []
        self._points: List[Tuple[str, MetricsPoint]] = []
        self._log_listeners: List[LogListener] = []
        self._log_batches: List[Tuple[str, List[Dict[str, Any]]]] = []
        self._state_deleted_listeners: List[Any] = []
        self._deleted: List[Tuple[str, Optional[str]]] = []
        self._calls: Dict[str, List[asyncio.Future]] = {}
        self._event_ids: "OrderedDict[str, None]" = OrderedDict()
        self._audit_listeners: List[Any] = []
        self._audits: List[AuditEntry] = []
        self._alert_listeners: List[Any] = []
        self._alerts_out: List[Alert] = []
        #: Активные проблемы: (тип, агент, раздел или воркер) → Alert.
        self._active_alerts: Dict[Tuple[str, str, str], Alert] = {}
        #: (агент, досланная ли) → ``at`` последней сохранённой точки метрик (прореживание;
        #: в памяти). Досланные прореживаются отдельно: их время раньше уже сохранённых живых.
        self._stored_at: Dict[Tuple[str, bool], int] = {}
        #: Сессия закрылась: агент → когда счесть его без связи, мс.
        self._offline: Dict[str, int] = {}
        self._tasks: Set[asyncio.Future] = set()
        self._lock: Optional[asyncio.Lock] = None
        self._reaper: Optional[asyncio.Task] = None
        self._closed = False

    # ── уведомления ─────────────────────────────────────────────────────

    def on(self, event: str, fn: Any) -> Callable[[], None]:
        """Подписка; ``fn`` — функция или корутина. Возвращает отписку.

        ``change`` — ``fn(Change)``: что изменилось (подробности — чтением);
        ``metrics`` — ``fn(agent_id, MetricsPoint)``: каждая точка (и досланная, и не сохранённая
        прореживанием);
        ``stateDeleted`` — ``fn(domain, agent_id)``: снимок удалён (``agent_id`` ``None`` — общий);
        приходит раньше ``change`` этой операции (в том числе раньше переизданного общего);
        ``audit`` — ``fn(AuditEntry)``: изменяющее действие API приложения (журнал SDK не хранит);
        ``alert`` — ``fn(Alert)``: проблема началась (``active``) или закончилась — по одному
        событию на начало и конец;
        ``log`` — ``fn(agent_id, entries)``: пачка записей лога агента ``[{at, level, source, msg,
        attrs?}]`` (SDK логи не хранит).
        """
        listeners = self._listeners_of(event)
        listeners.append(fn)
        return lambda: self.off(event, fn)

    def off(self, event: str, fn: Any) -> None:
        listeners = self._listeners_of(event)
        if fn in listeners:
            listeners.remove(fn)

    def _listeners_of(self, event: str) -> List[Any]:
        if event == "change":
            return self._listeners
        if event == "metrics":
            return self._metrics_listeners
        if event == "stateDeleted":
            return self._state_deleted_listeners
        if event == "audit":
            return self._audit_listeners
        if event == "alert":
            return self._alert_listeners
        if event == "log":
            return self._log_listeners
        raise ValueError(f"неизвестное событие {event!r}: есть 'change', 'metrics', 'stateDeleted', "
                         "'audit', 'alert' и 'log'")

    def _changed(self, kind: str, id: str) -> None:
        self._changes.append(Change(kind=kind, id=id))

    def _emit(self) -> None:
        changes, self._changes = self._changes, []
        points, self._points = self._points, []
        deleted, self._deleted = self._deleted, []
        audits, self._audits = self._audits, []
        alerts, self._alerts_out = self._alerts_out, []
        logs, self._log_batches = self._log_batches, []
        for domain, agent_id in deleted:
            for fn in list(self._state_deleted_listeners):
                self._notify("stateDeleted", fn, domain, agent_id)
        seen: Set[Tuple[str, str]] = set()
        for change in changes:
            if (change.kind, change.id) in seen:
                continue
            seen.add((change.kind, change.id))
            for fn in list(self._listeners):
                self._notify("change", fn, change)
        for agent_id, point in points:
            for fn in list(self._metrics_listeners):
                self._notify("metrics", fn, agent_id, point)
        for entry in audits:
            for fn in list(self._audit_listeners):
                self._notify("audit", fn, entry.copy())
        for alert in alerts:
            for fn in list(self._alert_listeners):
                self._notify("alert", fn, alert.copy())
        for agent_id, entries in logs:
            for fn in list(self._log_listeners):
                self._notify("log", fn, agent_id, [dict(e) for e in entries])

    def _audit(self, action: str, target: str, agent_id: Optional[str] = None,
               details: Optional[Dict[str, Any]] = None) -> None:
        """Запись аудита — подписчикам ``audit`` после операции; ``actor`` — из ``by``."""
        self._audits.append(AuditEntry(at=now_ms(), actor=_ACTOR.get(), action=action, target=target,
                                       agent_id=agent_id or None, details=details))

    def _alert(self, type: str, agent: Agent, active: bool, message: str = "", *,
               domain: Optional[str] = None, worker: Optional[str] = None) -> None:
        """Начало или конец проблемы: событие ``alert`` — только при смене (без повторов)."""
        key = (type, agent.id, domain or worker or "")
        if active == (key in self._active_alerts):
            return
        if not active:
            # Конец проблемы — с тем же текстом, что был при её начале.
            message = self._active_alerts.pop(key).message
        alert = Alert(type=type, agent_id=agent.id, agent_name=agent.name, active=active, message=message,
                      at=now_ms(), domain=domain, worker=worker)
        if active:
            self._active_alerts[key] = alert
        self._alerts_out.append(alert)

    def _status_alerts(self, agent: Agent, status: Dict[str, Any]) -> None:
        """``degraded`` и ``workerDown`` по ``status``."""
        self._alert(ALERT_DEGRADED, agent, status.get("state") == "degraded",
                    str(status.get("message") or "Агент не в порядке"))
        down: Dict[str, str] = {}
        for w in status.get("workers") or []:
            if isinstance(w, dict) and isinstance(w.get("name"), str) and w.get("state") in WORKER_DOWN_STATES:
                down[w["name"]] = str(w["state"])
        for name, state in down.items():
            self._alert(ALERT_WORKER_DOWN, agent, True, f"Воркер {name}: {state}", worker=name)
        degraded: Dict[str, str] = {}
        for w in status.get("workers") or []:
            if isinstance(w, dict) and isinstance(w.get("name"), str) and w.get("health") == "degraded":
                degraded[w["name"]] = str(w.get("message") or f"Воркер {w['name']} не в порядке")
        for name, message in degraded.items():
            self._alert(ALERT_WORKER_DEGRADED, agent, True, message, worker=name)
        for (type, agent_id, worker) in list(self._active_alerts):
            if agent_id != agent.id:
                continue
            if type == ALERT_WORKER_DOWN and worker not in down:
                self._alert(ALERT_WORKER_DOWN, agent, False, worker=worker)
            elif type == ALERT_WORKER_DEGRADED and worker not in degraded:
                self._alert(ALERT_WORKER_DEGRADED, agent, False, worker=worker)

    def alerts(self) -> List[Alert]:
        """Активные проблемы (``alert`` с ``active``), известные этому процессу."""
        return [a.copy() for a in self._active_alerts.values()]

    def by(self, actor: str) -> "Actor":
        """Те же изменяющие действия от имени ``actor``: поле ``actor`` в задаче, команде,
        снимке состояния и в записи аудита (``agents.by("ivan").set_state(...)``)."""
        return Actor(self, actor)

    def _notify(self, event: str, fn: Any, *args: Any) -> None:
        try:
            result = fn(*args)
            if inspect.isawaitable(result):
                self._spawn(result)
        except Exception:  # noqa: BLE001 — сбой подписчика не ломает ``Agents``
            self._logger_exception(f"подписчик {event} упал")

    def _spawn(self, aw: Awaitable[Any]) -> None:
        task = asyncio.ensure_future(aw)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)

    # ── служебное ───────────────────────────────────────────────────────

    def _start(self) -> None:
        """Замок и фоновая проверка — в цикле событий при первом использовании."""
        if self._lock is None:
            self._lock = asyncio.Lock()
        if self._reaper is None and not self._closed:
            self._reaper = asyncio.get_running_loop().create_task(self._reap_loop())

    @contextlib.asynccontextmanager
    async def _op(self) -> AsyncIterator[None]:
        """Операция ``Agents``: по одной; уведомления — после неё."""
        self._start()
        assert self._lock is not None
        try:
            async with self._lock:
                yield
        finally:
            self._emit()

    def _log(self, msg: str, **extra: Any) -> None:
        if isinstance(self._logger, logging.Logger):
            self._logger.info("%s %s", msg, json.dumps(extra, ensure_ascii=False) if extra else "")
        elif callable(self._logger):
            self._logger(msg, extra)

    def _warn(self, msg: str, **extra: Any) -> None:
        if isinstance(self._logger, logging.Logger):
            self._logger.warning("%s %s", msg, json.dumps(extra, ensure_ascii=False) if extra else "")
        elif callable(self._logger):
            self._logger(msg, extra)

    async def _declared(self, domain: str, agent_id: Optional[str]) -> bool:
        """Раздел состояния объявлен агентом ``agent_id`` (пусто — хоть одним известным)."""

        def has(caps: Optional[Dict[str, Any]]) -> bool:
            return domain in (((caps or {}).get("state") or {}).get("domains") or {})

        for ss in self._sessions.values():
            if (not agent_id or ss.agent.id == agent_id) and has(ss.agent.capabilities):
                return True
        if agent_id:
            agent = await self.store.get_agent(agent_id)
            return agent is not None and has(agent.capabilities)
        return any(has(a.capabilities) for a in await self.store.list_agents())

    def _logger_exception(self, msg: str) -> None:
        if isinstance(self._logger, logging.Logger):
            self._logger.exception(msg)
        else:
            self._log(msg)

    async def close(self) -> None:
        """Остановка: сессиям — 1012 (агенты переподключатся сразу, итоги ждут в outbox)."""
        self._closed = True
        if self._reaper is not None:
            self._reaper.cancel()
            self._reaper = None
        for ss in list(self._sessions.values()):
            ss.close(Close.RESTART)
        self._sessions.clear()
        for futures in self._calls.values():
            for fut in futures:
                if not fut.done():
                    fut.cancel()

    # ── сессия ──────────────────────────────────────────────────────────

    async def _open(self, ss: Session, env: Dict[str, Any]) -> None:
        """hello: новая сессия вытесняет прежнюю, welcome, сверка задач, доставка."""
        hello = env.get("data")
        if not isinstance(hello, dict) or not isinstance(hello.get("agent"), dict) \
                or not isinstance(hello.get("versions"), list):
            ss.close(Close.INVALID)
            return
        if MESSAGE_VERSION not in hello["versions"]:
            ss.close(Close.UNSUPPORTED)
            return
        agent = await self.store.get_agent(ss.agent.id)
        if agent is None or agent.revoked:
            ss.close(Close.UNAUTHORIZED)
            return
        ss.agent = agent
        self._offline.pop(agent.id, None)
        prev = self._sessions.get(agent.id)
        if prev is not None and prev is not ss:
            prev.close(Close.REPLACED)
        self._sessions[agent.id] = ss
        boot_id = hello["agent"].get("bootId")
        if agent.boot_id != boot_id:
            agent.boot_id = boot_id
            agent.last_seq = 0
        agent.hello = hello
        agent.online = True
        agent.transport = ss.mode
        if ss.address:
            agent.address = ss.address
        agent.last_seen_at = now_ms()
        agent.capabilities = hello.get("capabilities") or {}
        agent.labels = hello_labels(agent, hello.get("labels"))
        agent.subscriptions = _active(agent.subscriptions, now_ms())  # истёкшие удаляются
        self._learn_domains(ss, agent.capabilities)
        self._alert(ALERT_OFFLINE, agent, False)
        ss.subscription = _summary(agent.subscriptions, now_ms())
        config: Dict[str, Any] = {"statusIntervalMs": self.status_interval_ms,
                                  "metricsIntervalMs": self.metrics_interval_ms}
        if ss.subscription:
            config["subscription"] = dict(ss.subscription)
        ss.send("welcome", {
            "version": MESSAGE_VERSION, "agentId": agent.id, "sessionId": ss.id, "serverTime": now_ms(),
            "config": config,
        })
        await self.store.update_agent(agent)
        self._log("агент на связи", agent=agent.name, transport=ss.mode)
        await self._reconcile(ss, hello.get("jobs") or [])
        await self._deliver(ss)
        self._changed("agent", agent.id)

    async def _closed_session(self, ss: Session) -> None:
        """Сессия закрыта транспортом: если она текущая — агент без связи, но не сразу:
        ещё ``offline_grace_ms`` (переподключение не меняет ``online``)."""
        ss.close(Close.NORMAL)
        if self._sessions.get(ss.agent.id) is ss:
            del self._sessions[ss.agent.id]
            if self.offline_grace_ms > 0:
                self._offline[ss.agent.id] = now_ms() + self.offline_grace_ms
                await self._save_agent(ss.agent, now_ms())
                return
            await self._go_offline(ss.agent)

    async def _go_offline(self, agent: Agent) -> None:
        agent.online = False
        await self._save_agent(agent, now_ms())
        self._log("Агент без связи", agent=agent.name)
        if not agent.revoked:
            self._alert(ALERT_OFFLINE, agent, True, "Агент без связи")
        self._changed("agent", agent.id)

    async def _save_agent(self, agent: Agent, now: int) -> None:
        """Сохранить агента сессии. Отзыв и подписки меняют и другие процессы бэкенда —
        они берутся из записи (истёкшие подписки удаляются)."""
        stored = await self.store.get_agent(agent.id)
        if stored is not None:
            agent.revoked = stored.revoked
            agent.subscriptions = stored.subscriptions
            # Учётные данные меняют authenticate (повышение ключа) и revoke — тоже из записи.
            agent.secret_hash = stored.secret_hash
            agent.pending_secret_hash = stored.pending_secret_hash
            if agent.revoked:
                agent.online = False
        agent.subscriptions = _active(agent.subscriptions, now)
        await self.store.update_agent(agent)

    async def _sync_session(self, ss: Session, now: int) -> bool:
        """Сохранить агента сессии и применить запись: отозван — сессия закрывается кодом
        4401 (``False``); сводная подписка изменилась — ``config``."""
        await self._save_agent(ss.agent, now)
        if ss.agent.revoked:
            if self._sessions.get(ss.agent.id) is ss:
                del self._sessions[ss.agent.id]
            ss.close(Close.UNAUTHORIZED)
            self._offline.pop(ss.agent.id, None)
            self._log("агент отозван", agent=ss.agent.name)
            self._changed("agent", ss.agent.id)
            return False
        self._apply_subscription(ss, now)
        return True

    def _apply_subscription(self, ss: Session, now: int) -> None:
        """Агенту — ``config {subscription}``, если сводная подписка не та, что он знает
        (``{}`` — подписок нет)."""
        if ss.closed:
            return
        want = _summary(ss.agent.subscriptions, now)
        if ss.subscription != want:
            ss.subscription = want
            ss.send("config", {"subscription": dict(want)})

    async def _handle(self, ss: Session, env: Dict[str, Any]) -> None:
        """Сообщение открытой сессии; ack или error по классу доставки (§4)."""
        if ss.closed:
            return
        agent = ss.agent
        kind, seq, msg_id = env["type"], env.get("seq"), env.get("id")
        if kind in STREAM and seq:
            if seq <= agent.last_seq:
                ss.send("ack", {"seq": agent.last_seq})
                return
            agent.last_seq = seq
        status_before = json.dumps(agent.status, sort_keys=True) if kind == "status" else None
        try:
            err = await self._dispatch(ss, env)
        except (KeyError, TypeError, ValueError, AttributeError) as e:
            err = {"code": "MESSAGE_INVALID", "message": f"Некорректное {kind}: {e}", "retryable": False}
        if err:
            ss.send("error", err, re=msg_id)
        elif kind in RELIABLE and msg_id:
            ss.send("ack", {"ids": [msg_id]})
        elif kind in STREAM and seq:
            ss.send("ack", {"seq": seq})
        if not await self._sync_session(ss, now_ms()):
            return
        if ss.restart:
            # Ключ сменён (agent.rotateKey): ack уже в очереди — закрыть 1012, агент переподключится
            # с новым секретом.
            ss.close(Close.RESTART)
            await self._closed_session(ss)
            self._changed("agent", agent.id)
            return
        # «Агент изменился» — только если есть что обновить: тот же status (пульс),
        # досланная точка метрик и сообщения задач/команд состояние агента не меняют.
        if kind == "status":
            notify = json.dumps(agent.status, sort_keys=True) != status_before
        elif kind == "metrics":
            notify = (env.get("data") or {}).get("backfill") is not True
        else:
            notify = kind in ("inventory", "capabilities", "state.applied")
        if notify:
            self._changed("agent", agent.id)

    async def _dispatch(self, ss: Session, env: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        agent = ss.agent
        d = env.get("data") or {}
        kind = env["type"]
        now = now_ms()
        agent.last_seen_at = now

        if kind == "status":
            agent.status = d
            ss.has_status = True
            self._status_alerts(agent, d)
            for ref in d.get("jobs") or []:
                ss.pending.pop(ref.get("jobId"), None)
                job = await self._held(agent.id, ref)
                if job is not None:
                    job.lease_until = now + job.lease_seconds * 1000
                    await self.store.update_job(job)
            await self._fill(ss)
            return None
        if kind == "metrics":
            backfill = d.get("backfill") is True
            point = MetricsPoint(at=_point_time(d, now), backfill=backfill, metrics=d)
            if self._keep_point(agent.id, backfill, point.at):
                await self.store.add_metrics(agent.id, point)
            self._points.append((agent.id, point))
            if not backfill:
                agent.metrics = d
            return None
        if kind == "inventory":
            agent.inventory = d
            return None
        if kind == "log":
            entries = d.get("entries")
            if not isinstance(entries, list):
                return _invalid(kind)
            entries = [e for e in entries if isinstance(e, dict)]
            if entries:
                self._log_batches.append((agent.id, entries))
            return None
        if kind == "capabilities":
            agent.capabilities = merge_capabilities(agent.capabilities, d)
            self._learn_domains(ss, d)
            await self._deliver(ss)
            return None
        if kind == "event":
            if not isinstance(d.get("type"), str) or not d["type"]:
                return _invalid(kind)
            if msg_id := env.get("id"):
                if msg_id in self._event_ids:
                    return None  # повтор: уже сохранено
                self._event_ids[msg_id] = None
                while len(self._event_ids) > KEEP_EVENT_IDS:
                    self._event_ids.popitem(last=False)
            event = AgentEvent(agent_id=agent.id, agent_name=agent.name, source=str(d.get("source") or "agent"),
                               type=d["type"], data=d.get("data"), at=env.get("ts") or now, id=env.get("id"))
            await self.store.add_event(event)
            self._changed("event", env.get("id") or new_id())
            return None
        if kind == "job.accept":
            # Слот выданной задачи занят, пока её не перечислит status (или итог):
            # между accept и следующим status слоты агента ещё не пересчитаны.
            job = await self._held(agent.id, d)
            if job is not None and not job.accepted:
                job.accepted = True
                await self._save_job(job)
            return None
        if kind == "job.progress":
            job = await self._held(agent.id, d)
            if job is None:
                return None
            if isinstance(d.get("progress"), (int, float)):
                job.progress = d["progress"]
            if isinstance(d.get("text"), str):
                job.text = d["text"]
            if isinstance(d.get("log"), list):
                job.log = (job.log + [str(line) for line in d["log"]])[-KEEP_LOG:]
            await self._save_job(job)
            return None
        if kind == "job.event":
            job = await self._held(agent.id, d)
            if job is None:
                return _lease_lost()
            seq = int(d["seq"])
            if seq > job.event_seq:
                job.event_seq = seq
                entry: Dict[str, Any] = {"seq": seq, "type": d["type"], "at": env.get("ts") or now}
                if d.get("data") is not None:
                    entry["data"] = d["data"]
                job.events = job.events + [entry]
                await self._save_job(job)
            return None
        if kind == "job.urls":
            job = await self._held(agent.id, d)
            if job is None:
                return _lease_lost()
            urls = await self.files.urls(job, ss.base_url)
            inputs, outputs = urls.get("inputs") or {}, urls.get("outputs") or {}
            if isinstance(d.get("inputs"), list):
                inputs = {n: inputs[n] for n in d["inputs"] if n in inputs}
            if isinstance(d.get("outputs"), list):
                outputs = {n: outputs[n] for n in d["outputs"] if n in outputs}
            ss.send("job.urls", {"inputs": inputs, "outputs": outputs, "expiresAt": urls.get("expiresAt")},
                    re=env.get("id"))
            return None
        if kind in ("job.complete", "job.fail"):
            job = await self._held(agent.id, d)
            if job is None:
                return None if await self._finished_here(agent.id, d) else _lease_lost()
            ss.pending.pop(job.id, None)
            if kind == "job.complete":
                job.status, job.result, job.progress, job.error = JOB_COMPLETED, d.get("result"), 1, None
                job.finished_at = now
                await self._save_job(job)
                self._log("задача выполнена", job=job.id, queue=job.queue)
                await self._fill(ss)
            else:
                await self._fail_attempt(job, str(d["code"]), str(d.get("message") or ""), bool(d.get("retryable")))
            return None
        if kind == "job.reject":
            ss.pending.pop(d["jobId"], None)
            job = await self._held(agent.id, d)
            if job is not None:
                job.status, job.agent_id, job.accepted = JOB_QUEUED, None, False
                await self._save_job(job)
                await self._fill_all(exclude=ss)
            return None
        if kind == "cmd.accept":
            cmd = await self.store.get_command(d["commandId"])
            if cmd is not None and cmd.agent_id == agent.id and cmd.status == CMD_PENDING:
                cmd.status = CMD_RUNNING
                await self._save_command(cmd)
            return None
        if kind == "cmd.output":
            cmd = await self.store.get_command(d["commandId"])
            if cmd is not None and cmd.agent_id == agent.id:
                cmd.output = (cmd.output + str(d.get("chunk") or ""))[-KEEP_OUTPUT:]
                await self._save_command(cmd)
            return None
        if kind == "cmd.done":
            cmd = await self.store.get_command(d["commandId"])
            if cmd is not None and cmd.agent_id == agent.id and cmd.status in CMD_ACTIVE:
                cmd.status = CMD_SUCCEEDED if d.get("ok") else CMD_FAILED
                cmd.result, cmd.error, cmd.finished_at = d.get("result"), d.get("error"), now
                if _is_int(d.get("exitCode")):
                    cmd.exit_code = d["exitCode"]
                await self._finish_command(cmd)
                if cmd.name == ROTATE_KEY_COMMAND and d.get("ok"):
                    await self._rotated(ss, d.get("result"))
            return None
        if kind == "state.applied":
            domain, version = str(d["domain"]), int(d["version"])
            agent.state_applied = {**agent.state_applied, domain: d}
            if d.get("ok") and version > ss.known.get(domain, 0):
                ss.known[domain] = version
            self._alert(ALERT_STATE_FAILED, agent, not d.get("ok"), str(d.get("error") or ""), domain=domain)
            self._changed("state", domain)
            return None
        return {"code": "UNKNOWN_TYPE", "message": f"Неизвестный тип: {kind}", "retryable": False}

    async def _rotated(self, ss: Session, result: Any) -> None:
        """Итог ``agent.rotateKey``: хеш нового секрета — ожидающий; сессию — закрыть после ack."""
        secret_hash = result.get("secretHash") if isinstance(result, dict) else None
        if not isinstance(secret_hash, str) or not _SECRET_HASH.fullmatch(secret_hash):
            self._warn("agent.rotateKey: нет secretHash в итоге", agent=ss.agent.name)
            return
        stored = await self.store.get_agent(ss.agent.id)
        if stored is None or stored.revoked:
            return
        stored.pending_secret_hash = secret_hash
        await self.store.update_agent(stored)
        ss.agent.pending_secret_hash = secret_hash
        ss.restart = True
        self._log("агент сменил ключ: ждём входа с новым", agent=ss.agent.name)

    def _keep_point(self, agent_id: str, backfill: bool, at: int) -> bool:
        """Прореживание: в Store — точка не раньше последней сохранённой того же рода (живая
        или досланная) + интервал; первая каждого рода после старта — всегда."""
        key = (agent_id, backfill)
        last = self._stored_at.get(key)
        interval = self.metrics_store_interval_ms
        if interval > 0 and last is not None and at < last + interval:
            return False
        self._stored_at[key] = at
        return True

    def _learn_domains(self, ss: Session, caps: Optional[Dict[str, Any]]) -> None:
        for domain, version in (((caps or {}).get("state") or {}).get("domains") or {}).items():
            if version is not None and version > ss.known.get(domain, 0):
                ss.known[domain] = version

    async def _held(self, agent_id: str, ref: Dict[str, Any]) -> Optional[Job]:
        """Задача за агентом в этой попытке и выполняется."""
        job_id = ref.get("jobId")
        if not isinstance(job_id, str):
            raise ValueError("нет jobId")
        job = await self.store.get_job(job_id)
        if job is not None and job.status == JOB_RUNNING and job.agent_id == agent_id \
                and job.attempt == ref.get("attempt"):
            return job
        return None

    async def _finished_here(self, agent_id: str, ref: Dict[str, Any]) -> bool:
        """Итог этой попытки уже принят (повтор после потерянного ack) — подтвердить."""
        job = await self.store.get_job(ref["jobId"])
        return job is not None and job.status in (JOB_COMPLETED, JOB_FAILED) and job.agent_id == agent_id \
            and job.attempt == ref.get("attempt")

    async def _reconcile(self, ss: Session, reported: List[Dict[str, Any]]) -> None:
        """Сверка задач при hello (§6.3)."""
        listed = {(r.get("jobId"), r.get("attempt")) for r in reported if isinstance(r, dict)}
        for job in await self.store.list_jobs(status=JOB_RUNNING, agent_id=ss.agent.id):
            if (job.id, job.attempt) in listed:
                if job.stop_requested:
                    ss.send("job.stop", _ref(job))
            elif not job.accepted:
                ss.pending[job.id] = job.queue
                ss.send("job.assign", await self._assignment(ss, job))
            else:
                await self._fail_attempt(job, "AGENT_LOST", "Агент перезапустился и потерял задачу", True)
        for r in reported:
            if isinstance(r, dict) and await self._held(ss.agent.id, r) is None:
                ss.send("job.cancel", {"jobId": r.get("jobId"), "attempt": r.get("attempt")})

    async def _deliver(self, ss: Session) -> None:
        """Ожидающие команды объявленных имён и новые версии объявленных доменов."""
        if ss.closed:
            return
        caps = ss.agent.capabilities or {}
        names = set((caps.get("commands") or {}).get("names") or [])
        # Старые первыми.
        for cmd in reversed(await self.store.list_commands(status=CMD_PENDING, agent_id=ss.agent.id)):
            if cmd.id not in ss.sent and cmd.name in names:
                ss.sent.add(cmd.id)
                run: Dict[str, Any] = {"commandId": cmd.id, "name": cmd.name, "timeoutSec": cmd.timeout_sec}
                if cmd.args is not None:
                    run["args"] = cmd.args
                ss.send("cmd.run", run)
        for domain in ((caps.get("state") or {}).get("domains") or {}):
            st = await self._effective_state(domain, ss.agent.id)
            if st is not None and st.version > ss.known.get(domain, 0):
                ss.known[domain] = st.version
                ss.send("state.put", {"domain": domain, "version": st.version, "spec": st.spec})

    async def _effective_state(self, domain: str, agent_id: str) -> Optional[DesiredState]:
        """Снимок домена для агента: свой (персональный) заменяет общий."""
        own = await self.store.get_state(domain, agent_id)
        return own if own is not None else await self.store.get_state(domain, None)

    async def _fill(self, ss: Session) -> None:
        """Раздать ждущие задачи по свободным слотам сессии (``status.slots``)."""
        st = ss.agent.status
        if ss.closed or not ss.has_status or not st or st.get("state") in PAUSED:
            return
        slots = st.get("slots") or {}
        for queue in sorted(slots):
            free = int(slots[queue] or 0) - sum(1 for q in ss.pending.values() if q == queue)
            if free <= 0:
                continue
            for job in reversed(await self.store.list_jobs(status=JOB_QUEUED, queue=queue)):  # старые первыми
                if free <= 0:
                    break
                if job.pinned_agent_id and job.pinned_agent_id != ss.agent.id:
                    continue
                job.status, job.agent_id, job.accepted = JOB_RUNNING, ss.agent.id, False
                job.lease_until = now_ms() + job.lease_seconds * 1000
                await self._save_job(job)
                ss.pending[job.id] = queue
                ss.send("job.assign", await self._assignment(ss, job))
                free -= 1

    async def _fill_all(self, exclude: Optional[Session] = None) -> None:
        """Раздача по всем агентам: сначала менее загруженные (задач в работе и выданных)."""
        def load(ss: Session) -> int:
            return len((ss.agent.status or {}).get("jobs") or []) + len(ss.pending)

        for ss in sorted(self._sessions.values(), key=load):
            if ss is not exclude:
                await self._fill(ss)

    async def _assignment(self, ss: Session, job: Job) -> Dict[str, Any]:
        urls = await self.files.urls(job, ss.base_url)
        return {
            "jobId": job.id, "attempt": job.attempt, "queue": job.queue, "data": job.data,
            "leaseSeconds": job.lease_seconds, "inputs": urls.get("inputs") or {},
            "outputs": urls.get("outputs") or {}, "urlsExpireAt": urls.get("expiresAt"),
        }

    async def _fail_attempt(self, job: Job, code: str, message: str, retryable: bool) -> None:
        """Провал попытки: повтор, если попытки остались."""
        if job.agent_id and (ss := self._sessions.get(job.agent_id)) is not None:
            ss.pending.pop(job.id, None)
        job.error = {"code": code, "message": message}
        if retryable and job.attempt + 1 < job.max_attempts:
            job.status, job.attempt, job.agent_id = JOB_QUEUED, job.attempt + 1, None
            job.accepted, job.event_seq = False, 0
            await self._save_job(job)
            await self._fill_all()
            return
        job.status, job.finished_at = JOB_FAILED, now_ms()
        await self._save_job(job)
        self._log("задача провалена", job=job.id, code=code)

    async def _save_job(self, job: Job) -> None:
        await self.store.update_job(job)
        self._changed("job", job.id)

    async def _save_command(self, cmd: Command) -> None:
        await self.store.update_command(cmd)
        self._changed("command", cmd.id)

    async def _finish_command(self, cmd: Command) -> None:
        await self._save_command(cmd)
        for fut in self._calls.pop(cmd.id, []):
            if not fut.done():
                fut.set_result(None)

    # ── фоновая проверка ────────────────────────────────────────────────

    async def _reap_loop(self) -> None:
        # Записи «на связи», оставшиеся от упавшего процесса, снимает сверка по lastSeenAt
        # (_stale_offline): сбрасывать всех при старте нельзя — агенты соседних процессов на связи.
        next_prune = 0.0
        while not self._closed:
            if time.monotonic() >= next_prune:
                next_prune = time.monotonic() + PRUNE_INTERVAL
                try:
                    await self.prune_metrics()
                except Exception:  # noqa: BLE001 — сбой хранилища не останавливает проверку
                    self._logger_exception("удаление старых метрик не удалось")
            await asyncio.sleep(SWEEP_INTERVAL)
            try:
                await self.sweep()
            except Exception:  # noqa: BLE001 — сбой хранилища не останавливает проверку
                self._logger_exception("проверка аренд не удалась")

    async def prune_metrics(self, now: Optional[int] = None) -> int:
        """Удалить точки метрик старше срока хранения (``metrics_retention_ms``; 0 — ничего).
        Вызывается при старте и раз в час; ``now`` — для тестов. Возвращает, сколько удалено."""
        if self.metrics_retention_ms <= 0:
            return 0
        now = now if now is not None else now_ms()
        removed = await self.store.prune_metrics(now - self.metrics_retention_ms)
        if removed:
            self._log("удалены старые точки метрик", count=removed)
        return removed

    async def sweep(self, now: Optional[int] = None) -> None:
        """Истёкшие аренды задач (``LEASE_EXPIRED``), сроки команд (``TIMEOUT``), забытые
        HTTP-сессии, отсрочка offline, истёкшие подписки. Вызывается раз в секунду; ``now`` — для тестов."""
        now = now if now is not None else now_ms()
        async with self._op():
            for job in await self.store.list_jobs(status=JOB_RUNNING):
                if now > job.lease_until:
                    # Агент на связи, но задачу не перечисляет: её заберут — прервать.
                    ss = self._sessions.get(job.agent_id or "")
                    if ss is not None:
                        ss.send("job.cancel", _ref(job))
                    await self._fail_attempt(job, "LEASE_EXPIRED", "Агент перестал отвечать: аренда истекла", True)
            for status in CMD_ACTIVE:
                for cmd in await self.store.list_commands(status=status):
                    if now > cmd.created_at + cmd.timeout_sec * 1000 + COMMAND_GRACE_MS:
                        cmd.status, cmd.finished_at = CMD_FAILED, now
                        cmd.error = {"code": "TIMEOUT", "message": "Нет итога от агента"}
                        await self._finish_command(cmd)
            idle = time.monotonic() - SYNC_IDLE
            for ss in list(self._sessions.values()):
                if ss.mode == "http" and ss.active == 0 and ss.touched < idle:
                    await self._closed_session(ss)
            for agent_id, deadline in list(self._offline.items()):
                if now >= deadline:
                    del self._offline[agent_id]
                    agent = await self.store.get_agent(agent_id)
                    if agent is not None and agent_id not in self._sessions and agent.online:
                        await self._go_offline(agent)
            # Срок подписки истёк: агенту на связи здесь — новая сводная, из записи подписка удаляется.
            for ss in list(self._sessions.values()):
                if _expired(ss.agent.subscriptions, now):
                    await self._sync_session(ss, now)
            await self._expire_offline(now)
            await self._stale_offline(now)

    async def _expire_offline(self, now: int) -> None:
        """Истёкшие подписки агентов без связи удаляет сверка любого процесса (у агента на связи —
        процесс с его сессией). Перед записью — перечитать."""
        for agent in await self.store.list_agents():
            if agent.online or agent.id in self._sessions or not _expired(agent.subscriptions, now):
                continue
            fresh = await self.store.get_agent(agent.id)
            if fresh is None or fresh.online or not _expired(fresh.subscriptions, now):
                continue
            fresh.subscriptions = _active(fresh.subscriptions, now)
            await self.store.update_agent(fresh)

    async def _stale_offline(self, now: int) -> None:
        """Агенты «на связи» по записи без сессии в этом процессе и без вестей дольше
        ``offline_after_ms``: их процесс бэкенда упал, не сняв ``online``, — снять здесь (с ``alert``).
        Перед записью — перечитать: агент мог за это время подключиться к другому процессу."""
        stale_before = now - self.offline_after_ms
        for agent in await self.store.list_agents():
            if not agent.online or agent.id in self._sessions or agent.id in self._offline \
                    or (agent.last_seen_at or 0) >= stale_before:
                continue
            fresh = await self.store.get_agent(agent.id)
            if fresh is None or not fresh.online or (fresh.last_seen_at or 0) >= stale_before:
                continue
            fresh.online = False
            await self.store.update_agent(fresh)
            self._log("агент без вестей — без связи", agent=fresh.name)
            if not fresh.revoked:
                self._alert(ALERT_OFFLINE, fresh, True, "Агент без связи")
            self._changed("agent", fresh.id)

    # ── API приложения ──────────────────────────────────────────────────

    async def enqueue(self, queue: str, data: Any = None, *, max_attempts: int = 1, lease_seconds: int = 60,
                      inputs: Optional[Dict[str, Union[str, bytes]]] = None, outputs: Optional[List[str]] = None,
                      agent_id: Optional[str] = None) -> Job:
        """Поставить задачу. ``inputs`` — имя → содержимое (или URL — по провайдеру файлов),
        ``outputs`` — имена выходов, ``agent_id`` — закрепить за агентом."""
        if not queue or not isinstance(queue, str):
            raise AgentsError("MESSAGE_INVALID", "Нужна queue")
        _check_name("queue", queue)
        async with self._op():
            if agent_id and await self.store.get_agent(agent_id) is None:
                raise AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404)
            job = Job(id=new_id(), queue=queue, data=data if data is not None else {},
                      max_attempts=max(1, int(max_attempts or 1)),
                      lease_seconds=max(MIN_LEASE_SECONDS, int(lease_seconds or 60)),
                      pinned_agent_id=agent_id or None, inputs=sorted(inputs or {}),
                      outputs=[str(n) for n in (outputs or [])], created_at=now_ms(), actor=_ACTOR.get() or None)
            await self.files.prepare(job, dict(inputs or {}))
            await self.store.create_job(job)
            self._changed("job", job.id)
            self._audit("job.enqueue", job.id, agent_id, {"queue": queue})
            await self._fill_all()
            return (await self.store.get_job(job.id)) or job

    async def cancel_job(self, job_id: str) -> Job:
        """Отменить: агенту — ``job.cancel`` (итог не нужен)."""
        return await self._signal_job(job_id, "cancel")

    async def stop_job(self, job_id: str) -> Job:
        """Досрочная остановка: агенту — ``job.stop`` (довести шаг и сдать итог). Ждущая — отменяется."""
        return await self._signal_job(job_id, "stop")

    async def _signal_job(self, job_id: str, kind: str) -> Job:
        async with self._op():
            job = await self.store.get_job(job_id)
            if job is None:
                raise AgentsError("JOB_NOT_FOUND", "Задача не найдена", 404)
            if job.status not in JOB_ACTIVE:
                raise AgentsError("JOB_NOT_ACTIVE", "Задача уже завершена", 409)
            ss = self._sessions.get(job.agent_id) if job.agent_id else None
            if kind == "stop" and job.status == JOB_RUNNING:
                job.stop_requested = True
                if ss is not None:
                    ss.send("job.stop", _ref(job))
            else:
                if ss is not None and job.status == JOB_RUNNING:
                    ss.pending.pop(job.id, None)
                    ss.send("job.cancel", _ref(job))
                job.status, job.finished_at = JOB_CANCELLED, now_ms()
            await self._save_job(job)
            self._audit(f"job.{kind}", job.id, job.agent_id or job.pinned_agent_id)
            return job

    async def command(self, name: str, args: Any = None, *, timeout_sec: int = 60,
                      agent_id: Optional[str] = None) -> Command:
        """Команда агенту. ``agent_id`` пусто — агент на связи, объявивший команду
        (нет такого — ``AgentsError COMMAND_NOT_SUPPORTED``)."""
        return await self._command(name, args, timeout_sec=timeout_sec, agent_id=agent_id)

    async def _command(self, name: str, args: Any = None, *, timeout_sec: int = 60,
                       agent_id: Optional[str] = None, action: str = "command",
                       details: Optional[Dict[str, Any]] = None) -> Command:
        """Команда с записью аудита ``action`` (``target`` — id команды, для ``agent.*`` — id агента)."""
        if not name or not isinstance(name, str):
            raise AgentsError("MESSAGE_INVALID", "Нужно name")
        _check_name("name", name)
        async with self._op():
            if not agent_id:
                capable = [a for a in await self.store.list_agents()
                           if not a.revoked and name in (((a.capabilities or {}).get("commands") or {}).get("names") or [])]
                online = [a for a in capable if a.online]
                pick = (online or capable or [None])[0]
                if pick is None:
                    raise AgentsError("COMMAND_NOT_SUPPORTED", f"Ни один агент не объявил команду {name}", 404)
                agent_id = pick.id
            elif await self.store.get_agent(agent_id) is None:
                raise AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404)
            cmd = Command(id=new_id(), agent_id=agent_id, name=name, args=args,
                          timeout_sec=int(timeout_sec or 60), created_at=now_ms(), actor=_ACTOR.get() or None)
            await self.store.create_command(cmd)
            self._changed("command", cmd.id)
            if action == "command":
                self._audit(action, cmd.id, agent_id, {"name": name})
            else:
                self._audit(action, agent_id, agent_id, {"commandId": cmd.id, **(details or {})})
            ss = self._sessions.get(agent_id)
            if ss is not None:
                await self._deliver(ss)
            return cmd

    async def call(self, name: str, args: Any = None, *, timeout_sec: int = 60,
                   agent_id: Optional[str] = None) -> Command:
        """Команда и ждать итог: завершённая команда (``succeeded`` или ``failed``).

        Итог ловится тремя путями: завершение в этом процессе (агент подключён сюда),
        проверка store раз в ``CALL_POLL_INTERVAL`` с и по ``refresh`` (агент подключён к
        другому процессу бэкенда — итог приходит туда). Предел — срок команды +
        ``COMMAND_GRACE_MS`` + ``CALL_SLACK``: дальше — команда как есть.
        """
        cmd = await self.command(name, args, timeout_sec=timeout_sec, agent_id=agent_id)
        loop = asyncio.get_running_loop()
        deadline = loop.time() + (cmd.timeout_sec * 1000 + COMMAND_GRACE_MS) / 1000 + CALL_SLACK
        fut: Optional[asyncio.Future] = None
        try:
            while True:
                cur = await self.store.get_command(cmd.id)
                if cur is not None and cur.status in (CMD_SUCCEEDED, CMD_FAILED):
                    return cur
                left = deadline - loop.time()
                if left <= 0:
                    return cur or cmd
                if fut is None or fut.done():
                    fut = loop.create_future()
                    self._calls.setdefault(cmd.id, []).append(fut)
                try:
                    await asyncio.wait_for(asyncio.shield(fut), min(CALL_POLL_INTERVAL, left))
                except asyncio.TimeoutError:
                    pass
        finally:
            futures = self._calls.get(cmd.id)
            if futures and fut in futures:
                futures.remove(fut)
            if futures is not None and not futures:
                self._calls.pop(cmd.id, None)

    async def set_state(self, domain: str, spec: Any, agent_id: Optional[str] = None) -> DesiredState:
        """Новый снимок домена: общий или для агента ``agent_id``.

        Версия монотонна и между перезапусками (``max(прежняя + 1, now_ms)``); агенту
        достаётся более новый из общего и своего снимка. Доставка — агентам на связи,
        объявившим домен.
        """
        _check_domain(domain)
        async with self._op():
            return await self._set_state(domain, spec, agent_id, "state.set", None)

    async def _set_state(self, domain: str, spec: Any, agent_id: Optional[str], action: str,
                         details: Optional[Dict[str, Any]]) -> DesiredState:
        """Снимок, доставка и аудит (под замком)."""
        if agent_id and await self.store.get_agent(agent_id) is None:
            raise AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404)
        st = await self.store.set_state(domain, agent_id or None, spec, actor=_ACTOR.get() or None)
        if not await self._declared(domain, agent_id or None):
            self._warn("раздел состояния ещё никто не объявлял — снимок сохранён и уйдёт агентам, "
                       "когда воркер его объявит", domain=domain, agentId=agent_id or None)
        for ss in list(self._sessions.values()):
            if not agent_id or ss.agent.id == agent_id:
                await self._deliver(ss)
        self._changed("state", domain)
        self._audit(action, domain, agent_id, details)
        return st

    async def delete_state(self, domain: str, agent_id: Optional[str] = None) -> Optional[DesiredState]:
        """Удалить снимок домена: общий или личный снимок агента ``agent_id``.

        Без ``agent_id`` удаляется общий снимок; агентам ничего не отправляется, личные
        снимки остаются, результат — ``None``. С ``agent_id`` удаляется личный снимок; если он
        был и есть общий, общий сохраняется заново с новой версией и доставляется — иначе
        агент, применивший личный снимок с большей версией, общий не примет (остальные получат
        тот же общий повторно). Результат — общий снимок (если личного не было — текущий, без
        новой версии) или ``None``. Снимка (или агента) не было — не ошибка, уведомлений нет.
        Удалённый снимок — событие ``stateDeleted`` (раньше ``change``).
        """
        _check_domain(domain)
        async with self._op():
            self._audit("state.delete", domain, agent_id)
            if not agent_id:
                if await self.store.delete_state(domain, None):
                    self._deleted.append((domain, None))
                    self._changed("state", domain)
                return None
            common = await self.store.get_state(domain, None)
            own = await self.store.get_state(domain, agent_id)
            if own is None:
                return common
            st: Optional[DesiredState] = None
            if common is not None:
                # Сначала новый общий, потом удаление личного: версия общего — больше личной.
                st = await self.store.set_state(domain, None, common.spec, actor=common.actor)
            if await self.store.delete_state(domain, agent_id):
                self._deleted.append((domain, agent_id))
            if st is not None:
                for ss in list(self._sessions.values()):
                    await self._deliver(ss)
            self._changed("state", domain)
            return st

    async def state_history(self, domain: str, agent_id: Optional[str] = None, limit: int = 20) -> List[DesiredState]:
        """История снимков раздела (общего или агента ``agent_id``) от новых к старым."""
        _check_domain(domain)
        return await self.store.list_state_history(domain, agent_id or None, max(0, int(limit)))

    async def rollback_state(self, domain: str, version: int, agent_id: Optional[str] = None) -> DesiredState:
        """Вернуть spec версии ``version`` из истории раздела: обычный ``set_state`` — новая версия,
        тот же spec. Нет такой версии — ``AgentsError STATE_VERSION_NOT_FOUND`` (404)."""
        _check_domain(domain)
        async with self._op():
            history = await self.store.list_state_history(domain, agent_id or None, ROLLBACK_SEARCH_LIMIT)
            old = next((st for st in history if st.version == version), None)
            if old is None:
                raise AgentsError("STATE_VERSION_NOT_FOUND", f"Нет версии {version} раздела {domain}", 404)
            return await self._set_state(domain, old.spec, agent_id, "state.rollback", {"fromVersion": version})

    # ── метрики и подписки ──

    async def list_metrics(self, agent_id: str, since: Optional[int] = None) -> List[MetricsPoint]:
        """История метрик агента по возрастанию ``at``; ``since`` (мс) — только строго позже."""
        return await self.store.list_metrics(agent_id, since)

    async def subscribe(self, agent_id: str, *, id: Optional[str] = None, ttl_ms: int = SUBSCRIPTION_TTL_MS,
                        status: Optional[Dict[str, Any]] = None, metrics: Optional[Dict[str, Any]] = None,
                        logs: Optional[Dict[str, Any]] = None,
                        channels: Optional[Dict[str, Dict[str, Any]]] = None) -> Dict[str, Any]:
        """Подписка на агента: «присылай это, так часто, до срока». Возвращает ``{id, until}``.

        ``status`` — ``{intervalMs}``; ``metrics`` — ``{intervalMs?, groups?}`` (группы метрик узла
        сверх настройки агента); ``logs`` — ``{level}`` (debug|info|warn|error); ``channels`` —
        ``{"<канал показателей>": {intervalMs}}``. ``id`` — свой id подписки (нет — создаётся);
        тот же ``id`` продлевает срок (``ttl_ms`` от текущего момента) и заменяет содержимое.
        Подписчиков у агента несколько: агенту уходит сводная подписка (``config {subscription}``) —
        минимум интервалов, объединение групп, самый подробный уровень. Подписки пишутся в запись
        агента (``agent.subscriptions``): агенту на связи с другим процессом их доставит ``refresh``.

        Интервал меньше 200 мс, неверная группа, уровень или канал — ``MESSAGE_INVALID``; агента
        нет — ``AGENT_NOT_FOUND``, отозван — ``AGENT_REVOKED``."""
        if id is not None and (not isinstance(id, str) or not id):
            raise AgentsError("MESSAGE_INVALID", "id подписки — непустая строка")
        if isinstance(ttl_ms, bool) or not isinstance(ttl_ms, int) or ttl_ms <= 0:
            raise AgentsError("MESSAGE_INVALID", f"Неверный срок подписки {ttl_ms!r}: нужно целое мс больше 0")
        body = _subscription_body(status, metrics, logs, channels)
        async with self._op():
            agent = await self.store.get_agent(agent_id)
            if agent is None:
                raise AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404)
            if agent.revoked:
                raise AgentsError("AGENT_REVOKED", "Агент отозван", 409)
            now = now_ms()
            sub_id = id or new_id()
            sub = {"id": sub_id, "until": now + ttl_ms, **body}
            subs = [s for s in _active(agent.subscriptions, now) if s.get("id") != sub_id]
            subs.append(sub)
            await self._set_subscriptions(agent, subs, now)
            return {"id": sub_id, "until": sub["until"]}

    async def unsubscribe(self, agent_id: str, id: str) -> None:
        """Снять подписку ``id`` (нет такой — ничего); агенту — новая сводная. Агента нет —
        ``AGENT_NOT_FOUND``."""
        async with self._op():
            agent = await self.store.get_agent(agent_id)
            if agent is None:
                raise AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404)
            now = now_ms()
            if not any(s.get("id") == id for s in agent.subscriptions or []):
                return
            await self._set_subscriptions(agent, [s for s in _active(agent.subscriptions, now)
                                                  if s.get("id") != id], now)

    async def _set_subscriptions(self, agent: Agent, subs: List[Dict[str, Any]], now: int) -> None:
        """Записать подписки агента; агенту на связи здесь — сводная, если изменилась."""
        agent.subscriptions = subs
        ss = self._sessions.get(agent.id)
        if ss is not None:
            ss.agent.subscriptions = subs
        await self.store.update_agent(agent if ss is None else _with_creds(ss.agent, agent))
        if ss is not None:
            self._apply_subscription(ss, now)

    async def seal(self, agent_id: str, value: Any) -> Dict[str, str]:
        """Запечатать значение (любой JSON) для агента: ``{"$sealed": "v1.…"}`` — кладётся в снимок
        состояния где угодно; раскрывает только агент (его ключ — ``hello.agent.encryptionKey``).

        Нужен пакет ``cryptography`` (``pip install agent-sdk[crypto]``). Нет пакета или ключа у
        агента — ``AgentsError SEAL_NOT_AVAILABLE``; агента нет — ``AGENT_NOT_FOUND``."""
        agent = await self.store.get_agent(agent_id)
        if agent is None:
            raise AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404)
        key = (((agent.hello or {}).get("agent") or {}).get("encryptionKey"))
        if not isinstance(key, str) or not key:
            raise AgentsError("SEAL_NOT_AVAILABLE", "Агент не сообщил ключ шифрования (старая версия агента?)", 409)
        return {"$sealed": seal_value(key, value)}

    async def refresh(self, agent_id: Optional[str] = None) -> None:
        """Перечитать хранилище и доставить агенту (без ``agent_id`` — всем на связи с этим
        ``Agents``) то, что появилось в нём мимо этого ``Agents``: ожидающие команды, новые снимки
        состояния, задачи. Для бэкенда из нескольких процессов с общим Store: процесс,
        изменивший данные, уведомляет остальные (например, Postgres NOTIFY), каждый
        вызывает ``refresh`` — доставит тот, у кого сессия агента.

        Из записи агента применяются и отзыв (сессия закрывается кодом 4401, как ``revoke``),
        и подписки (сводная другая — ``config``). Ждущие ``call`` перечитывают итог."""
        for futures in list(self._calls.values()):
            for fut in futures:
                if not fut.done():
                    fut.set_result(None)
        async with self._op():
            now = now_ms()
            for sid, ss in list(self._sessions.items()):
                if agent_id is None or sid == agent_id:
                    if await self._sync_session(ss, now):
                        await self._deliver(ss)
            await self._fill_all()

    async def revoke(self, agent_id: str) -> Agent:
        """Отозвать агента: сессия закрывается кодом 4401, учётные данные больше не
        принимаются (WebSocket и HTTP sync — 401). Агенту нужна новая регистрация."""
        async with self._op():
            agent = await self.store.get_agent(agent_id)
            if agent is None:
                raise AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404)
            ss = self._sessions.pop(agent_id, None)
            if ss is not None:
                ss.close(Close.UNAUTHORIZED)
            self._offline.pop(agent_id, None)
            agent.revoked, agent.online, agent.subscriptions = True, False, []
            agent.pending_secret_hash = ""
            await self.store.update_agent(agent)
            self._log("агент отозван", agent=agent.name)
            self._changed("agent", agent.id)
            self._audit("agent.revoke", agent.id, agent.id)
            for (type, agent_id, sub) in list(self._active_alerts):
                if agent_id == agent.id:
                    al = self._active_alerts[(type, agent_id, sub)]
                    self._alert(type, agent, False, domain=al.domain, worker=al.worker)
            return agent

    async def rotate_key(self, agent_id: str) -> Command:
        """Сменить ключ агента без новой регистрации: команда ``agent.rotateKey`` (срок 60 с).

        Агент сохраняет новый секрет и отвечает его хешем; хеш становится ожидающим
        (``pending_secret_hash``), сессия закрывается кодом 1012, первый вход с новым секретом
        делает его основным. Агента нет — ``AGENT_NOT_FOUND``, отозван — ``AGENT_REVOKED``,
        команду не объявил — ``COMMAND_NOT_SUPPORTED``."""
        agent = await self.store.get_agent(agent_id)
        if agent is None:
            raise AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404)
        if agent.revoked:
            raise AgentsError("AGENT_REVOKED", "Агент отозван", 409)
        if ROTATE_KEY_COMMAND not in (((agent.capabilities or {}).get("commands") or {}).get("names") or []):
            raise AgentsError("COMMAND_NOT_SUPPORTED", f"Агент не объявил команду {ROTATE_KEY_COMMAND}", 404)
        return await self._command(ROTATE_KEY_COMMAND, None, timeout_sec=ROTATE_KEY_TIMEOUT_SEC, agent_id=agent_id,
                                   action="agent.rotateKey")

    # ── выпуск агента (§7) ──

    async def release(self) -> Optional[Dict[str, Any]]:
        """Манифест выпуска ``{version, artifacts: [{os, arch, file, sha256, signature}],
        workers?: [{name, version, os, arch, file, sha256, signature, restart?, stopTimeout?}]}``
        или ``None``."""
        return self._manifest()

    def _manifest(self) -> Optional[Dict[str, Any]]:
        if not self.releases_dir:
            return None
        try:
            with open(os.path.join(self.releases_dir, "manifest.json"), "rb") as f:
                manifest = json.load(f)
        except (OSError, ValueError):
            return None
        if not isinstance(manifest, dict) or not isinstance(manifest.get("version"), str) \
                or not isinstance(manifest.get("artifacts"), list):
            return None
        manifest["artifacts"] = [a for a in manifest["artifacts"] if isinstance(a, dict)]
        if "workers" in manifest:
            workers = manifest["workers"]
            manifest["workers"] = [w for w in workers if isinstance(w, dict)] if isinstance(workers, list) else []
        return manifest

    @staticmethod
    def _artifact(manifest: Dict[str, Any], agent: Agent) -> Optional[Dict[str, Any]]:
        """Сборка манифеста под ОС и архитектуру агента (``hello.host``); агент — с ``update.mode = self``."""
        caps = agent.capabilities or {}
        if (caps.get("update") or {}).get("mode") != "self":
            return None
        host = (agent.hello or {}).get("host") or {}
        for a in manifest["artifacts"]:
            if a.get("os") == host.get("os") and a.get("arch") == host.get("arch") and a.get("file"):
                return a
        return None

    async def update_candidates(self) -> List[UpdateCandidate]:
        """Агенты с ``update.mode = self``, чья версия ≠ версии манифеста и для чьих
        ОС и архитектуры есть сборка."""
        manifest = self._manifest()
        if manifest is None:
            return []
        out: List[UpdateCandidate] = []
        for agent in await self.store.list_agents():
            art = self._artifact(manifest, agent)
            current = str(((agent.hello or {}).get("agent") or {}).get("version") or "")
            if agent.revoked or art is None or current == manifest["version"]:
                continue
            out.append(UpdateCandidate(agent_id=agent.id, name=agent.name, online=agent.online, current=current,
                                       target=manifest["version"], os=art["os"], arch=art["arch"]))
        return out

    @staticmethod
    def _worker_artifact(manifest: Dict[str, Any], agent: Agent, name: str) -> Optional[Dict[str, Any]]:
        """Сборка воркера ``name`` под ОС и архитектуру агента; записей несколько — старшая версия
        (при равных — последняя в манифесте)."""
        host = (agent.hello or {}).get("host") or {}
        best: Optional[Dict[str, Any]] = None
        for w in manifest.get("workers") or []:
            if w.get("name") == name and w.get("os") == host.get("os") and w.get("arch") == host.get("arch") \
                    and w.get("file") and isinstance(w.get("version"), str):
                if best is None or _version_key(w["version"]) >= _version_key(best["version"]):
                    best = w
        return best

    @staticmethod
    def _updates_workers(agent: Agent) -> bool:
        """Агент объявил ``worker.update`` (``update.mode`` не учитывается: он про обновление
        самого агента)."""
        names = ((agent.capabilities or {}).get("commands") or {}).get("names") or []
        return WORKER_UPDATE_COMMAND in names

    @staticmethod
    def _release_workers(agent: Agent) -> Dict[str, str]:
        """Воркеры агента из выпуска (``status.workers[].release``) → их версии."""
        out: Dict[str, str] = {}
        for w in (agent.status or {}).get("workers") or []:
            if isinstance(w, dict) and w.get("release") is True and isinstance(w.get("name"), str):
                out[w["name"]] = str(w.get("version") or "")
        return out

    async def worker_update_candidates(self) -> List[WorkerUpdateCandidate]:
        """Воркеры из выпуска (``release: true`` в ``status.workers``) на неотозванных агентах,
        объявивших ``worker.update``, чья версия ≠ старшей версии воркера в манифесте под ОС и
        архитектуру агента. ``update.mode`` не учитывается."""
        manifest = self._manifest()
        if manifest is None or not manifest.get("workers"):
            return []
        out: List[WorkerUpdateCandidate] = []
        for agent in await self.store.list_agents():
            if agent.revoked or not self._updates_workers(agent):
                continue
            for name, current in self._release_workers(agent).items():
                art = self._worker_artifact(manifest, agent, name)
                if art is None or current == art["version"]:
                    continue
                out.append(WorkerUpdateCandidate(agent_id=agent.id, agent_name=agent.name, online=agent.online,
                                                 worker=name, current=current, target=art["version"],
                                                 os=art["os"], arch=art["arch"]))
        return out

    async def update_worker(self, agent_id: str, name: str) -> Command:
        """Команда ``worker.update`` ``{name, version, url, sha256, signature}`` со сборкой воркера
        под агента (срок 300 с); переустановка той же версии разрешена. Агента нет —
        ``AGENT_NOT_FOUND``; неверное имя — ``MESSAGE_INVALID``; нет выпуска или сборки, агент не
        объявил ``worker.update``, воркер не из выпуска — ``AgentsError UPDATE_NOT_AVAILABLE``.
        Аудит ``worker.update``."""
        agent = await self.store.get_agent(agent_id)
        if agent is None:
            raise AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404)
        if not isinstance(name, str) or not valid_name(name):
            raise AgentsError("MESSAGE_INVALID", f"Неверное имя воркера {name!r}: нужно {NAME_PATTERN}")
        manifest = self._manifest()
        art = self._worker_artifact(manifest, agent, name) if manifest is not None else None
        if art is None or not self._updates_workers(agent) or name not in self._release_workers(agent):
            raise AgentsError("UPDATE_NOT_AVAILABLE",
                              "Нет сборки воркера, воркер не из выпуска или агент не обновляет воркеры", 409)
        args = {"name": name, "version": art["version"],
                "url": f"{RELEASES_PATH}/{quote(str(art['file']), safe='')}",
                "sha256": art.get("sha256") or "", "signature": art.get("signature") or ""}
        return await self._command(WORKER_UPDATE_COMMAND, args, timeout_sec=UPDATE_TIMEOUT_SEC, agent_id=agent_id,
                                   action="worker.update", details={"worker": name, "version": art["version"]})

    async def pause_worker(self, agent_id: str, name: str, queues: Optional[List[str]] = None) -> Command:
        """Команда ``worker.pause`` ``{name, queues?}``: воркер ``name`` не берёт новые задачи очередей
        ``queues`` (без них — всех своих), выданные доделываются. Пауза от сервера и от самого воркера
        независимы: ``resume_worker`` снимает только свою. Агента нет — ``AGENT_NOT_FOUND``; неверное
        имя — ``MESSAGE_INVALID``. Команда доходит до агента, объявившего ``worker.pause`` (срок 30 с).
        Аудит ``worker.pause``."""
        return await self._worker_control(WORKER_PAUSE_COMMAND, agent_id, name, queues)

    async def resume_worker(self, agent_id: str, name: str, queues: Optional[List[str]] = None) -> Command:
        """Команда ``worker.resume`` ``{name, queues?}``: снять паузу ``pause_worker``. Аудит
        ``worker.resume``."""
        return await self._worker_control(WORKER_RESUME_COMMAND, agent_id, name, queues)

    async def _worker_control(self, command: str, agent_id: str, name: str,
                              queues: Optional[List[str]]) -> Command:
        if not isinstance(name, str) or not valid_name(name):
            raise AgentsError("MESSAGE_INVALID", f"Неверное имя воркера {name!r}: нужно {NAME_PATTERN}")
        names = [queues] if isinstance(queues, str) else list(queues or [])
        for q in names:
            if not isinstance(q, str) or not valid_name(q):
                raise AgentsError("MESSAGE_INVALID", f"Неверное имя очереди {q!r}: нужно {NAME_PATTERN}")
        args: Dict[str, Any] = {"name": name}
        if names:
            args["queues"] = names
        if await self.store.get_agent(agent_id) is None:
            raise AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404)
        return await self._command(command, args, timeout_sec=WORKER_CONTROL_TIMEOUT_SEC, agent_id=agent_id,
                                   action=command,
                                   details={"worker": name, **({"queues": names} if names else {})})

    def install_command(self, **opts: Any) -> str:
        """Команда установки агента одной строкой:
        ``curl -fsSL '<адрес>/api/v1/agent-link/install.sh' | sudo sh -s -- --token '…' [флаги]``.

        Опции (нужен ровно один из ``token`` и ``token_file``, остальные необязательны): ``base_url``
        (пусто — опция ``base_url`` ``Agents``), ``token``, ``token_file`` (токен из файла на узле —
        не виден в списке процессов), ``name``, ``privileged``, ``kill_mode`` (``process`` | ``mixed``),
        ``packages``, ``packages_by_manager`` (``{"apt": [...], "apk": [...]}`` — для своего менеджера
        заменяют ``packages``; менеджеры apt, dnf, yum, apk, zypper), ``sysctl`` (словарь; в
        команде — по алфавиту ключей), ``rw_paths``, ``ca_file``, ``workers`` (воркеры из выпуска: ``--worker``), ``stop_timeout``, ``user``, ``config``.
        Значения — в одинарных кавычках POSIX. Нет токена (или оба) или адреса, адрес не
        ``http(s)://хост[:порт][/путь]``, недопустимое имя пакета, менеджер, ``kill_mode`` или ключ
        sysctl, перевод строки, неверное имя воркера — ``AgentsError MESSAGE_INVALID``.
        ``config`` — путь к agent.yaml на узле (``--config``).
        """
        opts["base_url"] = opts.get("base_url") or self.base_url
        return install_command(**opts)

    async def update_agent(self, agent_id: str) -> Command:
        """Команда ``agent.update`` со сборкой манифеста под агента (срок 300 с).
        Нет сборки или агент не ``self`` — ``AgentsError UPDATE_NOT_AVAILABLE``."""
        agent = await self.store.get_agent(agent_id)
        if agent is None:
            raise AgentsError("AGENT_NOT_FOUND", "Агент не найден", 404)
        manifest = self._manifest()
        art = self._artifact(manifest, agent) if manifest is not None else None
        if manifest is None or art is None:
            raise AgentsError("UPDATE_NOT_AVAILABLE", "Нет сборки для агента или агент не обновляется сам", 409)
        args = {"version": manifest["version"], "url": f"{RELEASES_PATH}/{quote(str(art['file']), safe='')}",
                "sha256": art.get("sha256") or "", "signature": art.get("signature") or ""}
        return await self._command("agent.update", args, timeout_sec=UPDATE_TIMEOUT_SEC, agent_id=agent_id,
                                   action="agent.update", details={"version": manifest["version"]})

    # ── чтение ──

    async def list_agents(self) -> List[Agent]:
        return await self.store.list_agents()

    async def get_agent(self, agent_id: str) -> Optional[Agent]:
        return await self.store.get_agent(agent_id)

    async def get_job(self, job_id: str) -> Optional[Job]:
        return await self.store.get_job(job_id)

    async def list_jobs(self, *, status: Optional[str] = None, queue: Optional[str] = None,
                        agent_id: Optional[str] = None) -> List[Job]:
        """Задачи, новые первыми."""
        return await self.store.list_jobs(status=status, queue=queue, agent_id=agent_id)

    async def get_command(self, command_id: str) -> Optional[Command]:
        return await self.store.get_command(command_id)

    async def list_commands(self, *, status: Optional[str] = None, agent_id: Optional[str] = None) -> List[Command]:
        """Команды, новые первыми."""
        return await self.store.list_commands(status=status, agent_id=agent_id)

    async def list_states(self) -> List[DesiredState]:
        return await self.store.list_states()

    async def list_events(self, limit: int = 100) -> List[AgentEvent]:
        """Последние ``limit`` событий, новые первыми; ``limit`` ≤ 0 — все."""
        return await self.store.list_events(limit)


# ── помощники ───────────────────────────────────────────────────────────────

def _is_int(value: Any) -> bool:
    return isinstance(value, int) and not isinstance(value, bool)


def _live(sub: Any, now: int) -> bool:
    """Подписка из записи действует в момент ``now``."""
    return isinstance(sub, dict) and _is_int(sub.get("until")) and now < sub["until"]


def _active(subs: Optional[List[Dict[str, Any]]], now: int) -> List[Dict[str, Any]]:
    """Действующие подписки (новый список)."""
    return [s for s in subs or [] if _live(s, now)]


def _expired(subs: Optional[List[Dict[str, Any]]], now: int) -> bool:
    """В записи есть истёкшие подписки."""
    return any(not _live(s, now) for s in subs or [])


def _interval_ms(value: Any, what: str) -> int:
    if not _is_int(value) or value < MIN_SUBSCRIPTION_INTERVAL_MS:
        raise AgentsError("MESSAGE_INVALID", f"Неверный интервал {what} {value!r}: нужно целое "
                                             f"не меньше {MIN_SUBSCRIPTION_INTERVAL_MS} мс")
    return int(value)


def _spec(value: Any, what: str) -> Dict[str, Any]:
    if not isinstance(value, dict):
        raise AgentsError("MESSAGE_INVALID", f"{what} подписки — объект")
    return value


def _subscription_body(status: Any, metrics: Any, logs: Any, channels: Any) -> Dict[str, Any]:
    """Проверенное содержимое подписки ``{status?, metrics?, logs?, channels?}`` (без лишних полей)."""
    body: Dict[str, Any] = {}
    if status is not None:
        body["status"] = {"intervalMs": _interval_ms(_spec(status, "status").get("intervalMs"), "статуса")}
    if metrics is not None:
        spec = _spec(metrics, "metrics")
        out: Dict[str, Any] = {}
        if spec.get("intervalMs") is not None:
            out["intervalMs"] = _interval_ms(spec["intervalMs"], "метрик")
        groups = spec.get("groups")
        if groups is not None:
            if not isinstance(groups, list):
                raise AgentsError("MESSAGE_INVALID", "groups — список имён групп метрик")
            names: List[str] = []
            for g in groups:
                if not isinstance(g, str) or not _METRICS_GROUP.fullmatch(g):
                    raise AgentsError("MESSAGE_INVALID", f"Неверное имя группы метрик {g!r}")
                if g not in names:
                    names.append(g)
            if names:
                out["groups"] = names
        body["metrics"] = out
    if logs is not None:
        level = _spec(logs, "logs").get("level")
        if level not in LOG_LEVELS:
            raise AgentsError("MESSAGE_INVALID", f"Неверный уровень логов {level!r}: нужно {'|'.join(LOG_LEVELS)}")
        body["logs"] = {"level": level}
    if channels is not None:
        out = {}
        for name, spec in _spec(channels, "channels").items():
            if not valid_name(name):
                raise AgentsError("MESSAGE_INVALID", f"Неверное имя канала {name!r}")
            out[name] = {"intervalMs": _interval_ms(_spec(spec, f"channels.{name}").get("intervalMs"),
                                                    f"канала {name}")}
        body["channels"] = out
    return body


def _summary(subs: Optional[List[Dict[str, Any]]], now: int) -> Dict[str, Any]:
    """Сводная подписка агенту (§ ``subscription``): по действующим подпискам — минимум интервалов,
    объединение групп (в порядке появления), самый подробный уровень логов, по каналу — минимум.
    Поле, которого нет ни в одной подписке, не указывается."""
    out: Dict[str, Any] = {}
    groups: List[str] = []
    channels: Dict[str, int] = {}

    def least(key: str, value: Any) -> None:
        if _is_int(value) and (key not in out or value < out[key]):
            out[key] = value

    for sub in _active(subs, now):
        least("statusIntervalMs", (sub.get("status") or {}).get("intervalMs"))
        metrics = sub.get("metrics") or {}
        least("metricsIntervalMs", metrics.get("intervalMs"))
        groups.extend(g for g in metrics.get("groups") or [] if isinstance(g, str) and g not in groups)
        level = (sub.get("logs") or {}).get("level")
        if level in LOG_LEVELS and ("logLevel" not in out
                                    or LOG_LEVELS.index(level) < LOG_LEVELS.index(out["logLevel"])):
            out["logLevel"] = level
        for name, spec in (sub.get("channels") or {}).items():
            ms = (spec or {}).get("intervalMs") if isinstance(spec, dict) else None
            if _is_int(ms) and (name not in channels or ms < channels[name]):
                channels[name] = ms
    if groups:
        out["metrics"] = groups
    if channels:
        out["channels"] = channels
    return out


def _point_time(d: Dict[str, Any], now: int) -> int:
    """Время точки метрик по часам сервера (§6.2).

    ``collectedAt + clockOffsetMs`` — верно и для досланных, и для переотправленных точек;
    без смещения — момент получения.
    """
    collected, offset = d.get("collectedAt"), d.get("clockOffsetMs")
    if _is_int(collected) and _is_int(offset):
        return collected + offset
    return now


def _version_key(version: str) -> Tuple[int, ...]:
    """Ключ сравнения версий: числа по порядку (``1.10.0`` > ``1.9.3``)."""
    return tuple(int(n) for n in re.findall(r"\d+", version))


def _ref(job: Job) -> Dict[str, Any]:
    return {"jobId": job.id, "attempt": job.attempt}


def _check_name(field: str, name: str) -> None:
    """Имя очереди, команды или раздела состояния — по ``NAME_PATTERN``; иначе ``MESSAGE_INVALID``."""
    if not valid_name(name):
        raise AgentsError("MESSAGE_INVALID", f"Неверное имя ({field}) {name!r}: нужно {NAME_PATTERN}")


def _check_domain(domain: Any) -> None:
    if not domain or not isinstance(domain, str):
        raise AgentsError("MESSAGE_INVALID", "Нужен domain")
    _check_name("domain", domain)


def _with_creds(agent: Agent, stored: Agent) -> Agent:
    """Агент сессии с учётными данными из записи (их меняют authenticate и revoke)."""
    agent.secret_hash, agent.pending_secret_hash = stored.secret_hash, stored.pending_secret_hash
    return agent


def _invalid(kind: str) -> Dict[str, Any]:
    return {"code": "MESSAGE_INVALID", "message": f"Некорректное {kind}", "retryable": False}


def _lease_lost() -> Dict[str, Any]:
    return {"code": "JOB_LEASE_LOST", "message": "Задача больше не за этим агентом", "retryable": False}


class Actor:
    """Изменяющие действия ``Agents`` от имени ``actor`` (``agents.by(actor)``): то же API,
    ``actor`` попадает в задачу, команду, снимок состояния и запись аудита."""

    def __init__(self, agents: Agents, actor: str) -> None:
        self.agents = agents
        self.actor = actor

    async def _run(self, method: Callable[..., Awaitable[Any]], *args: Any, **kwargs: Any) -> Any:
        token = _ACTOR.set(self.actor)
        try:
            return await method(*args, **kwargs)
        finally:
            _ACTOR.reset(token)

    async def enqueue(self, queue: str, data: Any = None, **kwargs: Any) -> Job:
        return await self._run(self.agents.enqueue, queue, data, **kwargs)

    async def cancel_job(self, job_id: str) -> Job:
        return await self._run(self.agents.cancel_job, job_id)

    async def stop_job(self, job_id: str) -> Job:
        return await self._run(self.agents.stop_job, job_id)

    async def command(self, name: str, args: Any = None, **kwargs: Any) -> Command:
        return await self._run(self.agents.command, name, args, **kwargs)

    async def call(self, name: str, args: Any = None, **kwargs: Any) -> Command:
        return await self._run(self.agents.call, name, args, **kwargs)

    async def set_state(self, domain: str, spec: Any, agent_id: Optional[str] = None) -> DesiredState:
        return await self._run(self.agents.set_state, domain, spec, agent_id)

    async def delete_state(self, domain: str, agent_id: Optional[str] = None) -> Optional[DesiredState]:
        return await self._run(self.agents.delete_state, domain, agent_id)

    async def rollback_state(self, domain: str, version: int, agent_id: Optional[str] = None) -> DesiredState:
        return await self._run(self.agents.rollback_state, domain, version, agent_id)

    async def revoke(self, agent_id: str) -> Agent:
        return await self._run(self.agents.revoke, agent_id)

    async def update_agent(self, agent_id: str) -> Command:
        return await self._run(self.agents.update_agent, agent_id)

    async def update_worker(self, agent_id: str, name: str) -> Command:
        return await self._run(self.agents.update_worker, agent_id, name)

    async def rotate_key(self, agent_id: str) -> Command:
        return await self._run(self.agents.rotate_key, agent_id)

    async def pause_worker(self, agent_id: str, name: str, queues: Optional[List[str]] = None) -> Command:
        return await self._run(self.agents.pause_worker, agent_id, name, queues)

    async def resume_worker(self, agent_id: str, name: str, queues: Optional[List[str]] = None) -> Command:
        return await self._run(self.agents.resume_worker, agent_id, name, queues)
