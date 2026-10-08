"""Поток для интерфейса — WebSocket ``/api/ws`` (examples/API.md).

Сразу после подключения — ``snapshot``; дальше — изменения сущностей (``agent``, ``job``,
``command``, ``state``, ``event``) по ``agents.on("change")``, удаление снимка состояния
(``stateDeleted``: ``domain``, ``agentId`` — нет у общего), уведомления о проблемах
(``{type: "alert", data: Alert}``), записи аудита (``{type: "audit", data: AuditEntry}``), точки ``metrics``
и логи (``{type: "log", agentId, entries}``, с уровня клиента) агентов, на которые клиент подписан
(``subscribe {agentId, logLevel?}``, ``logLevel {agentId, level}``, ``unsubscribe``). Пока клиент
подписан на агента, сервер держит его подписку ``agents.subscribe`` (id — соединение и агент): метрики
и показатели всех воркеров агента раз в секунду, группы ``SUBSCRIPTION_METRICS``, лог с уровня
клиента; продлевает её, снимает при ``unsubscribe`` и закрытии соединения.

Соединение — любое с ``send_text(str)``, ``receive() -> str | None`` и ``close(code)``
(``http11.WebSocket``).
"""

from __future__ import annotations

import asyncio
import json
import uuid
from typing import Any, Awaitable, Callable, Dict, List, Optional, Set, Tuple

from agent_sdk.server import Alert, AuditEntry, Change, Agents, AgentsError, MetricsPoint

#: Продление подписки клиента на агента, с (срок подписки — SUBSCRIPTION_TTL_MS).
SUBSCRIPTION_RENEW = 20.0
SUBSCRIPTION_TTL_MS = 30_000
#: Частота метрик и показателей воркеров, пока карточку агента смотрят, мс.
SUBSCRIPTION_INTERVAL_MS = 1000
#: Группы метрик узла сверх настройки агента, пока карточку агента смотрят: диск, TCP-соединения,
#: процессы, температура.
SUBSCRIPTION_METRICS = ["diskio", "sockets", "processes", "temperatures"]
#: Уровни лога от подробного к важному; уровень подписки по умолчанию.
LOG_LEVELS = ("debug", "info", "warn", "error")
DEFAULT_LOG_LEVEL = "info"
#: Очередь исходящих клиента: переполнилась (клиент не успевает) — соединение закрывается.
MAX_QUEUE = 2000
#: Подписок на одного клиента — не больше.
MAX_SUBSCRIPTIONS = 256
#: Код закрытия для не успевающего клиента (Try Again Later).
CLOSE_SLOW = 1013
#: Сколько последних событий просматривать, чтобы найти новое по id.
EVENT_LOOKUP = 200

Snapshot = Callable[[], Awaitable[Dict[str, Any]]]
JobView = Callable[[Any], Awaitable[Dict[str, Any]]]


class Client:
    """Подключённый интерфейс: очередь исходящих и подписки на агентов."""

    def __init__(self, conn: Any) -> None:
        self.id = uuid.uuid4().hex
        self.conn = conn
        self.queue: "asyncio.Queue[Optional[str]]" = asyncio.Queue()
        #: Подписки: агент → уровень лога, с которого слать записи.
        self.subs: Dict[str, str] = {}
        #: Продление подписки по агентам.
        self.renewers: Dict[str, asyncio.Task] = {}
        #: snapshot уже в очереди: изменения — только после него.
        self.ready = False
        self.slow = False

    def push(self, message: Dict[str, Any]) -> None:
        self.push_raw(json.dumps(message, ensure_ascii=False))

    def push_raw(self, text: str) -> None:
        if self.slow:
            return
        if self.queue.qsize() >= MAX_QUEUE:
            self.slow = True
            self.queue.put_nowait(None)  # отправитель закроет соединение
            return
        self.queue.put_nowait(text)


def subscription_id(client: Client, agent_id: str) -> str:
    """Id подписки клиента на агента в ``Agents``: соединение и агент."""
    return f"{client.id}:{agent_id}"


class Live:
    """Поток изменений для интерфейсов поверх ``Agents``."""

    def __init__(self, agents: Agents, *, snapshot: Snapshot, job_view: JobView,
                 log: Optional[Callable[[str, Optional[Dict[str, Any]]], None]] = None) -> None:
        self.agents = agents
        self._snapshot = snapshot
        self._job_view = job_view
        self._log = log or (lambda msg, extra=None: None)
        self.clients: Set[Client] = set()
        #: Подписанные клиенты: агент → клиенты.
        self.subscribers: Dict[str, Set[Client]] = {}
        #: Снятие подписок в Agents, ещё не завершённое.
        self._unsubscribing: Set[asyncio.Future] = set()
        # Изменения обрабатываются по одному (порядок: snapshot клиента → его дельты).
        self._work: "Optional[asyncio.Queue[Tuple[str, Any]]]" = None
        self._pending: Set[Tuple[str, str]] = set()
        self._pump: Optional[asyncio.Task] = None
        self._off = [agents.on("change", self._on_change), agents.on("metrics", self._on_metrics),
                     agents.on("stateDeleted", self._on_state_deleted), agents.on("alert", self._on_alert),
                     agents.on("audit", self._on_audit), agents.on("log", self._on_log)]

    # ── события Agents ──────────────────────────────────────────────────

    def _on_change(self, change: Change) -> None:
        if not self.clients:
            return
        key = (change.kind, change.id)
        if key in self._pending:
            return  # уже в очереди: прочитается свежим
        self._pending.add(key)
        self._enqueue("change", key)

    def _on_state_deleted(self, domain: str, agent_id: Optional[str]) -> None:
        # Через ту же очередь, что и change: stateDeleted — раньше переизданного общего.
        if self.clients:
            # Уже ждущий change домена прочитался бы раньше: следующий встанет после stateDeleted.
            self._pending.discard(("state", domain))
            self._enqueue("message", {"type": "stateDeleted", "domain": domain,
                                      **({"agentId": agent_id} if agent_id else {})})

    def _on_alert(self, alert: Alert) -> None:
        # Через очередь: после snapshot клиента и в порядке с изменениями.
        if self.clients:
            self._enqueue("message", {"type": "alert", "data": alert.to_dict()})

    def _on_audit(self, entry: AuditEntry) -> None:
        if self.clients:
            self._enqueue("message", {"type": "audit", "data": entry.to_dict()})

    def _on_metrics(self, agent_id: str, point: MetricsPoint) -> None:
        clients = self.subscribers.get(agent_id)
        if not clients:
            return
        text = json.dumps({"type": "metrics", "agentId": agent_id, "point": point.to_dict()}, ensure_ascii=False)
        for client in list(clients):
            if client.ready:
                client.push_raw(text)

    def _on_log(self, agent_id: str, entries: List[Dict[str, Any]]) -> None:
        for client in list(self.subscribers.get(agent_id, ())):
            level = client.subs.get(agent_id)
            if not client.ready or level is None:
                continue
            least = LOG_LEVELS.index(level)
            mine = [e for e in entries if e.get("level") in LOG_LEVELS and LOG_LEVELS.index(e["level"]) >= least]
            if mine:
                client.push({"type": "log", "agentId": agent_id, "entries": mine})

    # ── обработка изменений ─────────────────────────────────────────────

    def _enqueue(self, kind: str, item: Any) -> None:
        if self._work is None:
            self._work = asyncio.Queue()
        if self._pump is None or self._pump.done():
            self._pump = asyncio.ensure_future(self._run())
        self._work.put_nowait((kind, item))

    async def _run(self) -> None:
        assert self._work is not None
        while True:
            kind, item = await self._work.get()
            try:
                if kind == "snapshot":
                    client: Client = item
                    if client in self.clients:
                        client.push({"type": "snapshot", "data": await self._snapshot()})
                        client.ready = True
                elif kind == "message":
                    self._broadcast(item)
                else:
                    self._pending.discard(item)
                    for message in await self._read(*item):
                        self._broadcast(message)
            except Exception as err:  # noqa: BLE001 — сбой чтения одной сущности не останавливает поток
                self._log("поток: ошибка", {"error": f"{type(err).__name__}: {err}"})

    async def _read(self, kind: str, id: str) -> List[Dict[str, Any]]:
        """Сущность по уведомлению ``change`` → сообщения потока."""
        agents = self.agents
        if kind == "agent":
            agent = await agents.get_agent(id)
            return [{"type": "agent", "data": agent.to_dict()}] if agent else []
        if kind == "job":
            job = await agents.get_job(id)
            return [{"type": "job", "data": await self._job_view(job)}] if job else []
        if kind == "command":
            cmd = await agents.get_command(id)
            return [{"type": "command", "data": cmd.to_dict()}] if cmd else []
        if kind == "state":
            # id — домен: изменился общий снимок или снимок агента — шлём все снимки домена.
            return [{"type": "state", "data": s.to_dict()} for s in await agents.list_states() if s.domain == id]
        if kind == "event":
            for event in await agents.list_events(EVENT_LOOKUP):
                if event.id == id:
                    return [{"type": "event", "data": event.to_dict()}]
            return []
        return []

    def _broadcast(self, message: Dict[str, Any]) -> None:
        text = json.dumps(message, ensure_ascii=False)
        for client in list(self.clients):
            if client.ready:
                client.push_raw(text)

    # ── подписки ────────────────────────────────────────────────────────

    def subscribe(self, client: Client, agent_id: str, level: str = DEFAULT_LOG_LEVEL) -> None:
        """Слать клиенту точки и логи агента; подписка в ``Agents`` — сразу и с продлением."""
        if agent_id in client.subs:
            self.set_log_level(client, agent_id, level)
            return
        if len(client.subs) >= MAX_SUBSCRIPTIONS:
            return
        client.subs[agent_id] = level
        self.subscribers.setdefault(agent_id, set()).add(client)
        client.renewers[agent_id] = asyncio.ensure_future(self._keep(client, agent_id))

    def set_log_level(self, client: Client, agent_id: str, level: str) -> None:
        """Другой уровень лога: подписка в ``Agents`` обновляется сразу."""
        if client.subs.get(agent_id) in (None, level):
            return
        client.subs[agent_id] = level
        task = client.renewers.pop(agent_id, None)
        if task is not None:
            task.cancel()
        client.renewers[agent_id] = asyncio.ensure_future(self._keep(client, agent_id))

    def unsubscribe(self, client: Client, agent_id: str) -> None:
        """Перестать слать точки и логи; подписка клиента в ``Agents`` снимается."""
        if client.subs.pop(agent_id, None) is None:
            return
        task = client.renewers.pop(agent_id, None)
        if task is not None:
            task.cancel()
        clients = self.subscribers.get(agent_id)
        if clients is not None:
            clients.discard(client)
            if not clients:
                del self.subscribers[agent_id]
        done = asyncio.ensure_future(self._unsubscribe(agent_id, subscription_id(client, agent_id)))
        self._unsubscribing.add(done)
        done.add_done_callback(self._unsubscribing.discard)

    async def _unsubscribe(self, agent_id: str, sub_id: str) -> None:
        try:
            await self.agents.unsubscribe(agent_id, sub_id)
        except AgentsError as err:
            self._log("поток: подписка не снята", {"agentId": agent_id, "code": err.code})

    async def _keep(self, client: Client, agent_id: str) -> None:
        """Подписка клиента на агента сразу и продление раз в SUBSCRIPTION_RENEW, пока клиент подписан.
        Каналы показателей — всех воркеров агента (по его возможностям на момент продления)."""
        try:
            while agent_id in client.subs:
                try:
                    agent = await self.agents.get_agent(agent_id)
                    telemetry = ((agent.capabilities if agent else None) or {}).get("telemetry") or {}
                    channels = {ch: {"intervalMs": SUBSCRIPTION_INTERVAL_MS}
                                for ch in telemetry.get("channels") or [] if isinstance(ch, str)}
                    await self.agents.subscribe(
                        agent_id, id=subscription_id(client, agent_id), ttl_ms=SUBSCRIPTION_TTL_MS,
                        metrics={"intervalMs": SUBSCRIPTION_INTERVAL_MS, "groups": SUBSCRIPTION_METRICS},
                        logs={"level": client.subs[agent_id]}, channels=channels)
                except AgentsError as err:
                    self._log("поток: подписка не продлена", {"agentId": agent_id, "code": err.code})
                await asyncio.sleep(SUBSCRIPTION_RENEW)
        finally:
            if client.renewers.get(agent_id) is asyncio.current_task():
                del client.renewers[agent_id]

    # ── соединение ──────────────────────────────────────────────────────

    async def serve(self, conn: Any) -> None:
        """Соединение интерфейса: snapshot, дельты, команды клиента — до закрытия."""
        client = Client(conn)
        self.clients.add(client)
        self._enqueue("snapshot", client)
        sender = asyncio.ensure_future(self._send_loop(client))
        try:
            while True:
                text = await conn.receive()
                if text is None:
                    break
                self._command(client, text)
        finally:
            self.clients.discard(client)
            for agent_id in list(client.subs):
                self.unsubscribe(client, agent_id)
            sender.cancel()
            await asyncio.gather(sender, return_exceptions=True)

    def _command(self, client: Client, text: str) -> None:
        try:
            msg = json.loads(text)
        except ValueError:
            return
        if not isinstance(msg, dict):
            return
        agent_id = msg.get("agentId")
        if not isinstance(agent_id, str) or not agent_id or len(agent_id) > 200:
            return
        if msg.get("type") == "subscribe":
            level = msg.get("logLevel")
            self.subscribe(client, agent_id, level if level in LOG_LEVELS else DEFAULT_LOG_LEVEL)
        elif msg.get("type") == "logLevel" and msg.get("level") in LOG_LEVELS:
            self.set_log_level(client, agent_id, msg["level"])
        elif msg.get("type") == "unsubscribe":
            self.unsubscribe(client, agent_id)

    async def _send_loop(self, client: Client) -> None:
        try:
            while True:
                text = await client.queue.get()
                if text is None:
                    await client.conn.close(CLOSE_SLOW)
                    return
                await client.conn.send_text(text)
        except (ConnectionError, OSError):
            await client.conn.close()

    async def close(self) -> None:
        """Остановка: отписка от ``Agents``, задачи — отменить."""
        for off in self._off:
            off()
        renewers = [t for c in self.clients for t in c.renewers.values()]
        tasks = [t for t in [self._pump, *renewers] if t is not None]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, *self._unsubscribing, return_exceptions=True)
        for client in self.clients:
            client.renewers.clear()
        for client in list(self.clients):
            await client.conn.close(1001)
