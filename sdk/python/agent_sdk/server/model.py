"""Модель серверной части: агенты, задачи, команды, желаемое состояние, события.

Поля — как в контракте SDK (§2 ``sdk/README.md``); JSON — camelCase
(``to_dict``). Служебные поля (секрет, ``seq`` потока, аренда) помечены
``hidden``: наружу не отдаются, но хранятся (``to_record``/``from_record`` —
для своего ``Store``). ``rev`` — версия записи для условной записи ``Store.update_*``.

``Agents`` не меняет вложенные списки и словари на месте — присваивает новые:
хранилищу достаточно поверхностной копии.
"""

from __future__ import annotations

import dataclasses
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type, TypeVar

T = TypeVar("T", bound="Model")

JOB_QUEUED = "queued"
JOB_RUNNING = "running"
JOB_COMPLETED = "completed"
JOB_FAILED = "failed"
JOB_CANCELLED = "cancelled"
#: Задача ещё не завершена.
JOB_ACTIVE = (JOB_QUEUED, JOB_RUNNING)

CMD_PENDING = "pending"
CMD_RUNNING = "running"
CMD_SUCCEEDED = "succeeded"
CMD_FAILED = "failed"
#: Команду отменили (``cancel_command``).
CMD_CANCELLED = "cancelled"
#: Команда ещё не завершена.
CMD_ACTIVE = (CMD_PENDING, CMD_RUNNING)


def _hidden() -> Dict[str, Any]:
    return {"hidden": True}


def camel(name: str) -> str:
    """``lease_seconds`` → ``leaseSeconds``."""
    head, *rest = name.split("_")
    return head + "".join(part[:1].upper() + part[1:] for part in rest)


def to_json(value: Any) -> Any:
    """Модель (и вложенные) → объект JSON; ``None`` у полей модели опускается."""
    if isinstance(value, Model):
        return value.to_dict()
    if isinstance(value, dict):
        return {k: to_json(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_json(v) for v in value]
    return value


class Model:
    """Сериализация dataclass-моделей в camelCase."""

    def to_dict(self) -> Dict[str, Any]:
        """Наружу (API приложения): без служебных полей и ``None``."""
        out: Dict[str, Any] = {}
        for f in dataclasses.fields(self):  # type: ignore[arg-type]
            if f.metadata.get("hidden"):
                continue
            value = getattr(self, f.name)
            if value is not None:
                out[camel(f.name)] = to_json(value)
        return out

    def to_record(self) -> Dict[str, Any]:
        """Для хранилища: все поля, включая служебные."""
        return {camel(f.name): to_json(getattr(self, f.name)) for f in dataclasses.fields(self)}  # type: ignore[arg-type]

    @classmethod
    def from_record(cls: Type[T], record: Dict[str, Any]) -> T:
        """Из ``to_record``/``to_dict``: незнакомые ключи пропускаются."""
        names = {camel(f.name): f.name for f in dataclasses.fields(cls)}  # type: ignore[arg-type]
        return cls(**{names[k]: v for k, v in record.items() if k in names})  # type: ignore[call-arg]

    def copy(self: T) -> T:
        """Поверхностная копия."""
        return dataclasses.replace(self)  # type: ignore[type-var]


@dataclass
class Agent(Model):
    """Агент парка. Секрет хранится только хешем (sha256)."""

    id: str
    name: str
    labels: Dict[str, str] = field(default_factory=dict)
    online: bool = False
    #: Учётные данные отозваны (``revoke``): нужна новая регистрация.
    revoked: bool = False
    transport: Optional[str] = None  # ws | http
    #: Адрес (IP без порта), с которого пришло последнее подключение (WebSocket или HTTP sync);
    #: из ``X-Forwarded-For`` — только с опцией ``trust_proxy``.
    address: Optional[str] = None
    enrolled_at: int = 0
    last_seen_at: Optional[int] = None
    hello: Optional[Dict[str, Any]] = None
    capabilities: Optional[Dict[str, Any]] = None
    status: Optional[Dict[str, Any]] = None
    #: Последняя точка ``metrics`` (не ``backfill``).
    metrics: Optional[Dict[str, Any]] = None
    #: Последнее ``inventory`` (§6.2).
    inventory: Optional[Dict[str, Any]] = None
    #: Домен → последний ``state.applied``.
    state_applied: Dict[str, Any] = field(default_factory=dict)
    #: Подписки на агента (``subscribe``): ``[{id, until, status?, metrics?, logs?, channels?}]``,
    #: ``until`` — мс UTC; в записи — чтобы их видели все процессы бэкенда. Агенту уходит сводная.
    subscriptions: List[Dict[str, Any]] = field(default_factory=list)
    secret_hash: str = field(default="", metadata=_hidden())
    #: Хеш нового секрета после ``agent.rotateKey``: принимается наравне с ``secret_hash``;
    #: первый вход с ним делает его основным (``authenticate``).
    pending_secret_hash: str = field(default="", metadata=_hidden())
    boot_id: Optional[str] = field(default=None, metadata=_hidden())
    #: Последний принятый ``seq`` потока в пределах ``boot_id``.
    last_seq: int = field(default=0, metadata=_hidden())
    #: Метки, выданные бэкендом при регистрации (``enroll``): при каждом ``hello``
    #: ``labels = {**hello.labels, **granted_labels}`` — выданные узел переписать не может.
    #: ``None`` — выданных меток нет.
    granted_labels: Optional[Dict[str, str]] = field(default=None, metadata=_hidden())
    #: Время точки ``metrics`` (часы сервера), из которой взяты текущие ``metrics``.
    metrics_at: int = field(default=0, metadata=_hidden())
    #: Активные уведомления о проблемах агента (``Alert.to_record()`` с ``active``).
    alerts: List[Dict[str, Any]] = field(default_factory=list, metadata=_hidden())
    #: Версия записи: ``Store.update_agent`` пишет, только если она не изменилась с чтения.
    rev: int = field(default=0, metadata=_hidden())

    def to_dict(self) -> Dict[str, Any]:
        """Наружу: без служебных полей; ``subscriptions`` — только если есть."""
        out = super().to_dict()
        if not out.get("subscriptions"):
            out.pop("subscriptions", None)
        return out


def hello_labels(agent: Agent, labels: Any) -> Dict[str, str]:
    """Метки агента после ``hello``: выданные при регистрации поверх меток из ``hello``
    (иначе узел выдал бы себя за другой); без выданных — метки из ``hello``."""
    sent = {str(k): str(v) for k, v in labels.items()} if isinstance(labels, dict) else {}
    return {**sent, **(agent.granted_labels or {})}


@dataclass
class Job(Model):
    """Задача очереди."""

    id: str
    queue: str
    data: Any = None
    status: str = JOB_QUEUED
    attempt: int = 0
    max_attempts: int = 1
    lease_seconds: int = 60
    agent_id: Optional[str] = None
    pinned_agent_id: Optional[str] = None
    accepted: bool = False
    progress: float = 0
    text: Optional[str] = None
    #: Последние 500 строк.
    log: List[str] = field(default_factory=list)
    events: List[Dict[str, Any]] = field(default_factory=list)
    result: Any = None
    error: Optional[Dict[str, str]] = None
    stop_requested: bool = False
    inputs: List[str] = field(default_factory=list)
    outputs: List[str] = field(default_factory=list)
    created_at: int = 0
    finished_at: Optional[int] = None
    #: Кто поставил (``agents.by(actor)``); ``None`` — не указано.
    actor: Optional[str] = None
    lease_until: int = field(default=0, metadata=_hidden())
    #: Последний принятый ``seq`` событий попытки.
    event_seq: int = field(default=0, metadata=_hidden())
    #: Версия записи: ``Store.update_job`` пишет, только если она не изменилась с чтения.
    rev: int = field(default=0, metadata=_hidden())


@dataclass
class Command(Model):
    """Команда агенту."""

    id: str
    agent_id: str
    name: str
    args: Any = None
    timeout_sec: int = 60
    status: str = CMD_PENDING
    #: Последние 256 КБ вывода.
    output: str = ""
    result: Any = None
    error: Optional[Dict[str, str]] = None
    #: Код выхода из ``cmd.done`` (команды, запускающие процесс).
    exit_code: Optional[int] = None
    created_at: int = 0
    finished_at: Optional[int] = None
    #: Кто отправил (``agents.by(actor)``); ``None`` — не указано.
    actor: Optional[str] = None
    #: Версия записи: ``Store.update_command`` пишет, только если она не изменилась с чтения.
    rev: int = field(default=0, metadata=_hidden())

    def finished(self) -> bool:
        """Команда завершена: ``succeeded``, ``failed`` или ``cancelled``."""
        return self.status not in CMD_ACTIVE


@dataclass
class DesiredState(Model):
    """Снимок домена: общий (``agent_id`` пусто) или для конкретного агента."""

    domain: str
    version: int
    spec: Any = None
    agent_id: Optional[str] = None
    updated_at: int = 0
    #: Кто записал (``agents.by(actor)``); ``None`` — не указано.
    actor: Optional[str] = None


@dataclass
class AgentEvent(Model):
    """Событие агента или воркера (§6.6)."""

    agent_id: str
    agent_name: str
    source: str
    type: str
    data: Any = None
    at: int = 0
    #: id конверта — для отбрасывания повтора.
    id: Optional[str] = field(default=None, metadata=_hidden())


@dataclass
class MetricsPoint(Model):
    """Точка истории метрик: ``at`` — мс по часам сервера, ``metrics`` — сообщение целиком."""

    at: int
    backfill: bool = False
    metrics: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UpdateCandidate(Model):
    """Агент, которого можно обновить до версии манифеста (``update_candidates``)."""

    agent_id: str
    name: str
    online: bool
    current: str
    target: str
    os: str
    arch: str


@dataclass
class WorkerUpdateCandidate(Model):
    """Воркер из выпуска (``release: true``) на агенте, которого можно обновить до версии
    манифеста (``worker_update_candidates``)."""

    agent_id: str
    agent_name: str
    online: bool
    worker: str
    current: str
    target: str
    os: str
    arch: str


@dataclass
class AuditEntry(Model):
    """Запись аудита: изменяющее действие API приложения (событие ``audit``).

    ``action``: ``job.enqueue``, ``job.cancel``, ``job.stop``, ``command`` (и ``call``),
    ``command.cancel``, ``state.set``, ``state.delete``, ``state.rollback``, ``agent.revoke``,
    ``agent.delete``, ``agent.update``, ``agent.rotateKey``, ``worker.update``, ``worker.pause``,
    ``worker.resume``. ``target`` — id задачи или команды, раздел состояния, id агента.
    ``actor`` — из ``agents.by(actor)``, без него пусто.
    """

    at: int
    actor: str
    action: str
    target: str
    agent_id: Optional[str] = None
    details: Optional[Dict[str, Any]] = None


#: Типы уведомлений о проблемах (``Alert.type``).
ALERT_OFFLINE = "offline"
ALERT_STATE_FAILED = "stateFailed"
ALERT_WORKER_DOWN = "workerDown"
ALERT_DEGRADED = "degraded"
ALERT_WORKER_DEGRADED = "workerDegraded"


@dataclass
class Alert(Model):
    """Уведомление о проблеме (событие ``alert``): ``active`` — началась (``True``) или закончилась.

    ``type``: ``offline`` (агент без связи), ``stateFailed`` (``state.applied ok=false``, ``domain``),
    ``workerDown`` (воркер в сбое, ``worker``), ``workerDegraded`` (воркер сам сообщил, что не в порядке:
    ``status.workers[].health == "degraded"``, ``worker``, ``message`` — его причина), ``degraded``
    (``status.state == "degraded"``). ``message`` — текст проблемы; в конце — тот же, что в начале.
    """

    type: str
    agent_id: str
    agent_name: str
    active: bool
    message: str = ""
    at: int = 0
    domain: Optional[str] = None
    worker: Optional[str] = None


@dataclass
class Change(Model):
    """Уведомление: что изменилось (``kind``: agent | job | command | state | event)."""

    kind: str
    id: str
