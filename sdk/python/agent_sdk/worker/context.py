"""Контекст воркера: что агент сообщает о себе (``worker.context``, §10 спецификации)."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict


@dataclass(frozen=True)
class ContextAgent:
    """Агент, запустивший воркер. ``id`` пусто, пока агент не зарегистрирован."""

    id: str = ""
    name: str = ""
    version: str = ""
    labels: Dict[str, str] = field(default_factory=dict)


@dataclass(frozen=True)
class WorkerContext:
    """Контекст воркера (``worker.context``).

    ``mode`` — ``run`` или ``cleanup`` (воркер запущен командой ``agent cleanup`` только для
    уборки: подготовку можно пропустить); ``online`` — у агента есть связь с сервером;
    ``metrics_interval_ms`` — текущая частота метрик агента (0 — ещё неизвестна);
    ``channels`` — подписки сервера на каналы показателей этого воркера: канал → частота, мс
    (нет подписки — канала нет); ``log_level`` — какие записи лога агент сейчас отправляет на сервер.
    """

    mode: str = "run"
    agent: ContextAgent = field(default_factory=ContextAgent)
    online: bool = False
    metrics_interval_ms: int = 0
    channels: Dict[str, int] = field(default_factory=dict)
    log_level: str = ""

    @classmethod
    def from_message(cls, data: Dict[str, Any]) -> "WorkerContext":
        """Из данных ``worker.context``; незнакомые и неверные поля — значения по умолчанию."""
        agent = data.get("agent") if isinstance(data.get("agent"), dict) else {}
        labels = agent.get("labels") if isinstance(agent.get("labels"), dict) else {}
        interval = data.get("metricsIntervalMs")
        channels = data.get("channels") if isinstance(data.get("channels"), dict) else {}
        return cls(
            mode=data.get("mode") if data.get("mode") in ("run", "cleanup") else "run",
            agent=ContextAgent(
                id=_text(agent.get("id")), name=_text(agent.get("name")), version=_text(agent.get("version")),
                labels={str(k): str(v) for k, v in labels.items()},
            ),
            online=data.get("online") is True,
            metrics_interval_ms=_ms(interval),
            channels={str(k): _ms(v) for k, v in channels.items() if _ms(v) > 0},
            log_level=_text(data.get("logLevel")),
        )


def _ms(value: Any) -> int:
    """Интервал, мс: положительное число, иначе 0."""
    return int(value) if isinstance(value, (int, float)) and not isinstance(value, bool) and value > 0 else 0


def _text(value: Any) -> str:
    return value if isinstance(value, str) else ""
