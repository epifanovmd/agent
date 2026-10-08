"""Сообщения агента: константы и помощники, общие для воркера и сервера.

Нормативный документ — ``sdk/spec/README.md``; здесь только то, что нужно
реализациям: пути, коды закрытия, классы доставки, конверт, объединение
возможностей.
"""

from __future__ import annotations

import json
import re
import time
import uuid
from typing import Any, Dict, Optional

#: Версия формата сообщений, которую знает SDK.
MESSAGE_VERSION = 1
#: Имя канала WebSocket (заголовок Sec-WebSocket-Protocol).
WS_CHANNEL = "agent.v1"

#: Пути транспорта (§2, §5).
LINK_PATH = "/api/v1/agent-link"
SYNC_PATH = "/api/v1/agent-link/sync"
ENROLL_PATH = "/api/v1/agent-link/enroll"
#: Раздача релизов агента (§7): манифест и сборки — ``RELEASES_PATH + "/<file>"``.
RELEASES_PATH = "/api/v1/agent-link/releases"
#: Установщик агента (§7).
INSTALL_PATH = "/api/v1/agent-link/install.sh"

#: Окружение воркера (§10).
ENV_IPC_FD = "AGENT_IPC_FD"
ENV_WORKER = "AGENT_WORKER"
ENV_AGENT_VERSION = "AGENT_VERSION"

#: Код ошибки задачи или команды.
CODE_PATTERN = re.compile(r"^[A-Z0-9_]+$")

#: Шаблон имени всего, что объявляет воркер: очереди, команды, разделы
#: состояния, каналы показателей.
NAME_PATTERN = r"^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$"
_NAME_RE = re.compile(NAME_PATTERN)


def valid_name(name: Any) -> bool:
    """Имя очереди, команды, раздела состояния или канала соответствует ``NAME_PATTERN``."""
    return isinstance(name, str) and _NAME_RE.fullmatch(name) is not None


#: Префиксы имён, принадлежащих агенту (§6.7).
RESERVED_PREFIXES = ("agent.", "worker.")


class Close:
    """Коды закрытия WebSocket (§2.1)."""

    NORMAL = 1000
    GOING_AWAY = 1001
    RESTART = 1012
    INVALID = 4400
    UNAUTHORIZED = 4401
    UNSUPPORTED = 4409
    REPLACED = 4410
    OVERLOADED = 4429


#: Надёжные сообщения агента: ``ack{ids}`` или ``error`` (§4).
RELIABLE = frozenset({
    "job.event", "job.complete", "job.fail", "job.reject", "cmd.done", "state.applied", "event",
})
#: Потоковые сообщения агента: ``seq``, подтверждаются ``ack{seq}`` (§4).
STREAM = frozenset({
    "status", "metrics", "inventory", "capabilities",
    "job.accept", "job.progress", "cmd.accept", "cmd.output", "log",
})
#: Уровни записей ``log`` — от подробного к важному.
LOG_LEVELS = ("debug", "info", "warn", "error")
#: Запросы агента: ответ с ``re`` (§4).
REQUESTS = frozenset({"job.urls"})


class MessageError(Exception):
    """Некорректное сообщение: сообщение не разобрано."""


def now_ms() -> int:
    """Текущее время, мс UTC."""
    return int(time.time() * 1000)


def new_id() -> str:
    """Уникальный id (UUID v4)."""
    return str(uuid.uuid4())


def envelope(type: str, data: Any = None, *, id: Optional[str] = None, re: Optional[str] = None,
             seq: Optional[int] = None, ts: Optional[int] = None) -> Dict[str, Any]:
    """Конверт сообщения (§4); пустые поля не включаются."""
    env: Dict[str, Any] = {"type": type}
    if id:
        env["id"] = id
    if re:
        env["re"] = re
    if seq:
        env["seq"] = seq
    env["ts"] = ts if ts is not None else now_ms()
    if data is not None:
        env["data"] = data
    return env


def encode(env: Dict[str, Any]) -> str:
    """Конверт → строка JSON (UTF-8 без экранирования)."""
    return json.dumps(env, ensure_ascii=False, separators=(",", ":"))


def decode(raw: Any) -> Dict[str, Any]:
    """Строка или байты JSON → конверт; ``MessageError`` — не конверт."""
    try:
        env = json.loads(raw)
    except (TypeError, ValueError) as err:
        raise MessageError(f"не JSON: {err}") from None
    if not isinstance(env, dict) or not isinstance(env.get("type"), str) or not env["type"]:
        raise MessageError("нет type")
    if "data" in env and env["data"] is not None and not isinstance(env["data"], dict):
        raise MessageError("data — не объект")
    for key in ("seq", "ts"):
        if key in env and env[key] is not None and (not isinstance(env[key], int) or isinstance(env[key], bool)):
            raise MessageError(f"{key} — не целое")
    return env


def merge_capabilities(cur: Optional[Dict[str, Any]], add: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Объединение возможностей (§6.1 ``capabilities``).

    Новые имена добавляются, у домена — большая применённая версия; сужение —
    только новым ``hello``. Исходные объекты не меняются.
    """
    out: Dict[str, Any] = dict(cur or {})
    add = add or {}

    def union(a: Any, b: Any) -> list:
        return sorted(set(a or []) | set(b or []))

    for key, value in add.items():
        if key == "commands":
            out[key] = {"names": union((out.get(key) or {}).get("names"), (value or {}).get("names"))}
        elif key == "telemetry":
            out[key] = {"channels": union((out.get(key) or {}).get("channels"), (value or {}).get("channels"))}
        elif key == "state":
            domains = dict((out.get(key) or {}).get("domains") or {})
            for domain, version in ((value or {}).get("domains") or {}).items():
                prev = domains.get(domain)
                if domain not in domains or prev is None or (version is not None and version > prev):
                    domains[domain] = version
            out[key] = {"domains": domains}
        else:
            # jobs, update и незнакомые — последнее объявленное.
            out[key] = value
    return out
