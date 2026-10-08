"""Канал воркера с агентом: unix socket (дескриптор ``AGENT_IPC_FD``), по
строке JSON на конверт сообщения (§10 спецификации). Свой транспорт — ``Channel(sock)``
с любым двусторонним сокетом (socketpair в тестах, unix-сокет)."""

from __future__ import annotations

import json
import os
import socket
import threading
import uuid
from typing import Any, Dict, Iterator, Optional, Tuple

from ..message import ENV_IPC_FD, envelope
from .errors import AgentError, MessageTooLarge

#: Ответ агента на запрос — не дольше (агент сам ждёт сервер до 30 с).
REQUEST_TIMEOUT = 45.0
#: Предел строки канала (§10), байт: длиннее агент не примет.
MAX_LINE = 16 << 20


class Channel:
    """Канал IPC: отправка из любых потоков, ответы на запросы по ``re``."""

    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._reader = sock.makefile("rb")
        self._write_lock = threading.Lock()
        self._pending_lock = threading.Lock()
        self._pending: Dict[str, Tuple[threading.Event, list]] = {}
        self._closed = threading.Event()

    @classmethod
    def from_env(cls) -> "Channel":
        """Канал, унаследованный от агента; без агента — понятная ошибка."""
        fd = os.environ.get(ENV_IPC_FD)
        if not fd:
            raise RuntimeError(
                "воркер запускается агентом (AGENT_IPC_FD не задан): "
                "опишите его в workers конфигурации агента"
            )
        return cls(socket.socket(fileno=int(fd)))

    def send(self, type: str, data: Any = None, *, id: Optional[str] = None,
             re: Optional[str] = None) -> None:
        """Отправить сообщение. Данные не превращаются в JSON — ``TypeError`` или ``ValueError``;
        строка длиннее ``MAX_LINE`` — ``MessageTooLarge`` (канал цел); канал закрыт — ``OSError``."""
        line = json.dumps(envelope(type, data, id=id, re=re), ensure_ascii=False, separators=(",", ":"),
                          allow_nan=False).encode() + b"\n"
        if len(line) > MAX_LINE:
            raise MessageTooLarge(f"{type}: сообщение больше 16 МБ")
        with self._write_lock:
            self._sock.sendall(line)

    def request(self, type: str, data: Any, timeout: float = REQUEST_TIMEOUT) -> Any:
        """Запрос с ответом (``job.urls``); ошибка агента — ``AgentError`` (канал закрыт —
        код ``CHANNEL_CLOSED``, нет ответа — ``TIMEOUT``)."""
        request_id = uuid.uuid4().hex
        done = threading.Event()
        slot: list = []
        with self._pending_lock:
            if self.closed:
                raise AgentError("CHANNEL_CLOSED", "канал с агентом закрыт")
            self._pending[request_id] = (done, slot)
        try:
            try:
                self.send(type, data, id=request_id)
            except OSError:
                raise AgentError("CHANNEL_CLOSED", "канал с агентом закрыт") from None
            answered = done.wait(timeout)
            if not slot:
                if answered or self.closed:
                    raise AgentError("CHANNEL_CLOSED", "канал с агентом закрыт")
                raise AgentError("TIMEOUT", f"нет ответа агента на {type}")
            reply = slot[0]
        finally:
            with self._pending_lock:
                self._pending.pop(request_id, None)
        if reply.get("type") == "error":
            err = reply.get("data") or {}
            raise AgentError(err.get("code", "ERROR"), err.get("message", ""), err.get("retryable", True))
        return reply.get("data")

    def messages(self) -> Iterator[Dict[str, Any]]:
        """Входящие сообщения агента (кроме ответов на запросы) до закрытия канала."""
        try:
            for raw in self._reader:
                try:
                    message = json.loads(raw)
                except ValueError:
                    continue
                if not isinstance(message, dict):
                    continue
                if message.get("re") and self._resolve(message):
                    continue
                yield message
        except (OSError, ValueError):
            pass
        finally:
            with self._pending_lock:
                self._closed.set()
                for done, _ in self._pending.values():
                    done.set()

    def close(self) -> None:
        with self._pending_lock:
            self._closed.set()
            for done, _ in self._pending.values():
                done.set()
        try:
            self._sock.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._sock.close()

    @property
    def closed(self) -> bool:
        return self._closed.is_set()

    def _resolve(self, message: Dict[str, Any]) -> bool:
        with self._pending_lock:
            waiter = self._pending.get(message["re"])
        if not waiter:
            return False
        done, slot = waiter
        slot.append(message)
        done.set()
        return True
