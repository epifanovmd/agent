"""Команда в руках обработчика: аргументы, вывод потоком, отмена."""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any, Dict, Optional

from .errors import Cancelled

if TYPE_CHECKING:
    from .channel import Channel

#: Кусок вывода в одном ``cmd.output`` (по спецификации — не больше 64 КБ).
CHUNK_MAX = 64 * 1024


class Command:
    """Команда, переданная воркеру агентом (``cmd.run``).

    Вывод (``write``) уходит на сервер по мере записи; результат обработчика —
    итог команды. Срок истёк — ``cancelled`` становится ``True``,
    ``check_cancelled()`` бросает ``Cancelled``: итог уже никому не нужен.
    """

    def __init__(self, channel: "Channel", run: Dict[str, Any]) -> None:
        self._channel = channel
        self.id: str = run["commandId"]
        self.name: str = run["name"]
        self.args: Any = run.get("args") or {}
        self.timeout_sec: int = run.get("timeoutSec", 60)
        self._cancelled = threading.Event()

    def write(self, text: str) -> None:
        """Вывод команды (виден на сервере по мере выполнения)."""
        data = str(text)
        for start in range(0, len(data), CHUNK_MAX // 4):
            self._channel.send("cmd.output", {"commandId": self.id, "chunk": data[start:start + CHUNK_MAX // 4]})

    @property
    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    @property
    def cancel_event(self) -> threading.Event:
        """Устанавливается, когда срок команды истёк (``cmd.cancel``) или канал закрыт."""
        return self._cancelled

    def wait(self, timeout: Optional[float] = None) -> bool:
        """Подождать ``timeout`` секунд (``None`` — до отмены); ``True`` — команду отменили."""
        return self._cancelled.wait(timeout)

    def check_cancelled(self) -> None:
        """Бросить ``Cancelled``, если срок команды истёк."""
        if self._cancelled.is_set():
            raise Cancelled(self.id)

    def cancel(self) -> None:
        self._cancelled.set()
