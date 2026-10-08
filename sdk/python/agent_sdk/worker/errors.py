"""Исключения SDK воркера."""

from __future__ import annotations

from typing import Any


class Cancelled(Exception):
    """Задачу отменили: работу прекратить, результат не нужен."""


class JobFailed(Exception):
    """Задача провалена с кодом; ``retryable=False`` — без повторов."""

    def __init__(self, code: str, message: str, retryable: bool = True) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.retryable = retryable


class CommandFailed(Exception):
    """Команда завершилась ошибкой с кодом (``^[A-Z0-9_]+$``)."""

    def __init__(self, code: str, message: str) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message


class StateFailed(Exception):
    """Снимок состояния не применился, но есть отчёт: ``state.applied {ok: false, error, report}``
    (``raise StateFailed("порт занят", report={...})``). Обычное исключение — без отчёта."""

    def __init__(self, message: str, report: Any = None) -> None:
        super().__init__(message)
        self.message = message
        self.report = report


class AgentError(Exception):
    """Агент или сервер отклонили запрос воркера (ответ ``error``)."""

    def __init__(self, code: str, message: str, retryable: bool = True) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.retryable = retryable
