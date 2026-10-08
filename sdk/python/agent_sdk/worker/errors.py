"""Исключения SDK воркера."""

from __future__ import annotations

import re
from typing import Any, Tuple


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


class MessageTooLarge(ValueError):
    """Сообщение длиннее строки канала (16 МБ): не отправлено, канал цел."""


class AgentError(Exception):
    """Агент или сервер отклонили запрос воркера (ответ ``error``)."""

    def __init__(self, code: str, message: str, retryable: bool = True) -> None:
        super().__init__(f"{code}: {message}")
        self.code = code
        self.message = message
        self.retryable = retryable


#: Коды, которые выставляет SDK.
WORKER_ERROR = "WORKER_ERROR"
WORKER_STOPPING = "WORKER_STOPPING"
COMMAND_FAILED = "COMMAND_FAILED"
COMMAND_UNKNOWN = "COMMAND_UNKNOWN"
#: Итог (результат задачи, команды, отчёт состояния) не уместился в строку канала (16 МБ).
RESULT_TOO_LARGE = "RESULT_TOO_LARGE"
#: Подтверждение отмены задачи: job.fail после job.cancel, когда обработчик завершился.
CANCELLED = "CANCELLED"

_CODE = re.compile(r"^[A-Z0-9_]{1,64}$")


def normalize_code(code: Any, message: str, fallback: str) -> Tuple[str, str]:
    """Код по схеме спецификации; негодный заменяется общим ``fallback``, исходный уходит в текст."""
    if isinstance(code, str) and _CODE.fullmatch(code):
        return code, message
    return fallback, f"{code}: {message}"
