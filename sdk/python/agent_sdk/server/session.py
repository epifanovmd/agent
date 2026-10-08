"""Сессия агента и ошибка API ``Agents``."""

from __future__ import annotations

import asyncio
import time
from typing import Any, Dict, List, Optional, Set, Tuple

from ..message import envelope, new_id
from .model import Agent

Reply = Tuple[int, Dict[str, Any]]


class HttpReply(tuple):
    """Ответ ``(статус, тело)`` с заголовками ``headers`` (например, ``Retry-After``).

    Распаковывается как обычная пара: ``status, body = reply``; заголовки — ``reply.headers``.
    """

    headers: Dict[str, str]

    def __new__(cls, status: int, body: Dict[str, Any], headers: Optional[Dict[str, str]] = None) -> "HttpReply":
        reply = super().__new__(cls, (status, body))
        reply.headers = dict(headers or {})
        return reply


class AgentsError(Exception):
    """Ошибка API приложения: код (``^[A-Z0-9_]+$``) и HTTP-статус."""

    def __init__(self, code: str, message: str, status: int = 400) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.status = status

    def to_dict(self) -> Dict[str, Any]:
        return {"code": self.code, "message": self.message}


class Session:
    """Сессия агента: исходящее копится в очереди; WebSocket пишет его сразу,
    HTTP sync отдаёт ответом на запрос."""

    def __init__(self, agent: Agent, mode: str, base_url: str) -> None:
        self.id = new_id()
        self.agent = agent
        self.mode = mode  # ws | http
        #: Адрес сервера, по которому агент до него дошёл: ссылки на файлы задач.
        self.base_url = base_url
        self.outq: List[Dict[str, Any]] = []
        self.closed = False
        self.code = 0
        #: Выданные, ещё не принятые задачи: jobId → очередь (занимают слот).
        self.pending: Dict[str, str] = {}
        #: Команды, отправленные в этой сессии (``cmd.run``) и ещё не завершённые.
        self.sent: Set[str] = set()
        #: Домен → версия, известная агенту.
        self.known: Dict[str, int] = {}
        self.touched = time.monotonic()
        #: HTTP-запросов сессии в работе.
        self.active = 0
        #: В этой сессии пришёл status: слоты известны (status прошлой сессии не в счёт).
        self.has_status = False
        #: Сводная подписка, которую знает агент (welcome или последний ``config``); ``{}`` — подписок нет.
        self.subscription: Dict[str, Any] = {}
        #: Адрес клиента подключения (IP без порта); пусто — транспорт не сообщил.
        self.address = ""
        #: Закрыть сессию кодом 1012 после ответа на текущее сообщение (ключ сменён).
        self.restart = False
        #: Изменения уведомлений о проблемах по текущему сообщению: ``fn(запись агента) -> [Alert]``
        #: (применяются к свежей записи при её записи).
        self.alert_ops: List[Any] = []
        self._waiters: Set[asyncio.Future] = set()

    def send(self, type: str, data: Any, re: Optional[str] = None) -> None:
        if self.closed:
            return
        self.outq.append(envelope(type, data, re=re))
        self._wake()

    def take(self) -> List[Dict[str, Any]]:
        out, self.outq = self.outq, []
        return out

    def close(self, code: int) -> None:
        if self.closed:
            return
        self.closed = True
        self.code = code
        self._wake()

    async def wait(self, timeout: Optional[float]) -> None:
        """Ждать нового исходящего или закрытия (не дольше ``timeout``)."""
        if self.outq or self.closed:
            return
        fut = asyncio.get_running_loop().create_future()
        self._waiters.add(fut)
        try:
            await asyncio.wait_for(fut, timeout)
        except asyncio.TimeoutError:
            pass
        finally:
            self._waiters.discard(fut)

    def _wake(self) -> None:
        for fut in list(self._waiters):
            if not fut.done():
                fut.set_result(None)
