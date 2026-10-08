"""Задача в руках обработчика: данные, файлы, прогресс, события, отмена."""

from __future__ import annotations

import logging
import shutil
import tempfile
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Union

from . import files
from .errors import AgentError, Cancelled

if TYPE_CHECKING:
    from .channel import Channel

log = logging.getLogger("agent_sdk.worker")

#: Прогресс и лог уходят агенту не чаще этого: частые вызовы схлопываются.
PROGRESS_MIN_INTERVAL = 0.5
#: Лог копится не больше этого числа строк между отправками.
LOG_BATCH = 100
#: Попыток загрузки выходного файла (каждая следующая — со свежей ссылкой).
UPLOAD_ATTEMPTS = 4
#: Пауза между попытками загрузки, секунд (растёт с номером попытки).
UPLOAD_RETRY_SECONDS = 5
#: Ссылку, истекающую раньше этого, обновить до использования.
URL_REFRESH_MARGIN_MS = 60_000


class Job:
    """Задача, выданная воркеру агентом.

    Прогресс, лог и события уходят агенту; связь с сервером, повторы и
    досылка после обрыва — его забота. Отмена: ``job.cancelled`` становится
    ``True``, ``check_cancelled()`` бросает ``Cancelled``. Остановка
    (``stop_requested``) — довести шаг и вернуть результат как обычно.
    """

    def __init__(self, channel: "Channel", assign: Dict[str, Any]) -> None:
        self._channel = channel
        self.id: str = assign["jobId"]
        self.queue: str = assign["queue"]
        self.data: Any = assign.get("data")
        self.attempt: int = assign.get("attempt", 0)
        self.lease_seconds: int = assign.get("leaseSeconds", 60)
        self._inputs: Dict[str, str] = dict(assign.get("inputs") or {})
        self._outputs: Dict[str, Dict[str, str]] = dict(assign.get("outputs") or {})
        self._urls_expire_at: int = assign.get("urlsExpireAt") or 0

        self._lock = threading.Lock()
        self._progress: Optional[float] = None
        self._text: Optional[str] = None
        self._log: List[str] = []
        self._last_sent = 0.0
        self._timer: Optional[threading.Timer] = None
        self._event_seq = 0
        self._cancelled = threading.Event()
        self._stop_requested = threading.Event()
        self._tmp = Path(tempfile.mkdtemp(prefix=f"job-{self.id[:8]}-"))

    @property
    def ref(self) -> Dict[str, Any]:
        return {"jobId": self.id, "attempt": self.attempt}

    # ── для обработчика ─────────────────────────────────────────────────

    @property
    def cancelled(self) -> bool:
        return self._cancelled.is_set()

    @property
    def stop_requested(self) -> bool:
        """Попросили завершиться досрочно: довести шаг и вернуть результат."""
        return self._stop_requested.is_set()

    def check_cancelled(self) -> None:
        """Бросить ``Cancelled``, если задачу отменили."""
        if self._cancelled.is_set():
            raise Cancelled(self.id)

    def progress(self, value: float, text: Optional[str] = None) -> None:
        """Прогресс 0..1 и что делается сейчас."""
        with self._lock:
            self._progress = max(0.0, min(1.0, float(value)))
            if text is not None:
                self._text = text[:200]
        self._schedule()

    def log(self, line: str) -> None:
        """Строка лога задачи (хвост виден на сервере)."""
        with self._lock:
            self._log.append(str(line)[:1000])
            urgent = len(self._log) >= LOG_BATCH
        if urgent:
            self.flush()
        else:
            self._schedule()

    def event(self, type: str, data: Any = None) -> None:
        """Событие для хука очереди на сервере (``onEvent``): итог этапа и т. п.

        Доставка надёжная и по порядку: агент хранит событие до подтверждения
        сервером, повтор сервер отбрасывает по номеру ``seq``.
        """
        with self._lock:
            self._event_seq += 1
            seq = self._event_seq
        self.flush()
        self._channel.send("job.event", {**self.ref, "seq": seq, "type": type[:50], "data": data})

    @property
    def inputs(self) -> List[str]:
        return list(self._inputs)

    @property
    def outputs(self) -> List[str]:
        return list(self._outputs)

    def input_path(self, name: str) -> Path:
        """Скачать входной файл во временный каталог задачи (один раз)."""
        target = self._tmp / "inputs" / name
        if not target.exists():
            self.download(name, target)
        return target

    def download(self, name: str, target: Union[str, Path]) -> Path:
        """Скачать входной файл в указанное место атомарно (постоянный кэш и т. п.)."""
        if name not in self._inputs:
            raise KeyError(f"нет входного файла {name!r}")
        if self._expiring():
            self.refresh_urls(inputs=[name])
        try:
            return files.download(self._inputs[name], Path(target))
        except OSError:
            # Ссылка могла истечь (долгая задача) — свежая и ещё раз.
            self.refresh_urls(inputs=[name])
            return files.download(self._inputs[name], Path(target))

    def upload(self, name: str, source: Union[str, Path, bytes]) -> None:
        """Загрузить выходной файл по подписанной ссылке (PUT).

        Неудача (сеть, истёкшая ссылка) — повтор со свежей ссылкой от
        сервера; после ``UPLOAD_ATTEMPTS`` попыток — исключение.
        """
        if name not in self._outputs:
            raise KeyError(f"нет выходного файла {name!r}")
        if self._expiring():
            self.refresh_urls(outputs=[name])
        for attempt in range(1, UPLOAD_ATTEMPTS + 1):
            target = self._outputs[name]
            try:
                files.upload(target["url"], source, target.get("contentType"))
                return
            except OSError as err:
                if attempt == UPLOAD_ATTEMPTS:
                    raise
                log.warning("загрузка %s задачи %s: %s — повтор", name, self.id, err)
                time.sleep(UPLOAD_RETRY_SECONDS * attempt)
                try:
                    self.refresh_urls(outputs=[name])
                except AgentError as refresh_err:
                    log.warning("свежая ссылка %s не получена: %s", name, refresh_err)

    def refresh_urls(self, inputs: Optional[List[str]] = None,
                     outputs: Optional[List[str]] = None) -> None:
        """Получить свежие подписанные ссылки (все или перечисленные)."""
        request: Dict[str, Any] = dict(self.ref)
        if inputs is not None:
            request["inputs"] = inputs
        if outputs is not None:
            request["outputs"] = outputs
        urls = self._channel.request("job.urls", request) or {}
        self._inputs.update(urls.get("inputs") or {})
        self._outputs.update(urls.get("outputs") or {})
        self._urls_expire_at = urls.get("expiresAt") or self._urls_expire_at

    # ── жизненный цикл (вызывает Worker) ────────────────────────────────

    def flush(self) -> None:
        """Отправить накопленный прогресс и лог сейчас."""
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None
            if self._progress is None and self._text is None and not self._log:
                return
            update: Dict[str, Any] = dict(self.ref)
            if self._progress is not None:
                update["progress"] = self._progress
            if self._text is not None:
                update["text"] = self._text
            if self._log:
                update["log"] = self._log[:LOG_BATCH]
            self._progress, self._text = None, None
            self._log = self._log[LOG_BATCH:]
            self._last_sent = time.monotonic()
        try:
            self._channel.send("job.progress", update)
        except OSError:
            pass

    def cancel(self) -> None:
        if not self._cancelled.is_set():
            log.info("задача %s отменена", self.id)
        self._cancelled.set()

    def request_stop(self) -> None:
        if not self._stop_requested.is_set():
            log.info("задачу %s просят завершить досрочно", self.id)
        self._stop_requested.set()

    def close(self) -> None:
        with self._lock:
            if self._timer:
                self._timer.cancel()
                self._timer = None
        shutil.rmtree(self._tmp, ignore_errors=True)

    def _schedule(self) -> None:
        with self._lock:
            wait = self._last_sent + PROGRESS_MIN_INTERVAL - time.monotonic()
            if wait > 0:
                if self._timer is None:
                    self._timer = threading.Timer(wait, self.flush)
                    self._timer.daemon = True
                    self._timer.start()
                return
        self.flush()

    def _expiring(self) -> bool:
        return bool(self._urls_expire_at) and (
            self._urls_expire_at - time.time() * 1000 < URL_REFRESH_MARGIN_MS
        )
