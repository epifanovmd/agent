"""Файлы задачи по подписанным ссылкам: скачивание и загрузка (urllib, без
зависимостей)."""

from __future__ import annotations

import functools
import os
import shutil
import ssl
import urllib.request
from pathlib import Path
from typing import BinaryIO, Optional, Union

#: Блок копирования.
CHUNK = 1024 * 1024
#: Таймаут сокета на операцию (не на весь файл).
TIMEOUT = 300


def _context() -> Optional[ssl.SSLContext]:
    """TLS для файлов задач: системные корни плюс CA сервера из ``AGENT_SERVER_CA_FILE``
    (агент задаёт его по ``server.caFile``); переменной нет — ``None`` (по умолчанию urllib)."""
    ca_file = os.environ.get("AGENT_SERVER_CA_FILE", "")
    return _context_for(ca_file) if ca_file else None


@functools.lru_cache(maxsize=4)
def _context_for(ca_file: str) -> ssl.SSLContext:
    context = ssl.create_default_context()
    context.load_verify_locations(cafile=ca_file)
    return context


def download(url: str, target: Path) -> Path:
    """Скачать атомарно: файл появляется целиком или не появляется."""
    target.parent.mkdir(parents=True, exist_ok=True)
    partial = target.with_name(f".{target.name}.{os.getpid()}.part")
    try:
        with urllib.request.urlopen(url, timeout=TIMEOUT, context=_context()) as response, open(partial, "wb") as file:
            shutil.copyfileobj(response, file, CHUNK)
        partial.replace(target)
    finally:
        partial.unlink(missing_ok=True)
    return target


def upload(url: str, source: Union[str, Path, bytes], content_type: Optional[str]) -> None:
    """PUT по подписанной ссылке; ``Content-Type`` — ровно подписанный."""
    headers = {"Content-Type": content_type} if content_type else {}
    if isinstance(source, bytes):
        _put(url, source, headers)
        return
    path = Path(source)
    headers["Content-Length"] = str(path.stat().st_size)
    with open(path, "rb") as file:
        _put(url, file, headers)


def _put(url: str, body: Union[bytes, BinaryIO], headers: dict) -> None:
    request = urllib.request.Request(url, data=body, method="PUT", headers=headers)
    with urllib.request.urlopen(request, timeout=TIMEOUT, context=_context()) as response:
        response.read()
