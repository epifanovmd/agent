"""Файлы задач: провайдер ссылок ``Files`` и ``MemoryFiles`` (в памяти).

Провайдер выдаёт агенту ссылки на входы и выходы задачи (``job.assign``,
``job.urls``). Свой провайдер — подписанные ссылки своего хранилища (S3 и т. п.).
"""

from __future__ import annotations

import abc
from typing import Any, Dict, List, Optional, Tuple, Union

from ..message import now_ms
from .model import Job

#: Срок ссылок по умолчанию, мс.
URL_TTL_MS = 3600_000

Content = Union[str, bytes]


class Files(abc.ABC):
    """Провайдер ссылок на файлы задач."""

    async def prepare(self, job: Job, inputs: Dict[str, Content]) -> None:
        """Входы запроса задачи (имя → содержимое или URL — по провайдеру) при постановке."""

    @abc.abstractmethod
    async def urls(self, job: Job, base_url: str) -> Dict[str, Any]:
        """``{inputs: {имя: url}, outputs: {имя: {url, contentType}}, expiresAt}`` для всех файлов задачи."""

    async def handle(self, method: str, key: str, body: bytes = b"") -> Tuple[int, bytes]:
        """``GET``/``PUT /files/<key>`` — если файлы обслуживает сам транспорт ``Agents``."""
        return 404, b""

    async def outputs(self, job: Job) -> List[str]:
        """Имена загруженных выходов задачи (для интерфейса); неизвестно — пусто."""
        return []


class MemoryFiles(Files):
    """Файлы в памяти; ``GET``/``PUT /files/<jobId>/(in|out)/<имя>`` обслуживает
    транспорт ``Agents`` (``agents.handle_file``). Входы запроса задачи — содержимое."""

    def __init__(self, ttl_ms: int = URL_TTL_MS) -> None:
        self.ttl_ms = ttl_ms
        self._data: Dict[str, bytes] = {}

    async def prepare(self, job: Job, inputs: Dict[str, Content]) -> None:
        for name, content in inputs.items():
            self._data[f"{job.id}/in/{name}"] = content.encode() if isinstance(content, str) else bytes(content)

    async def urls(self, job: Job, base_url: str) -> Dict[str, Any]:
        base = f"{base_url.rstrip('/')}/files/{job.id}"
        return {
            "inputs": {name: f"{base}/in/{name}" for name in job.inputs},
            "outputs": {name: {"url": f"{base}/out/{name}", "contentType": "application/octet-stream"}
                        for name in job.outputs},
            "expiresAt": now_ms() + self.ttl_ms,
        }

    async def handle(self, method: str, key: str, body: bytes = b"") -> Tuple[int, bytes]:
        if not _valid_key(key):
            return 404, b""
        if method == "PUT":
            self._data[key] = bytes(body)
            return 200, b""
        if method in ("GET", "HEAD"):
            data = self._data.get(key)
            return (200, data) if data is not None else (404, b"")
        return 405, b""

    async def outputs(self, job: Job) -> List[str]:
        return [name for name in job.outputs if f"{job.id}/out/{name}" in self._data]

    def get(self, key: str) -> Optional[bytes]:
        """Содержимое по ключу ``<jobId>/(in|out)/<имя>``."""
        return self._data.get(key)

    def put(self, key: str, data: bytes) -> None:
        self._data[key] = bytes(data)


def _valid_key(key: str) -> bool:
    parts = key.split("/")
    return len(parts) == 3 and all(parts) and parts[1] in ("in", "out") and ".." not in parts
