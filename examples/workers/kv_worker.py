#!/usr/bin/env python3
"""Эталонный воркер-сервис (без очередей): держит на узле желаемое состояние.

Домен ``example.kv`` — снимок ``{"ключ": "значение"}``: воркер приводит к нему
каталог ``KV_DIR`` (по файлу на ключ, лишние удаляет). Команда ``example.kv.get``
— значение ключа; канал телеметрии ``example.kv`` — число ключей (с частотой метрик
агента: чаще, пока на узел смотрят); событие ``kv.applied`` — после каждого применения;
самочувствие (``set_health``) — не в порядке, пока каталог не удаётся привести к снимку; уборка (``agent cleanup`` при удалении
агента с узла) удаляет каталог ``KV_DIR``. Так же устроен любой «сервис» на узле: конфигурация, кэш, фоновая служба.

Запускает агент (``workers`` в его конфигурации — ``examples/agent.demo.yaml``).
"""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path

from agent_sdk.worker import Command, CommandFailed, Worker

logging.basicConfig(
    level=os.environ.get("WORKER_LOG_LEVEL", "INFO").upper(),
    format="%(levelname)s %(name)s: %(message)s",
)

ROOT = Path(os.environ.get("KV_DIR", "/tmp/agent-example-kv"))
worker = Worker("kv", version="1.0.0")
#: Последнее сообщённое самочувствие: set_health — только при смене.
healthy = True


@worker.state("example.kv")
def apply(version: int, spec: dict) -> dict:
    """Идемпотентно: повтор того же снимка ничего не меняет. Сбой записи — воркер «не в
    порядке» (узел degraded, на сервере alert workerDegraded), агент повторит снимок."""
    global healthy
    try:
        keys = write(spec)
    except OSError as err:
        healthy = False
        worker.set_health(False, f"каталог {ROOT} не записан: {err.strerror or err}")
        raise
    if not healthy:
        healthy = True
        worker.set_health(True)
    worker.event("kv.applied", {"version": version, "keys": keys})
    return {"keys": keys}


def write(spec: dict) -> int:
    ROOT.mkdir(parents=True, exist_ok=True)
    wanted = {str(k): str(v) for k, v in (spec or {}).items()}
    for path in ROOT.iterdir():
        if path.name not in wanted:
            path.unlink()
    for key, value in wanted.items():
        path = ROOT / key
        if not path.exists() or path.read_text(encoding="utf-8") != value:
            tmp = path.with_suffix(".tmp")
            tmp.write_text(value, encoding="utf-8")
            tmp.replace(path)
    return len(wanted)


@worker.command("example.kv.get")
def get(cmd: Command) -> dict:
    key = str(cmd.args.get("key", ""))
    path = ROOT / key
    if not key or "/" in key or not path.is_file():
        raise CommandFailed("KEY_NOT_FOUND", f"нет ключа {key!r}")
    cmd.write(f"читаю {key}\n")
    return {"key": key, "value": path.read_text(encoding="utf-8")}


# KV_TELEMETRY_INTERVAL (с) — своя частота; без него — "auto": частота подписки на канал, без подписки —
# частота метрик агента.
@worker.telemetry("example.kv", interval=float(os.environ.get("KV_TELEMETRY_INTERVAL") or 0) or "auto")
def stats() -> dict:
    return {"keys": len(list(ROOT.iterdir())) if ROOT.is_dir() else 0}


@worker.cleanup
def cleanup() -> None:
    """Удаление агента с узла: убрать всё, что воркер создал, — каталог ключей."""
    shutil.rmtree(ROOT, ignore_errors=True)


if __name__ == "__main__":
    worker.run()
