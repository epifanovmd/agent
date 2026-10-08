#!/usr/bin/env python3
"""Эталонный воркер пакетной обработки (имитация): задача идёт этапами.

API:
- очередь ``example.batch``, данные задачи: ``{"stages": 5, "stepsPerStage": 10,
  "stepSeconds": 0.05, "rate": 0.3, "failAtStage"?: N}``;
- на каждом шаге — прогресс, после каждого этапа — запись в журнал и событие
  ``stage`` ``{"stage", "score"}``;
- ``job.stop`` — досрочно завершить: результат по готовым этапам; ``job.cancel`` — бросить;
- выходной файл ``result`` (если сервер его просит) — JSON итога лучшего этапа;
- результат задачи: ``{"bestStage", "score", "stages", "stopped"}``;
- ошибки: ``BATCH_INVALID`` (неверные данные, без повтора), ``BATCH_FAILED``
  (сбой на этапе ``failAtStage`` при первой попытке, с повтором);
- команда ``example.batch.status`` — что обрабатывается сейчас;
- канал телеметрии ``example.batch`` — ``{"running", "stage", "score"}``.

Переменные окружения: ``BATCH_CONCURRENCY`` (1), ``BATCH_TELEMETRY_INTERVAL`` (5 с).
"""

from __future__ import annotations

import json
import logging
import os
import random
import threading
import time

from agent_sdk.worker import Command, Job, JobFailed, Worker

logging.basicConfig(
    level=os.environ.get("WORKER_LOG_LEVEL", "INFO").upper(),
    format="%(levelname)s %(name)s: %(message)s",
)

worker = Worker("batch", version="1.0.0")
_lock = threading.Lock()
_running: dict = {}  # jobId → {"stage", "score"}
_last: dict = {"stage": None, "score": None}


@worker.job("example.batch", concurrency=int(os.environ.get("BATCH_CONCURRENCY", "1")))
def run_batch(job: Job) -> dict:
    data = job.data or {}
    try:
        stages = max(1, int(data.get("stages", 5)))
        steps = max(1, int(data.get("stepsPerStage", 10)))
        step_seconds = float(data.get("stepSeconds", 0.05))
        rate = float(data.get("rate", 0.3))
        fail_at = data.get("failAtStage")
        fail_at = None if fail_at is None else int(fail_at)
    except (TypeError, ValueError) as err:
        raise JobFailed("BATCH_INVALID", f"неверные данные задачи: {err}", retryable=False) from err
    if not 0 < rate <= 1:
        raise JobFailed("BATCH_INVALID", "rate — в (0, 1]", retryable=False)

    rng = random.Random(job.id)
    score = 0.0
    best = {"stage": 0, "score": 0.0}
    done = 0
    with _lock:
        _running[job.id] = {"stage": 0, "score": None}
    try:
        for stage in range(1, stages + 1):
            if fail_at is not None and stage == fail_at and job.attempt == 0:
                raise JobFailed("BATCH_FAILED", f"сбой на этапе {stage}")
            for step in range(steps):
                job.check_cancelled()
                score += (1 - score) * rate * 0.1 + rng.uniform(-0.005, 0.005)
                job.progress(((stage - 1) * steps + step + 1) / (stages * steps), f"этап {stage}/{stages}")
                time.sleep(step_seconds)
            score = round(min(1.0, max(0.0, score)), 4)
            done = stage
            job.log(f"этап {stage}: score={score}")
            job.event("stage", {"stage": stage, "score": score})
            with _lock:
                _running[job.id] = {"stage": stage, "score": score}
                _last.update(stage=stage, score=score)
            if score >= best["score"]:
                best = {"stage": stage, "score": score}
            if job.stop_requested:
                job.log(f"остановка по запросу после этапа {stage}")
                break
    finally:
        with _lock:
            _running.pop(job.id, None)

    if "result" in job.outputs:
        job.upload("result", json.dumps({"stage": best["stage"], "score": best["score"], "stages": done}).encode())
    return {"bestStage": best["stage"], "score": best["score"], "stages": done, "stopped": job.stop_requested}


@worker.command("example.batch.status")
def status(cmd: Command) -> dict:
    with _lock:
        running = dict(_running)
    for job_id, info in running.items():
        cmd.write(f"{job_id}: этап {info['stage']}, score {info['score']}\n")
    return {"running": len(running), "jobs": running}


@worker.telemetry("example.batch", interval=float(os.environ.get("BATCH_TELEMETRY_INTERVAL", "5")))
def stats() -> dict:
    with _lock:
        return {"running": len(_running), "stage": _last["stage"], "score": _last["score"]}


if __name__ == "__main__":
    worker.run()
