"""Воркер на узле (роль worker контракта SDK, §10 спецификации).

Воркер запускает агент (``workers`` в его конфигурации) и передаёт канал IPC
(``AGENT_IPC_FD``); связь с сервером, повторы, учётные данные и обновление —
забота агента, воркер знает только свою предметную область::

    from agent_sdk.worker import Job, Worker

    worker = Worker("echo", version="1.0.0")

    @worker.job("example.echo", concurrency=2)
    def echo(job: Job) -> dict:
        return {"echo": job.data["text"]}

    worker.run()
"""

from .. import __version__
from .channel import Channel
from .command import Command
from .context import ContextAgent, WorkerContext
from .errors import AgentError, Cancelled, CommandFailed, JobFailed, MessageTooLarge, StateFailed
from .job import Job
from .worker import AUTO_INTERVAL, Worker

__all__ = [
    "AUTO_INTERVAL", "AgentError", "Cancelled", "Channel", "Command", "CommandFailed", "ContextAgent", "Job",
    "JobFailed", "MessageTooLarge", "StateFailed", "Worker", "WorkerContext", "__version__",
]
