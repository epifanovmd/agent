"""Хранилище ``Agents``: интерфейс ``Store`` и ``MemoryStore`` (в памяти).

Свой ``Store`` (Postgres и т. п.) — та же механика связи с данными в БД:
модели сериализуются ``to_record``/``from_record``. Методы асинхронные.
"""

from __future__ import annotations

import abc
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

from ..message import now_ms
from .model import CMD_ACTIVE, JOB_ACTIVE, Agent, AgentEvent, Command, DesiredState, Job, MetricsPoint


class Store(abc.ABC):
    """Хранение агентов, задач, команд, желаемого состояния и событий.

    ``get_*`` возвращают копию или ``None``; ``Agents`` меняет копию и сохраняет её
    ``update_*``. Задачи и команды (``list_jobs``, ``list_commands``) — новые первыми; агенты и
    снимки — в порядке создания.
    """

    # ── агенты ──
    @abc.abstractmethod
    async def create_agent(self, agent: Agent) -> None: ...

    @abc.abstractmethod
    async def get_agent(self, agent_id: str) -> Optional[Agent]: ...

    @abc.abstractmethod
    async def update_agent(self, agent: Agent) -> None: ...

    @abc.abstractmethod
    async def list_agents(self) -> List[Agent]: ...

    # ── задачи ──
    @abc.abstractmethod
    async def create_job(self, job: Job) -> None: ...

    @abc.abstractmethod
    async def get_job(self, job_id: str) -> Optional[Job]: ...

    @abc.abstractmethod
    async def update_job(self, job: Job) -> None: ...

    @abc.abstractmethod
    async def list_jobs(self, *, status: Optional[str] = None, queue: Optional[str] = None,
                        agent_id: Optional[str] = None) -> List[Job]: ...

    # ── команды ──
    @abc.abstractmethod
    async def create_command(self, command: Command) -> None: ...

    @abc.abstractmethod
    async def get_command(self, command_id: str) -> Optional[Command]: ...

    @abc.abstractmethod
    async def update_command(self, command: Command) -> None: ...

    @abc.abstractmethod
    async def list_commands(self, *, status: Optional[str] = None,
                            agent_id: Optional[str] = None) -> List[Command]: ...

    # ── желаемое состояние ──
    @abc.abstractmethod
    async def set_state(self, domain: str, agent_id: Optional[str], spec: object, *,
                        actor: Optional[str] = None) -> DesiredState:
        """Новый снимок. Версия монотонна по домену и между перезапусками:
        ``max(наибольшая версия домена + 1, now_ms)``. ``actor`` — кто записал (поле снимка).
        Снимок попадает и в историю (``list_state_history``)."""

    @abc.abstractmethod
    async def get_state(self, domain: str, agent_id: Optional[str]) -> Optional[DesiredState]: ...

    @abc.abstractmethod
    async def delete_state(self, domain: str, agent_id: Optional[str]) -> bool:
        """Удалить снимок (без ``agent_id`` — общий). Был ли он. Версия домена не откатывается:
        следующий ``set_state`` — больше любой выданной."""

    @abc.abstractmethod
    async def list_states(self) -> List[DesiredState]: ...

    @abc.abstractmethod
    async def list_state_history(self, domain: str, agent_id: Optional[str], limit: int) -> List[DesiredState]:
        """Снимки ``set_state`` раздела (``agent_id`` пусто — общего) от новых к старым, не больше
        ``limit`` (≤ 0 — все). ``delete_state`` историю не удаляет."""

    # ── события ──
    @abc.abstractmethod
    async def add_event(self, event: AgentEvent) -> None: ...

    @abc.abstractmethod
    async def list_events(self, limit: int = 100) -> List[AgentEvent]:
        """Последние ``limit`` событий, новые первыми; ``limit`` ≤ 0 — все."""

    # ── история метрик ──
    @abc.abstractmethod
    async def add_metrics(self, agent_id: str, point: MetricsPoint) -> None: ...

    @abc.abstractmethod
    async def list_metrics(self, agent_id: str, since: Optional[int] = None) -> List[MetricsPoint]:
        """Точки агента по возрастанию ``at``; ``since`` — только строго позже."""

    @abc.abstractmethod
    async def prune_metrics(self, before: int) -> int:
        """Удалить точки всех агентов с ``at`` < ``before``; сколько удалено.

        ``Agents`` вызывает при старте и раз в час (срок хранения ``metrics_retention_ms``).
        """


class MemoryStore(Store):
    """Хранилище в памяти процесса: для разработки и одного процесса.

    Завершённые задачи и команды сверх ``keep_*`` удаляются (старые первыми),
    события — сверх ``keep_events``, точки метрик — сверх ``keep_metrics`` на агента
    (4320: сутки при 20 с или 72 минуты при 1 с), история состояния — сверх ``keep_state_history``
    на раздел (общий или агента).
    """

    def __init__(self, *, keep_jobs: int = 1000, keep_commands: int = 500, keep_events: int = 1000,
                 keep_metrics: int = 4320, keep_state_history: int = 50) -> None:
        self.keep_state_history = keep_state_history
        #: (domain, agent_id или "") → снимки по возрастанию версии.
        self._state_history: Dict[Tuple[str, str], List[DesiredState]] = {}
        self.keep_jobs = keep_jobs
        self.keep_commands = keep_commands
        self.keep_events = keep_events
        self.keep_metrics = keep_metrics
        self._metrics: Dict[str, List[MetricsPoint]] = {}
        self._agents: "OrderedDict[str, Agent]" = OrderedDict()
        self._jobs: "OrderedDict[str, Job]" = OrderedDict()
        self._commands: "OrderedDict[str, Command]" = OrderedDict()
        self._states: "OrderedDict[Tuple[str, str], DesiredState]" = OrderedDict()
        #: Наибольшая выданная версия домена: после удаления снимков версия не откатывается.
        self._state_versions: Dict[str, int] = {}
        self._events: List[AgentEvent] = []

    # ── агенты ──
    async def create_agent(self, agent: Agent) -> None:
        self._agents[agent.id] = agent.copy()

    async def get_agent(self, agent_id: str) -> Optional[Agent]:
        agent = self._agents.get(agent_id)
        return agent.copy() if agent else None

    async def update_agent(self, agent: Agent) -> None:
        if agent.id in self._agents:
            self._agents[agent.id] = agent.copy()

    async def list_agents(self) -> List[Agent]:
        return [a.copy() for a in self._agents.values()]

    # ── задачи ──
    async def create_job(self, job: Job) -> None:
        self._jobs[job.id] = job.copy()
        _trim(self._jobs, self.keep_jobs, lambda j: j.status not in JOB_ACTIVE)

    async def get_job(self, job_id: str) -> Optional[Job]:
        job = self._jobs.get(job_id)
        return job.copy() if job else None

    async def update_job(self, job: Job) -> None:
        if job.id in self._jobs:
            self._jobs[job.id] = job.copy()

    async def list_jobs(self, *, status: Optional[str] = None, queue: Optional[str] = None,
                        agent_id: Optional[str] = None) -> List[Job]:
        return [j.copy() for j in reversed(self._jobs.values())
                if (status is None or j.status == status) and (queue is None or j.queue == queue)
                and (agent_id is None or j.agent_id == agent_id)]

    # ── команды ──
    async def create_command(self, command: Command) -> None:
        self._commands[command.id] = command.copy()
        _trim(self._commands, self.keep_commands, lambda c: c.status not in CMD_ACTIVE)

    async def get_command(self, command_id: str) -> Optional[Command]:
        cmd = self._commands.get(command_id)
        return cmd.copy() if cmd else None

    async def update_command(self, command: Command) -> None:
        if command.id in self._commands:
            self._commands[command.id] = command.copy()

    async def list_commands(self, *, status: Optional[str] = None,
                            agent_id: Optional[str] = None) -> List[Command]:
        return [c.copy() for c in reversed(self._commands.values())
                if (status is None or c.status == status) and (agent_id is None or c.agent_id == agent_id)]

    # ── желаемое состояние ──
    async def set_state(self, domain: str, agent_id: Optional[str], spec: object, *,
                        actor: Optional[str] = None) -> DesiredState:
        now = now_ms()
        state = DesiredState(domain=domain, version=max(self._state_versions.get(domain, 0) + 1, now), spec=spec,
                             agent_id=agent_id or None, updated_at=now, actor=actor or None)
        self._state_versions[domain] = state.version
        self._states[(domain, agent_id or "")] = state
        history = self._state_history.setdefault((domain, agent_id or ""), [])
        history.append(state.copy())
        if len(history) > self.keep_state_history:
            del history[: len(history) - self.keep_state_history]
        return state.copy()

    async def get_state(self, domain: str, agent_id: Optional[str]) -> Optional[DesiredState]:
        state = self._states.get((domain, agent_id or ""))
        return state.copy() if state else None

    async def delete_state(self, domain: str, agent_id: Optional[str]) -> bool:
        return self._states.pop((domain, agent_id or ""), None) is not None

    async def list_states(self) -> List[DesiredState]:
        return [s.copy() for s in self._states.values()]

    async def list_state_history(self, domain: str, agent_id: Optional[str], limit: int) -> List[DesiredState]:
        history = self._state_history.get((domain, agent_id or "")) or []
        if limit > 0:
            history = history[-limit:]
        return [s.copy() for s in reversed(history)]

    # ── события ──
    async def add_event(self, event: AgentEvent) -> None:
        self._events.append(event.copy())
        if len(self._events) > self.keep_events:
            del self._events[: len(self._events) - self.keep_events]

    async def list_events(self, limit: int = 100) -> List[AgentEvent]:
        events = self._events[-limit:] if limit > 0 else self._events
        return [e.copy() for e in reversed(events)]

    # ── история метрик ──
    async def add_metrics(self, agent_id: str, point: MetricsPoint) -> None:
        points = self._metrics.setdefault(agent_id, [])
        # Досланная (backfill) точка может быть старше последней: история — по возрастанию at.
        i = len(points)
        while i > 0 and points[i - 1].at > point.at:
            i -= 1
        points.insert(i, point.copy())
        if len(points) > self.keep_metrics:
            del points[: len(points) - self.keep_metrics]

    async def list_metrics(self, agent_id: str, since: Optional[int] = None) -> List[MetricsPoint]:
        return [p.copy() for p in self._metrics.get(agent_id) or () if since is None or p.at > since]

    async def prune_metrics(self, before: int) -> int:
        removed = 0
        for agent_id, points in list(self._metrics.items()):
            kept = [p for p in points if p.at >= before]
            removed += len(points) - len(kept)
            if kept:
                self._metrics[agent_id] = kept
            else:
                del self._metrics[agent_id]
        return removed


def _trim(items: "OrderedDict[str, object]", keep: int, removable) -> None:  # type: ignore[no-untyped-def]
    """Удалить старые завершённые сверх ``keep``."""
    if len(items) <= keep:
        return
    for key in [k for k, v in items.items() if removable(v)]:
        if len(items) <= keep:
            return
        del items[key]
