// Клиент REST API сервера примера (examples/API.md); поток — stream.ts.
import {
  type Agent,
  type Command,
  type DesiredState,
  type Job,
  type MetricsPoint,
  type Releases,
  type Snapshot,
} from "./types";

async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
  const res = await fetch(path, {
    method,
    headers: body === undefined ? undefined : { "Content-Type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.message ?? `HTTP ${res.status}`);
  return data as T;
}

export interface EnqueueRequest {
  queue: string;
  data?: unknown;
  maxAttempts?: number;
  leaseSeconds?: number;
  inputs?: Record<string, string>;
  outputs?: string[];
  /** Закрепить за агентом. */
  agentId?: string;
}

export const api = {
  snapshot: () => call<Snapshot>("GET", "/api/snapshot"),
  enqueue: (req: EnqueueRequest) => call<Job>("POST", "/api/jobs", req),
  cancelJob: (id: string) => call<Job>("POST", `/api/jobs/${id}/cancel`),
  stopJob: (id: string) => call<Job>("POST", `/api/jobs/${id}/stop`),
  command: (req: { agentId?: string; name: string; args?: unknown; timeoutSec?: number }) =>
    call<Command>("POST", "/api/commands", req),
  /** Снимок домена: общий или для агента (agentId). */
  setState: (domain: string, spec: unknown, agentId?: string) =>
    call<DesiredState>(
      "PUT",
      `/api/state/${encodeURIComponent(domain)}${agentId ? `?agentId=${encodeURIComponent(agentId)}` : ""}`,
      spec,
    ),
  /** Удалить снимок: с agentId — личный (агенту вернётся общий), без — общий. */
  deleteState: (domain: string, agentId?: string) =>
    call<{ state: DesiredState | null }>(
      "DELETE",
      `/api/state/${encodeURIComponent(domain)}${agentId ? `?agentId=${encodeURIComponent(agentId)}` : ""}`,
    ),
  /** История метрик агента; since — только точки строго позже. */
  metrics: (agentId: string, since?: number) =>
    call<MetricsPoint[]>(
      "GET",
      `/api/agents/${encodeURIComponent(agentId)}/metrics${since != null ? `?since=${since}` : ""}`,
    ),
  revoke: (agentId: string) => call<Agent>("POST", `/api/agents/${encodeURIComponent(agentId)}/revoke`),
  releases: () => call<Releases>("GET", "/api/releases"),
  updateAgent: (agentId: string) => call<Command>("POST", `/api/agents/${encodeURIComponent(agentId)}/update`),
  /** Обновить воркер из выпуска (команда worker.update). */
  updateWorker: (agentId: string, name: string) =>
    call<Command>("POST", `/api/agents/${encodeURIComponent(agentId)}/workers/${encodeURIComponent(name)}/update`),
  /** Пауза воркера с сервера (команда worker.pause): queues пусто — все его очереди. */
  pauseWorker: (agentId: string, name: string, queues?: string[]) =>
    call<Command>(
      "POST",
      `/api/agents/${encodeURIComponent(agentId)}/workers/${encodeURIComponent(name)}/pause`,
      queues?.length ? { queues } : {},
    ),
  /** Снять паузу сервера (команда worker.resume); пауза, выставленная самим воркером, остаётся. */
  resumeWorker: (agentId: string, name: string, queues?: string[]) =>
    call<Command>(
      "POST",
      `/api/agents/${encodeURIComponent(agentId)}/workers/${encodeURIComponent(name)}/resume`,
      queues?.length ? { queues } : {},
    ),
  /** Сменить ключ агента (команда agent.rotateKey). */
  rotateKey: (agentId: string) => call<Command>("POST", `/api/agents/${encodeURIComponent(agentId)}/rotate-key`),
  /** История версий раздела: общая или агента, новые первыми. */
  stateHistory: (domain: string, agentId?: string, limit = 20) =>
    call<DesiredState[]>(
      "GET",
      `/api/state/${encodeURIComponent(domain)}/history?limit=${limit}${agentId ? `&agentId=${encodeURIComponent(agentId)}` : ""}`,
    ),
  /** Откатить раздел к версии: тот же spec под новой версией. */
  rollbackState: (domain: string, version: number, agentId?: string) =>
    call<DesiredState>("POST", `/api/state/${encodeURIComponent(domain)}/rollback`, { version, agentId }),
  fileUrl: (jobId: string, name: string) => `/files/${jobId}/out/${name}`,
};

/** Снимок домена, который действует для агента: свой важнее общего. */
export function desiredFor(snapshot: Snapshot, domain: string, agentId?: string): DesiredState | undefined {
  const own = agentId ? snapshot.states.find((s) => s.domain === domain && s.agentId === agentId) : undefined;
  return own ?? snapshot.states.find((s) => s.domain === domain && !s.agentId);
}

/** Все объявленные имена у агентов (для выпадающих списков). */
export function declared(snapshot: Snapshot) {
  const queues = new Set<string>();
  const commands = new Set<string>();
  const domains = new Set<string>();
  for (const a of snapshot.agents) {
    a.capabilities?.jobs?.queues.forEach((q) => queues.add(q.name));
    Object.keys(a.status?.capacity ?? {}).forEach((q) => queues.add(q));
    a.capabilities?.commands?.names.forEach((c) => commands.add(c));
    Object.keys(a.capabilities?.state?.domains ?? {}).forEach((d) => domains.add(d));
  }
  const sorted = (s: Set<string>) => [...s].sort();
  return { queues: sorted(queues), commands: sorted(commands), domains: sorted(domains) };
}
