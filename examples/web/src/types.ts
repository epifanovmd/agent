// Снимок сервера примера (GET /api/snapshot, поток WebSocket /api/ws).

export interface Capabilities {
  jobs?: { queues: { name: string; concurrency: number }[] };
  commands?: { names: string[] };
  state?: { domains: Record<string, number | null> };
  telemetry?: { channels: string[] };
  update?: { mode: string };
}

export interface WorkerStatus {
  name: string;
  state: string;
  instances: number;
  version?: string;
  /** Исполняемый файл воркера ведёт агент (выпуск): его можно обновить командой worker.update. */
  release?: boolean;
  /** Что воркер сам сообщил (worker.health): degraded — не в порядке, причина — message. */
  health?: "ok" | "degraded" | string;
  message?: string;
  /** Хоть одна очередь воркера на паузе (воркером или сервером). */
  paused?: boolean;
}

export interface Agent {
  id: string;
  name: string;
  labels: Record<string, string>;
  /** На связи (сервер держит online ещё offlineGraceMs после обрыва). */
  online: boolean;
  /** Отозван: учётные данные больше не принимаются. */
  revoked?: boolean;
  transport?: "ws" | "http";
  /** Адрес, с которого пришло последнее подключение. */
  address?: string;
  lastSeenAt?: number;
  hello?: {
    agent: { name: string; version: string; sdk?: string };
    host: { hostname: string; os: string; arch: string; platform?: string; cpus?: number; memoryBytes?: number };
  };
  capabilities?: Capabilities;
  status?: {
    state: string;
    message?: string;
    slots: Record<string, number>;
    capacity?: Record<string, number>;
    jobs: { jobId: string; attempt: number; queue: string }[];
    workers: WorkerStatus[];
    outbox: number;
  };
  metrics?: Metrics;
  inventory?: Inventory;
  stateApplied: Record<string, { domain: string; version: number; ok: boolean; error?: string; report?: unknown }>;
}

/** Сообщение metrics агента. */
export interface Metrics {
  collectedAt: number;
  backfill?: boolean;
  /** Поля по группам метрик агента: нет группы — нет полей. */
  host?: {
    cpuPercent?: number;
    cpuCores?: number[];
    load1?: number;
    load5?: number;
    load15?: number;
    memUsedBytes?: number;
    memTotalBytes?: number;
    memAvailableBytes?: number;
    swapUsedBytes?: number;
    swapTotalBytes?: number;
    diskUsedBytes?: number;
    diskTotalBytes?: number;
    disks?: { mount: string; usedBytes: number; totalBytes: number; inodesUsed?: number; inodesTotal?: number }[];
    diskReadBps?: number;
    diskWriteBps?: number;
    diskReadIops?: number;
    diskWriteIops?: number;
    netRxBps?: number;
    netTxBps?: number;
    netErrors?: number;
    netDrops?: number;
    uptimeSec?: number;
    conntrack?: number;
    conntrackMax?: number;
    interfaces?: { name: string; rxBps: number; txBps: number; errors?: number; drops?: number }[];
    tcp?: { established?: number; timeWait?: number; closeWait?: number; listen?: number };
    processes?: number;
    threads?: number;
    fdsOpen?: number;
    fdsMax?: number;
    temperatures?: { maxC?: number; sensors?: { name: string; c: number }[] };
  };
  gpus?: {
    index: number;
    name?: string;
    utilPercent?: number;
    memUsedBytes?: number;
    memTotalBytes?: number;
    temperatureC?: number;
  }[];
  channels?: Record<string, unknown>;
}

/** Точка истории метрик (GET /api/agents/:id/metrics). */
export interface MetricsPoint {
  /** Время по часам сервера, мс. */
  at: number;
  /** Собрана без связи и дослана. */
  backfill: boolean;
  metrics: Metrics;
}

/** Уровень записи лога. */
export type LogLevel = "debug" | "info" | "warn" | "error";
export const LOG_LEVELS: LogLevel[] = ["debug", "info", "warn", "error"];

/** Запись лога агента или воркера (сообщение потока log). */
export interface LogEntry {
  /** Момент записи (часы агента), мс. */
  at: number;
  level: LogLevel;
  /** "agent" или имя воркера. */
  source: string;
  msg: string;
  attrs?: Record<string, unknown>;
}

/** Сведения об узле (inventory). */
export interface Inventory {
  collectedAt: number;
  os?: { hostname?: string; platform?: string; kernel?: string; arch?: string; virtualization?: string };
  cpu?: { model?: string; cores?: number; threads?: number };
  memoryBytes?: number;
  disks?: { mount: string; fs?: string; totalBytes?: number }[];
  interfaces?: { name: string; mac?: string; addresses?: string[] }[];
  gpus?: { index: number; name?: string; memoryBytes?: number }[];
  ports?: { tcp?: number[]; udp?: number[] };
}

/** GET /api/releases. */
export interface Releases {
  release: {
    version: string;
    artifacts: { os: string; arch: string; file: string; sha256: string; signature?: string }[];
    workers?: { name: string; version: string; os: string; arch: string; file: string }[];
  } | null;
  candidates: {
    agentId: string;
    name: string;
    online: boolean;
    current: string;
    target: string;
    os: string;
    arch: string;
  }[];
  /** Воркеры из выпуска (release: true), чья версия не как в манифесте. */
  workerCandidates?: {
    agentId: string;
    agentName: string;
    online: boolean;
    worker: string;
    current: string;
    target: string;
    os: string;
    arch: string;
  }[];
  installCommand: string;
}

export type JobStatus = "queued" | "running" | "completed" | "failed" | "cancelled";

export interface Job {
  id: string;
  queue: string;
  data: unknown;
  status: JobStatus;
  attempt: number;
  maxAttempts: number;
  agentId?: string;
  /** Задача закреплена за агентом. */
  pinnedAgentId?: string;
  progress: number;
  text?: string;
  log: string[];
  events: { seq: number; type: string; data?: any; at: number }[];
  result?: any;
  error?: { code: string; message: string };
  stopRequested: boolean;
  outputs: string[];
  files: string[];
  createdAt: number;
  finishedAt?: number;
  /** Кто поставил (X-Actor). */
  actor?: string;
}

export type CommandStatus = "pending" | "running" | "succeeded" | "failed" | "cancelled";

export interface Command {
  id: string;
  agentId: string;
  name: string;
  args?: unknown;
  timeoutSec: number;
  status: CommandStatus;
  output: string;
  result?: any;
  error?: { code: string; message: string };
  createdAt: number;
  /** Кто отправил (X-Actor). */
  actor?: string;
}

export interface DesiredState {
  domain: string;
  /** Пусто — общий снимок; иначе — для этого агента (важнее общего). */
  agentId?: string;
  version: number;
  spec: unknown;
  updatedAt: number;
  /** Кто задал (X-Actor). */
  actor?: string;
}

export interface AgentEvent {
  agentId: string;
  agentName: string;
  source: string;
  type: string;
  data?: any;
  at: number;
}

/** Уведомление о проблеме: active — началась, иначе закончилась. */
export interface Alert {
  type: "offline" | "stateFailed" | "workerDown" | "workerDegraded" | "degraded" | string;
  agentId: string;
  agentName: string;
  active: boolean;
  message: string;
  at: number;
  domain?: string;
  worker?: string;
}

/** Запись журнала аудита (поток /api/ws, сервер её не хранит). */
export interface AuditEntry {
  at: number;
  actor: string;
  action: string;
  agentId?: string;
  target: string;
  details?: Record<string, unknown>;
}

export interface Snapshot {
  serverTime: number;
  agents: Agent[];
  jobs: Job[];
  commands: Command[];
  states: DesiredState[];
  events: AgentEvent[];
  /** Активные уведомления о проблемах. */
  alerts: Alert[];
  /** Последние записи аудита из потока с открытия страницы (в снимке сервера их нет). */
  audit: AuditEntry[];
}

export const emptySnapshot: Snapshot = {
  serverTime: 0,
  agents: [],
  jobs: [],
  commands: [],
  states: [],
  events: [],
  alerts: [],
  audit: [],
};
