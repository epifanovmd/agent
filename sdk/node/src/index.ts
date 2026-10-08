// Сообщения агента: константы и типы (нормативный документ —
// sdk/spec/README.md). Типы — то, что нужно SDK; неизвестные поля
// сохраняются и игнорируются (§4, §8).
import { randomUUID } from "node:crypto";

/** Версия SDK: уходит в `worker.register.sdk` как `node/<версия>`. */
export const SDK_VERSION = "1.1.0";
/** Версия формата сообщений, которую знает SDK. */
export const MESSAGE_VERSION = 1;

export const LINK_PATH = "/api/v1/agent-link";
export const SYNC_PATH = "/api/v1/agent-link/sync";
export const ENROLL_PATH = "/api/v1/agent-link/enroll";
export const WS_CHANNEL = "agent.v1";
/** Раздача релизов агента (§7): манифест и сборки. */
export const RELEASES_PATH = "/api/v1/agent-link/releases";
/** Установщик агента (§7): адрес сервера и ключ подставлены. */
export const INSTALL_PATH = "/api/v1/agent-link/install.sh";

/** Коды закрытия WebSocket (§2.1). */
export const Close = {
  Normal: 1000,
  GoingAway: 1001,
  Restart: 1012,
  Invalid: 4400,
  Unauthorized: 4401,
  Unsupported: 4409,
  Replaced: 4410,
  Overloaded: 4429,
} as const;

/** Конверт сообщения (§4). */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- data задаётся типом сообщения; any — умолчание публичного API
export interface Envelope<T = any> {
  type: string;
  id?: string;
  re?: string;
  seq?: number;
  ts?: number;
  data?: T;
}

export interface QueueCapacity {
  name: string;
  concurrency: number;
}

export interface Capabilities {
  jobs?: { queues: QueueCapacity[] };
  commands?: { names: string[] };
  /** Домен → применённая версия (null — не применялся). */
  state?: { domains: Record<string, number | null> };
  telemetry?: { channels: string[] };
  update?: { mode: string };
  [key: string]: unknown;
}

export interface JobRef {
  jobId: string;
  attempt: number;
}

export interface Hello {
  versions: number[];
  agent: {
    name: string;
    version: string;
    sdk?: string;
    codeHash?: string;
    bootId: string;
    startedAt: number;
    /** Открытый ключ X25519 агента (base64, 32 байта): им сервер запечатывает секреты состояния. */
    encryptionKey?: string;
  };
  host: {
    hostname: string;
    os: string;
    arch: string;
    platform?: string;
    kernel?: string;
    cpus?: number;
    memoryBytes?: number;
  };
  labels?: Record<string, string>;
  capabilities: Capabilities;
  jobs: JobRef[];
}

export interface Welcome {
  version: number;
  agentId: string;
  sessionId: string;
  serverTime: number;
  config: {
    statusIntervalMs?: number;
    metricsIntervalMs?: number;
    /**
     * Сводная подписка сервера: в welcome — текущая (нет поля — подписок нет); в config —
     * заменить целиком ({} — подписок нет, нет поля — не менять).
     */
    subscription?: Subscription;
    [key: string]: unknown;
  };
}

/**
 * Сводная подписка (welcome.config, config): что сервер сейчас просит присылать чаще и
 * подробнее обычного. Агент ужесточает ей свои настройки, не ослабляет их. Нет поля —
 * подписка его не касается.
 */
export interface Subscription {
  /** Частота статуса, мс (минимум по подпискам). */
  statusIntervalMs?: number;
  /** Частота метрик, мс (минимум по подпискам). */
  metricsIntervalMs?: number;
  /** Группы метрик узла (METRIC_GROUPS) сверх telemetry.metrics агента. */
  metrics?: string[];
  /** С какого уровня слать лог, если подробнее log.forward агента. */
  logLevel?: LogEntryLevel;
  /** Канал показателей воркера → частота, мс (минимум по подпискам). */
  channels?: Record<string, number>;
}

/** Уровень записи лога. */
export type LogEntryLevel = "debug" | "info" | "warn" | "error";

/** Запись лога агента или воркера (сообщение log). */
export interface LogEntry {
  /** Момент записи (часы агента), мс. */
  at: number;
  level: LogEntryLevel;
  /** "agent" или имя воркера. */
  source: string;
  msg: string;
  attrs?: Record<string, unknown>;
}

/** log (A→S, поток — seq, ack{seq}): пачка записей лога. */
export interface Log {
  entries: LogEntry[];
}

export interface Status {
  state: "starting" | "idle" | "busy" | "draining" | "updating" | "degraded" | string;
  message?: string;
  /** Свободные места по очередям. */
  slots: Record<string, number>;
  capacity?: Record<string, number>;
  jobs: (JobRef & { queue: string; startedAt?: number })[];
  /** release — исполняемый файл воркера ведёт агент (выпуск, команда worker.update). */
  workers: StatusWorker[];
  outbox: number;
}

/** Воркер в status.workers. */
export interface StatusWorker {
  name: string;
  state: string;
  instances: number;
  version?: string;
  /** Исполняемый файл воркера ведёт агент (выпуск, команда worker.update). */
  release?: boolean;
  /** Что воркер сам сообщил (worker.health): degraded — не в порядке. */
  health?: "ok" | "degraded" | string;
  /** Причина degraded (worker.health.message). */
  message?: string;
  /** Хоть одна очередь воркера на паузе (воркером или сервером). */
  paused?: boolean;
}

/** Сеть по интерфейсу (metrics.host.interfaces, группа interfaces). */
export interface InterfaceMetrics {
  name: string;
  rxBps: number;
  txBps: number;
  /** Ошибки и отброшенные пакеты за интервал. */
  errors?: number;
  drops?: number;
}

/** Точка монтирования (metrics.host.disks, группа disk, настройка telemetry.disks). */
export interface DiskMetrics {
  mount: string;
  usedBytes: number;
  totalBytes: number;
  inodesUsed?: number;
  inodesTotal?: number;
}

/** TCP-соединения по состояниям (группа sockets). */
export interface TcpMetrics {
  established?: number;
  timeWait?: number;
  closeWait?: number;
  listen?: number;
}

/** Температуры (группа temperatures): максимум и датчики, где они доступны. */
export interface TemperatureMetrics {
  maxC?: number;
  sensors?: { name: string; c: number }[];
}

/**
 * metrics.host: поля по группам настройки агента telemetry.metrics (и групп подписки сервера);
 * нет группы — нет её полей.
 */
export interface HostMetrics {
  // cpu
  cpuPercent?: number;
  /** По ядрам — если в настройке есть cpu.cores. */
  cpuCores?: number[];
  // load
  load1?: number;
  load5?: number;
  load15?: number;
  // memory
  memUsedBytes?: number;
  memTotalBytes?: number;
  memAvailableBytes?: number;
  // swap
  swapUsedBytes?: number;
  swapTotalBytes?: number;
  // disk: корень «/» и точки монтирования по telemetry.disks
  diskUsedBytes?: number;
  diskTotalBytes?: number;
  disks?: DiskMetrics[];
  // diskio: сумма по физическим дискам
  diskReadBps?: number;
  diskWriteBps?: number;
  diskReadIops?: number;
  diskWriteIops?: number;
  // network: за интервал, сумма по физическим интерфейсам
  netRxBps?: number;
  netTxBps?: number;
  netErrors?: number;
  netDrops?: number;
  // interfaces
  interfaces?: InterfaceMetrics[];
  // conntrack (Linux)
  conntrack?: number;
  conntrackMax?: number;
  // sockets
  tcp?: TcpMetrics;
  // processes
  processes?: number;
  threads?: number;
  // fds (Linux)
  fdsOpen?: number;
  fdsMax?: number;
  // uptime
  uptimeSec?: number;
  // temperatures
  temperatures?: TemperatureMetrics;
  [key: string]: unknown;
}

/** Группы метрик узла (telemetry.metrics агента, Subscription.metrics). */
export const METRIC_GROUPS = [
  "cpu",
  "load",
  "memory",
  "swap",
  "disk",
  "diskio",
  "network",
  "interfaces",
  "conntrack",
  "sockets",
  "processes",
  "fds",
  "uptime",
  "temperatures",
] as const;
export type MetricGroup = (typeof METRIC_GROUPS)[number];

/** metrics (§6.2). */
export interface Metrics {
  /** Момент сбора (часы агента). */
  collectedAt: number;
  /** Часы сервера − часы агента, мс (по welcome.serverTime): время точки = collectedAt + clockOffsetMs. */
  clockOffsetMs?: number;
  /** Точка собрана без связи и дослана после подключения. */
  backfill?: boolean;
  host?: HostMetrics;
  gpus?: Record<string, unknown>[];
  channels?: Record<string, unknown>;
}

/** inventory (§6.2): сведения об узле, которые меняются редко. */
export interface Inventory {
  collectedAt: number;
  os?: { hostname?: string; platform?: string; kernel?: string; arch?: string; virtualization?: string };
  cpu?: { model?: string; cores?: number; threads?: number };
  memoryBytes?: number;
  disks?: { mount: string; fs?: string; totalBytes?: number }[];
  interfaces?: { name: string; mac?: string; addresses?: string[] }[];
  gpus?: { index: number; name?: string; memoryBytes?: number }[];
  ports?: { tcp?: number[]; udp?: number[] };
  [key: string]: unknown;
}

/** Ссылка на выходной файл: PUT ровно с этим Content-Type. */
export interface OutputUrl {
  url: string;
  contentType?: string;
}

/** job.assign (§6.3). */
export interface JobAssign extends JobRef {
  queue: string;
  data?: unknown;
  leaseSeconds: number;
  inputs?: Record<string, string>;
  outputs?: Record<string, OutputUrl>;
  urlsExpireAt?: number;
}

/** Ответ job.urls (§6.3). */
export interface JobUrls {
  inputs: Record<string, string>;
  outputs: Record<string, OutputUrl>;
  expiresAt: number;
}

export interface JobProgress extends JobRef {
  progress?: number;
  text?: string;
  log?: string[];
}

export interface JobFail extends JobRef {
  code: string;
  message: string;
  retryable: boolean;
}

/** cmd.run (§6.4). */
export interface CommandRun {
  commandId: string;
  name: string;
  args?: unknown;
  timeoutSec: number;
}

export interface CommandDone {
  commandId: string;
  ok: boolean;
  exitCode?: number;
  result?: unknown;
  error?: { code: string; message: string };
}

/** state.put (§6.5). */
export interface StatePut {
  domain: string;
  version: number;
  spec: unknown;
}

export interface StateApplied {
  domain: string;
  version: number;
  ok: boolean;
  error?: string;
  report?: unknown;
}

export interface MessageError {
  code: string;
  message: string;
  retryable: boolean;
}

/** worker.register (§10). */
export interface WorkerRegister {
  name: string;
  version?: string;
  sdk?: string;
  queues: QueueCapacity[];
  commands?: string[];
  domains?: string[];
  channels?: string[];
  /** Воркер отвечает на worker.ping: агент проверяет, не завис ли он. */
  ping?: boolean;
}

/** worker.ready (§10). */
export interface WorkerReady {
  agentVersion: string;
  rejected?: string[];
}

/** worker.cleaned (§10): ответ на worker.cleanup (re = id запроса); шлёт только `agent cleanup`. */
export interface WorkerCleaned {
  ok: boolean;
  error?: string;
}

/** worker.context (агент → воркер, §10): что воркеру знать об агенте; шлётся после worker.ready и при изменении. */
export interface WorkerContext {
  /** cleanup — воркер запущен командой `agent cleanup` только для уборки. */
  mode: "run" | "cleanup" | string;
  /** id пустой, пока агент не зарегистрирован. */
  agent: { id: string; name: string; version: string; labels: Record<string, string> };
  /** Есть ли у агента связь с сервером. */
  online: boolean;
  /** Действующая частота метрик агента (с учётом подписки); 0 — неизвестна. */
  metricsIntervalMs: number;
  /** Подписки на каналы показателей этого воркера: канал → частота, мс. Нет подписок — поля нет. */
  channels?: Record<string, number>;
  /** Какие записи лога агент сейчас отправляет на сервер. */
  logLevel: string;
}

/** worker.health (воркер → агент): в порядке или нет (причина). */
export interface WorkerHealth {
  ok: boolean;
  message?: string;
}

/** worker.pause / worker.resume (воркер → агент): без queues — все очереди воркера. */
export interface WorkerPause {
  queues?: string[];
}

/** worker.restart (воркер → агент): заменить воркер штатно. */
export interface WorkerRestart {
  reason?: string;
}

/** Надёжные сообщения агента: подтверждаются ack{ids} (§4). */
export const RELIABLE: ReadonlySet<string> = new Set([
  "job.event",
  "job.complete",
  "job.fail",
  "job.reject",
  "cmd.done",
  "state.applied",
  "event",
]);

/** Потоковые сообщения агента: seq, подтверждаются ack{seq} (§4). */
export const STREAM: ReadonlySet<string> = new Set([
  "status",
  "metrics",
  "inventory",
  "capabilities",
  "job.accept",
  "job.progress",
  "cmd.accept",
  "cmd.output",
  "log",
]);

/**
 * Имя очереди, команды, раздела состояния или показателей (§6.7): латиница, цифры,
 * «.», «_», «-»; начало — буква или цифра; до 64 символов.
 */
export const NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$/;

/** Подходит ли имя под NAME_PATTERN. */
export function validName(name: unknown): name is string {
  return typeof name === "string" && NAME_PATTERN.test(name);
}

/** Код ошибки задачи или команды (§6.3). */
export const ERROR_CODE = /^[A-Z0-9_]{1,64}$/;

export function envelope<T>(type: string, data?: T, opts: { id?: string; re?: string } = {}): Envelope<T> {
  const env: Envelope<T> = { type, ts: Date.now() };
  if (opts.id) env.id = opts.id;
  if (opts.re) env.re = opts.re;
  if (data !== undefined) env.data = data;
  return env;
}

export const newId = (): string => randomUUID();

/**
 * Объединение возможностей (§6.1 capabilities): новые имена добавляются,
 * у домена — большая применённая версия; сужение — только новым hello.
 */
export function mergeCapabilities(cur: Capabilities | undefined, add: Capabilities): Capabilities {
  const out: Capabilities = { ...(cur ?? {}) };
  const union = (a: string[] = [], b: string[] = []) => [...new Set([...a, ...b])].sort();
  if (add.jobs) out.jobs = add.jobs;
  if (add.commands) out.commands = { names: union(out.commands?.names, add.commands.names) };
  if (add.telemetry) out.telemetry = { channels: union(out.telemetry?.channels, add.telemetry.channels) };
  if (add.state) {
    const domains = { ...(out.state?.domains ?? {}) };
    for (const [domain, version] of Object.entries(add.state.domains ?? {})) {
      const prev = domains[domain];
      if (!(domain in domains) || prev == null || (version != null && version > prev)) domains[domain] = version;
    }
    out.state = { domains };
  }
  if (add.update) out.update = add.update;
  return out;
}
