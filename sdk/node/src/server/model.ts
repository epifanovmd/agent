// Модель сервера (одинаковые поля во всех SDK, JSON — camelCase).
import type { Capabilities, Hello, Inventory, LogEntryLevel, Metrics, StateApplied, Status } from "../index";

export interface Agent {
  id: string;
  name: string;
  labels: Record<string, string>;
  /** На связи; после обрыва — ещё offlineGraceMs (переподключение не мигает). */
  online: boolean;
  /** Отозван: учётные данные не принимаются, нужна новая регистрация. */
  revoked: boolean;
  transport?: "ws" | "http";
  /** Адрес (IP без порта), с которого пришло последнее подключение; за прокси — опция trustProxy. */
  address?: string;
  enrolledAt: number;
  lastSeenAt?: number;
  hello?: Hello;
  capabilities?: Capabilities;
  status?: Status;
  /** Последняя точка метрик (не backfill). */
  metrics?: Metrics;
  /** Последнее inventory агента. */
  inventory?: Inventory;
  /** Домен → последний state.applied. */
  stateApplied: Record<string, StateApplied>;
  /**
   * Подписки (Agents.subscribe): что присылать чаще и подробнее, до какого момента (мс).
   * Общее для всех процессов бэкенда; сводная из действующих уходит агенту (config.subscription).
   */
  subscriptions?: AgentSubscription[];
}

/** Подписка в записи агента (Agents.subscribe). */
export interface AgentSubscription {
  id: string;
  /** До какого момента действует, мс. */
  until: number;
  /** Частота статуса. */
  status?: { intervalMs: number };
  /** Частота метрик и группы метрик узла сверх настройки агента. */
  metrics?: { intervalMs?: number; groups?: string[] };
  /** С какого уровня слать лог. */
  logs?: { level: LogEntryLevel };
  /** Канал показателей воркера → частота. */
  channels?: Record<string, { intervalMs: number }>;
}

/** Агент в хранилище: секрет — только sha256; служебное — для потока seq. */
export interface AgentRecord extends Omit<Agent, "revoked"> {
  /** Отозван; нет поля — не отозван (наружу всегда отдаётся revoked: boolean). */
  revoked?: boolean;
  secretHash: string;
  /** sha256 нового секрета после agent.rotateKey: принят при входе — становится secretHash. */
  pendingSecretHash?: string;
  /** bootId текущего запуска агента: начало нумерации seq. */
  bootId?: string;
  /** Последний принятый seq потока в пределах bootId. */
  lastSeq: number;
  /**
   * Метки, выданные бэкендом при регистрации (opts.enroll): при каждом hello
   * labels = { ...hello.labels, ...grantedLabels } — выданные узел переписать
   * не может. Нет поля — выданных меток нет.
   */
  grantedLabels?: Record<string, string>;
}

/** Точка истории метрик. */
export interface MetricsPoint {
  /** Время точки по часам сервера, мс: collectedAt + clockOffsetMs; без смещения — время получения. */
  at: number;
  /** Собрана без связи и дослана: по ней решений не принимают. */
  backfill: boolean;
  /** Сообщение metrics целиком (интерфейсы, GPU, каналы воркеров). */
  metrics: Metrics;
}

/** Сборка агента в манифесте выпуска (§7). */
export interface ReleaseArtifact {
  os: string;
  arch: string;
  file: string;
  sha256: string;
  signature?: string;
}

/** Сборка воркера в манифесте выпуска: её ставит install.sh --worker и обновляет worker.update. */
export interface WorkerArtifact {
  name: string;
  version: string;
  os: string;
  arch: string;
  file: string;
  sha256: string;
  signature?: string;
  /** Способ замены по умолчанию для записи в agent.yaml при установке: rolling | stop-first. */
  restart?: string;
  /** Срок остановки по умолчанию для записи в agent.yaml при установке (например "30s"). */
  stopTimeout?: string;
}

/** manifest.json каталога выпуска. */
export interface ReleaseManifest {
  version: string;
  artifacts: ReleaseArtifact[];
  /** Сборки воркеров (необязательно). */
  workers?: WorkerArtifact[];
}

/** Кандидат на обновление: агент self, версия не как в манифесте, сборка есть. */
export interface UpdateCandidate {
  agentId: string;
  name: string;
  online: boolean;
  current: string;
  target: string;
  os: string;
  arch: string;
}

/** Кандидат на обновление воркера: воркер из выпуска (release), версия не как в манифесте, сборка есть. */
export interface WorkerUpdateCandidate {
  agentId: string;
  agentName: string;
  online: boolean;
  worker: string;
  /** Версия воркера на узле (из worker.register); пусто — неизвестна. */
  current: string;
  target: string;
  os: string;
  arch: string;
}

export type JobStatus = "queued" | "running" | "completed" | "failed" | "cancelled";

export interface JobEventRecord {
  seq: number;
  type: string;
  data?: unknown;
  at: number;
}

export interface Job {
  id: string;
  queue: string;
  data: unknown;
  status: JobStatus;
  /** Номер попытки с 0. */
  attempt: number;
  maxAttempts: number;
  leaseSeconds: number;
  /** Агент текущей попытки. */
  agentId?: string;
  /** Задача закреплена за агентом: раздаётся только ему. */
  pinnedAgentId?: string;
  /** Агент подтвердил job.accept. */
  accepted: boolean;
  progress: number;
  text?: string;
  /** Последние 500 строк. */
  log: string[];
  events: JobEventRecord[];
  result?: unknown;
  error?: { code: string; message: string };
  stopRequested: boolean;
  /** Имена входных файлов. */
  inputs: string[];
  /** Имена выходных файлов. */
  outputs: string[];
  createdAt: number;
  finishedAt?: number;
  /** Кто поставил (agents.by(actor)); пусто — не указано. */
  actor?: string;
}

/** Задача в хранилище: срок аренды и номер последнего принятого события. */
export interface JobRecord extends Job {
  leaseUntil: number;
  eventSeq: number;
}

export type CommandStatus = "pending" | "running" | "succeeded" | "failed";

export interface Command {
  id: string;
  agentId: string;
  name: string;
  args?: unknown;
  timeoutSec: number;
  status: CommandStatus;
  /** Последние 256 КБ вывода. */
  output: string;
  result?: unknown;
  error?: { code: string; message: string };
  /** Код выхода из cmd.done (команды, запускающие процесс). */
  exitCode?: number;
  createdAt: number;
  finishedAt?: number;
  /** Кто отправил (agents.by(actor)); пусто — не указано. */
  actor?: string;
}

export interface DesiredState {
  domain: string;
  /** Пусто — общий снимок домена; иначе — для этого агента (важнее общего). */
  agentId?: string;
  version: number;
  spec: unknown;
  updatedAt: number;
  /** Кто задал (agents.by(actor)); пусто — не указано. */
  actor?: string;
}

/** Изменяющее действие (журнал аудита). */
export type AuditAction =
  | "job.enqueue"
  | "job.cancel"
  | "job.stop"
  | "command"
  | "state.set"
  | "state.delete"
  | "state.rollback"
  | "agent.revoke"
  | "agent.update"
  | "agent.rotateKey"
  | "worker.update"
  | "worker.pause"
  | "worker.resume";

/** Запись аудита (событие audit): кто, что и над чем сделал. SDK её не хранит. */
export interface AuditEntry {
  at: number;
  /** agents.by(actor); без by — пусто. */
  actor: string;
  action: AuditAction;
  agentId?: string;
  /** id задачи или команды, раздел состояния, id агента. */
  target: string;
  details?: Record<string, unknown>;
}

export type AlertType = "offline" | "stateFailed" | "workerDown" | "workerDegraded" | "degraded";

/** Уведомление о проблеме (событие alert): active — началась (true) или закончилась (false). */
export interface Alert {
  type: AlertType;
  agentId: string;
  agentName: string;
  active: boolean;
  /** Текст проблемы; в конце (active: false) — тот же, что в начале. */
  message: string;
  at: number;
  /** stateFailed — раздел. */
  domain?: string;
  /** workerDown, workerDegraded — воркер. */
  worker?: string;
}

export interface AgentEvent {
  agentId: string;
  agentName: string;
  /** Имя воркера или "agent". */
  source: string;
  type: string;
  data?: unknown;
  at: number;
}

export interface JobRequest {
  queue: string;
  data?: unknown;
  /** Попыток всего (по умолчанию 1). */
  maxAttempts?: number;
  /** Аренда — допустимое время без связи, с (по умолчанию 60). */
  leaseSeconds?: number;
  /** Входные файлы: имя → содержимое или URL (по провайдеру файлов). */
  inputs?: Record<string, string | Uint8Array>;
  /** Имена выходных файлов. */
  outputs?: string[];
  /** Закрепить задачу за агентом. */
  agentId?: string;
}

export interface CommandRequest {
  name: string;
  args?: unknown;
  /** Срок, с (по умолчанию 60). */
  timeoutSec?: number;
  /** Агент; пусто — агент на связи, объявивший команду. */
  agentId?: string;
}

export type ChangeKind = "agent" | "job" | "command" | "state" | "event";

/** Что изменилось; подробности — чтением. */
export interface Change {
  kind: ChangeKind;
  id: string;
}

export interface JobFilter {
  status?: JobStatus | JobStatus[];
  queue?: string;
  agentId?: string;
}

export interface CommandFilter {
  status?: CommandStatus | CommandStatus[];
  agentId?: string;
}

/** Ошибка API приложения: код ошибки и HTTP-статус для ответа. */
export class AgentsError extends Error {
  readonly code: string;
  readonly status: number;
  /** Через сколько секунд повторить (429 — заголовок Retry-After). */
  readonly retryAfterSec?: number;
  constructor(code: string, message: string, status = 400, retryAfterSec?: number) {
    super(message);
    this.name = "AgentsError";
    this.code = code;
    this.status = status;
    if (retryAfterSec !== undefined) this.retryAfterSec = retryAfterSec;
  }
}

export function publicAgent(a: AgentRecord): Agent {
  const { secretHash, pendingSecretHash, bootId, lastSeq, grantedLabels, ...agent } = a;
  return { ...agent, revoked: agent.revoked ?? false };
}

/**
 * Метки агента после hello: выданные при регистрации поверх меток из hello
 * (иначе узел выдал бы себя за другой).
 */
export function helloLabels(
  a: Pick<AgentRecord, "grantedLabels">,
  labels: Record<string, string> | undefined,
): Record<string, string> {
  return { ...labels, ...a.grantedLabels };
}

export function publicJob(j: JobRecord): Job {
  const { leaseUntil, eventSeq, ...job } = j;
  return job;
}
