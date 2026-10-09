// Данные серверной части: записи Store, события и то, что видит бэкенд.
import type {
  ConfigReport,
  ErrorInfo,
  Hello,
  HelloWorker,
  LogLevel,
  Metrics,
  Status,
  WorkerStatus,
} from "../protocol/messages";

/** Наблюдатель `watch` (§9); живёт в памяти процесса. */
export interface Watcher {
  metricsIntervalMs?: number;
  logLevel?: LogLevel;
  /** До какого времени, мс Unix. */
  until: number;
}

/** Что агент сообщил о ключе настроек: версия на диске и итог её применения. */
export interface ConfigReported extends ConfigReport {
  /** Последняя версия, применённая успешно. */
  appliedVersion?: number;
  /** Когда пришло, мс. */
  at: number;
}

export type AlertType =
  | "offline"
  | "workerDown"
  | "workerInvalid"
  | "workerUnhealthy"
  | "configFailed";

/** Проблема агента: начинается и заканчивается сама по сообщениям агента. */
export interface Alert {
  /** Ключ проблемы в пределах агента: `offline`, `workerDown:<воркер>`, `workerInvalid:<воркер>`, `configFailed:<воркер>/<ключ>`. */
  key: string;
  agentId: string;
  agentName: string;
  type: AlertType;
  worker?: string;
  configKey?: string;
  message: string;
  /** С какого времени, мс. */
  since: number;
}

/** Сессия агента: какой процесс бэкенда держит соединение. */
export interface SessionInfo {
  id: string;
  /** `instanceId` объекта Agents, у которого соединение. */
  instance: string;
  since: number;
}

/**
 * Запись агента в Store: ключ, учёт потока и последнее, что агент сообщил (hello, status,
 * метрики, итоги настроек, проблемы) — чтобы getAgent работал из любого процесса. `rev` — версия
 * записи для условной записи (store.md); остальное Store хранит как есть (одним JSON).
 */
export interface AgentRecord {
  id: string;
  name: string;
  /** Метки, выданные при регистрации (хук enroll): сильнее меток из hello. */
  grantedLabels: Record<string, string>;
  labels: Record<string, string>;
  revoked: boolean;
  online: boolean;
  enrolledAt: number;
  secretHash: string;
  /** Хеш нового секрета после agent.rotateKey: принимается при входе, затем становится основным. */
  pendingSecretHash?: string;
  lastSeenAt?: number;
  connectedAt?: number;
  /** IP, с которого пришло последнее подключение. */
  address?: string;
  session?: SessionInfo;
  hello?: Hello;
  status?: Status;
  statusAt?: number;
  /** Последняя точка метрик. */
  metrics?: MetricsPoint;
  /** Учёт потока (§3): запуск агента и последний принятый seq. */
  bootId?: string;
  lastSeq: number;
  /** Что агент сообщил о настройках: воркер → ключ → итог. */
  configs: Record<string, Record<string, ConfigReported>>;
  /** Текущие проблемы. */
  alerts: Alert[];
  rev: number;
}

/** Воркер агента: из hello и последнего status. */
export interface AgentWorker extends Partial<WorkerStatus>, HelloWorker {
  name: string;
}

/** Агент, как его видит бэкенд. */
export interface Agent {
  id: string;
  name: string;
  labels: Record<string, string>;
  online: boolean;
  revoked: boolean;
  enrolledAt: number;
  lastSeenAt?: number;
  connectedAt?: number;
  address?: string;
  /** Версия агента и запуск. */
  version?: string;
  bootId?: string;
  startedAt?: number;
  host?: Hello["host"];
  workers: AgentWorker[];
  hello?: Hello;
  status?: Status;
  statusAt?: number;
  metrics?: MetricsPoint;
  alerts: Alert[];
  session?: SessionInfo;
}

/** Желаемые настройки ключа воркера на агенте. */
export interface ConfigRecord {
  agentId: string;
  worker: string;
  key: string;
  /** Растёт с каждым setConfig по этому ключу (и после deleteConfig). */
  version: number;
  data: unknown;
  updatedAt: number;
  actor?: string;
}

export type ConfigState =
  "pending" | "applying" | "applied" | "failed" | "deleting" | "deleted";

/** Статус ключа настроек: желаемая, доставленная, применённая версия. */
export interface ConfigStatus {
  agentId: string;
  worker: string;
  key: string;
  /** Желаемая версия; null — ключ удалён на сервере, агент ещё не удалил. */
  version: number | null;
  /** Версия на диске агента. */
  delivered?: number;
  /** Последняя версия, применённая воркером. */
  applied?: number;
  /**
   * pending — агенту ещё не доставлено; applying — доставлено, применяется; applied — применена
   * желаемая версия; failed — воркер отказал (error); deleting — удаляется; deleted — агент
   * подтвердил удаление (только в событии config).
   */
  state: ConfigState;
  error?: ErrorInfo;
  updatedAt?: number;
}

/** Событие воркера (опция onEvent и событие event). */
export interface AgentEvent {
  /** id сообщения агента: при повторной доставке тот же — по нему бэкенд отсекает повтор. */
  id: string;
  agentId: string;
  worker: string;
  type: string;
  data?: unknown;
  /** Когда случилось на узле, мс. */
  at: number;
  receivedAt: number;
}

/** Точка метрик. */
export interface MetricsPoint extends Metrics {
  /** collectedAt или время приёма. */
  at: number;
}

export type ActionName =
  | "worker.restart"
  | "worker.update"
  | "agent.update"
  | "agent.rotateKey"
  | "agent.logs";

/** Итог встроенного действия (§10): событие action. */
export interface ActionRecord {
  id: string;
  agentId: string;
  name: ActionName;
  args?: Record<string, unknown>;
  actor?: string;
  status: "done" | "failed";
  result?: unknown;
  error?: ErrorInfo;
  createdAt: number;
  finishedAt: number;
  /** Итог отложенной замены воркера (пришёл в action.done). */
  deferred?: true;
}

/** Агент, которого можно обновить до версии выпуска. */
export interface UpdateCandidate {
  agentId: string;
  name: string;
  online: boolean;
  current: string;
  target: string;
  os: string;
  arch: string;
}

/** Воркер из выпуска, которого можно обновить. */
export interface WorkerUpdateCandidate {
  agentId: string;
  agentName: string;
  online: boolean;
  worker: string;
  current: string;
  target: string;
  os: string;
  arch: string;
}

export type AuditAction =
  | "agent.enroll"
  | "agent.revoke"
  | "agent.delete"
  | "agent.rotateKey"
  | "agent.update"
  | "agent.logs"
  | "worker.restart"
  | "worker.update"
  | "config.set"
  | "config.delete"
  | "fetch";

/** Запись аудита: кто что сделал (agents.by(actor)). */
export interface AuditEntry {
  at: number;
  /** Пусто — без by(). */
  actor: string;
  action: AuditAction;
  agentId: string;
  details?: Record<string, unknown>;
}
