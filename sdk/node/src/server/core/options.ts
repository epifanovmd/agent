// Настройки Agents: что задаёт бэкенд и значения по умолчанию.
import { format } from "node:util";

import type { AgentEvent, WorkerRequest } from "../model/types";
import type { Store } from "../store/store";
import { invalid } from "./errors";

/** Сведения из запроса регистрации — для хука enroll. */
export interface EnrollInfo {
  name: string;
  labels: Record<string, string>;
  host: {
    os?: string;
    arch?: string;
    hostname?: string;
    [key: string]: unknown;
  };
  /** IP клиента (с trustProxy — из X-Forwarded-For). */
  address: string;
}

/** Решение хука enroll: true или объект — принять (labels — метки агента от сервера), иначе — отказ. */
export type EnrollVerdict =
  | boolean
  | null
  | undefined
  | { labels?: Record<string, string>; name?: string };

export interface AgentsOptions {
  /** Токен регистрации агентов (или своя проверка — enroll). */
  enrollToken?: string;
  /** Своя проверка токена регистрации. */
  enroll?: (
    token: string,
    info: EnrollInfo,
  ) => EnrollVerdict | Promise<EnrollVerdict>;
  /** Хранилище (по умолчанию MemoryStore). */
  store?: Store;
  /** Имя этого процесса бэкенда в Agent.session.instance (по умолчанию случайное). */
  instanceId?: string;
  /** welcome.statusIntervalMs (по умолчанию 30 000). */
  statusIntervalMs?: number;
  /** welcome.metricsIntervalMs (по умолчанию 10 000). */
  metricsIntervalMs?: number;
  /**
   * Событие воркера: Agents ждёт обработчик и только после успеха подтверждает событие агенту;
   * ошибка — без подтверждения, агент пришлёт событие снова (с тем же event.id). Без обработчика
   * событие подтверждается сразу и передаётся только как событие event.
   */
  onEvent?: (event: AgentEvent) => void | Promise<void>;
  /**
   * Запрос воркера к серверу (§12): результат уходит воркеру как data ответа. Ошибка — отказ
   * воркеру: у AgentsError — её code и message, у другой — REQUEST_FAILED. Нет обработчика —
   * отказ REQUEST_UNHANDLED. Запрос обрабатывает процесс, с которым агент на связи.
   */
  onWorkerRequest?: (req: WorkerRequest) => unknown;
  /**
   * Агент после обрыва связи остаётся online столько, мс (по умолчанию 3000): переподключение за
   * это время не считается потерей связи.
   */
  offlineGraceMs?: number;
  /**
   * Ping агенту раз в столько мс (по умолчанию 5000); на два ping подряд нет pong — соединение
   * закрывается (обрыв без закрытия замечается за 10–15 с).
   */
  pingIntervalMs?: number;
  /**
   * Несколько процессов: агент online, но без сессии в этом процессе и без вестей дольше — offline
   * (процесс с его сессией упал). По умолчанию max(3 × statusIntervalMs, 30 000) + offlineGraceMs.
   */
  offlineAfterMs?: number;
  /** Срок ответа на действие, мс (по умолчанию 60 000). */
  actionTimeoutMs?: number;
  /** Срок ответа на worker.update и agent.update, мс (по умолчанию 300 000). */
  updateTimeoutMs?: number;
  /**
   * Каталог сборок воркеров проекта: manifest.json от `agent manifest` и сборки. Узлы берут их
   * по `GET /api/v1/agent-link/releases/…`, updateWorker — сборку из него.
   */
  releasesDir?: string;
  /** Неудачных регистраций с одного адреса за окно, после которых — 429 (по умолчанию 10; 0 — без предела). */
  enrollFailureLimit?: number;
  /** Окно подсчёта неудачных регистраций, мс (по умолчанию 60 000). */
  enrollFailureWindowMs?: number;
  /**
   * Сервер за доверенным прокси: адрес агента — первый из X-Forwarded-For, адрес сервера — из
   * X-Forwarded-Host и X-Forwarded-Proto. По умолчанию false: адрес сокета и Host.
   */
  trustProxy?: boolean;
  log?: (msg: string, extra?: Record<string, unknown>) => void;
}

/** Внутренние сроки (тесты уменьшают). */
export const agentsDefaults = {
  sweepIntervalMs: 1000,
  /** Запас к сроку fetch на стороне сервера, мс. */
  fetchSlackMs: 5000,
};

type Optional =
  "enroll" | "enrollToken" | "onEvent" | "onWorkerRequest" | "releasesDir";

/** Журнал по умолчанию — строки в stdout. */
const stdoutLog = (msg: string, extra?: Record<string, unknown>) => {
  process.stdout.write(
    `${format("agents: %s", msg, ...(extra ? [extra] : []))}\n`,
  );
};

/** Настройки со значениями по умолчанию (store и instanceId — отдельно, у Agents). */
export type Settings = Required<
  Omit<AgentsOptions, Optional | "store" | "instanceId">
> &
  Pick<AgentsOptions, Optional>;

/** Имена опций — по типам: новая опция без записи здесь не соберётся. */
const OPTION_KEYS = {
  enrollToken: true,
  enroll: true,
  store: true,
  instanceId: true,
  statusIntervalMs: true,
  metricsIntervalMs: true,
  onEvent: true,
  onWorkerRequest: true,
  offlineGraceMs: true,
  pingIntervalMs: true,
  offlineAfterMs: true,
  actionTimeoutMs: true,
  updateTimeoutMs: true,
  releasesDir: true,
  enrollFailureLimit: true,
  enrollFailureWindowMs: true,
  trustProxy: true,
  log: true,
} satisfies Record<keyof AgentsOptions, true>;

/**
 * Незнакомая опция — ошибка при создании Agents: опечатка или имя, которого у SDK нет, иначе
 * молча не действовали бы.
 */
const checkKeys = (opts: AgentsOptions): void => {
  const unknown = Object.keys(opts).filter(k => !Object.hasOwn(OPTION_KEYS, k));

  if (unknown.length)
    throw invalid(`Agents: неизвестная опция ${unknown.join(", ")}`);
};

export const resolveOptions = (opts: AgentsOptions): Settings => {
  checkKeys(opts);
  const statusIntervalMs = opts.statusIntervalMs ?? 30_000;
  const offlineGraceMs = opts.offlineGraceMs ?? 3000;

  return {
    enrollToken: opts.enrollToken,
    enroll: opts.enroll,
    statusIntervalMs,
    metricsIntervalMs: opts.metricsIntervalMs ?? 10_000,
    onEvent: opts.onEvent,
    onWorkerRequest: opts.onWorkerRequest,
    offlineGraceMs,
    pingIntervalMs: opts.pingIntervalMs ?? 5000,
    offlineAfterMs:
      opts.offlineAfterMs ??
      Math.max(3 * statusIntervalMs, 30_000) + offlineGraceMs,
    actionTimeoutMs: opts.actionTimeoutMs ?? 60_000,
    updateTimeoutMs: opts.updateTimeoutMs ?? 300_000,
    releasesDir: opts.releasesDir,
    enrollFailureLimit: opts.enrollFailureLimit ?? 10,
    enrollFailureWindowMs: opts.enrollFailureWindowMs ?? 60_000,
    trustProxy: opts.trustProxy ?? false,
    log: opts.log ?? stdoutLog,
  };
};
