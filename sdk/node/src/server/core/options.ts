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

/** Методы SDK, которые пересылаются в процесс с сессией агента. */
export const RELAY_METHODS = [
  "fetch",
  "restartWorker",
  "updateWorker",
  "updateAgent",
  "rotateKey",
  "logs",
  "watch",
  "unwatch",
  "runJob",
] as const;

export type RelayMethod = (typeof RELAY_METHODS)[number];

/** Пересылаемый вызов: метод SDK, агент, от чьего имени (by) и аргументы — всё JSON. */
export interface RelayCall {
  method: RelayMethod;
  agentId: string;
  /** Кто действует (agents.by); пусто — без by. */
  actor?: string;
  args: unknown[];
}

/**
 * Вызов, который нужно доставить в процесс с сессией агента: передать body и headers как тело и
 * заголовки POST-запроса на маршрут, где тот процесс вызывает handleRelay, и вернуть его ответ.
 */
export interface RelayRequest {
  /** Что вызывается (для журнала и своей маршрутизации); то же, что в body. */
  call: RelayCall;
  /** Тело запроса (JSON вызова). */
  body: string;
  /** Заголовки запроса: content-type и секрет (relaySecret). */
  headers: Record<string, string>;
  /** Отмена: вызывающий отменил вызов или истёк срок — прервать запрос. */
  signal: AbortSignal;
}

/** Доставка вызова в процесс instanceId: ответ его handleRelay как есть (тело — потоком). */
export type RelayFunction = (
  instanceId: string,
  request: RelayRequest,
) => Promise<Response>;

/** Общее для удалённых источников сборок агента. */
interface AgentReleasesCommon {
  /** Как часто проверять новую версию, мс (по умолчанию 3 600 000 — 1 ч); первая проверка — при старте. */
  checkIntervalMs?: number;
  /**
   * Сборки из удалённого источника отдавать узлам через бэкенд потоком (узлам без доступа к
   * GitHub). По умолчанию false: `GET …/releases/<file>` отвечает 302 на ссылку источника.
   */
  proxy?: boolean;
  /** Своя функция fetch (тесты, прокси); по умолчанию — глобальная. */
  fetch?: typeof globalThis.fetch;
}

/**
 * Удалённый источник сборок агента: релизы GitHub (github) или постоянная база сборок (url —
 * каталог с manifest.json, например `https://github.com/<owner>/<repo>/releases/download/v1.1.0`).
 */
export type AgentReleasesOptions = AgentReleasesCommon &
  (
    | {
        /** Репозиторий `owner/repo`: релизы без prerelease и draft. */
        github: string;
        /** Диапазон версий semver (по умолчанию `^<мажорная версия SDK>`, например `^1`). */
        range?: string;
        /** Токен GitHub для API (лимиты запросов). */
        token?: string;
        /** Адрес API GitHub (по умолчанию https://api.github.com). */
        apiUrl?: string;
      }
    | {
        /** База сборок: `<url>/manifest.json`, `<url>/<file>`. */
        url: string;
      }
  );

/** Что проверять по схемам манифеста воркера (опция validate). */
export interface ValidateOptions {
  /** Значение setConfig по configs[].schema: не подходит — CONFIG_INVALID (400). */
  configs?: boolean;
  /** data runJob по jobs[].schema: не подходит — JOB_INVALID (400). */
  jobs?: boolean;
  /**
   * Тело fetch по routes[].request (не подходит или не JSON — REQUEST_INVALID, 400) и data
   * запроса воркера по requests[].schema (не подходит — отказ воркеру REQUEST_INVALID).
   */
  requests?: boolean;
  /**
   * data события по events[].schema: off — нет; log — журнал, invalidEvent и problems в
   * событии, событие обрабатывается как обычно; reject — журнал и invalidEvent, событие
   * подтверждается агенту (повтор доставки ничего не исправит), но onEvent и событие event его
   * не получают.
   */
  events?: "off" | "log" | "reject";
}

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
   * Несколько процессов: доставить вызов (fetch, действия, logs, watch, runJob) в процесс с
   * сессией агента. Нет — такие вызовы в другом процессе — AGENT_ELSEWHERE.
   */
  relay?: RelayFunction;
  /**
   * Общий секрет пересылки: отправляется в заголовке x-agents-relay-secret, handleRelay без него
   * отвечает 401. Нет — handleRelay доверяет любому вызывающему (закройте маршрут сетью).
   */
  relaySecret?: string;
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
   * Каталог сборок (manifest.json и сборки). С agentReleases — воркеры проекта:
   * manifest.json от `agent manifest` и их сборки; сборки агента из него не берутся, пока
   * получены удалённые сборки.
   */
  releasesDir?: string;
  /**
   * Откуда брать агента и его воркеры (netprobe): релизы GitHub (GitHub Releases) или база сборок по ссылке. SDK
   * сам следит за новыми версиями; итоговые сборки — удалённые плюс воркеры проекта из
   * releasesDir.
   */
  agentReleases?: AgentReleasesOptions;
  /** Неудачных регистраций с одного адреса за окно, после которых — 429 (по умолчанию 10; 0 — без предела). */
  enrollFailureLimit?: number;
  /** Окно подсчёта неудачных регистраций, мс (по умолчанию 60 000). */
  enrollFailureWindowMs?: number;
  /**
   * Сервер за доверенным прокси: адрес агента — первый из X-Forwarded-For, адрес сервера — из
   * X-Forwarded-Host и X-Forwarded-Proto. По умолчанию false: адрес сокета и Host.
   */
  trustProxy?: boolean;
  /**
   * Проверка значений по JSON Schema из манифеста воркера (§12); нет манифеста или схемы — без
   * проверки. По умолчанию всё выключено.
   */
  validate?: ValidateOptions;
  log?: (msg: string, extra?: Record<string, unknown>) => void;
}

/** Внутренние сроки (тесты уменьшают). */
export const agentsDefaults = {
  sweepIntervalMs: 1000,
  /** Запас к сроку fetch на стороне сервера, мс. */
  fetchSlackMs: 5000,
};

type Optional =
  | "enroll"
  | "enrollToken"
  | "onEvent"
  | "onWorkerRequest"
  | "releasesDir"
  | "agentReleases"
  | "relay"
  | "relaySecret";

/** Журнал по умолчанию — строки в stdout. */
const stdoutLog = (msg: string, extra?: Record<string, unknown>) => {
  process.stdout.write(
    `${format("agents: %s", msg, ...(extra ? [extra] : []))}\n`,
  );
};

/** Настройки со значениями по умолчанию (store и instanceId — отдельно, у Agents). */
export type Settings = Required<
  Omit<AgentsOptions, Optional | "store" | "instanceId" | "validate">
> &
  Pick<AgentsOptions, Optional> & { validate: Required<ValidateOptions> };

/** Ключи объекта со всеми ветками объединения. */
type KeysOf<T> = T extends unknown ? keyof T : never;

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
  relay: true,
  relaySecret: true,
  offlineAfterMs: true,
  actionTimeoutMs: true,
  updateTimeoutMs: true,
  releasesDir: true,
  agentReleases: true,
  enrollFailureLimit: true,
  enrollFailureWindowMs: true,
  trustProxy: true,
  validate: true,
  log: true,
} satisfies Record<keyof AgentsOptions, true>;

const VALIDATE_KEYS = {
  configs: true,
  jobs: true,
  requests: true,
  events: true,
} satisfies Record<keyof ValidateOptions, true>;

const RELEASES_KEYS = {
  checkIntervalMs: true,
  proxy: true,
  fetch: true,
  github: true,
  range: true,
  token: true,
  apiUrl: true,
  url: true,
} satisfies Record<KeysOf<AgentReleasesOptions>, true>;

/**
 * Незнакомая опция — ошибка при создании Agents: опечатка или имя, которого у SDK нет, иначе
 * молча не действовали бы.
 */
const checkKeys = (
  what: string,
  value: object | undefined,
  known: Record<string, true>,
): void => {
  const unknown = Object.keys(value ?? {}).filter(
    k => !Object.hasOwn(known, k),
  );

  if (unknown.length)
    throw invalid(
      `Agents: неизвестная опция ${unknown.map(k => what + k).join(", ")}`,
    );
};

export const resolveOptions = (opts: AgentsOptions): Settings => {
  checkKeys("", opts, OPTION_KEYS);
  checkKeys("validate.", opts.validate, VALIDATE_KEYS);
  checkKeys("agentReleases.", opts.agentReleases, RELEASES_KEYS);
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
    relay: opts.relay,
    relaySecret: opts.relaySecret,
    offlineAfterMs:
      opts.offlineAfterMs ??
      Math.max(3 * statusIntervalMs, 30_000) + offlineGraceMs,
    actionTimeoutMs: opts.actionTimeoutMs ?? 60_000,
    updateTimeoutMs: opts.updateTimeoutMs ?? 300_000,
    releasesDir: opts.releasesDir,
    agentReleases: opts.agentReleases,
    enrollFailureLimit: opts.enrollFailureLimit ?? 10,
    enrollFailureWindowMs: opts.enrollFailureWindowMs ?? 60_000,
    trustProxy: opts.trustProxy ?? false,
    validate: {
      configs: opts.validate?.configs ?? false,
      jobs: opts.validate?.jobs ?? false,
      requests: opts.validate?.requests ?? false,
      events: opts.validate?.events ?? "off",
    },
    log: opts.log ?? stdoutLog,
  };
};
