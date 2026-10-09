// Манифест воркера (§12): что воркер умеет — ключи настроек, маршруты, события, задачи, запросы
// к серверу.
import {
  JOB_EVENT_PREFIX,
  JOB_EVENTS,
  type ManifestRoute,
  type WorkerManifest,
} from "../protocol/messages";
import type { Agent, WorkerCapabilities } from "./types";

/** Что проверить в манифесте: все заданные условия должны выполняться. */
export interface SupportsQuery {
  /** Маршрут для fetch: метод и путь (параметры после ? не учитываются). */
  route?: { method: string; path: string };
  /** Ключ настроек. */
  config?: string;
  /** Тип события. */
  event?: string;
}

/** Манифест воркера из последнего status (или hello); нет — воркер себя не описывает. */
export const workerManifest = (
  agent: Pick<Agent, "workers">,
  worker: string,
): WorkerManifest | undefined =>
  agent.workers.find(w => w.name === worker)?.manifest;

/** Путь без параметров, с раскрытыми %XX (как сравнивает агент); неверный %XX — как есть. */
const plainPath = (path: string): string => {
  const p = path.split("?")[0].split("#")[0];

  try {
    return decodeURIComponent(p);
  } catch {
    return p;
  }
};

/**
 * Путь подходит под шаблон маршрута (§7): {name} — один непустой сегмент, кроме «.» и «..»;
 * параметры после ? не учитываются.
 */
export const matchRoute = (template: string, path: string): boolean => {
  const want = template.split("/");
  const got = plainPath(path).split("/");

  return (
    want.length === got.length &&
    want.every((part, i) =>
      /^\{[^/{}]+\}$/.test(part)
        ? got[i] !== "" && got[i] !== "." && got[i] !== ".."
        : part === got[i],
    )
  );
};

/** Маршрут манифеста для метода и пути; нет — undefined. */
export const findRoute = (
  m: WorkerManifest | undefined,
  method: string,
  path: string,
): ManifestRoute | undefined =>
  m?.routes?.find(
    r => r.method === method.toUpperCase() && matchRoute(r.path, path),
  );

/**
 * Агент примет от воркера событие type (§12): объявленное в events; события задач — если
 * объявлены jobs.
 */
export const declaresEvent = (
  m: WorkerManifest | undefined,
  type: string,
): boolean =>
  type.startsWith(JOB_EVENT_PREFIX)
    ? Boolean(m?.jobs?.length) &&
      (JOB_EVENTS as readonly string[]).includes(type)
    : (m?.events ?? []).some(e => e.type === type);

/** Что умеет воркер по манифесту; манифеста нет — undefined. */
export const capabilities = (
  agent: Pick<Agent, "workers">,
  worker: string,
): WorkerCapabilities | undefined => {
  const m = workerManifest(agent, worker);

  if (!m) return undefined;
  const out: WorkerCapabilities = {
    configs: m.configs ?? [],
    routes: m.routes ?? [],
    events: m.events ?? [],
    jobs: m.jobs ?? [],
    requests: m.requests ?? [],
  };

  if (m.version !== undefined) out.version = m.version;
  if (m.description !== undefined) out.description = m.description;

  return out;
};

/**
 * Воркер worker агента описал в манифесте всё, что задано в query: маршрут, ключ настроек, тип
 * события. Нет манифеста — false.
 */
export const supports = (
  agent: Pick<Agent, "workers">,
  worker: string,
  query: SupportsQuery,
): boolean => {
  const m = workerManifest(agent, worker);

  if (!m) return false;
  const { route, config, event } = query;

  return (
    (!route || findRoute(m, route.method, route.path) !== undefined) &&
    (config === undefined || (m.configs ?? []).some(c => c.key === config)) &&
    (event === undefined || (m.events ?? []).some(e => e.type === event))
  );
};
