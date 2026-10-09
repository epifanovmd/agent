// Манифест воркера (§12): что воркер умеет — ключи настроек, маршруты, события.
import type { WorkerManifest } from "../protocol/messages";
import type { Agent } from "./types";

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

/** Путь подходит под шаблон маршрута: {name} — один непустой сегмент. */
export const matchRoute = (template: string, path: string): boolean => {
  const want = template.split("/");
  const got = path.split("?")[0].split("/");

  return (
    want.length === got.length &&
    want.every((part, i) =>
      /^\{[^/{}]+\}$/.test(part) ? got[i] !== "" : part === got[i],
    )
  );
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
    (!route ||
      (m.routes ?? []).some(
        r =>
          r.method === route.method.toUpperCase() &&
          matchRoute(r.path, route.path),
      )) &&
    (config === undefined || (m.configs ?? []).some(c => c.key === config)) &&
    (event === undefined || (m.events ?? []).some(e => e.type === event))
  );
};
