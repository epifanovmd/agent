// Манифест воркера (§12): что воркер умеет — ключи настроек, маршруты, события, задачи, запросы
// к серверу; проверка значений по схемам из него.
import { jsonSchemaProblems } from "../lib/json-schema";
import type { ManifestRoute, WorkerManifest } from "../protocol/messages";
import type { Agent } from "./types";

/** Чьей схемой из манифеста проверять: ключа настроек, события, задачи, запроса воркера, тела маршрута. */
export type SchemaTarget =
  | { config: string }
  | { event: string }
  | { job: string }
  | { request: string }
  | { route: { method: string; path: string } };

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
 * Замечания к data по JSON Schema из манифеста воркера (§12): «поле: замечание», не больше трёх;
 * пусто — подходит. Нет манифеста, записи в нём или схемы у записи — пусто (проверять нечем);
 * схему нельзя применить — исключение. Тело маршрута (route) — разобранный JSON.
 */
export const schemaProblems = (
  agent: Pick<Agent, "workers">,
  worker: string,
  target: SchemaTarget,
  data: unknown,
): string[] => {
  const m = workerManifest(agent, worker);
  const schema =
    "config" in target
      ? m?.configs?.find(c => c.key === target.config)?.schema
      : "event" in target
        ? m?.events?.find(e => e.type === target.event)?.schema
        : "job" in target
          ? m?.jobs?.find(j => j.type === target.job)?.schema
          : "request" in target
            ? m?.requests?.find(r => r.type === target.request)?.schema
            : findRoute(m, target.route.method, target.route.path)?.request;

  return schema ? jsonSchemaProblems(schema, data ?? null) : [];
};
