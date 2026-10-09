// Итоги настроек без побочных эффектов: что агент сообщил о ключах (hello, status,
// config.applied) и статус ключа — желаемое против сообщённого.
import type { ConfigReport, Status } from "../protocol/messages";
import type {
  AgentRecord,
  ConfigRecord,
  ConfigReported,
  ConfigStatus,
} from "./types";

type Reported = AgentRecord["configs"];

/** Итог ключа из сообщения агента поверх прежнего: appliedVersion помнит последнюю удачную версию. */
export const report = (
  prev: ConfigReported | undefined,
  r: ConfigReport,
  at: number,
): ConfigReported => {
  const next: ConfigReported = { version: r.version, at };

  if (r.ok !== undefined) next.ok = r.ok;
  if (r.ok === false && r.error)
    next.error = { code: r.error.code, message: r.error.message };
  if (r.ok === true) next.appliedVersion = r.version;
  else if (prev?.appliedVersion !== undefined)
    next.appliedVersion = prev.appliedVersion;
  // Та же версия без итога (hello, status во время применения) — прежний итог остаётся.
  if (prev && prev.version === r.version && r.ok === undefined) {
    if (prev.ok !== undefined) next.ok = prev.ok;
    if (prev.error) next.error = prev.error;
    if (prev.appliedVersion !== undefined)
      next.appliedVersion = prev.appliedVersion;
  }

  return next;
};

/** Версии на диске агента из hello: ключей, которых нет в hello, у агента нет. */
export const reportedFromHello = (
  prev: Reported,
  configs: Record<string, Record<string, number>> | undefined,
  at: number,
): Reported => {
  const out: Reported = {};

  for (const [worker, keys] of Object.entries(configs ?? {})) {
    for (const [key, version] of Object.entries(keys)) {
      (out[worker] ??= {})[key] = report(prev[worker]?.[key], { version }, at);
    }
  }

  return out;
};

/**
 * Итоги из status: у воркеров из status — ровно перечисленные ключи (удалённый ключ пропадает);
 * воркер, которого нет в status, — неизвестен агенту: остаются только отказы WORKER_UNKNOWN.
 */
export const reportedFromStatus = (
  prev: Reported,
  status: Status,
  at: number,
): Reported => {
  const out: Reported = {};
  const listed = new Set<string>();

  for (const w of status.workers) {
    listed.add(w.name);
    for (const [key, r] of Object.entries(w.configs ?? {})) {
      (out[w.name] ??= {})[key] = report(prev[w.name]?.[key], r, at);
    }
  }
  for (const [worker, keys] of Object.entries(prev)) {
    if (listed.has(worker)) continue;
    for (const [key, r] of Object.entries(keys)) {
      if (r.error?.code === "WORKER_UNKNOWN") (out[worker] ??= {})[key] = r;
    }
  }

  return out;
};

/** Статус ключа: желаемое (Store) против сообщённого агентом. */
export const configStatus = (
  agentId: string,
  worker: string,
  key: string,
  desired: ConfigRecord | undefined,
  r: ConfigReported | undefined,
): ConfigStatus => {
  const s: ConfigStatus = {
    agentId,
    worker,
    key,
    version: desired?.version ?? null,
    state: "pending",
  };

  if (r) {
    s.delivered = r.version;
    if (r.appliedVersion !== undefined) s.applied = r.appliedVersion;
    s.updatedAt = r.at;
  }
  if (!desired) s.state = "deleting";
  else if (!r || r.version < desired.version) s.state = "pending";
  else if (r.ok === undefined) s.state = "applying";
  else s.state = r.ok ? "applied" : "failed";
  if (r?.ok === false && r.error && s.state === "failed") s.error = r.error;

  return s;
};

/** Все ключи: желаемые и сообщённые агентом. */
export const configStatuses = (
  agent: AgentRecord,
  desired: ConfigRecord[],
  worker?: string,
): ConfigStatus[] => {
  const keys = new Map<string, [string, string]>();

  for (const d of desired) keys.set(`${d.worker}/${d.key}`, [d.worker, d.key]);
  for (const [w, list] of Object.entries(agent.configs))
    for (const k of Object.keys(list)) keys.set(`${w}/${k}`, [w, k]);
  const out: ConfigStatus[] = [];

  for (const [w, k] of [...keys.values()].sort((a, b) =>
    (a[0] + "/" + a[1]).localeCompare(b[0] + "/" + b[1]),
  )) {
    if (worker && w !== worker) continue;
    const d = desired.find(c => c.worker === w && c.key === k);

    out.push(configStatus(agent.id, w, k, d, agent.configs[w]?.[k]));
  }

  return out;
};

/** Ключи "воркер/ключ", у которых изменился итог (время приёма не в счёт). */
export const changedKeys = (a: Reported, b: Reported): string[] => {
  const keys = new Set<string>();

  for (const m of [a, b])
    for (const [w, ks] of Object.entries(m))
      for (const k of Object.keys(ks)) keys.add(`${w}/${k}`);

  return [...keys].filter(k => {
    const [w, key] = k.split("/");

    return !sameReport(a[w]?.[key], b[w]?.[key]);
  });
};

/** Итоги совпадают без учёта времени приёма. */
export const sameReport = (
  a: ConfigReported | undefined,
  b: ConfigReported | undefined,
): boolean => {
  const strip = (r: ConfigReported | undefined) =>
    r ? JSON.stringify({ ...r, at: 0 }) : "";

  return strip(a) === strip(b);
};
