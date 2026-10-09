// Проблемы агента без побочных эффектов: выводятся из status и итогов настроек; offline ведёт
// связь.
import type { AgentRecord, Alert } from "./types";

/** Проблемы по status и итогам настроек (без offline — её ведёт связь). */
const derivedAlerts = (a: AgentRecord): Omit<Alert, "since">[] => {
  const base = { agentId: a.id, agentName: a.name };
  const out: Omit<Alert, "since">[] = [];

  for (const w of a.status?.workers ?? []) {
    if (w.state === "backoff" || w.state === "stopped")
      out.push({
        ...base,
        key: `workerDown:${w.name}`,
        type: "workerDown",
        worker: w.name,
        message: `воркер ${w.name} не работает (${w.state}, перезапусков: ${w.restarts ?? 0})`,
      });
    else if (w.state === "invalid")
      out.push({
        ...base,
        key: `workerInvalid:${w.name}`,
        type: "workerInvalid",
        worker: w.name,
        message: `воркер ${w.name} не зарегистрирован: ${w.message || "ответ GET /health или GET /manifest не подошёл"}`,
      });
    else if (w.state === "running" && w.health?.ok === false)
      out.push({
        ...base,
        key: `workerUnhealthy:${w.name}`,
        type: "workerUnhealthy",
        worker: w.name,
        message: `воркер ${w.name}: ${w.health.message || "плохое самочувствие"}`,
      });
  }
  for (const [worker, keys] of Object.entries(a.configs)) {
    for (const [key, r] of Object.entries(keys)) {
      if (r.ok !== false) continue;
      out.push({
        ...base,
        key: `configFailed:${worker}/${key}`,
        type: "configFailed",
        worker,
        configKey: key,
        message: `настройки ${worker}/${key} версии ${r.version} не применены: ${r.error?.message ?? r.error?.code ?? ""}`,
      });
    }
  }

  return out;
};

/**
 * Привести a.alerts к списку want (offline не трогается); результат — начавшиеся и закончившиеся.
 */
const syncAlerts = (
  a: AgentRecord,
  want: Omit<Alert, "since">[],
  now: number,
): { started: Alert[]; ended: Alert[] } => {
  const started: Alert[] = [];
  const ended: Alert[] = [];
  const wantKeys = new Set(want.map(w => w.key));
  const next: Alert[] = [];

  for (const cur of a.alerts) {
    if (cur.type === "offline" || wantKeys.has(cur.key)) next.push(cur);
    else ended.push(cur);
  }
  for (const w of want) {
    const i = next.findIndex(x => x.key === w.key);

    if (i < 0) {
      const al = { ...w, since: now };

      next.push(al);
      started.push(al);
    } else next[i] = { ...w, since: next[i].since };
  }
  a.alerts = next;

  return { started, ended };
};

/** Пересчитать проблемы по status и итогам настроек. */
export const recheckAlerts = (
  a: AgentRecord,
  now: number,
): { started: Alert[]; ended: Alert[] } => {
  return syncAlerts(a, derivedAlerts(a), now);
};

/** Начать проблему offline; уже есть — undefined. */
export const raiseOffline = (
  a: AgentRecord,
  now: number,
): Alert | undefined => {
  if (a.alerts.some(x => x.type === "offline")) return undefined;
  const al: Alert = {
    key: "offline",
    agentId: a.id,
    agentName: a.name,
    type: "offline",
    message: "агент без связи",
    since: now,
  };

  a.alerts.push(al);

  return al;
};

/** Закончить проблему offline; результат — закончившиеся. */
export const clearOffline = (a: AgentRecord): Alert[] => {
  const ended = a.alerts.filter(x => x.type === "offline");

  a.alerts = a.alerts.filter(x => x.type !== "offline");

  return ended;
};
