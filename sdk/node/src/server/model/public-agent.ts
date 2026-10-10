// Вид записи агента для бэкенда.
import type { Agent, AgentRecord } from "./types";

/** Агент для бэкенда: без секретов и служебного учёта. */
export const publicAgent = (r: AgentRecord): Agent => {
  const helloWorkers = r.hello?.workers ?? [];
  const status = r.status?.workers ?? [];
  const names = [
    ...new Set([...helloWorkers.map(w => w.name), ...status.map(w => w.name)]),
  ];
  const workers = names.map(name => ({
    ...helloWorkers.find(w => w.name === name),
    ...status.find(w => w.name === name),
    name,
  }));
  const a: Agent = {
    id: r.id,
    name: r.name,
    labels: { ...r.labels },
    online: r.online,
    revoked: r.revoked,
    enrolledAt: r.enrolledAt,
    workers,
    alerts: [...r.alerts],
  };
  const put = <K extends keyof Agent>(k: K, v: Agent[K] | undefined) => {
    if (v !== undefined) a[k] = v;
  };

  put("lastSeenAt", r.lastSeenAt);
  put("connectedAt", r.connectedAt);
  put("address", r.address);
  put("version", r.hello?.agent?.version);
  put("bootId", r.hello?.agent?.bootId);
  put("startedAt", r.hello?.agent?.startedAt);
  put("host", r.hello?.host);
  put("hello", r.hello);
  put("status", r.status);
  // status свежее hello: в нём update пропадает, когда новой версии больше нет.
  put("update", r.status ? r.status.update : r.hello?.agent?.update);
  put("statusAt", r.statusAt);
  put("metrics", r.metrics);
  put("session", r.online ? r.session : undefined);

  return a;
};
