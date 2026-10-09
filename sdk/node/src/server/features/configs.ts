// Настройки воркеров на агентах (§4, §5): желаемые значения с версиями, доставка агенту на связи
// и сверка при подключении, итоги применения (config.applied), события config.
import type { AlertChanges, Context } from "../core/context";
import { codeError, invalid, revoked, valid } from "../core/errors";
import type { Session } from "../core/session";
import { jsonSchemaProblems } from "../lib/json-schema";
import { recheckAlerts } from "../model/alerts";
import {
  configStatus,
  configStatuses,
  report,
  sameReport,
} from "../model/config-report";
import { workerManifest } from "../model/manifest";
import { publicAgent } from "../model/public-agent";
import type { AgentRecord, ConfigRecord, ConfigStatus } from "../model/types";
import { parseConfigApplied, parseName } from "../protocol/checks";
import { type Envelope, MAX_CONFIG_BYTES } from "../protocol/messages";

export class Configs {
  private readonly ctx: Context;

  constructor(ctx: Context) {
    this.ctx = ctx;
  }

  async set(
    actor: string,
    agentId: string,
    worker: string,
    key: string,
    data: unknown,
  ): Promise<ConfigRecord> {
    valid(parseName("worker", worker));
    valid(parseName("key", key));
    const raw = toJSON(data);

    if (raw === undefined) throw invalid("data — значение JSON");
    if (Buffer.byteLength(raw) > MAX_CONFIG_BYTES)
      throw codeError("BODY_TOO_LARGE", "значение настройки больше 4 МБ");
    const a = await this.ctx.agent(agentId);

    if (a.revoked) throw revoked();
    if (this.ctx.settings.validateConfigs) this.validate(a, worker, key, data);
    const reported = a.configs[worker]?.[key];
    // Версия — больше, чем уже у агента (Store мог быть сброшен).
    const rec = await this.ctx.store.setConfig(agentId, worker, key, data, {
      actor: actor || undefined,
      minVersion: (reported?.version ?? 0) + 1,
    });

    this.ctx.audit(actor, "config.set", agentId, {
      worker,
      key,
      version: rec.version,
    });
    const ss = this.ctx.live(agentId);

    if (ss) this.send(ss, rec);
    else this.ctx.change(agentId, "config");
    this.ctx.emit("config", configStatus(agentId, worker, key, rec, reported));

    return rec;
  }

  /** Удалить ключ; false — ключа не было. */
  async delete(
    actor: string,
    agentId: string,
    worker: string,
    key: string,
  ): Promise<boolean> {
    valid(parseName("worker", worker));
    valid(parseName("key", key));
    if (!(await this.ctx.store.deleteConfig(agentId, worker, key)))
      return false;
    this.ctx.audit(actor, "config.delete", agentId, { worker, key });
    const ss = this.ctx.live(agentId);

    if (ss) sendDelete(ss, `${worker}/${key}`);
    else this.ctx.change(agentId, "config");
    const r = (await this.ctx.store.getAgent(agentId))?.configs[worker]?.[key];

    if (r)
      this.ctx.emit("config", configStatus(agentId, worker, key, undefined, r));

    return true;
  }

  async status(agentId: string, worker?: string): Promise<ConfigStatus[]> {
    const a = await this.ctx.agent(agentId);

    return configStatuses(a, await this.ctx.store.listConfigs(agentId), worker);
  }

  /** Версии на диске агента из hello — известны сессии. */
  known(
    ss: Session,
    configs: Record<string, Record<string, number>> | undefined,
  ): void {
    for (const [worker, keys] of Object.entries(configs ?? {}))
      for (const [key, v] of Object.entries(keys))
        ss.known.set(`${worker}/${key}`, v);
  }

  /** Сверка настроек сессии с желаемыми (§4): новее — config.put, лишние — config.delete. */
  async sync(ss: Session): Promise<void> {
    const desired = await this.ctx.store.listConfigs(ss.agentId);
    const want = new Set(desired.map(d => `${d.worker}/${d.key}`));

    for (const d of desired) this.send(ss, d);
    for (const k of [...ss.known.keys()]) if (!want.has(k)) sendDelete(ss, k);
  }

  /** Важное сообщение config.applied: итог применения ключа; ошибка — причина отказа. */
  async applied(ss: Session, env: Envelope): Promise<string> {
    const p = parseConfigApplied(env.data);

    if (!p.ok) return p.error;
    const { worker, key, ...result } = p.value;
    const now = Date.now();
    let alerts: AlertChanges = { started: [], ended: [] };
    const { agent, changed } = await this.ctx.mutate(ss.agentId, a => {
      alerts = { started: [], ended: [] };
      const prev = a.configs[worker]?.[key];

      if (prev && prev.version > result.version) return false;
      const next = report(prev, result, now);

      if (prev && sameReport(prev, next)) return false;
      (a.configs[worker] ??= {})[key] = next;
      a.lastSeenAt = now;
      alerts = recheckAlerts(a, now);

      return true;
    });

    if (agent && changed) {
      await this.emitStatuses(agent, [`${worker}/${key}`]);
      this.ctx.emitAlerts(alerts);
    }

    return "";
  }

  /** События config по ключам "воркер/ключ", у которых изменился итог. */
  async emitStatuses(agent: AgentRecord, keys: string[]): Promise<void> {
    if (!keys.length || !this.ctx.listens("config")) return;
    const desired = await this.ctx.store
      .listConfigs(agent.id)
      .catch(() => [] as ConfigRecord[]);

    for (const k of keys) {
      const [worker, key] = k.split("/");
      const d = desired.find(c => c.worker === worker && c.key === key);
      const r = agent.configs[worker]?.[key];

      if (d || r)
        this.ctx.emit("config", configStatus(agent.id, worker, key, d, r));
    }
  }

  /**
   * Значение по JSON Schema ключа из манифеста воркера; не подходит — CONFIG_INVALID. Нет схемы
   * — без проверки; схему нельзя применить — запись в журнал, без проверки.
   */
  private validate(
    a: AgentRecord,
    worker: string,
    key: string,
    data: unknown,
  ): void {
    const schema = workerManifest(publicAgent(a), worker)?.configs?.find(
      c => c.key === key,
    )?.schema;

    if (!schema) return;
    let problems: string[];

    try {
      problems = jsonSchemaProblems(schema, data);
    } catch (e) {
      this.ctx.log("схема ключа из манифеста воркера не применяется", {
        agentId: a.id,
        worker,
        key,
        err: String(e),
      });

      return;
    }
    if (problems.length)
      throw codeError(
        "CONFIG_INVALID",
        `${worker}/${key}: ${problems.join("; ")}`,
        400,
      );
  }

  /** config.put, если у агента версия старее. */
  private send(ss: Session, rec: ConfigRecord): void {
    const k = `${rec.worker}/${rec.key}`;

    if (rec.version <= (ss.known.get(k) ?? 0)) return;
    ss.known.set(k, rec.version);
    ss.send({
      type: "config.put",
      data: {
        worker: rec.worker,
        key: rec.key,
        version: rec.version,
        data: rec.data,
      },
    });
  }
}

const sendDelete = (ss: Session, k: string): void => {
  ss.known.delete(k);
  const [worker, key] = k.split("/");

  ss.send({ type: "config.delete", data: { worker, key } });
};

/** JSON значения; не сериализуется — undefined. */
const toJSON = (data: unknown): string | undefined => {
  try {
    return JSON.stringify(data);
  } catch {
    return undefined;
  }
};
