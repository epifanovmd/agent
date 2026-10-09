// История стенда в памяти: события воркеров, точки метрик и итоги действий — кольцами
// ограниченного размера. SDK историю не хранит: бэкенд пишет её сам из onEvent и событий Agents
// (metrics, action). Настоящий бэкенд пишет то же в свою БД.
import type {
  ActionRecord,
  AgentEvent,
  Agents,
  AuditEntry,
  MetricsEvent,
  MetricsPoint,
} from "agent-sdk/server";

export interface HistoryOptions {
  /** Событий всего (по умолчанию 10 000). */
  events?: number;
  /** Точек метрик на агента (по умолчанию 2000). */
  metrics?: number;
  /** Итогов действий на агента (по умолчанию 200). */
  actions?: number;
  /** Точку метрик — не чаще, мс (по умолчанию 0 — каждую). */
  metricsEveryMs?: number;
}

export interface EventFilter {
  agentId?: string;
  worker?: string;
  type?: string;
  /** Строго раньше (receivedAt), мс — постраничное чтение. */
  before?: number;
  /** По умолчанию 100. */
  limit?: number;
}

/** Добавить в конец; лишнее убирается из начала и возвращается. */
const push = <T>(list: T[], item: T, keep: number): T[] => {
  list.push(item);

  return list.length > keep ? list.splice(0, list.length - keep) : [];
};

const eventKey = (e: AgentEvent) => `${e.agentId}\n${e.id}`;

export class History {
  private readonly keep: Required<HistoryOptions>;
  private events: AgentEvent[] = [];
  /** Ключи сохранённых событий "агент\nid": повтор доставки не задваивается. */
  private readonly eventKeys = new Set<string>();
  private readonly metrics = new Map<string, MetricsPoint[]>();
  private readonly actions = new Map<string, ActionRecord[]>();

  constructor(opts: HistoryOptions = {}) {
    this.keep = {
      events: opts.events ?? 10_000,
      metrics: opts.metrics ?? 2000,
      actions: opts.actions ?? 200,
      metricsEveryMs: opts.metricsEveryMs ?? 0,
    };
  }

  /** Для опции Agents onEvent: событие сохранено — Agents подтверждает его агенту. */
  addEvent = (e: AgentEvent): void => {
    if (this.eventKeys.has(eventKey(e))) return;
    this.eventKeys.add(eventKey(e));
    for (const old of push(this.events, e, this.keep.events))
      this.eventKeys.delete(eventKey(old));
  };

  /** Подписаться на события Agents: метрики, итоги действий, удаление агента. */
  follow(agents: Agents): void {
    agents.on("metrics", m => this.addMetrics(m));
    agents.on("action", a => this.addAction(a));
    agents.on("audit", e => this.audit(e));
  }

  /** События, новые первыми. */
  listEvents(f: EventFilter = {}): AgentEvent[] {
    const limit = f.limit ?? 100;

    return this.events
      .filter(
        e =>
          (!f.agentId || e.agentId === f.agentId) &&
          (!f.worker || e.worker === f.worker) &&
          (!f.type || e.type === f.type) &&
          (f.before === undefined || e.receivedAt < f.before),
      )
      .reverse()
      .slice(0, limit > 0 ? limit : undefined);
  }

  /** Точки метрик агента по возрастанию времени; since — строго позже, limit — последние. */
  listMetrics(
    agentId: string,
    f: { since?: number; limit?: number } = {},
  ): MetricsPoint[] {
    const list = (this.metrics.get(agentId) ?? []).filter(
      p => f.since === undefined || p.at > f.since,
    );

    return f.limit !== undefined && f.limit > 0 ? list.slice(-f.limit) : list;
  }

  /** Итоги действий агента, новые первыми. */
  listActions(agentId: string, limit = 50): ActionRecord[] {
    return [...(this.actions.get(agentId) ?? [])].reverse().slice(0, limit);
  }

  private addMetrics({ agentId, ...p }: MetricsEvent): void {
    const list = this.metrics.get(agentId) ?? [];
    const last = list.at(-1);

    this.metrics.set(agentId, list);
    if (last && p.at < last.at) {
      // Досылка после разрыва: на своё место по времени.
      list.splice(list.findLastIndex(x => x.at <= p.at) + 1, 0, p);
      if (list.length > this.keep.metrics) list.shift();

      return;
    }
    if (last && p.at - last.at < this.keep.metricsEveryMs) return;
    push(list, p, this.keep.metrics);
  }

  private addAction(a: ActionRecord): void {
    const list = this.actions.get(a.agentId) ?? [];

    push(list, a, this.keep.actions);
    this.actions.set(a.agentId, list);
  }

  /** Агента удалили — его история больше не нужна. */
  private audit(e: AuditEntry): void {
    if (e.action !== "agent.delete") return;
    this.metrics.delete(e.agentId);
    this.actions.delete(e.agentId);
    this.events = this.events.filter(x => x.agentId !== e.agentId);
    this.eventKeys.clear();
    for (const x of this.events) this.eventKeys.add(eventKey(x));
  }
}
