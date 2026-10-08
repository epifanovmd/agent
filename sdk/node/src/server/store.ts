// Хранилище Agents: агенты, задачи, команды, желаемое состояние, события.
// MemoryStore — в памяти процесса (разработка, один процесс); свой Store
// (Postgres и т. п.) — та же механика связи с данными в БД.
import type {
  AgentEvent,
  AgentRecord,
  CommandFilter,
  Command,
  DesiredState,
  JobFilter,
  JobRecord,
  MetricsPoint,
} from "./model";

/**
 * Хранилище. Методы асинхронны; Agents сериализует изменения сам (одна операция
 * за раз), поэтому Store не обязан уметь транзакции между вызовами.
 * Записи передаются целиком: update* заменяет запись.
 */
export interface Store {
  createAgent(agent: AgentRecord): Promise<void>;
  getAgent(id: string): Promise<AgentRecord | undefined>;
  updateAgent(agent: AgentRecord): Promise<void>;
  listAgents(): Promise<AgentRecord[]>;

  createJob(job: JobRecord): Promise<void>;
  getJob(id: string): Promise<JobRecord | undefined>;
  updateJob(job: JobRecord): Promise<void>;
  /** Новые первыми. */
  listJobs(filter?: JobFilter): Promise<JobRecord[]>;

  createCommand(cmd: Command): Promise<void>;
  getCommand(id: string): Promise<Command | undefined>;
  updateCommand(cmd: Command): Promise<void>;
  /** Новые первыми. */
  listCommands(filter?: CommandFilter): Promise<Command[]>;

  /**
   * Новый снимок домена (общий — agentId пусто). Версия монотонна в пределах
   * домена и между перезапусками: не меньше max(прежняя + 1, now_ms).
   */
  setState(domain: string, agentId: string | undefined, spec: unknown, actor?: string): Promise<DesiredState>;
  /**
   * История снимков (domain, agentId; общий — agentId пусто), новые первыми, не больше limit.
   * Каждый setState — запись истории; deleteState историю не стирает.
   */
  listStateHistory(domain: string, agentId: string | undefined, limit: number): Promise<DesiredState[]>;
  /** Удалить снимок (общий — agentId пусто); false — его не было. Счётчик версий домена не сбрасывается. */
  deleteState(domain: string, agentId?: string): Promise<boolean>;
  getState(domain: string, agentId?: string): Promise<DesiredState | undefined>;
  listStates(): Promise<DesiredState[]>;

  addEvent(event: AgentEvent): Promise<void>;
  /** Последние limit событий, новые первыми; limit ≤ 0 — все. */
  listEvents(limit?: number): Promise<AgentEvent[]>;

  /** Точка истории метрик агента (backfill может прийти старше уже сохранённых). */
  addMetrics(agentId: string, point: MetricsPoint): Promise<void>;
  /** История по возрастанию at; since — строго позже. */
  listMetrics(agentId: string, since?: number): Promise<MetricsPoint[]>;
  /** Удалить точки истории всех агентов с at строго раньше before; результат — сколько удалено. */
  pruneMetrics(before: number): Promise<number>;
}

export interface MemoryStoreOptions {
  /** Завершённых задач хранить (по умолчанию 1000). */
  keepJobs?: number;
  /** Завершённых команд хранить (по умолчанию 500). */
  keepCommands?: number;
  /** Событий хранить (по умолчанию 1000). */
  keepEvents?: number;
  /** Точек метрик на агента (по умолчанию 4320: сутки по 20 с). */
  keepMetrics?: number;
  /** Снимков истории состояния на (раздел, агент) (по умолчанию 50). */
  keepStateHistory?: number;
}

const clone = <T>(v: T): T => structuredClone(v);
const has = <T>(want: T | T[] | undefined, v: T) =>
  want === undefined || (Array.isArray(want) ? want.includes(v) : want === v);

/** Store в памяти: записи копируются на входе и выходе (как у настоящей БД). */
export class MemoryStore implements Store {
  private agents = new Map<string, AgentRecord>();
  private jobs = new Map<string, JobRecord>();
  private commands = new Map<string, Command>();
  private states = new Map<string, DesiredState>();
  private history = new Map<string, DesiredState[]>(); // stateKey → снимки, старые первыми
  private domainVersion = new Map<string, number>();
  private events: AgentEvent[] = [];
  private metrics = new Map<string, MetricsPoint[]>();
  private readonly keepMetrics: number;
  private readonly keepJobs: number;
  private readonly keepCommands: number;
  private readonly keepEvents: number;
  private readonly keepStateHistory: number;

  constructor(opts: MemoryStoreOptions = {}) {
    this.keepJobs = opts.keepJobs ?? 1000;
    this.keepCommands = opts.keepCommands ?? 500;
    this.keepEvents = opts.keepEvents ?? 1000;
    this.keepMetrics = opts.keepMetrics ?? 4320;
    this.keepStateHistory = opts.keepStateHistory ?? 50;
  }

  async createAgent(agent: AgentRecord) {
    this.agents.set(agent.id, clone(agent));
  }
  async getAgent(id: string) {
    const a = this.agents.get(id);
    return a && clone(a);
  }
  async updateAgent(agent: AgentRecord) {
    if (this.agents.has(agent.id)) this.agents.set(agent.id, clone(agent));
  }
  async listAgents() {
    return [...this.agents.values()].map(clone);
  }

  async createJob(job: JobRecord) {
    this.jobs.set(job.id, clone(job));
    trim(this.jobs, this.keepJobs, (j) => j.status !== "queued" && j.status !== "running");
  }
  async getJob(id: string) {
    const j = this.jobs.get(id);
    return j && clone(j);
  }
  async updateJob(job: JobRecord) {
    if (this.jobs.has(job.id)) this.jobs.set(job.id, clone(job));
  }
  async listJobs(f: JobFilter = {}) {
    const out: JobRecord[] = [];
    for (const j of this.jobs.values()) {
      if (has(f.status, j.status) && (!f.queue || j.queue === f.queue) && (!f.agentId || j.agentId === f.agentId))
        out.push(clone(j));
    }
    return out.reverse();
  }

  async createCommand(cmd: Command) {
    this.commands.set(cmd.id, clone(cmd));
    trim(this.commands, this.keepCommands, (c) => c.status === "succeeded" || c.status === "failed");
  }
  async getCommand(id: string) {
    const c = this.commands.get(id);
    return c && clone(c);
  }
  async updateCommand(cmd: Command) {
    if (this.commands.has(cmd.id)) this.commands.set(cmd.id, clone(cmd));
  }
  async listCommands(f: CommandFilter = {}) {
    const out: Command[] = [];
    for (const c of this.commands.values()) {
      if (has(f.status, c.status) && (!f.agentId || c.agentId === f.agentId)) out.push(clone(c));
    }
    return out.reverse();
  }

  async setState(domain: string, agentId: string | undefined, spec: unknown, actor?: string) {
    // Состояние в памяти: версия — не меньше текущего времени в мс, чтобы после
    // перезапуска сервера быть новее версии, применённой агентом (§6.5).
    const version = Math.max((this.domainVersion.get(domain) ?? 0) + 1, Date.now());
    this.domainVersion.set(domain, version);
    const st: DesiredState = { domain, version, spec: clone(spec), updatedAt: Date.now() };
    if (agentId) st.agentId = agentId;
    if (actor) st.actor = actor;
    const key = stateKey(domain, agentId);
    this.states.set(key, st);
    let hist = this.history.get(key);
    if (!hist) this.history.set(key, (hist = []));
    hist.push(clone(st));
    if (hist.length > this.keepStateHistory) hist.splice(0, hist.length - this.keepStateHistory);
    return clone(st);
  }
  async deleteState(domain: string, agentId?: string) {
    return this.states.delete(stateKey(domain, agentId));
  }
  async getState(domain: string, agentId?: string) {
    const st = this.states.get(stateKey(domain, agentId));
    return st && clone(st);
  }
  async listStates() {
    return [...this.states.values()].map(clone);
  }
  async listStateHistory(domain: string, agentId: string | undefined, limit: number) {
    const hist = this.history.get(stateKey(domain, agentId)) ?? [];
    return (limit > 0 ? hist.slice(-limit) : [...hist]).reverse().map(clone);
  }

  async addEvent(event: AgentEvent) {
    this.events.push(clone(event));
    if (this.events.length > this.keepEvents) this.events.splice(0, this.events.length - this.keepEvents);
  }
  async listEvents(limit = 100) {
    return (limit > 0 ? this.events.slice(-limit) : [...this.events]).reverse().map(clone);
  }

  async addMetrics(agentId: string, point: MetricsPoint) {
    let list = this.metrics.get(agentId);
    if (!list) this.metrics.set(agentId, (list = []));
    // Обычно точка новее всех; backfill — вставка на своё место по at.
    let i = list.length;
    while (i > 0 && list[i - 1].at > point.at) i--;
    list.splice(i, 0, clone(point));
    if (list.length > this.keepMetrics) list.splice(0, list.length - this.keepMetrics);
  }
  async listMetrics(agentId: string, since?: number) {
    const list = this.metrics.get(agentId) ?? [];
    let i = list.length;
    if (since != null) while (i > 0 && list[i - 1].at > since) i--;
    else i = 0;
    return list.slice(i).map(clone);
  }
  async pruneMetrics(before: number) {
    let removed = 0;
    for (const [agentId, list] of this.metrics) {
      // Список по возрастанию at: удаляется начало.
      let i = 0;
      while (i < list.length && list[i].at < before) i++;
      removed += i;
      if (i === list.length) this.metrics.delete(agentId);
      else if (i > 0) list.splice(0, i);
    }
    return removed;
  }
}

const stateKey = (domain: string, agentId?: string) => `${domain}\u0000${agentId ?? ""}`;

function trim<T>(map: Map<string, T>, keep: number, removable: (v: T) => boolean): void {
  if (map.size <= keep) return;
  for (const [id, v] of map) {
    if (map.size <= keep) return;
    if (removable(v)) map.delete(id);
  }
}
