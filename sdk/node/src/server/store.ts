// Хранилище Agents: агенты, задачи, команды, желаемое состояние, события.
// MemoryStore — в памяти процесса (разработка, один процесс); свой Store
// (Postgres и т. п.) — та же механика связи с данными в БД.
import {
  commandFinished,
  jobFinished,
  type AgentEvent,
  type AgentRecord,
  type CommandFilter,
  type CommandRecord,
  type DesiredState,
  type JobFilter,
  type JobRecord,
  type MetricsPoint,
  type PageFilter,
  type StorePruneOptions,
} from "./model";

/**
 * Хранилище. Методы асинхронны и могут вызываться одновременно (в том числе из
 * нескольких процессов бэкенда с общим Store). Записи агента, задачи и команды
 * передаются целиком; update* — условная запись по версии rev: Store пишет запись,
 * только если в хранилище rev такой же, как у переданной (запись не менялась с
 * чтения), и сохраняет её с rev + 1 (у переданной rev тоже становится rev + 1).
 * Результат false — запись изменилась (или её нет): Agents перечитает и повторит.
 * В SQL: UPDATE … SET record = $2, rev = rev + 1 WHERE id = $1 AND rev = $3.
 */
export interface Store {
  /** Сохранить новую запись как есть (Agents создаёт с rev = 0). */
  createAgent(agent: AgentRecord): Promise<void>;
  getAgent(id: string): Promise<AgentRecord | undefined>;
  /** Условная запись по rev; false — запись изменилась или её нет. */
  updateAgent(agent: AgentRecord): Promise<boolean>;
  listAgents(): Promise<AgentRecord[]>;
  /** Удалить запись агента и его историю метрик; false — её не было. */
  deleteAgent(id: string): Promise<boolean>;

  createJob(job: JobRecord): Promise<void>;
  getJob(id: string): Promise<JobRecord | undefined>;
  /** Условная запись по rev; false — запись изменилась или её нет. */
  updateJob(job: JobRecord): Promise<boolean>;
  /** Новые первыми; limit и after — постраничное чтение (PageFilter). */
  listJobs(filter?: JobFilter): Promise<JobRecord[]>;

  createCommand(cmd: CommandRecord): Promise<void>;
  getCommand(id: string): Promise<CommandRecord | undefined>;
  /** Условная запись по rev; false — запись изменилась или её нет. */
  updateCommand(cmd: CommandRecord): Promise<boolean>;
  /** Новые первыми; limit и after — постраничное чтение (PageFilter). */
  listCommands(filter?: CommandFilter): Promise<CommandRecord[]>;

  /**
   * Уборка: завершённые задачи и команды с finishedAt раньше границы, события с at раньше
   * границы (нет границы или 0 — не трогать). Результат — сколько записей удалено.
   */
  prune(opts: StorePruneOptions): Promise<number>;

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
  private commands = new Map<string, CommandRecord>();
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
    return put(this.agents, agent);
  }
  async listAgents() {
    return [...this.agents.values()].map(clone);
  }
  async deleteAgent(id: string) {
    this.metrics.delete(id);
    return this.agents.delete(id);
  }

  async createJob(job: JobRecord) {
    this.jobs.set(job.id, clone(job));
    trim(this.jobs, this.keepJobs, jobFinished);
  }
  async getJob(id: string) {
    const j = this.jobs.get(id);
    return j && clone(j);
  }
  async updateJob(job: JobRecord) {
    return put(this.jobs, job);
  }
  async listJobs(f: JobFilter = {}) {
    return page(
      this.jobs,
      f,
      (j) => has(f.status, j.status) && (!f.queue || j.queue === f.queue) && (!f.agentId || j.agentId === f.agentId),
    );
  }

  async createCommand(cmd: CommandRecord) {
    this.commands.set(cmd.id, clone(cmd));
    trim(this.commands, this.keepCommands, commandFinished);
  }
  async getCommand(id: string) {
    const c = this.commands.get(id);
    return c && clone(c);
  }
  async updateCommand(cmd: CommandRecord) {
    return put(this.commands, cmd);
  }
  async listCommands(f: CommandFilter = {}) {
    return page(this.commands, f, (c) => has(f.status, c.status) && (!f.agentId || c.agentId === f.agentId));
  }

  async prune(opts: StorePruneOptions) {
    let removed = 0;
    const before = (v: number | undefined) => (v && v > 0 ? v : undefined);
    const jobs = before(opts.jobsBefore);
    if (jobs !== undefined)
      for (const [id, j] of this.jobs)
        if (jobFinished(j) && (j.finishedAt ?? 0) < jobs && this.jobs.delete(id)) removed++;
    const commands = before(opts.commandsBefore);
    if (commands !== undefined)
      for (const [id, c] of this.commands)
        if (commandFinished(c) && (c.finishedAt ?? 0) < commands && this.commands.delete(id)) removed++;
    const events = before(opts.eventsBefore);
    if (events !== undefined) {
      const left = this.events.filter((e) => e.at >= events);
      removed += this.events.length - left.length;
      this.events = left;
    }
    return removed;
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

/**
 * Условная запись (проверка и запись — без await между ними): rev в хранилище равен rev
 * записи — сохранить с rev + 1. Записи нет или rev другой — false.
 */
function put<T extends { id: string; rev: number }>(map: Map<string, T>, rec: T): boolean {
  const cur = map.get(rec.id);
  if (!cur || (cur.rev ?? 0) !== (rec.rev ?? 0)) return false;
  rec.rev = (rec.rev ?? 0) + 1;
  map.set(rec.id, clone(rec));
  return true;
}

/** Записи по условию, новые первыми, с постраничным чтением (map — в порядке создания). */
function page<T extends { id: string }>(map: Map<string, T>, f: PageFilter, match: (v: T) => boolean): T[] {
  const all = [...map.values()];
  let end = all.length; // записи до end (старее after)
  if (f.after) {
    end = all.findIndex((v) => v.id === f.after);
    if (end < 0) return [];
  }
  const limit = f.limit && f.limit > 0 ? Math.floor(f.limit) : Infinity;
  const out: T[] = [];
  for (let i = end - 1; i >= 0 && out.length < limit; i--) if (match(all[i])) out.push(clone(all[i]));
  return out;
}

const stateKey = (domain: string, agentId?: string) => `${domain}\u0000${agentId ?? ""}`;

function trim<T>(map: Map<string, T>, keep: number, removable: (v: T) => boolean): void {
  if (map.size <= keep) return;
  for (const [id, v] of map) {
    if (map.size <= keep) return;
    if (removable(v)) map.delete(id);
  }
}
