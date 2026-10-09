// MemoryStore — Store в памяти процесса: разработка, тесты, один процесс или несколько объектов
// Agents в одном процессе.
import type { AgentRecord, ConfigRecord } from "../model/types";
import type { SetConfigOptions, Store } from "./store";

const clone = <T>(v: T): T => structuredClone(v);
const configId = (agentId: string, worker: string, key: string) =>
  `${agentId}\n${worker}\n${key}`;

/** Store в памяти: записи копируются на входе и выходе, как у настоящей БД. */
export class MemoryStore implements Store {
  private agents = new Map<string, AgentRecord>();
  private configs = new Map<string, ConfigRecord>();
  /** Счётчики версий ключей: переживают deleteConfig. */
  private versions = new Map<string, number>();

  async createAgent(agent: AgentRecord): Promise<void> {
    if (this.agents.has(agent.id))
      throw new Error(`агент ${agent.id} уже есть`);
    this.agents.set(agent.id, clone(agent));
  }

  async getAgent(id: string): Promise<AgentRecord | undefined> {
    const a = this.agents.get(id);

    return a && clone(a);
  }

  async listAgents(): Promise<AgentRecord[]> {
    return [...this.agents.values()].map(clone);
  }

  async updateAgent(agent: AgentRecord): Promise<boolean> {
    const cur = this.agents.get(agent.id);

    if (!cur || cur.rev !== agent.rev) return false;
    agent.rev += 1;
    this.agents.set(agent.id, clone(agent));

    return true;
  }

  async deleteAgent(id: string): Promise<boolean> {
    if (!this.agents.delete(id)) return false;
    const mine = (k: string) => k.startsWith(`${id}\n`);

    for (const k of this.configs.keys()) if (mine(k)) this.configs.delete(k);
    for (const k of this.versions.keys()) if (mine(k)) this.versions.delete(k);

    return true;
  }

  async setConfig(
    agentId: string,
    worker: string,
    key: string,
    data: unknown,
    opts: SetConfigOptions = {},
  ): Promise<ConfigRecord> {
    const id = configId(agentId, worker, key);
    const version = Math.max(
      (this.versions.get(id) ?? 0) + 1,
      opts.minVersion ?? 0,
    );
    const rec: ConfigRecord = {
      agentId,
      worker,
      key,
      version,
      data: clone(data),
      updatedAt: Date.now(),
    };

    if (opts.actor) rec.actor = opts.actor;
    this.versions.set(id, version);
    this.configs.set(id, rec);

    return clone(rec);
  }

  async listConfigs(agentId: string): Promise<ConfigRecord[]> {
    return [...this.configs.values()]
      .filter(c => c.agentId === agentId)
      .map(clone);
  }

  async deleteConfig(
    agentId: string,
    worker: string,
    key: string,
  ): Promise<boolean> {
    return this.configs.delete(configId(agentId, worker, key));
  }
}
