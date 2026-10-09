// Хранилище серверной части: интерфейс Store. Свой Store (Postgres и т. п.) — общий для
// нескольких процессов бэкенда; как написать свой — sdk/docs/store.md.
import type { AgentRecord, ConfigRecord } from "../model/types";

/**
 * Хранилище: агенты и настройки воркеров. Методы асинхронны и могут вызываться одновременно, в
 * том числе из нескольких процессов с общим Store.
 */
export interface Store {
  /** Новая запись как есть (rev = 0). */
  createAgent(agent: AgentRecord): Promise<void>;
  getAgent(id: string): Promise<AgentRecord | undefined>;
  listAgents(): Promise<AgentRecord[]>;
  /**
   * Условная запись: только если в хранилище `rev` такой же, как у переданной; сохраняется с
   * `rev + 1` (у переданной `rev` тоже становится `rev + 1`). false — запись изменилась с чтения
   * или её нет: Agents перечитает и повторит.
   */
  updateAgent(agent: AgentRecord): Promise<boolean>;
  /** Удалить агента и его настройки; false — его не было. */
  deleteAgent(id: string): Promise<boolean>;

  /**
   * Записать значение ключа настроек. Версия — max(счётчик ключа + 1, minVersion): счётчик
   * хранится и после deleteConfig, поэтому версия по ключу только растёт. Две записи
   * одновременно получают разные версии.
   */
  setConfig(
    agentId: string,
    worker: string,
    key: string,
    data: unknown,
    opts?: SetConfigOptions,
  ): Promise<ConfigRecord>;
  /** Все ключи агента (без удалённых). */
  listConfigs(agentId: string): Promise<ConfigRecord[]>;
  /** Удалить значение (счётчик версий остаётся); false — ключа не было. */
  deleteConfig(agentId: string, worker: string, key: string): Promise<boolean>;
}

export interface SetConfigOptions {
  /** Кто изменил (agents.by). */
  actor?: string;
  /** Версия не меньше этой: на агенте уже есть версия новее счётчика (хранилище сброшено). */
  minVersion?: number;
}
