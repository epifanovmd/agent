// Регистрация и ключи агентов: регистрация с пределом неудач, вход по ключу, смена ключа,
// отзыв и удаление.
import { createHash, randomBytes, timingSafeEqual } from "node:crypto";

import { LRUCache } from "lru-cache";
import { z } from "zod";

import type { Context } from "../core/context";
import { AgentsError, notFound, valid } from "../core/errors";
import type { EnrollInfo, EnrollVerdict } from "../core/options";
import { publicAgent } from "../model/public-agent";
import type { Agent, AgentRecord } from "../model/types";
import { parse, parseEnroll } from "../protocol/checks";
import { newId } from "../protocol/messages";
import { labelsSchema } from "../protocol/schemas";

/** Решение хука enroll объектом: метки от сервера и имя агента (пустое — из запроса). */
const grantSchema = z.object({
  labels: labelsSchema,
  name: z.string().optional().catch(undefined),
});

/** Адресов с неудачными регистрациями помнить не больше. */
const MAX_CLIENTS = 10_000;

/** Ключ агента после регистрации. */
interface Credentials {
  agentId: string;
  secret: string;
}

/** Закрыть сессию агента в этом процессе (отзыв, удаление). */
type DropSession = (agentId: string, reason: string) => void;

export class Enrollment {
  private readonly ctx: Context;
  private readonly dropSession: DropSession;
  private readonly failures: FailureLimiter;

  constructor(ctx: Context, dropSession: DropSession) {
    this.ctx = ctx;
    this.dropSession = dropSession;
    this.failures = new FailureLimiter(
      ctx.settings.enrollFailureLimit,
      ctx.settings.enrollFailureWindowMs,
    );
  }

  /**
   * Регистрация: тело (или функция, читающая его) → ключ агента. Неудачи с адреса remote
   * считаются; больше enrollFailureLimit за окно — RATE_LIMITED.
   */
  async enroll(body: unknown, remote = "*"): Promise<Credentials> {
    const client = remote || "*";

    this.failures.check(client);
    try {
      const b =
        typeof body === "function"
          ? await (body as () => Promise<unknown>)()
          : body;

      return await this.register(b, client);
    } catch (e) {
      this.failures.add(client);
      throw e;
    }
  }

  /** Агент по `Authorization: Agent <id>.<secret>`; новый секрет после смены ключа становится основным. */
  async authenticate(
    header: string | undefined,
  ): Promise<AgentRecord | undefined> {
    const raw = header?.startsWith("Agent ")
      ? header.slice(6).trim()
      : undefined;
    const dot = raw?.indexOf(".") ?? -1;

    if (!raw || dot <= 0) return undefined;
    const id = raw.slice(0, dot);
    const hash = sha256(raw.slice(dot + 1));
    const agent = await this.ctx.store.getAgent(id);

    if (!agent || agent.revoked) return undefined;
    if (safeEqual(hash, agent.secretHash)) return agent;
    if (!agent.pendingSecretHash || !safeEqual(hash, agent.pendingSecretHash))
      return undefined;
    // Вход с новым секретом: он становится основным.
    let ok = false;
    const r = await this.ctx.mutate(id, a => {
      ok =
        !a.revoked &&
        (safeEqual(hash, a.secretHash) ||
          (!!a.pendingSecretHash && safeEqual(hash, a.pendingSecretHash)));
      if (!ok || safeEqual(hash, a.secretHash)) return false;
      a.secretHash = a.pendingSecretHash!;
      delete a.pendingSecretHash;

      return true;
    });

    if (r.changed)
      this.ctx.log("ключ агента сменён", { agent: r.agent!.name, id });

    return ok ? r.agent : undefined;
  }

  /** Принять хеш нового секрета из итога agent.rotateKey; агента нет — false. */
  async acceptKey(agentId: string, hash: string): Promise<boolean> {
    const { agent } = await this.ctx.mutate(agentId, a => {
      if (a.revoked || a.secretHash === hash || a.pendingSecretHash === hash)
        return false;
      a.pendingSecretHash = hash;

      return true;
    });

    return !!agent;
  }

  async revoke(actor: string, agentId: string): Promise<Agent> {
    const { agent } = await this.ctx.mutate(agentId, a => {
      if (a.revoked) return false;
      Object.assign(a, { revoked: true, online: false });
      delete a.session;
      delete a.pendingSecretHash;
      a.alerts = [];

      return true;
    });

    if (!agent) throw notFound();
    this.dropSession(agentId, "ключ отозван");
    this.ctx.audit(actor, "agent.revoke", agentId);
    this.ctx.change(agentId, "revoke");
    this.ctx.emitAgent(agent);

    return publicAgent(agent);
  }

  async remove(actor: string, agentId: string): Promise<void> {
    if (!(await this.ctx.store.deleteAgent(agentId))) throw notFound();
    this.dropSession(agentId, "агент удалён");
    this.ctx.audit(actor, "agent.delete", agentId);
    this.ctx.change(agentId, "delete");
  }

  /** Проверить тело и токен, создать запись агента. */
  private async register(body: unknown, address: string): Promise<Credentials> {
    const { token, name, labels, host } = valid(parseEnroll(body));
    const verdict = await this.verdict(token, {
      name,
      labels: { ...labels },
      host,
      address,
    });

    if (!verdict)
      throw new AgentsError(
        "ENROLL_DENIED",
        "токен регистрации не принят",
        401,
      );
    const grant: z.output<typeof grantSchema> =
      verdict === true
        ? { labels: {} }
        : valid(parse(grantSchema, verdict, "enroll"));
    const granted = grant.labels;
    const secret = randomBytes(24).toString("base64url");
    const agent: AgentRecord = {
      id: newId(),
      name: grant.name || name,
      grantedLabels: granted,
      labels: { ...labels, ...granted },
      revoked: false,
      online: false,
      enrolledAt: Date.now(),
      secretHash: sha256(secret),
      lastSeq: 0,
      configs: {},
      alerts: [],
      rev: 0,
    };

    if (address !== "*") agent.address = address;
    await this.ctx.store.createAgent(agent);
    this.ctx.log("агент зарегистрирован", { agent: agent.name, id: agent.id });
    this.ctx.audit("", "agent.enroll", agent.id, { name: agent.name });
    this.ctx.emitAgent(agent);

    return { agentId: agent.id, secret };
  }

  /** Решение по токену: хук enroll или сравнение с enrollToken. */
  private async verdict(
    token: string,
    info: EnrollInfo,
  ): Promise<EnrollVerdict> {
    const { enroll, enrollToken } = this.ctx.settings;

    if (enroll) return enroll(token, info);

    return !!enrollToken && safeEqual(token, enrollToken);
  }
}

/**
 * Неудачные попытки по адресам за скользящее окно; limit 0 — без предела. Адрес забывается через
 * окно после последней неудачи; адресов больше MAX_CLIENTS — забываются давние.
 */
class FailureLimiter {
  private readonly times: LRUCache<string, number[]>;
  private readonly limit: number;
  private readonly windowMs: number;

  constructor(limit: number, windowMs: number) {
    this.limit = limit;
    this.windowMs = windowMs;
    this.times = new LRUCache({
      max: MAX_CLIENTS,
      ...(windowMs > 0 && { ttl: Math.ceil(windowMs) }),
    });
  }

  /** Предел исчерпан — RATE_LIMITED с Retry-After. */
  check(client: string): void {
    if (this.limit <= 0) return;
    const now = Date.now();
    const recent = this.recent(client, now);

    if (recent.length < this.limit) return;
    const retry = Math.max(
      1,
      Math.ceil(
        (recent[recent.length - this.limit] + this.windowMs - now) / 1000,
      ),
    );

    throw new AgentsError(
      "RATE_LIMITED",
      "много неудачных регистраций, повторите позже",
      429,
      retry,
    );
  }

  add(client: string): void {
    if (this.limit <= 0) return;
    const now = Date.now();

    this.times.set(client, [...this.recent(client, now), now]);
  }

  /** Неудачи адреса в окне. */
  private recent(client: string, now: number): number[] {
    return (this.times.get(client) ?? []).filter(t => t > now - this.windowMs);
  }
}

const sha256 = (s: string): string => {
  return createHash("sha256").update(s).digest("hex");
};

const safeEqual = (a: string, b: string): boolean => {
  const x = Buffer.from(a);
  const y = Buffer.from(b);

  return x.length === y.length && timingSafeEqual(x, y);
};
