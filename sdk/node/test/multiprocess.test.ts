// Несколько процессов бэкенда с общим Store: два объекта Agents на своих портах и один
// MemoryStore. Событие change одного процесса передаётся другому как refresh(agentId).
// Наблюдатели и ожидание итогов действий — в памяти процесса.
import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";

import {
  agentsDefaults,
  type ChangeEvent,
  MemoryStore,
} from "../src/server/index";
import {
  enroll,
  FakeAgent,
  startServer,
  type TestServer,
  until,
} from "./helpers";

agentsDefaults.sweepIntervalMs = 50;

let running: TestServer[] = [];

afterEach(async () => {
  for (const s of running) await s.close();
  running = [];
});

/** Два процесса; change одного — refresh в другом (так делает NOTIFY в настоящем бэкенде). */
const pair = async (link = true): Promise<[TestServer, TestServer]> => {
  const store = new MemoryStore();
  const a = await startServer({ store, instanceId: "a", offlineGraceMs: 0 });
  const b = await startServer({ store, instanceId: "b", offlineGraceMs: 0 });

  running.push(a, b);
  if (link) {
    const relay = (to: TestServer) => (e: ChangeEvent) =>
      void to.agents.refresh(e.agentId);

    a.agents.on("change", relay(b));
    b.agents.on("change", relay(a));
  }

  return [a, b];
};

describe("несколько процессов", () => {
  it("fetch и действия — только из процесса с сессией: иначе AGENT_ELSEWHERE", async () => {
    const [a, b] = await pair();
    const c = await enroll(a.url);

    await FakeAgent.connect(a.ws, c);
    const ag = await b.agents.getAgent(c.agentId);

    assert.deepEqual([ag?.online, ag?.session?.instance], [true, "a"]);
    await assert.rejects(b.agents.fetch(c.agentId, "echo", "/x"), {
      code: "AGENT_ELSEWHERE",
      status: 421,
    });
    await assert.rejects(b.agents.restartWorker(c.agentId, "echo"), {
      code: "AGENT_ELSEWHERE",
    });
  });

  it("настройки из другого процесса доходят через refresh", async () => {
    const [a, b] = await pair();
    const c = await enroll(a.url);
    const fa = await FakeAgent.connect(a.ws, c, { configs: {} });
    const rec = await b.agents.setConfig(c.agentId, "echo", "main", { a: 1 });

    assert.equal((await fa.next("config.put")).data.version, rec.version);
    await b.agents.deleteConfig(c.agentId, "echo", "main");
    await fa.next("config.delete");
  });

  it("watch действует в процессе с сессией агента", async () => {
    const [a, b] = await pair();
    const c = await enroll(a.url);
    const fa = await FakeAgent.connect(a.ws, c);

    await b.agents.watch(c.agentId, { metricsIntervalMs: 2000 });
    await a.agents.watch(c.agentId, { metricsIntervalMs: 1000 });
    assert.equal((await fa.next("watch")).data.metricsIntervalMs, 1000);
    assert.deepEqual(await fa.collect("watch", 100), []);
  });

  it("отзыв в другом процессе закрывает сессию 4401", async () => {
    const [a, b] = await pair();
    const c = await enroll(a.url);
    const fa = await FakeAgent.connect(a.ws, c);

    await b.agents.revoke(c.agentId);
    assert.equal((await fa.closed).code, 4401);
  });

  it("переподключение к другому процессу: прежняя сессия закрывается 4409, агент остаётся online", async () => {
    const [a, b] = await pair();
    const c = await enroll(a.url);
    const fa = await FakeAgent.connect(a.ws, c);

    await FakeAgent.connect(b.ws, c);
    assert.equal((await fa.closed).code, 4409);
    await until(
      async () =>
        (await a.agents.getAgent(c.agentId))?.session?.instance === "b",
    );
    assert.equal((await a.agents.getAgent(c.agentId))?.online, true);
  });

  it("поток: принятое одним процессом другой не обрабатывает повторно", async () => {
    const [a, b] = await pair();
    const c = await enroll(a.url);
    let inB = 0;

    b.agents.on("log", () => (inB += 1));
    const fa = await FakeAgent.connect(a.ws, c);
    const log = {
      entries: [{ at: 1, level: "warn", source: "agent", msg: "x" }],
    };

    for (let i = 1; i <= 3; i += 1) fa.stream("log", log, i);
    await fa.ackSeq(3);
    fa.ws.terminate();
    const fb = await FakeAgent.connect(b.ws, c);

    fb.stream("log", log, 2);
    fb.stream("log", log, 3);
    fb.stream("log", log, 4);
    await fb.ackSeq(4);
    assert.equal(inB, 1);
  });

  it("итог действия пришёл в другой процесс: там подтверждается, ожидающий — TIMEOUT", async () => {
    const [a, b] = await pair();
    const c = await enroll(a.url);
    const fa = await FakeAgent.connect(a.ws, c);
    const p = a.agents.restartWorker(c.agentId, "echo", { timeoutMs: 300 });
    let inB = 0;

    b.agents.on("action", () => (inB += 1));
    const act = await fa.next("action");

    fa.ws.terminate();
    const fb = await FakeAgent.connect(b.ws, c);

    fb.send({
      type: "action.result",
      id: "res-1",
      re: act.id,
      data: { ok: true },
    });
    await fb.ackOf("res-1");
    await assert.rejects(p, { code: "TIMEOUT" });
    assert.equal(inB, 0);
  });

  it("процесс с сессией упал — другой отмечает агента offline по offlineAfterMs", async () => {
    const store = new MemoryStore();
    const b = await startServer({
      store,
      instanceId: "b",
      offlineAfterMs: 200,
    });

    running.push(b);
    const c = await enroll(b.url);
    // Запись, которую оставил упавший процесс: online, сессия у него.
    const rec = (await store.getAgent(c.agentId))!;

    Object.assign(rec, {
      online: true,
      lastSeenAt: Date.now(),
      session: { id: "s", instance: "dead", since: 1 },
    });
    assert.ok(await store.updateAgent(rec));
    await until(
      async () => (await b.agents.getAgent(c.agentId))?.online === false,
    );
    assert.deepEqual(
      (await b.agents.listAlerts(c.agentId)).map(x => x.type),
      ["offline"],
    );
  });
});
