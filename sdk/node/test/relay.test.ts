// Пересылка вызовов между процессами: два объекта Agents с общим MemoryStore на своих
// HTTP-серверах; relay — настоящий HTTP-запрос на маршрут handleRelay другого процесса. Агент на
// связи с процессом a, вызовы делает процесс b.
import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";

import {
  type AgentsOptions,
  type AuditEntry,
  MemoryStore,
  type RelayRequest,
} from "../src/server/index";
import {
  type Creds,
  enroll,
  FakeAgent,
  RELAY_PATH,
  sleep,
  startServer,
  type TestServer,
  until,
} from "./helpers";

let running: TestServer[] = [];

afterEach(async () => {
  for (const s of running) await s.close();
  running = [];
});

interface Pair {
  a: TestServer;
  b: TestServer;
  c: Creds;
  fa: FakeAgent;
  /** Сколько вызовов b переслал в a. */
  relayed: RelayRequest[];
}

/** Процессы a и b, агент на связи с a. */
const pair = async (
  opts: { a?: AgentsOptions; b?: AgentsOptions } = {},
): Promise<Pair> => {
  const store = new MemoryStore();
  const urls = new Map<string, string>();
  const relayed: RelayRequest[] = [];
  const relay = (instanceId: string, req: RelayRequest) => {
    relayed.push(req);

    return fetch(`${urls.get(instanceId)}${RELAY_PATH}`, {
      method: "POST",
      headers: req.headers,
      body: req.body,
      signal: req.signal,
    });
  };
  const a = await startServer({
    store,
    instanceId: "a",
    relay,
    ...opts.a,
  });
  const b = await startServer({
    store,
    instanceId: "b",
    relay,
    ...opts.b,
  });

  running.push(a, b);
  urls.set("a", a.url).set("b", b.url);
  const c = await enroll(a.url);
  const fa = await FakeAgent.connect(a.ws, c, { configs: {} });

  return { a, b, c, fa, relayed };
};

/** Ответ агента на fetch: заголовок и (необязательно) куски. */
const head = (fa: FakeAgent, re: string, status = 200) =>
  fa.send({
    type: "fetch.head",
    re,
    data: { status, headers: { "content-type": "text/plain" } },
  });
const chunk = (fa: FakeAgent, re: string, text: string) =>
  fa.send({ type: "fetch.chunk", re, data: { data: text, encoding: "utf8" } });
const end = (fa: FakeAgent, re: string, error?: object) =>
  fa.send({ type: "fetch.end", re, data: error ? { error } : {} });

describe("пересылка между процессами (relay)", () => {
  it("fetch: запрос и ответ через процесс с сессией, тело и заголовки", async () => {
    const { b, c, fa, relayed } = await pair();
    const p = b.agents.fetch(c.agentId, "echo", "/echo?x=1", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: '{"text":"привет"}',
      timeoutMs: 5000,
    });
    const req = await fa.next("fetch");

    assert.deepEqual(req.data, {
      worker: "echo",
      method: "POST",
      path: "/echo?x=1",
      headers: { "content-type": "application/json" },
      body: '{"text":"привет"}',
      timeoutMs: 5000,
    });
    head(fa, req.id!, 201);
    chunk(fa, req.id!, "ПРИВЕТ");
    end(fa, req.id!);
    const res = await p;

    assert.equal(res.status, 201);
    assert.equal(res.headers.get("content-type"), "text/plain");
    assert.equal(await res.text(), "ПРИВЕТ");
    assert.equal(relayed.length, 1);
    assert.equal(relayed[0].call.method, "fetch");

    // Двоичное тело запроса доходит как есть.
    const bin = b.agents.fetch(c.agentId, "echo", "/bytes", {
      method: "PUT",
      body: new Uint8Array([0, 1, 254, 255]),
    });
    const breq = await fa.next("fetch");

    assert.deepEqual(
      [...Buffer.from(breq.data.body, "base64")],
      [0, 1, 254, 255],
    );
    head(fa, breq.id!, 204);
    end(fa, breq.id!);
    assert.equal((await bin).status, 204);
  });

  it("fetch: потоковое тело приходит по частям, ошибка после заголовка — ошибка потока", async () => {
    const { b, c, fa } = await pair();
    const res = b.agents.fetch(c.agentId, "echo", "/stream");
    const req = await fa.next("fetch");

    head(fa, req.id!);
    const reader = (await res).body!.getReader();

    chunk(fa, req.id!, "раз");
    assert.equal(
      Buffer.from((await reader.read()).value!).toString(),
      "раз",
      "первый кусок — до конца ответа",
    );
    chunk(fa, req.id!, "два");
    assert.equal(Buffer.from((await reader.read()).value!).toString(), "два");
    end(fa, req.id!, { code: "TIMEOUT", message: "срок истёк" });
    await assert.rejects(reader.read(), { code: "TIMEOUT" });

    // Ошибка до заголовка — отказ промиса с кодом агента.
    const p = b.agents.fetch(c.agentId, "nope", "/x");
    const r2 = await fa.next("fetch");

    end(fa, r2.id!, { code: "WORKER_UNKNOWN", message: "нет воркера" });
    await assert.rejects(p, { code: "WORKER_UNKNOWN", status: 404 });
  });

  it("fetch: отмена до заголовка и после — fetch.cancel агенту, CANCELLED вызывающему", async () => {
    const { b, c, fa } = await pair();
    const before = new AbortController();
    const p = b.agents.fetch(c.agentId, "echo", "/slow", {
      signal: before.signal,
    });
    const req = await fa.next("fetch");

    before.abort();
    await assert.rejects(p, { code: "CANCELLED" });
    assert.equal((await fa.next("fetch.cancel")).re, req.id);

    const after = new AbortController();
    const res = await (async () => {
      const r = b.agents.fetch(c.agentId, "echo", "/stream", {
        signal: after.signal,
      });
      const q = await fa.next("fetch");

      head(fa, q.id!);
      chunk(fa, q.id!, "раз");

      return { r: await r, id: q.id! };
    })();
    const reader = res.r.body!.getReader();

    await reader.read();
    after.abort();
    await assert.rejects(reader.read(), { code: "CANCELLED" });
    assert.equal((await fa.next("fetch.cancel")).re, res.id);
  });

  it("действия и logs: выполняются в процессе с сессией, actor — в аудите там", async () => {
    const { a, b, c, fa } = await pair();
    const audit: AuditEntry[] = [];

    a.agents.on("audit", e => audit.push(e));
    const p = b.agents.by("admin").restartWorker(c.agentId, "echo");
    const act = await fa.next("action");

    assert.deepEqual(act.data, {
      name: "worker.restart",
      args: { name: "echo" },
    });
    fa.send({
      type: "action.result",
      id: "r1",
      re: act.id,
      data: { ok: true, result: { deferred: true, pending: "restart" } },
    });
    assert.deepEqual(await p, {
      deferred: true,
      pending: "restart",
      actionId: act.id,
    });
    assert.deepEqual(
      audit.map(e => [e.actor, e.action]),
      [["admin", "worker.restart"]],
    );

    const logs = b.agents.logs(c.agentId, { worker: "echo", lines: 5 });
    const la = await fa.next("action");
    const entries = [{ at: 1, level: "info", source: "echo", msg: "x" }];

    assert.deepEqual(la.data.args, { worker: "echo", lines: 5 });
    fa.send({
      type: "action.result",
      id: "r2",
      re: la.id,
      data: { ok: true, result: { entries } },
    });
    assert.deepEqual(await logs, entries);

    // Ошибка действия доходит с кодом и статусом.
    const upd = b.agents.updateAgent(c.agentId);

    await assert.rejects(upd, { code: "UPDATE_NOT_AVAILABLE" });
  });

  it("watch и unwatch: наблюдатель — в процессе с сессией", async () => {
    const { b, c, fa } = await pair();
    const ref = await b.agents.watch(c.agentId, { metricsIntervalMs: 1000 });

    assert.equal((await fa.next("watch")).data.metricsIntervalMs, 1000);
    await b.agents.unwatch(c.agentId, ref.id);
    assert.deepEqual((await fa.next("watch")).data, {});
  });

  it("runJob: задача и её итог по событиям — в процессе с сессией", async () => {
    const { b, c, fa } = await pair();

    fa.stream("status", {
      workers: [
        {
          name: "report",
          state: "running",
          manifest: { version: "1", jobs: [{ type: "report.build" }] },
        },
      ],
    });
    await fa.ackSeq(1);
    const p = b.agents.runJob(c.agentId, "report", {
      type: "report.build",
      jobId: "job-1",
      data: { days: 3 },
    });
    const req = await fa.next("fetch");

    assert.deepEqual(JSON.parse(req.data.body), {
      type: "report.build",
      jobId: "job-1",
      data: { days: 3 },
    });
    head(fa, req.id!, 202);
    chunk(fa, req.id!, '{"id":"w1"}');
    end(fa, req.id!);
    fa.send({
      type: "event",
      id: "e1",
      data: {
        worker: "report",
        type: "job.done",
        data: { jobId: "job-1", id: "w1", result: { items: 3 } },
      },
    });
    assert.deepEqual(await p, {
      jobId: "job-1",
      id: "w1",
      state: "done",
      result: { items: 3 },
    });
  });

  it("relaySecret: верный секрет — вызов проходит, неверный — UNAUTHORIZED", async () => {
    const ok = await pair({
      a: { relaySecret: "s3cret" },
      b: { relaySecret: "s3cret" },
    });
    const w = ok.b.agents.watch(ok.c.agentId, { metricsIntervalMs: 2000 });

    await w;
    assert.equal(ok.relayed[0].headers["x-agents-relay-secret"], "s3cret");
    await ok.fa.next("watch");

    const bad = await pair({
      a: { relaySecret: "s3cret" },
      b: { relaySecret: "wrong" },
    });

    await assert.rejects(bad.b.agents.watch(bad.c.agentId), {
      code: "UNAUTHORIZED",
      status: 401,
    });
    await assert.rejects(bad.b.agents.fetch(bad.c.agentId, "echo", "/x"), {
      code: "UNAUTHORIZED",
    });
  });

  it("процесс с сессией недоступен — RELAY_FAILED; агент без связи — AGENT_OFFLINE без пересылки", async () => {
    const store = new MemoryStore();
    const b = await startServer({
      store,
      instanceId: "b",
      relay: () => Promise.reject(new Error("нет связи")),
    });

    running.push(b);
    const c = await enroll(b.url);
    const rec = (await store.getAgent(c.agentId))!;

    await assert.rejects(b.agents.restartWorker(c.agentId, "echo"), {
      code: "AGENT_OFFLINE",
    });
    Object.assign(rec, {
      online: true,
      lastSeenAt: Date.now(),
      session: { id: "s", instance: "a", since: 1 },
    });
    assert.ok(await store.updateAgent(rec));
    await assert.rejects(b.agents.restartWorker(c.agentId, "echo"), {
      code: "RELAY_FAILED",
      status: 502,
    });
  });

  it("агент переподключился к процессу b — вызовы b идут без пересылки", async () => {
    const { b, c, fa, relayed } = await pair();

    fa.ws.terminate();
    const fb = await FakeAgent.connect(b.ws, c, { configs: {} });

    await until(
      async () => (await b.agents.getAgent(c.agentId))?.online === true,
    );
    const p = b.agents.fetch(c.agentId, "echo", "/x");
    const req = await fb.next("fetch");

    head(fb, req.id!, 204);
    end(fb, req.id!);
    assert.equal((await p).status, 204);
    assert.equal(relayed.length, 0);
    await sleep(10);
  });
  it("запрос воркера приходит в процесс с сессией и обрабатывается там", async () => {
    const handledBy: string[] = [];
    const handler =
      (name: string) => (req: { type: string; data?: unknown }) => {
        handledBy.push(name);

        return { by: name, echo: req.data };
      };
    const { c, fa, relayed } = await pair({
      a: { onWorkerRequest: handler("a") },
      b: { onWorkerRequest: handler("b") },
    });

    fa.send({
      type: "request",
      id: "q1",
      data: { worker: "echo", type: "echo.lookup", data: { k: 1 } },
    });
    const res = await fa.next("request.result", e => e.re === "q1");

    assert.deepEqual(res.data, { ok: true, data: { by: "a", echo: { k: 1 } } });
    assert.deepEqual(handledBy, ["a"]);
    assert.equal(relayed.length, 0);
    assert.ok(c.agentId);
  });
});
