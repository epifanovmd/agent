// Задачи воркера (§12): runJob — POST /jobs через агента, ожидание итога по событиям job.*,
// срок, отмена, проверка типа и data по манифесту, состояние и отмена задачи.
import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";

import type { AgentEvent, AgentsOptions } from "../src/server/index";
import {
  type Creds,
  enroll,
  FakeAgent,
  startServer,
  type TestServer,
} from "./helpers";

const servers: TestServer[] = [];

afterEach(async () => {
  for (const s of servers.splice(0)) await s.close();
});

const MANIFEST = {
  version: "1.0.0",
  jobs: [
    {
      type: "report.build",
      schema: {
        type: "object",
        properties: { days: { type: "integer", minimum: 1 } },
        required: ["days"],
      },
    },
    { type: "report.check" },
  ],
};

/** Сервер и агент на связи, у воркера report — задачи из MANIFEST. */
const setup = async (
  opts: AgentsOptions = {},
): Promise<{ s: TestServer; c: Creds; fa: FakeAgent }> => {
  const s = await startServer(opts);

  servers.push(s);
  const c = await enroll(s.url);
  const fa = await FakeAgent.connect(s.ws, c, { configs: {} });

  fa.stream("status", {
    workers: [{ name: "report", state: "running", manifest: MANIFEST }],
  });
  await fa.ackSeq(1);

  return { s, c, fa };
};

/** Ответ агента на fetch: статус и тело JSON. */
const reply = (fa: FakeAgent, re: string, status: number, body?: unknown) => {
  fa.send({ type: "fetch.head", re, data: { status, headers: {} } });
  if (body !== undefined)
    fa.send({
      type: "fetch.chunk",
      re,
      data: { data: JSON.stringify(body), encoding: "utf8" },
    });
  fa.send({ type: "fetch.end", re, data: {} });
};

let eventSeq = 0;
const jobEvent = (fa: FakeAgent, type: string, data: object) => {
  eventSeq += 1;
  const id = `ev-${eventSeq}`;

  fa.send({ type: "event", id, data: { worker: "report", type, data } });

  return fa.ackOf(id);
};

describe("задачи воркера", () => {
  it("не дождались итога — state: running с последним ходом; событие доходит и до onEvent", async () => {
    const got: AgentEvent[] = [];
    const { s, c, fa } = await setup({ onEvent: e => void got.push(e) });
    const p = s.agents.runJob(c.agentId, "report", {
      type: "report.build",
      jobId: "j1",
      data: { days: 1 },
      timeoutMs: 300,
    });

    reply(fa, (await fa.next("fetch")).id!, 202, { id: "w1" });
    await jobEvent(fa, "job.progress", {
      jobId: "j1",
      id: "w1",
      progress: 0.5,
    });
    assert.deepEqual(await p, {
      jobId: "j1",
      id: "w1",
      state: "running",
      progress: 0.5,
    });
    assert.deepEqual(
      got.map(e => e.type),
      ["job.progress"],
    );
  });

  it("signal: ожидание прервано, долгой задаче — POST /jobs/{id}/cancel", async () => {
    const { s, c, fa } = await setup();
    const abort = new AbortController();
    const p = s.agents.runJob(c.agentId, "report", {
      type: "report.check",
      signal: abort.signal,
    });

    reply(fa, (await fa.next("fetch")).id!, 202, { id: "w2" });
    await new Promise(r => setTimeout(r, 20));
    abort.abort();
    await assert.rejects(p, { code: "CANCELLED" });
    const cancel = await fa.next("fetch");

    assert.deepEqual(
      [cancel.data.method, cancel.data.path],
      ["POST", "/jobs/w2/cancel"],
    );
    reply(fa, cancel.id!, 200, { id: "w2", state: "cancelled" });
  });

  it("отказ воркера — JOB_REJECTED с его статусом и текстом", async () => {
    const { s, c, fa } = await setup();
    const p = s.agents.runJob(c.agentId, "report", {
      type: "report.check",
    });

    reply(fa, (await fa.next("fetch")).id!, 409, { message: "воркер занят" });
    await assert.rejects(p, {
      code: "JOB_REJECTED",
      status: 409,
      message: /воркер занят/,
    });
  });

  it("validateJobs: data не по схеме — JOB_INVALID до отправки; без опции — уходит воркеру", async () => {
    const { s, c, fa } = await setup({ validateJobs: true });

    await assert.rejects(
      s.agents.runJob(c.agentId, "report", {
        type: "report.build",
        data: { days: 0 },
      }),
      { code: "JOB_INVALID", status: 400, message: /days/ },
    );
    await assert.rejects(
      s.agents.runJob(c.agentId, "report", { type: "Bad Type" }),
      { code: "MESSAGE_INVALID" },
    );
    await assert.rejects(
      s.agents.runJob(c.agentId, "nope", { type: "report.build" }),
      { code: "JOB_UNKNOWN" },
    );
    assert.deepEqual(await fa.collect("fetch", 50), []);

    const plain = await setup();
    const q = plain.s.agents.runJob(plain.c.agentId, "report", {
      type: "report.build",
      data: { days: 0 },
    });

    reply(plain.fa, (await plain.fa.next("fetch")).id!, 200, {
      result: { ok: false },
    });
    assert.deepEqual((await q).result, { ok: false });
  });

  it("jobStatus и cancelJob; нет задачи — JOB_NOT_FOUND; actor — в аудите", async () => {
    const { s, c, fa } = await setup();
    const audit: string[] = [];

    s.agents.on("audit", e => audit.push(`${e.actor} ${e.action}`));
    const st = s.agents.by("admin").jobStatus(c.agentId, "report", "w3");
    const req = await fa.next("fetch");

    assert.deepEqual([req.data.method, req.data.path], ["GET", "/jobs/w3"]);
    reply(fa, req.id!, 200, {
      id: "w3",
      state: "running",
      progress: 0.25,
      extra: 1,
    });
    assert.deepEqual(await st, {
      id: "w3",
      state: "running",
      progress: 0.25,
      extra: 1,
    });
    assert.deepEqual(audit, ["admin fetch"]);

    const cancel = s.agents.cancelJob(c.agentId, "report", "w3");

    reply(fa, (await fa.next("fetch")).id!, 200, {
      id: "w3",
      state: "cancelled",
    });
    assert.equal((await cancel).state, "cancelled");

    const missing = s.agents.jobStatus(c.agentId, "report", "zz");

    reply(fa, (await fa.next("fetch")).id!, 404, { message: "нет задачи" });
    await assert.rejects(missing, { code: "JOB_NOT_FOUND", status: 404 });
    await assert.rejects(s.agents.jobStatus(c.agentId, "report", "a/b"), {
      code: "MESSAGE_INVALID",
    });
  });
});
