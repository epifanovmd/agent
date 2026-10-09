// Строгие возможности воркера на сервере: capabilities из манифеста, подписки на события
// (subscribeEvents, waitEvent), проверка data событий (validateEvents), тела fetch и запросов
// воркера (validateRequests), запросы воркера к серверу (onWorkerRequest).
import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";

import {
  type AgentEvent,
  AgentsError,
  type AgentsOptions,
  type InvalidEvent,
  type WorkerRequest,
} from "../src/server/index";
import {
  type Creds,
  enroll,
  FakeAgent,
  samples,
  sleep,
  startServer,
  type TestServer,
  until,
} from "./helpers";

/** Манифест воркера report из образца worker.manifest. */
const manifest = () =>
  structuredClone(samples().get("worker.manifest")!.response!.body) as Record<
    string,
    unknown
  >;

let running: TestServer[] = [];

afterEach(async () => {
  for (const s of running) await s.close();
  running = [];
});

interface Stand {
  s: TestServer;
  c: Creds;
  fa: FakeAgent;
  logged: string[];
}

/** Сервер и агент, у которого воркер report с манифестом из образца. */
const stand = async (opts: AgentsOptions = {}): Promise<Stand> => {
  const logged: string[] = [];
  const s = await startServer({ log: msg => logged.push(msg), ...opts });

  running.push(s);
  const c = await enroll(s.url);
  const fa = await FakeAgent.connect(s.ws, c, { configs: {} });

  fa.stream(
    "status",
    { workers: [{ name: "report", state: "running", manifest: manifest() }] },
    1,
  );
  await fa.ackSeq(1);

  return { s, c, fa, logged };
};

/** Событие воркера report; итог — ack. */
const event = (fa: FakeAgent, id: string, type: string, data?: unknown) => {
  fa.send({
    type: "event",
    id,
    data: { worker: "report", type, at: 1, ...(data ? { data } : {}) },
  });

  return fa.ackOf(id);
};

describe("возможности воркера", () => {
  it("capabilities: маршруты, события, задачи, ключи и запросы со схемами", async () => {
    const { s, c } = await stand();
    const m = manifest() as Record<string, unknown[]>;
    const caps = await s.agents.capabilities(c.agentId, "report");

    assert.deepEqual(caps?.routes, m.routes);
    assert.deepEqual(caps?.events, m.events);
    assert.deepEqual(caps?.jobs, m.jobs);
    assert.deepEqual(caps?.configs, m.configs);
    assert.deepEqual(caps?.requests, m.requests);
    assert.equal(caps?.version, "1.4.0");
    assert.equal(await s.agents.capabilities(c.agentId, "other"), undefined);
    await assert.rejects(s.agents.capabilities("nope", "report"), {
      code: "AGENT_NOT_FOUND",
    });
  });

  it("subscribeEvents: только объявленный тип, после onEvent; отписка", async () => {
    const order: string[] = [];
    const { s, c, fa } = await stand({
      onEvent: e => void order.push(`onEvent ${e.type}`),
    });
    const got: AgentEvent[] = [];

    await assert.rejects(
      s.agents.subscribeEvents(
        { agentId: c.agentId, worker: "report", type: "report.lost" },
        () => {},
      ),
      { code: "EVENT_UNDECLARED", status: 409 },
    );
    await assert.rejects(
      s.agents.subscribeEvents({ worker: "Report", type: "x" }, () => {}),
      { code: "MESSAGE_INVALID" },
    );
    const off = await s.agents.subscribeEvents(
      { agentId: c.agentId, worker: "report", type: "report.sent" },
      e => {
        order.push(`подписка ${e.type}`);
        got.push(e);
        throw new Error("подписчик упал");
      },
    );

    await event(fa, "e1", "report.sent", { target: "x" });
    await until(() => got.length === 1);
    assert.deepEqual(order, ["onEvent report.sent", "подписка report.sent"]);
    assert.deepEqual(got[0].data, { target: "x" });
    off();
    await event(fa, "e2", "report.sent", { target: "y" });
    await sleep(20);
    assert.equal(got.length, 1);
  });

  it("subscribeEvents без агента: тип сверяется по первому событию — предупреждение", async () => {
    const { s, fa, logged } = await stand();
    const got: AgentEvent[] = [];

    await s.agents.subscribeEvents(
      { worker: "report", type: "report.lost" },
      e => void got.push(e),
    );
    await s.agents.subscribeEvents(
      { worker: "report", type: "report.sent" },
      e => void got.push(e),
    );
    await event(fa, "e1", "report.sent", { target: "x" });
    await event(fa, "e2", "report.sent", { target: "x" });
    await until(() => got.length === 2);
    assert.equal(
      logged.filter(
        m => m === "подписка на событие, которого нет в манифесте воркера",
      ).length,
      1,
    );
  });

  it("waitEvent: первое подходящее событие, TIMEOUT, CANCELLED", async () => {
    const { s, c, fa } = await stand();
    const p = s.agents.waitEvent(c.agentId, "report", "report.sent", {
      match: e => (e.data as { target?: string }).target === "b",
    });

    await sleep(10);
    await event(fa, "e1", "report.sent", { target: "a" });
    await event(fa, "e2", "report.sent", { target: "b" });
    assert.equal((await p).id, "e2");
    await assert.rejects(
      s.agents.waitEvent(c.agentId, "report", "report.sent", { timeoutMs: 30 }),
      { code: "TIMEOUT", status: 504 },
    );
    const ac = new AbortController();
    const w = s.agents.waitEvent(c.agentId, "report", "report.sent", {
      signal: ac.signal,
    });

    await sleep(10);
    ac.abort();
    await assert.rejects(w, { code: "CANCELLED" });
    await assert.rejects(
      s.agents.waitEvent(c.agentId, "report", "report.lost"),
      { code: "EVENT_UNDECLARED" },
    );
  });

  for (const mode of ["log", "reject"] as const) {
    it(`validateEvents: ${mode}`, async () => {
      const seen: AgentEvent[] = [];
      const invalid: InvalidEvent[] = [];
      const { s, fa } = await stand({
        validateEvents: mode,
        onEvent: e => void seen.push(e),
      });

      s.agents.on("invalidEvent", e => invalid.push(e));
      await event(fa, "ok", "report.sent", { target: "x", items: 1 });
      await event(fa, "bad", "report.sent", { items: "много" });
      assert.deepEqual(
        invalid.map(e => [e.event.id, e.rejected]),
        [["bad", mode === "reject"]],
      );
      assert.deepEqual(invalid[0].problems, [
        "нет поля target",
        "items: ожидается целое число",
      ]);
      assert.deepEqual(
        seen.map(e => e.id),
        mode === "reject" ? ["ok"] : ["ok", "bad"],
      );
      // Событие задачи схемой не проверяется.
      await event(fa, "job", "job.done", { jobId: "j1" });
      assert.equal(invalid.length, 1);
    });
  }

  it("validateRequests: тело fetch по routes[].request", async () => {
    const { s, c, fa } = await stand({ validateRequests: true });

    await assert.rejects(
      s.agents.fetch(c.agentId, "report", "/reports/7/send", {
        method: "POST",
        body: JSON.stringify({ to: 1 }),
      }),
      { code: "REQUEST_INVALID", status: 400, message: /to: ожидается строка/ },
    );
    await assert.rejects(
      s.agents.fetch(c.agentId, "report", "/reports/7/send?x=1", {
        method: "POST",
        body: "не json",
      }),
      { code: "REQUEST_INVALID", message: /не JSON/ },
    );
    await assert.rejects(
      s.agents.fetch(c.agentId, "report", "/reports/7/send", {
        method: "POST",
      }),
      { code: "REQUEST_INVALID", message: /ожидается объект/ },
    );
    const p = s.agents.fetch(c.agentId, "report", "/reports/7/send", {
      method: "POST",
      body: new TextEncoder().encode(JSON.stringify({ to: "ops" })),
    });
    const req = await fa.next("fetch");

    fa.send({
      type: "fetch.head",
      re: req.id,
      data: { status: 204, headers: {} },
    });
    fa.send({ type: "fetch.end", re: req.id, data: {} });
    assert.equal((await p).status, 204);
  });
});

describe("запросы воркера к серверу", () => {
  /** Запрос воркера report; итог — data request.result. */
  const ask = async (
    fa: FakeAgent,
    id: string,
    data: Record<string, unknown>,
  ) => {
    fa.send({ type: "request", id, data: { worker: "report", ...data } });

    return (await fa.next("request.result", e => e.re === id)).data;
  };

  it("onWorkerRequest: data, ошибки обработчика, предел ответа, проверка data", async () => {
    const seen: WorkerRequest[] = [];
    const { c, fa } = await stand({
      validateRequests: true,
      onWorkerRequest: async req => {
        seen.push(req);
        const report = (req.data as { report?: string } | undefined)?.report;

        if (report === "denied")
          throw new AgentsError("EXAMPLE_DENIED", "нельзя", 403);
        if (report === "broken") throw new Error("сломалось");
        if (report === "huge") return "x".repeat(5 * 1024 * 1024);
        if (report === "none") return undefined;

        return [`${report}@example.com`];
      },
    });

    assert.deepEqual(
      await ask(fa, "q1", {
        type: "report.recipients",
        data: { report: "ops" },
        timeoutMs: 5000,
      }),
      { ok: true, data: ["ops@example.com"] },
    );
    assert.deepEqual(
      [seen[0].agentId, seen[0].worker, seen[0].type, seen[0].timeoutMs],
      [c.agentId, "report", "report.recipients", 5000],
    );
    assert.equal(seen[0].agent.id, c.agentId);
    assert.equal(seen[0].signal.aborted, false);
    const fail = async (id: string, report: unknown) =>
      (await ask(fa, id, { type: "report.recipients", data: { report } })).error
        ?.code;

    assert.equal(await fail("q2", "denied"), "EXAMPLE_DENIED");
    assert.equal(await fail("q3", "broken"), "REQUEST_FAILED");
    assert.equal(await fail("q4", "huge"), "BODY_TOO_LARGE");
    assert.equal(await fail("q5", 7), "REQUEST_INVALID");
    assert.deepEqual(
      await ask(fa, "q6", {
        type: "report.recipients",
        data: { report: "none" },
      }),
      { ok: true },
    );
    assert.equal(
      (await ask(fa, "q7", { type: "Bad" })).error?.code,
      "MESSAGE_INVALID",
    );
  });

  it("срок и разрыв связи прерывают обработчик (signal)", async () => {
    const aborted: string[] = [];
    const { fa } = await stand({
      onWorkerRequest: req =>
        new Promise(resolve =>
          req.signal.addEventListener("abort", () => {
            aborted.push(req.id);
            resolve("поздно");
          }),
        ),
    });

    // Обработчик завершился уже после срока — воркер получает TIMEOUT, а не его итог.
    const slow = await ask(fa, "slow", {
      type: "report.recipients",
      timeoutMs: 30,
    });

    assert.equal(slow.ok, false);
    assert.equal(slow.error?.code, "TIMEOUT");
    assert.ok(aborted.includes("slow"));
    fa.send({
      type: "request",
      id: "gone",
      data: { worker: "report", type: "report.recipients" },
    });
    await sleep(20);
    fa.ws.terminate();
    await until(() => aborted.includes("gone"));
  });
});
