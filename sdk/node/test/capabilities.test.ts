// Возможности воркера на сервере: проверка значений по схемам манифеста (schemaProblems) и
// запросы воркера к серверу (onWorkerRequest).
import assert from "node:assert/strict";
import { afterEach, describe, it } from "node:test";

import {
  AgentsError,
  type AgentsOptions,
  schemaProblems,
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

describe("проверка по схемам манифеста", () => {
  it("schemaProblems: настройки, события, задачи, запросы и тело маршрута", async () => {
    const { s, c } = await stand();
    const agent = (await s.agents.getAgent(c.agentId))!;
    const check = (
      target: Parameters<typeof schemaProblems>[2],
      data: unknown,
    ) => schemaProblems(agent, "report", target, data);

    assert.deepEqual(
      check({ event: "report.sent" }, { target: "x", items: 1 }),
      [],
    );
    assert.deepEqual(check({ event: "report.sent" }, { items: "много" }), [
      "нет поля target",
      "items: ожидается целое число",
    ]);
    assert.match(
      check(
        { route: { method: "post", path: "/reports/7/send?x=1" } },
        { to: 1 },
      )[0],
      /to: ожидается строка/,
    );
    assert.match(
      check(
        { route: { method: "POST", path: "/reports/7/send" } },
        undefined,
      )[0],
      /ожидается объект/,
    );
    assert.match(
      check({ request: "report.recipients" }, { report: 7 })[0],
      /report/,
    );
    // Нет записи в манифесте, воркера или манифеста — проверять нечем.
    assert.deepEqual(check({ event: "report.lost" }, 1), []);
    assert.deepEqual(check({ config: "nope" }, 1), []);
    assert.deepEqual(check({ job: "nope" }, 1), []);
    assert.deepEqual(
      schemaProblems(agent, "other", { event: "report.sent" }, 1),
      [],
    );
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
      onWorkerRequest: async req => {
        seen.push(req);
        const problems = schemaProblems(
          req.agent,
          req.worker,
          { request: req.type },
          req.data,
        );

        if (problems.length)
          throw new AgentsError("REQUEST_INVALID", problems.join("; "), 400);
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
