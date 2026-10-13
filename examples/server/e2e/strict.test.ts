// Строгие возможности воркеров: агент пропускает только маршруты и задачи из манифеста, воркер
// спрашивает бэкенд (запрос echo.lookup), сервер отдаёт манифест воркера и сам проверяет тела
// запросов и data событий по схемам из него (schemaProblems).
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import {
  type AgentEvent,
  schemaProblems,
  type WorkerManifest,
} from "agent-sdk/server";

import { Stand, waitFor } from "./stand";

const ECHOES = ["echo", "node-echo"];
const NAME = "e2e-strict";

/** Ответ API стенда с ошибкой: статус и код. */
const apiError = async (res: Response) => ({
  status: res.status,
  code: ((await res.json()) as { code: string }).code,
});

describe("строгие возможности воркеров", () => {
  let s: Stand;

  before(async () => {
    s = await Stand.start({ name: NAME, validate: true });
  });
  after(() => s?.close());

  it("необъявленный маршрут — ROUTE_UNDECLARED, до воркера не доходит", async () => {
    for (const w of ECHOES) {
      assert.deepEqual(await apiError(await s.fetchWorker(w, "/nope")), {
        status: 404,
        code: "ROUTE_UNDECLARED",
      });
      assert.deepEqual(
        await apiError(await s.fetchWorker(w, "/echo", { method: "GET" })),
        { status: 404, code: "ROUTE_UNDECLARED" },
      );
    }
    assert.deepEqual(await apiError(await s.fetchWorker("sysinfo", "/nope")), {
      status: 404,
      code: "ROUTE_UNDECLARED",
    });
    // netprobe объявляет GET /results: до первого круга воркер сам отвечает 404.
    const res = await s.fetchWorker("netprobe", "/results");

    assert.ok([200, 404].includes(res.status));
    if (res.status === 404)
      assert.equal(
        ((await res.json()) as { code?: string }).code,
        undefined,
        "404 — ответ воркера, а не агента",
      );
  });

  it("тип задачи не из манифеста — JOB_UNKNOWN от агента", async () => {
    for (const w of ECHOES) {
      const res = await s.fetchWorker(w, "/jobs", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ type: "echo.none", jobId: "j-strict" }),
      });

      assert.deepEqual(await apiError(res), {
        status: 409,
        code: "JOB_UNKNOWN",
      });
    }
  });

  it("тело запроса не по схеме маршрута — REQUEST_INVALID на сервере", async () => {
    const res = await s.fetchWorker("echo", "/echo", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ text: 5 }),
    });

    assert.deepEqual(await apiError(res), {
      status: 400,
      code: "REQUEST_INVALID",
    });
  });

  it("запрос воркера к бэкенду: echo.quick с lookup — префикс от сервера", async () => {
    for (const w of ECHOES) {
      const r = await s.job(w, {
        type: "echo.quick",
        data: { text: "привет", lookup: true },
      });

      assert.deepEqual(
        [r.status, r.result],
        [200, { text: `${NAME}: ПРИВЕТ`, prefix: `${NAME}: ` }],
        w,
      );
    }
  });

  it("событие echo.started после перезапуска воркера; data — по схеме манифеста", async () => {
    for (const w of ECHOES) {
      const since = Date.now();

      await s.api("POST", `/api/agents/${s.agentId}/workers/${w}/restart`);
      const e = await waitFor(`${w}: echo.started`, async () =>
        (
          await s.api<AgentEvent[]>(
            "GET",
            `/api/events?agentId=${s.agentId}&worker=${w}&type=echo.started`,
          )
        ).find(x => x.receivedAt >= since),
      );

      assert.equal(typeof (e.data as { pid?: unknown }).pid, "number", w);
      assert.deepEqual(
        schemaProblems(await s.agent(), w, { event: e.type }, e.data),
        [],
        w,
      );
    }
  });

  it("манифест воркера: маршруты, события, задачи и запросы со схемами", async () => {
    const m = await s.api<WorkerManifest>(
      "GET",
      `/api/agents/${s.agentId}/workers/echo/manifest`,
    );

    assert.deepEqual(
      m.requests?.map(r => r.type),
      ["echo.lookup"],
    );
    assert.equal(typeof m.requests?.[0].schema, "object");
    assert.equal(typeof m.routes?.[0].request, "object");
    assert.equal(typeof m.events?.[0].schema, "object");
    assert.deepEqual(
      m.jobs?.map(j => j.type),
      ["echo.quick", "echo.long"],
    );
    const res = await fetch(
      `${s.url}/api/agents/${s.agentId}/workers/nope/manifest`,
    );

    assert.equal(res.status, 404);
  });
});
