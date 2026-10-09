// Строгие возможности воркеров: агент пропускает только маршруты и задачи из манифеста, воркер
// спрашивает бэкенд (запрос echo.lookup), сервер ждёт событие (waitEvent) и описывает воркер
// (capabilities). Сервер проверяет тела запросов и data событий по схемам манифестов.
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import type { InvalidEvent, WorkerCapabilities } from "agent-sdk/server";

import { Stand } from "./stand";

const ECHOES = ["echo", "node-echo"];
const NAME = "e2e-strict";

/** Ответ API стенда с ошибкой: статус и код. */
const apiError = async (res: Response) => ({
  status: res.status,
  code: ((await res.json()) as { code: string }).code,
});

describe("строгие возможности воркеров", () => {
  let s: Stand;
  const invalid: InvalidEvent[] = [];

  before(async () => {
    s = await Stand.start({
      name: NAME,
      server: { validateRequests: true, validateEvents: "reject" },
    });
    s.agents.on("invalidEvent", e => invalid.push(e));
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
        [r.state, r.result],
        ["done", { text: `${NAME}: ПРИВЕТ`, prefix: `${NAME}: ` }],
        w,
      );
    }
  });

  it("waitEvent: событие echo.started после перезапуска воркера; data — по схеме", async () => {
    for (const w of ECHOES) {
      const started = s.agents.waitEvent(s.agentId, w, "echo.started", {
        timeoutMs: 30_000,
      });

      await s.api("POST", `/api/agents/${s.agentId}/workers/${w}/restart`);
      const e = await started;

      assert.equal(typeof (e.data as { pid?: unknown }).pid, "number", w);
    }
    assert.deepEqual(invalid, []);
  });

  it("capabilities: маршруты, события, задачи и запросы со схемами", async () => {
    const caps = await s.api<WorkerCapabilities>(
      "GET",
      `/api/agents/${s.agentId}/workers/echo/capabilities`,
    );

    assert.deepEqual(
      caps.requests.map(r => r.type),
      ["echo.lookup"],
    );
    assert.equal(typeof caps.requests[0].schema, "object");
    assert.equal(typeof caps.routes[0].request, "object");
    assert.equal(typeof caps.events[0].schema, "object");
    assert.deepEqual(
      caps.jobs.map(j => j.type),
      ["echo.quick", "echo.long"],
    );
    const res = await fetch(
      `${s.url}/api/agents/${s.agentId}/workers/nope/capabilities`,
    );

    assert.equal(res.status, 404);
  });
});
