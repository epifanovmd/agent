// Обязательный минимум воркера (§12): воркер без GET /manifest не зарегистрирован (invalid с
// причиной, запросы к нему — WORKER_INVALID); манифест появился — агент регистрирует воркер при
// повторной проверке. Агент сверяется с манифестом: необъявленное событие отклонено
// (EVENT_UNDECLARED), необъявленный ключ настроек — failed CONFIG_KEY_UNKNOWN.
import assert from "node:assert/strict";
import { rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";

import type { ConfigRecord, ConfigStatus } from "agent-sdk/server";

import { Stand, waitFor } from "./stand";

describe("регистрация воркера", () => {
  let s: Stand;
  let manifest = "";

  before(async () => {
    manifest = join(
      process.env.TMPDIR ?? "/tmp",
      `bare-manifest-${process.pid}.json`,
    );
    await rm(manifest, { force: true });
    s = await Stand.start({
      name: "e2e-registration",
      workers: [
        {
          name: "bare",
          command: [
            process.execPath,
            join(import.meta.dirname, "bare-worker.mjs"),
          ],
          env: { BARE_MANIFEST: manifest },
          lifecycle: {
            stopTimeout: "5s",
            backoff: { min: "200ms", max: "1s" },
          },
        },
      ],
    });
  });
  after(async () => {
    await s?.close();
    await rm(manifest, { force: true });
  });

  it("без манифеста — invalid с причиной, запрос — WORKER_INVALID", async () => {
    const w = await waitFor("bare: invalid", async () => {
      const w = await s.worker("bare");

      return w?.state === "invalid" && w;
    });

    assert.match(w.message ?? "", /GET \/manifest: HTTP 404/);
    const res = await s.fetchWorker("bare", "/emit?type=bare.done", {
      method: "POST",
    });

    assert.equal(res.status, 502);
    assert.equal(
      ((await res.json()) as { code: string }).code,
      "WORKER_INVALID",
    );
    const alerts = await s.api<{ type: string }[]>(
      "GET",
      `/api/alerts?agentId=${s.agentId}`,
    );

    assert.ok(alerts.some(a => a.type === "workerInvalid"));
  });

  it("манифест появился — воркер зарегистрирован", async () => {
    await writeFile(
      manifest,
      JSON.stringify({
        version: "1.0.0",
        routes: [{ method: "POST", path: "/emit" }],
        events: [{ type: "bare.done" }],
      }),
    );
    const w = await waitFor("bare: running", async () => {
      const w = await s.worker("bare");

      return w?.state === "running" && w;
    });

    assert.equal(w.version, "1.0.0");
    assert.equal(w.message, undefined);
  });

  it("событие не из манифеста отклонено агентом", async () => {
    const emit = async (type: string) => {
      const res = await s.fetchWorker("bare", `/emit?type=${type}`, {
        method: "POST",
      });

      return ((await res.json()) as { status: number }).status;
    };

    assert.equal(await emit("bare.undeclared"), 400);
    assert.equal(await emit("bare.done"), 202);
  });

  it("ключ настроек не из манифеста — failed CONFIG_KEY_UNKNOWN", async () => {
    const rec = await s.api<ConfigRecord>(
      "PUT",
      `/api/agents/${s.agentId}/configs/echo/retention`,
      { days: 7 },
    );
    const c = await waitFor("echo/retention: failed", async () => {
      const { status } = await s.api<{ status: ConfigStatus[] }>(
        "GET",
        `/api/agents/${s.agentId}/configs`,
      );

      return status.find(
        c =>
          c.worker === "echo" &&
          c.key === "retention" &&
          c.state === "failed" &&
          c.version === rec.version,
      );
    });

    assert.equal(c.error?.code, "CONFIG_KEY_UNKNOWN");
    assert.equal(await s.echo("echo", "a"), "A", "echo работает по-прежнему");
  });
});
