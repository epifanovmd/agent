// Манифест воркеров (§12): echo, node-echo и sysinfo описывают себя, агент передаёт
// манифест в status; неверная настройка отклонена сервером стенда по схеме из манифеста до
// отправки агенту.
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import type { ConfigRecord, ConfigStatus } from "agent-sdk/server";

import { ApiError, Stand, VERSION, waitFor, WORKERS } from "./stand";

describe("манифест воркеров", () => {
  let s: Stand;

  before(async () => {
    s = await Stand.start({ name: "e2e-manifest", validate: true });
  });
  after(() => s?.close());

  it("манифест каждого воркера-примера в статусе агента, версия — из манифеста", async () => {
    const a = await waitFor("манифесты в status", async () => {
      const agent = await s.agent();

      return (
        WORKERS.every(
          name => agent.workers.find(w => w.name === name)?.manifest,
        ) && agent
      );
    });
    const keys: Record<string, string> = {
      echo: "settings",
      "node-echo": "settings",
      sysinfo: "banner",
    };

    for (const name of WORKERS) {
      const w = a.workers.find(x => x.name === name)!;

      assert.equal(w.version, "1.0.0", name);
      assert.equal(w.manifest?.version, "1.0.0", name);
      assert.equal(w.manifest?.configs?.[0].key, keys[name], name);
      assert.equal(typeof w.manifest?.configs?.[0].schema, "object", name);
    }
    const routes = ["POST /echo", "GET /stream", "GET /bytes", "POST /hang"];

    for (const name of ["echo", "node-echo"]) {
      const m = a.workers.find(x => x.name === name)!.manifest!;

      assert.deepEqual(
        m.routes?.map(r => `${r.method} ${r.path}`),
        routes,
        name,
      );
      assert.deepEqual(
        m.events?.map(e => e.type),
        ["echo.started"],
        name,
      );
      assert.deepEqual(
        m.jobs?.map(j => j.type),
        ["echo.quick", "echo.long"],
        name,
      );
      assert.equal(typeof m.jobs?.[1].schema, "object", name);
    }
    // Встроенный sysmetrics тоже отвечает GET /manifest: версия — версия агента.
    assert.equal(
      a.workers.find(w => w.name === "sysmetrics")?.manifest?.version,
      VERSION,
    );
  });

  it("неверная настройка отклонена по схеме манифеста до отправки агенту", async () => {
    const path = `/api/agents/${s.agentId}/configs/echo/settings`;
    const err = await s.api("PUT", path, { prefix: 5, extra: 1 }).then(
      () => assert.fail("ждали CONFIG_INVALID"),
      (e: unknown) => e,
    );

    assert.ok(err instanceof ApiError);
    assert.equal(err.status, 400);
    assert.equal(err.code, "CONFIG_INVALID");
    assert.match(err.message, /prefix: ожидается строка/);
    assert.match(err.message, /extra: лишнее поле/);
    const { configs } = await s.api<{
      configs: ConfigRecord[];
      status: ConfigStatus[];
    }>("GET", `/api/agents/${s.agentId}/configs`);

    assert.deepEqual(configs, [], "на сервере не сохранено");
    assert.equal(await s.echo("echo", "a"), "A", "воркер работает по-прежнему");

    // Схема sysinfo из манифеста: text баннера — строка.
    await assert.rejects(
      s.api("PUT", `/api/agents/${s.agentId}/configs/sysinfo/banner`, {
        text: 5,
      }),
      (e: unknown) =>
        e instanceof ApiError &&
        e.code === "CONFIG_INVALID" &&
        /text: ожидается строка/.test(e.message),
    );

    // Верная настройка проходит проверку и применяется воркером.
    const rec = await s.api<ConfigRecord>("PUT", path, { prefix: "> " });

    await waitFor("echo/settings применена", async () => {
      const { status } = await s.api<{ status: ConfigStatus[] }>(
        "GET",
        `/api/agents/${s.agentId}/configs`,
      );

      return status.some(
        c => c.key === "settings" && c.applied === rec.version,
      );
    });
    assert.equal(await s.echo("echo", "a"), "> A");
  });
});
