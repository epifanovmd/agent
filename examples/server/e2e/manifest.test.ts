// Манифест воркеров (§12): echo, node-echo, sysinfo и netprobe описывают себя, агент передаёт
// манифест в status; supports по манифесту; validateConfigs — неверная настройка отклонена
// сервером до отправки агенту.
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import type { ConfigRecord, ConfigStatus } from "agent-sdk/server";

import { ApiError, Stand, VERSION, waitFor, WORKERS } from "./stand";

describe("манифест воркеров", () => {
  let s: Stand;
  const supported = async (worker: string, query: string) =>
    (
      await s.api<{ supported: boolean }>(
        "GET",
        `/api/agents/${s.agentId}/workers/${worker}/supports?${query}`,
      )
    ).supported;

  before(async () => {
    s = await Stand.start({
      name: "e2e-manifest",
      server: { validateConfigs: true },
    });
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
      netprobe: "targets",
    };

    for (const name of WORKERS) {
      const w = a.workers.find(x => x.name === name)!;

      assert.equal(w.version, "1.0.0", name);
      assert.equal(w.manifest?.version, "1.0.0", name);
      assert.equal(w.manifest?.configs?.[0].key, keys[name], name);
      assert.equal(typeof w.manifest?.configs?.[0].schema, "object", name);
    }
    const routes = ["POST /echo", "GET /stream", "GET /bytes", "POST /work"];
    const events = ["echo.started", "echo.progress", "echo.done"];
    // echo ещё отдаёт ход работы и прерывает её.
    const extra = {
      echo: {
        routes: ["GET /work/{id}", "POST /work/{id}/cancel", "POST /hang"],
        events: ["echo.cancelled"],
      },
      "node-echo": { routes: ["POST /hang"], events: [] },
    };

    for (const [name, more] of Object.entries(extra)) {
      const m = a.workers.find(x => x.name === name)!.manifest!;

      assert.deepEqual(
        m.routes?.map(r => `${r.method} ${r.path}`),
        [...routes, ...more.routes],
        name,
      );
      assert.deepEqual(
        m.events?.map(e => e.type),
        [...events, ...more.events],
        name,
      );
    }
    // Встроенный sysmetrics тоже отвечает GET /manifest: версия — версия агента.
    assert.equal(
      a.workers.find(w => w.name === "sysmetrics")?.manifest?.version,
      VERSION,
    );
  });

  it("supports: маршрут, ключ, событие по манифесту", async () => {
    assert.equal(await supported("echo", "method=POST&path=/echo"), true);
    assert.equal(
      await supported("node-echo", "method=GET&path=/stream%3Fn%3D3"),
      true,
    );
    assert.equal(await supported("echo", "method=GET&path=/echo"), false);
    assert.equal(
      await supported("echo", "config=settings&event=echo.done"),
      true,
    );
    assert.equal(await supported("sysinfo", "method=GET&path=/info"), true);
    assert.equal(await supported("netprobe", "config=banner"), false);
    assert.equal(await supported("sysmetrics", "config=settings"), false);
  });

  it("validateConfigs: неверная настройка отклонена до отправки агенту", async () => {
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

    // Схема netprobe из манифеста: id цели — непустая строка.
    await assert.rejects(
      s.api("PUT", `/api/agents/${s.agentId}/configs/netprobe/targets`, {
        targets: [{ id: "", host: "192.0.2.1" }],
      }),
      (e: unknown) =>
        e instanceof ApiError &&
        e.code === "CONFIG_INVALID" &&
        /targets\[0\]\.id/.test(e.message),
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
