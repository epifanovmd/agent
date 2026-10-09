// Настройки воркеров (применение, отказ, удаление, повторная передача после перезапусков),
// гарантированная доставка событий при остановленном сервере и уборка `agent cleanup`.
import assert from "node:assert/strict";
import { existsSync } from "node:fs";
import { readFile, rm } from "node:fs/promises";
import { after, before, describe, it } from "node:test";

import type { AgentEvent, ConfigRecord, ConfigStatus } from "agent-sdk/server";

import { run, Stand, waitFor } from "./stand";

describe("настройки и доставка", () => {
  let s: Stand;
  const path = () => `/api/agents/${s.agentId}/configs/echo/settings`;
  const status = async (): Promise<ConfigStatus | undefined> => {
    const { status } = await s.api<{ status: ConfigStatus[] }>(
      "GET",
      `/api/agents/${s.agentId}/configs`,
    );

    return status.find(c => c.worker === "echo" && c.key === "settings");
  };
  const waitState = (state: string, version: number) =>
    waitFor(`echo/settings: ${state}, версия ${version}`, async () => {
      const c = await status();

      return c?.state === state && c.version === version && c;
    });
  const echoPid = async () =>
    (await s.worker("echo"))?.health?.info?.pid as number | undefined;

  before(async () => {
    s = await Stand.start({ name: "e2e-configs" });
  });
  after(() => s?.close());

  it("новая настройка применяется: ответ echo меняется", async () => {
    const rec = await s.api<ConfigRecord>("PUT", path(), { prefix: "> " });
    const c = await waitState("applied", rec.version);

    assert.equal(c.applied, rec.version);
    assert.equal(await s.echo("echo", "a"), "> A");
  });

  it("неверное значение: failed с текстом воркера, работает прежняя", async () => {
    const rec = await s.api<ConfigRecord>("PUT", path(), { prefix: 5 });
    const c = await waitState("failed", rec.version);

    assert.match(c.error?.message ?? "", /prefix/);
    const alerts = await s.api<{ type: string }[]>(
      "GET",
      `/api/alerts?agentId=${s.agentId}`,
    );

    assert.ok(alerts.some(a => a.type === "configFailed"));
    assert.equal(await s.echo("echo", "a"), "> A");
  });

  it("следующая версия применяется, проблема снимается", async () => {
    const rec = await s.api<ConfigRecord>("PUT", path(), {
      prefix: "# ",
      upper: false,
    });

    await waitState("applied", rec.version);
    assert.equal(await s.echo("echo", "b"), "# b");
    await waitFor("проблема configFailed снята", async () => {
      const alerts = await s.api<{ type: string }[]>(
        "GET",
        `/api/alerts?agentId=${s.agentId}`,
      );

      return !alerts.some(a => a.type === "configFailed");
    });
  });

  it("перезапуск воркера: настройка передаётся новому процессу", async () => {
    const pid = await echoPid();

    await s.api("POST", `/api/agents/${s.agentId}/workers/echo/restart`);
    await waitFor("новый процесс echo", async () => {
      const p = await echoPid();

      return p !== undefined && p !== pid;
    });
    assert.equal(await s.echo("echo", "b"), "# b");
  });

  it("перезапуск агента без сервера: настройка — с диска агента", async () => {
    const stateFile = s.stateFile("echo");

    await s.stopServer();
    await s.stopAgent();
    // Воркеры пережили агента; запущенный заново echo не знает настроек.
    await s.stopWorkers();
    await rm(stateFile, { force: true });
    s.startAgent();
    // Сервера нет: echo получает настройку от агента из <dataDir>/configs.
    const state = await waitFor("echo применил настройку с диска", async () =>
      existsSync(stateFile)
        ? (JSON.parse(await readFile(stateFile, "utf8")) as {
            settings: { prefix: string };
          })
        : undefined,
    );

    assert.equal(state.settings.prefix, "# ");
    await s.startServer();
    await s.waitWorkers();
    assert.equal(await s.echo("echo", "b"), "# b");
  });

  it("удаление настройки: воркер возвращается к значениям по умолчанию", async () => {
    assert.deepEqual(await s.api("DELETE", path()), { deleted: true });
    await waitFor(
      "echo без префикса",
      async () => (await s.echo("echo", "c")) === "C",
    );
    await waitFor("ключ удалён у агента", async () => !(await status()));
  });

  it("события при остановленном сервере доходят после восстановления", async () => {
    const res = await s.fetchWorker("echo", "/work", {
      method: "POST",
      body: JSON.stringify({ steps: 4, delayMs: 300 }),
    });
    const { id } = (await res.json()) as { id: string };

    await s.stopServer();
    // Без связи события ждут в outbox агента (видно в agent status --json).
    await waitFor(
      "5 событий в outbox",
      async () => {
        const { stdout } = await run(s.agentBin, [
          "status",
          "--json",
          "-config",
          s.configPath,
        ]);
        const { status } = JSON.parse(stdout) as {
          status?: { online: boolean; outbox: number };
        };

        return status && !status.online && status.outbox >= 5;
      },
      20_000,
    );
    await s.startServer();
    const events = await waitFor("все события дошли", async () => {
      const list = (
        await s.api<AgentEvent[]>(
          "GET",
          `/api/events?agentId=${s.agentId}&worker=echo&limit=1000`,
        )
      ).filter(e => (e.data as { id?: string })?.id === id);

      return list.some(e => e.type === "echo.done") && list;
    });
    const steps = events
      .filter(e => e.type === "echo.progress")
      .map(e => (e.data as { step: number }).step)
      .sort();

    assert.deepEqual(steps, [1, 2, 3, 4], "каждый шаг — один раз");
    await s.waitWorkers();
  });

  it("уборка agent cleanup: воркеры получают POST /cleanup", async () => {
    await s.api("PUT", path(), { prefix: "! " });
    await waitFor("файл echo на узле", () => existsSync(s.stateFile("echo")));
    await s.stopAgent();
    const { stdout } = await run(s.agentBin, [
      "cleanup",
      "-config",
      s.configPath,
    ]);

    for (const w of ["echo", "node-echo", "sysinfo", "netprobe"])
      assert.match(stdout, new RegExp(`^${w}: убрано$`, "m"));
    assert.equal(existsSync(s.stateFile("echo")), false);
    assert.equal(existsSync(s.stateFile("node-echo")), false);
  });
});
