// Обнаружение потери связи с настройками по умолчанию (offlineGraceMs, ping): убитый агент —
// offline за секунды; упавший процесс воркера — новое состояние в status сразу.
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import type { Agent } from "agent-sdk/server";

import { Stand, waitFor } from "./stand";

describe("обнаружение потери связи", () => {
  let s: Stand;

  before(async () => {
    // offlineGraceMs и pingIntervalMs — по умолчанию SDK.
    s = await Stand.start({
      name: "e2e-detect",
      server: { offlineGraceMs: undefined },
    });
  });
  after(async () => {
    await s?.close();
  });

  /** Первое событие agent этого агента, подходящее под check, после вызова. */
  const nextAgent = (check: (a: Agent) => boolean): Promise<Agent> =>
    new Promise(resolve => {
      const on = (a: Agent) => {
        if (a.id !== s.agentId || !check(a)) return;
        s.agents.off("agent", on);
        resolve(a);
      };

      s.agents.on("agent", on);
    });

  it("упал процесс воркера — status с новым состоянием не позже 2 с", async () => {
    const pid = (await s.worker("echo"))?.health?.info?.pid as number;

    assert.ok(pid > 0);
    const changed = nextAgent(
      a => a.workers.find(w => w.name === "echo")?.state !== "running",
    );
    const killedAt = Date.now();

    process.kill(pid, "SIGKILL");
    const a = await changed;
    const took = Date.now() - killedAt;

    assert.ok(took <= 2000, `состояние воркера через ${took} мс`);
    assert.ok(
      ["backoff", "starting", "stopped"].includes(
        a.workers.find(w => w.name === "echo")!.state!,
      ),
    );
    await s.waitWorkers();
  });

  it("убит процесс агента — offline не позже 8 с, lastSeenAt — до обрыва", async () => {
    await waitFor("агент на связи", async () => (await s.agent()).online);
    const offline = nextAgent(a => !a.online);
    const killedAt = Date.now();

    await s.killAgent();
    const a = await offline;
    const took = Date.now() - killedAt;

    assert.ok(took <= 8000, `offline через ${took} мс`);
    assert.ok(a.lastSeenAt! <= killedAt + 100, "lastSeenAt — последняя весть");
    assert.ok(killedAt - a.lastSeenAt! < 6000, "lastSeenAt — недавняя");
    assert.deepEqual(
      a.alerts.map(x => x.type),
      ["offline"],
    );
  });
});
