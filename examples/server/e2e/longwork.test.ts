// Долгая работа воркера: переживает перезапуск агента (SIGTERM и запуск — агент подхватывает
// воркер), замена занятого воркера ждёт окончания работы (pending), force — сразу; ход работы
// echo хранит на диске и продолжает после своего перезапуска.
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import type { AgentEvent } from "agent-sdk/server";

import { Stand, waitFor } from "./stand";

interface Work {
  id: string;
  step: number;
  of: number;
  state: string;
}

describe("долгая работа", () => {
  let s: Stand;

  before(async () => {
    s = await Stand.start({ name: "e2e-longwork" });
  });
  after(async () => {
    await s?.close();
  });

  /** Начать работу echo: steps шагов по delayMs. */
  const startWork = async (steps: number, delayMs: number): Promise<string> => {
    const res = await s.fetchWorker("echo", "/work", {
      method: "POST",
      body: JSON.stringify({ steps, delayMs }),
    });

    assert.equal(res.status, 202);

    return ((await res.json()) as { id: string }).id;
  };

  /** События работы id, по порядку прихода. */
  const events = async (id: string): Promise<AgentEvent[]> =>
    (
      await s.api<AgentEvent[]>(
        "GET",
        `/api/events?agentId=${s.agentId}&worker=echo&limit=10000`,
      )
    )
      .filter(e => (e.data as { id?: string })?.id === id)
      .reverse();

  const steps = (list: AgentEvent[]): number[] =>
    [
      ...new Set(
        list
          .filter(e => e.type === "echo.progress")
          .map(e => (e.data as { step: number }).step),
      ),
    ].sort((a, b) => a - b);

  const done = async (id: string): Promise<boolean> =>
    (await events(id)).some(e => e.type === "echo.done");

  const echoPid = async (): Promise<number | undefined> =>
    (await s.worker("echo"))?.health?.info?.pid as number | undefined;

  const work = async (id: string): Promise<Work> =>
    (await (await s.fetchWorker("echo", `/work/${id}`)).json()) as Work;

  it("перезапуск агента не прерывает работу: тот же процесс, события идут, итог дошёл", async () => {
    const pid = await echoPid();
    const id = await startWork(20, 400);

    await waitFor(
      "работа идёт",
      async () => steps(await events(id)).length >= 2,
    );
    const boot = (await s.agent()).hello?.agent.bootId;

    await s.restartAgent();
    await s.waitAgent(
      "новый запуск агента на связи",
      a => a.online && a.hello?.agent.bootId !== boot,
    );
    await s.waitWorkers();
    assert.equal(await echoPid(), pid, "echo подхвачен, а не запущен заново");
    const w = await work(id);

    assert.equal(w.state, "running");
    await waitFor("итог работы", () => done(id), 60_000);
    assert.deepEqual(
      steps(await events(id)),
      Array.from({ length: 20 }, (_, i) => i + 1),
      "все шаги дошли",
    );
    assert.equal((await s.worker("echo"))?.restarts, 0);
  });

  it("restartWorker во время работы ждёт её окончания (pending: restart), force — сразу", async () => {
    const pid = await echoPid();
    const id = await startWork(8, 500);

    await waitFor(
      "echo занят",
      async () => (await s.worker("echo"))?.health?.busy === true,
    );
    const restarted = s.api(
      "POST",
      `/api/agents/${s.agentId}/workers/echo/restart`,
    );

    await waitFor(
      "замена ждёт",
      async () => (await s.worker("echo"))?.pending === "restart",
    );
    assert.equal(await echoPid(), pid, "занятый воркер не заменён");
    await restarted;
    assert.ok(await done(id), "замена — после окончания работы");
    const next = await waitFor("новый процесс echo", async () => {
      const p = await echoPid();

      return p !== undefined && p !== pid && p;
    });

    assert.equal((await s.worker("echo"))?.pending, undefined);

    // force: замена сразу; ход работы на диске — новый процесс продолжает её.
    const long = await startWork(10, 500);

    await waitFor(
      "echo занят",
      async () => (await s.worker("echo"))?.health?.busy === true,
    );
    await waitFor("шаг сделан", async () => (await work(long)).step >= 2);
    await s.api("POST", `/api/agents/${s.agentId}/workers/echo/restart`, {
      force: true,
    });
    await waitFor("новый процесс echo", async () => {
      const p = await echoPid();

      return p !== undefined && p !== next;
    });
    assert.equal(await done(long), false, "force прервал работу");
    await waitFor("работа продолжена и закончена", () => done(long), 60_000);
    assert.deepEqual(
      steps(await events(long)),
      Array.from({ length: 10 }, (_, i) => i + 1),
    );
  });
});
