// Долгая задача воркера: переживает перезапуск агента (SIGTERM и запуск — агент подхватывает
// воркер); замена занятого воркера откладывается — restartWorker сразу отвечает deferred, итог
// приходит событием action после окончания задачи; force — сразу; ход задачи echo хранит на
// диске и продолжает после своего перезапуска.
import assert from "node:assert/strict";
import { after, before, describe, it } from "node:test";

import type { ActionRecord, AgentEvent } from "agent-sdk/server";

import { Stand, waitFor } from "./stand";

interface Job {
  id: string;
  state: string;
  progress: number;
}

describe("долгая задача", () => {
  let s: Stand;

  before(async () => {
    s = await Stand.start({ name: "e2e-longwork" });
  });
  after(async () => {
    await s?.close();
  });

  /** Начать долгую задачу echo: steps шагов по delayMs; итог не ждать. */
  const startJob = async (
    steps: number,
    delayMs: number,
  ): Promise<{ jobId: string; id: string }> => {
    const r = await s.job("echo", {
      type: "echo.long",
      data: { steps, delayMs },
      timeoutMs: 1,
    });

    assert.equal(r.state, "running");

    return { jobId: r.jobId, id: r.id! };
  };

  /** Шаги, о которых сообщили события job.progress (без повторов). */
  const steps = (list: AgentEvent[], of: number): number[] =>
    [
      ...new Set(
        list
          .filter(e => e.type === "job.progress")
          .map(e => Math.round((e.data as { progress: number }).progress * of)),
      ),
    ].sort((a, b) => a - b);

  const done = async (jobId: string): Promise<boolean> =>
    (await s.jobEvents("echo", jobId)).some(e => e.type === "job.done");

  const echoPid = async (): Promise<number | undefined> =>
    (await s.worker("echo"))?.health?.info?.pid as number | undefined;

  const job = (id: string): Promise<Job> =>
    s.api<Job>("GET", `/api/agents/${s.agentId}/workers/echo/jobs/${id}`);

  const actions = (): Promise<ActionRecord[]> =>
    s.api<ActionRecord[]>("GET", `/api/agents/${s.agentId}/actions`);

  it("перезапуск агента не прерывает задачу: тот же процесс, события идут, итог дошёл", async () => {
    const pid = await echoPid();
    const { jobId, id } = await startJob(20, 400);

    await waitFor(
      "задача идёт",
      async () => steps(await s.jobEvents("echo", jobId), 20).length >= 2,
    );
    const boot = (await s.agent()).hello?.agent.bootId;

    await s.restartAgent();
    await s.waitAgent(
      "новый запуск агента на связи",
      a => a.online && a.hello?.agent.bootId !== boot,
    );
    await s.waitWorkers();
    assert.equal(await echoPid(), pid, "echo подхвачен, а не запущен заново");
    assert.equal((await job(id)).state, "running");
    await waitFor("итог задачи", () => done(jobId), 60_000);
    assert.deepEqual(
      steps(await s.jobEvents("echo", jobId), 20),
      Array.from({ length: 20 }, (_, i) => i + 1),
      "все шаги дошли",
    );
    assert.equal((await s.worker("echo"))?.restarts, 0);
  });

  it("restartWorker во время задачи: сразу deferred (pending: restart), замена и событие action — после её окончания; force — сразу", async () => {
    const pid = await echoPid();
    const { jobId } = await startJob(8, 500);

    await waitFor(
      "echo занят",
      async () => (await s.worker("echo"))?.health?.busy === true,
    );
    const began = Date.now();
    const r = await s.api<{
      deferred: boolean;
      pending: string;
      actionId: string;
    }>("POST", `/api/agents/${s.agentId}/workers/echo/restart`);

    assert.ok(Date.now() - began < 3000, "ответ — сразу, без ожидания замены");
    assert.deepEqual([r.deferred, r.pending], [true, "restart"]);
    await waitFor(
      "замена ждёт",
      async () => (await s.worker("echo"))?.pending === "restart",
    );
    assert.equal(await echoPid(), pid, "занятый воркер не заменён");
    const fin = await waitFor(
      "итог отложенной замены — событие action",
      async () => (await actions()).find(a => a.id === r.actionId),
      60_000,
    );

    assert.deepEqual([fin.status, fin.deferred], ["done", true]);
    assert.ok(await done(jobId), "замена — после окончания задачи");
    const next = await waitFor("новый процесс echo", async () => {
      const p = await echoPid();

      return p !== undefined && p !== pid && p;
    });

    assert.equal((await s.worker("echo"))?.pending, undefined);

    // force: замена сразу; ход задачи на диске — новый процесс продолжает её.
    const long = await startJob(10, 500);

    await waitFor(
      "echo занят",
      async () => (await s.worker("echo"))?.health?.busy === true,
    );
    await waitFor(
      "шаг сделан",
      async () => (await job(long.id)).progress >= 0.2,
    );
    const forced = await s.api<{ deferred: boolean }>(
      "POST",
      `/api/agents/${s.agentId}/workers/echo/restart`,
      { force: true },
    );

    assert.equal(forced.deferred, false);
    await waitFor("новый процесс echo", async () => {
      const p = await echoPid();

      return p !== undefined && p !== next;
    });
    assert.equal(await done(long.jobId), false, "force прервал задачу");
    await waitFor(
      "задача продолжена и закончена",
      () => done(long.jobId),
      60_000,
    );
    assert.deepEqual(
      steps(await s.jobEvents("echo", long.jobId), 10),
      Array.from({ length: 10 }, (_, i) => i + 1),
    );
  });
});
