// Действия и выпуск: обновление агента из каталога выпуска (подпись тестовым ключом),
// перезапуск зависшего воркера, смена ключа агента, отзыв.
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { copyFile, mkdtemp, rm } from "node:fs/promises";
import { request } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";

import {
  type Agent,
  type AgentEvent,
  LINK_PATH,
  type LogEntry,
  type UpdateCandidate,
  WS_CHANNEL,
} from "agent-sdk/server";

import {
  ApiError,
  BIN,
  bin,
  NEXT_VERSION,
  run,
  Stand,
  VERSION,
  waitFor,
} from "./stand";

/** Пара ключей подписи выпуска (agent-release keygen). */
const keygen = async () => {
  const { stdout } = await run(bin("agent-release"), ["keygen"]);
  const value = (name: string) =>
    stdout.match(new RegExp(`^${name}=(.+)$`, "m"))![1];

  return {
    signing: value("AGENT_SIGNING_KEY"),
    public: value("AGENT_UPDATE_PUBLIC_KEY"),
  };
};

/** manifest.json каталога выпуска с подписью ключом signing. */
const manifest = (dir: string, signing: string) =>
  run(bin("agent-release"), ["manifest", dir, NEXT_VERSION], {
    env: { ...process.env, AGENT_SIGNING_KEY: signing },
  });

/** Ответ сервера на подключение агента по WebSocket с ключом id.secret: 101 — принят. */
const linkStatus = (url: string, agentId: string, secret: string) =>
  new Promise<number>((ok, fail) => {
    const req = request(url + LINK_PATH, {
      headers: {
        connection: "Upgrade",
        upgrade: "websocket",
        "sec-websocket-version": "13",
        "sec-websocket-key": randomBytes(16).toString("base64"),
        "sec-websocket-protocol": WS_CHANNEL,
        authorization: `Agent ${agentId}.${secret}`,
      },
    });

    req.on("upgrade", (_res, socket) => {
      socket.destroy();
      ok(101);
    });
    req.on("response", res => {
      res.resume();
      ok(res.statusCode ?? 0);
    });
    req.on("error", fail);
    req.end();
  });

describe("действия и выпуск", () => {
  let s: Stand;
  let releases: string;
  let keys: { signing: string; public: string };

  before(async () => {
    assert.ok(NEXT_VERSION, `нет сборки следующей версии в ${BIN}/next`);
    releases = await mkdtemp(join(tmpdir(), "agent-e2e-release-"));
    keys = await keygen();
    await copyFile(bin("agent", join(BIN, "next")), bin("agent", releases));
    // Сначала выпуск подписан чужим ключом.
    await manifest(releases, (await keygen()).signing);
    s = await Stand.start({
      name: "e2e-actions",
      releasesDir: releases,
      updateKey: keys.public,
    });
  });
  after(async () => {
    await s?.close();
    await rm(releases, { recursive: true, force: true });
  });

  describe("обновление агента", () => {
    it("кандидаты обновления и команда установки", async () => {
      const r = await s.api<{
        release: { version: string };
        candidates: UpdateCandidate[];
        installCommand: string;
      }>("GET", "/api/releases");

      assert.equal(r.release.version, NEXT_VERSION);
      const c = r.candidates.find(x => x.agentId === s.agentId);

      assert.equal(c?.current, VERSION);
      assert.equal(c?.target, NEXT_VERSION);
      assert.match(r.installCommand, /install\.sh' \| sudo sh -s -- --token/);
    });

    it("чужая подпись: UPDATE_FAILED, агент работает прежней версией", async () => {
      await assert.rejects(
        s.api("POST", `/api/agents/${s.agentId}/update`),
        (e: ApiError) =>
          e.code === "UPDATE_FAILED" && /подпись/.test(e.message),
      );
      const a = await s.agent();

      assert.equal(a.online, true);
      assert.equal(a.version, VERSION);
    });

    it("подписанный выпуск: агент ставит новую версию и перезапускается, работа воркера не прерывается", async () => {
      await manifest(releases, keys.signing);
      const starts = s.agentStarts;
      const echoPid = async () =>
        (await s.worker("echo"))?.health?.info?.pid as number | undefined;
      const pid = await echoPid();
      const work = await s.fetchWorker("echo", "/work", {
        method: "POST",
        body: JSON.stringify({ steps: 15, delayMs: 400 }),
      });
      const { id } = (await work.json()) as { id: string };
      const result = await s.api("POST", `/api/agents/${s.agentId}/update`);

      assert.deepEqual(result, { version: NEXT_VERSION, previous: VERSION });
      assert.equal(s.agentStarts, starts + 1, "процесс агента запущен снова");
      const a = await s.waitAgent(
        "агент на связи с новой версией",
        a => a.online && a.version === NEXT_VERSION,
      );

      assert.equal(a.hello?.agent.version, NEXT_VERSION);
      const { stdout } = await run(s.agentBin, ["version"]);

      assert.equal(stdout.trim(), NEXT_VERSION);
      const { candidates } = await s.api<{ candidates: UpdateCandidate[] }>(
        "GET",
        "/api/releases",
      );

      assert.deepEqual(candidates, []);
      await s.waitWorkers();
      assert.equal(await echoPid(), pid, "echo пережил обновление агента");
      const finished = await waitFor(
        "работа echo закончена",
        async () => {
          const list = (
            await s.api<AgentEvent[]>(
              "GET",
              `/api/events?agentId=${s.agentId}&worker=echo&limit=10000`,
            )
          ).filter(e => (e.data as { id?: string })?.id === id);

          return list.some(e => e.type === "echo.done") && list;
        },
        60_000,
      );
      const steps = new Set(
        finished
          .filter(e => e.type === "echo.progress")
          .map(e => (e.data as { step: number }).step),
      );

      assert.equal(steps.size, 15, "все шаги дошли");
    });
  });

  it("restartWorker: воркер запускается заново, действие записано", async () => {
    const before = (await s
      .fetchWorker("sysinfo", "/info")
      .then(r => r.json())) as { pid: number };

    await s.api("POST", `/api/agents/${s.agentId}/workers/sysinfo/restart`);
    const after = await waitFor("новый процесс sysinfo", async () => {
      const info = (await (await s.fetchWorker("sysinfo", "/info")).json()) as {
        pid: number;
      };

      return info.pid !== before.pid && info;
    });

    assert.ok(after.pid > 0);
    const actions = await s.api<{ name: string; status: string }[]>(
      "GET",
      `/api/agents/${s.agentId}/actions`,
    );

    assert.equal(actions[0].name, "worker.restart");
    assert.equal(actions[0].status, "done");
  });

  it("зависший воркер (нет ответа на GET /health) перезапускается агентом", async () => {
    const pidOf = async () =>
      (await s.worker("echo"))?.health?.info?.pid as number | undefined;
    const pid = await pidOf();
    const res = await s.fetchWorker("echo", "/hang", { method: "POST" });

    assert.equal(res.status, 202);
    // GET /health у echo — раз в 2 с (stand.ts), три пропуска подряд — перезапуск.
    await waitFor(
      "новый процесс echo",
      async () => {
        const p = await pidOf();

        return p !== undefined && p !== pid;
      },
      60_000,
    );
    const w = await s.worker("echo");

    assert.equal(w?.restarts, 1);
    assert.equal(w?.health?.ok, true);
    const log = await s.api<LogEntry[]>(
      "GET",
      `/api/agents/${s.agentId}/logs?lines=1000`,
    );

    assert.ok(log.some(e => e.level === "error" && e.msg.includes("завис")));
  });

  it("rotateKey: прежний ключ не принимается, агент на связи с новым", async () => {
    const old = await s.credentials();
    const session = (await s.agent()).session?.id;

    await s.api("POST", `/api/agents/${s.agentId}/rotate-key`);
    await waitFor(
      "новый ключ в каталоге агента",
      async () => (await s.credentials()).secret !== old.secret,
    );
    await s.waitAgent(
      "агент переподключился",
      a => a.online && a.session?.id !== session,
    );
    assert.equal(await linkStatus(s.url, old.agentId, old.secret), 401);
    assert.equal(await s.echo("node-echo", "ключ"), "КЛЮЧ");
  });

  it("revoke: агент отключён, ключ не принимается; с токеном — новая регистрация", async () => {
    const creds = await s.credentials();
    const a = await s.api<Agent>("POST", `/api/agents/${s.agentId}/revoke`);

    assert.equal(a.revoked, true);
    await s.waitAgent("агент не на связи", a => a.revoked && !a.online);
    assert.notEqual(await linkStatus(s.url, creds.agentId, creds.secret), 101);
    await assert.rejects(s.echo("echo", "x"), (e: Error) =>
      /POST \/echo: 409/.test(e.message),
    );
    // В настройках агента есть токен регистрации: он регистрируется заново — с новым id.
    const fresh = await waitFor("новая регистрация", async () =>
      (await s.api<Agent[]>("GET", "/api/agents")).find(
        x => x.name === a.name && x.id !== a.id && x.online,
      ),
    );

    assert.equal((await s.credentials()).agentId, fresh.id);
  });
});
