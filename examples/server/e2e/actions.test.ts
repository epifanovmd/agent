// Действия и обновления: обновление агента из его каталога сборок (update.releases — папка,
// устроенная как релизы; подпись тестовым ключом), перезапуск зависшего воркера, смена ключа
// агента, отзыв.
import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import { copyFile, mkdir, mkdtemp, rm } from "node:fs/promises";
import { request } from "node:http";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { after, before, describe, it } from "node:test";

import { type Agent, LINK_PATH, type LogEntry } from "agent-sdk/server";

import {
  ApiError,
  BIN,
  bin,
  keygen,
  NEXT_VERSION,
  ROOT,
  run,
  Stand,
  VERSION,
  waitFor,
} from "./stand";

/** manifest.json каталога сборок с подписью ключом signing. */
const manifest = (dir: string, signing: string) =>
  run(bin("agent"), ["manifest", dir, NEXT_VERSION], {
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
        "sec-websocket-protocol": "agent.v2",
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

describe("действия и обновления", () => {
  let s: Stand;
  let releases: string;
  let catalog: string;
  let keys: { signing: string; public: string };

  /** Версия NEXT_VERSION в каталоге сборок агента, подпись ключом signing; она же — последняя. */
  const publish = async (signing: string) => {
    const dir = join(catalog, "download", `v${NEXT_VERSION}`);
    const latest = join(catalog, "latest", "download");

    await manifest(dir, signing);
    await mkdir(latest, { recursive: true });
    await copyFile(join(dir, "manifest.json"), join(latest, "manifest.json"));
  };

  before(async () => {
    assert.ok(NEXT_VERSION, `нет сборки следующей версии в ${BIN}/next`);
    releases = await mkdtemp(join(tmpdir(), "agent-e2e-release-"));
    catalog = await mkdtemp(join(tmpdir(), "agent-e2e-catalog-"));
    keys = keygen();
    const dir = join(catalog, "download", `v${NEXT_VERSION}`);

    await mkdir(dir, { recursive: true });
    await copyFile(bin("agent", join(BIN, "next")), bin("agent", dir));
    await copyFile(
      join(ROOT, "deploy/install/install.sh"),
      join(releases, "install.sh"),
    );
    // Сначала сборки подписаны чужим ключом.
    await publish(keygen().signing);
    s = await Stand.start({
      name: "e2e-actions",
      releasesDir: releases,
      agentReleases: catalog,
      updateKey: keys.public,
    });
  });
  after(async () => {
    await s?.close();
    await rm(releases, { recursive: true, force: true });
    await rm(catalog, { recursive: true, force: true });
  });

  describe("обновление агента", () => {
    it("агент нашёл новую версию в своём каталоге сборок; команда установки", async () => {
      const a = await s.waitAgent("агент сообщил о новой версии", x =>
        Boolean(x.update?.latest),
      );

      assert.equal(a.version, VERSION);
      assert.equal(a.update?.latest, NEXT_VERSION);
      const r = await s.api<{ installCommand: string }>("GET", "/api/releases");

      assert.match(
        r.installCommand,
        /^curl -fsSL '.+\/install\.sh' \| sudo sh -s -- '--server' '.+' '--token' /,
      );
      assert.ok(r.installCommand.includes(`'--update-key' '${keys.public}'`));
      assert.match(
        await (await fetch(`${s.url}/install.sh`)).text(),
        /--bundle URL/,
      );
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

    it("подписанные сборки: агент ставит новую версию и перезапускается, работа воркера не прерывается", async () => {
      await publish(keys.signing);
      const starts = s.agentStarts;
      const echoPid = async () =>
        (await s.worker("echo"))?.health?.info?.pid as number | undefined;
      const pid = await echoPid();
      const { jobId } = await s.job("echo", {
        type: "echo.long",
        data: { steps: 15, delayMs: 400 },
      });
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
      assert.equal(a.update, undefined, "новее нет");
      await s.waitWorkers();
      assert.equal(await echoPid(), pid, "echo пережил обновление агента");
      const finished = await waitFor(
        "задача echo закончена",
        async () => {
          const list = await s.jobEvents("echo", jobId);

          return list.some(e => e.type === "job.done") && list;
        },
        60_000,
      );
      const steps = new Set(
        finished
          .filter(e => e.type === "job.progress")
          .map(e => (e.data as { progress: number }).progress),
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
    // GET /health — раз в 5 с, три пропуска подряд — перезапуск.
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
