// Сборки агента с другого сервера: агент и netprobe (подпись ключом автора агента)
// лежит на отдельном HTTP-сервере, как на GitHub (agentReleases.url); воркер проекта — netprobe
// следующей версии — в releasesDir (подпись ключом проекта). Агент знает оба ключа
// (update.publicKeys): воркер обновляется из releasesDir, агент — из удалённого источника по
// перенаправлению 302.
import assert from "node:assert/strict";
import { copyFile, mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { after, before, describe, it } from "node:test";
import { fileURLToPath } from "node:url";

import type { ReleaseView, UpdateCandidate } from "agent-sdk/server";

import { BIN, bin, NEXT_VERSION, run, Stand, VERSION } from "./stand";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

/** Пара ключей подписи сборок (agent-release keygen). */
const keygen = async () => {
  const { stdout } = await run(bin("agent-release"), ["keygen"]);
  const value = (name: string) =>
    stdout.match(new RegExp(`^${name}=(.+)$`, "m"))![1];

  return {
    signing: value("AGENT_SIGNING_KEY"),
    public: value("AGENT_UPDATE_PUBLIC_KEY"),
  };
};

/** manifest.json в dir, подпись ключом signing; args — ещё аргументы agent-release manifest. */
const manifest = (dir: string, signing: string, ...args: string[]) =>
  run(bin("agent-release"), ["manifest", dir, NEXT_VERSION, ...args], {
    env: { ...process.env, AGENT_SIGNING_KEY: signing },
  });

/** Статический сервер каталога dir (как файлы релиза GitHub); запросы — в журнал. */
const serveDir = async (dir: string) => {
  const requests: string[] = [];
  const server: Server = createServer(async (req, res) => {
    requests.push(req.url ?? "");
    const name = decodeURIComponent((req.url ?? "").split("/").pop() ?? "");

    try {
      const body = await readFile(join(dir, name));

      res.writeHead(200, { "content-length": body.length });
      res.end(body);
    } catch {
      res.writeHead(404).end();
    }
  });

  await new Promise<void>(r => server.listen(0, "127.0.0.1", r));

  return {
    url: `http://127.0.0.1:${(server.address() as AddressInfo).port}/download/v${NEXT_VERSION}`,
    requests,
    close: () =>
      new Promise<void>(r => {
        server.closeAllConnections();
        server.close(() => r());
      }),
  };
};

describe("сборки агента с другого сервера", () => {
  let s: Stand;
  let remoteDir: string;
  let projectDir: string;
  let remote: Awaited<ReturnType<typeof serveDir>>;
  let author: { signing: string; public: string };
  let project: { signing: string; public: string };
  const platform = bin("agent").split("agent-").pop()!;

  before(async () => {
    assert.ok(NEXT_VERSION, `нет сборок следующей версии в ${BIN}/next`);
    [author, project] = await Promise.all([keygen(), keygen()]);
    remoteDir = await mkdtemp(join(tmpdir(), "agent-e2e-remote-"));
    projectDir = await mkdtemp(join(tmpdir(), "agent-e2e-project-"));
    const next = join(BIN, "next");

    // Сборки автора агента: агент, netprobe и install.sh (как scripts/release.sh).
    await copyFile(bin("agent", next), bin("agent", remoteDir));
    await copyFile(
      bin("netprobe", next),
      join(remoteDir, `netprobe-${NEXT_VERSION}-${platform}`),
    );
    await copyFile(
      join(ROOT, "deploy/install/install.sh"),
      join(remoteDir, "install.sh"),
    );
    await manifest(
      remoteDir,
      author.signing,
      "--worker",
      `netprobe=${NEXT_VERSION}`,
    );
    // Воркеры проекта: netprobe той же версии, подпись ключом проекта.
    await copyFile(
      bin("netprobe", next),
      join(projectDir, `netprobe-${NEXT_VERSION}-${platform}`),
    );
    await manifest(
      projectDir,
      project.signing,
      "--worker",
      `netprobe=${NEXT_VERSION}`,
    );
    remote = await serveDir(remoteDir);
    s = await Stand.start({
      name: "e2e-releases",
      releasesDir: projectDir,
      updateKeys: [project.public, author.public],
      releaseNetprobe: true,
      server: {
        agentReleases: { url: remote.url },
        updatePublicKeys: [project.public],
      },
    });
  });
  after(async () => {
    await s?.close();
    await remote?.close();
    await rm(remoteDir, { recursive: true, force: true });
    await rm(projectDir, { recursive: true, force: true });
  });

  it("итоговый список сборок: агент из источника, netprobe — из releasesDir", async () => {
    const r = await s.api<{
      release: ReleaseView;
      candidates: UpdateCandidate[];
    }>("GET", "/api/releases");

    assert.equal(r.release.version, NEXT_VERSION);
    assert.equal(r.release.remote?.from, remote.url);
    assert.equal(r.release.remote?.publicKey, author.public);
    assert.deepEqual(
      r.release.artifacts.map(a => [a.source, a.url]),
      [["remote", `${remote.url}/agent-${platform}`]],
    );
    assert.deepEqual(
      r.release.workers?.map(w => [w.name, w.source]),
      [["netprobe", "local"]],
    );
    assert.equal(
      r.candidates.find(c => c.agentId === s.agentId)?.target,
      NEXT_VERSION,
    );
  });

  it("install.sh из источника с адресом сервера и ключами проекта и автора; сборка — 302", async () => {
    const sh = await (
      await fetch(`${s.url}/api/v1/agent-link/install.sh`)
    ).text();

    assert.match(sh, new RegExp(`^DEFAULT_SERVER="${s.url}"$`, "m"));
    assert.ok(
      sh.includes(
        `\nDEFAULT_UPDATE_KEYS="${project.public} ${author.public}"\n`,
      ),
      "ключи проекта и автора",
    );
    const res = await fetch(
      `${s.url}/api/v1/agent-link/releases/agent-${platform}`,
      { redirect: "manual" },
    );

    assert.equal(res.status, 302);
    assert.equal(
      res.headers.get("location"),
      `${remote.url}/agent-${platform}`,
    );
  });

  it("воркер проекта обновляется (ключ проекта), агент — из источника (ключ автора)", async () => {
    const w = await s.api<{ version: string; previous: string }>(
      "POST",
      `/api/agents/${s.agentId}/workers/netprobe/update`,
    );

    assert.equal(w.version, NEXT_VERSION);
    await s.waitAgent("netprobe новой версии работает", a =>
      Boolean(
        a.status?.workers.some(
          x =>
            x.name === "netprobe" &&
            x.state === "running" &&
            x.version === NEXT_VERSION &&
            x.health?.ok,
        ),
      ),
    );
    await s.waitWorkers();
    const a = await s.api("POST", `/api/agents/${s.agentId}/update`);

    assert.deepEqual(a, { version: NEXT_VERSION, previous: VERSION });
    await s.waitAgent(
      "агент на связи с новой версией",
      x => x.online && x.version === NEXT_VERSION,
    );
    assert.ok(
      remote.requests.includes(`/download/v${NEXT_VERSION}/agent-${platform}`),
      "сборка агента скачана из источника",
    );
    const { stdout } = await run(s.agentBin, ["version"]);

    assert.equal(stdout.trim(), NEXT_VERSION);
  });
});
