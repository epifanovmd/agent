// Каталог сборок агента на отдельном HTTP-сервере, устроенный как релизы GitHub
// (update.releases; подпись ключом автора агента); воркер проекта — netprobe следующей версии —
// в releasesDir сервера (подпись ключом проекта). Агент знает оба ключа (update.publicKeys):
// воркер обновляется с сервера, агент — из своего каталога сборок.
import assert from "node:assert/strict";
import { copyFile, mkdtemp, readFile, rm } from "node:fs/promises";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { after, before, describe, it } from "node:test";
import { fileURLToPath } from "node:url";

import type { ReleaseView } from "agent-sdk/server";

import { BIN, bin, keygen, NEXT_VERSION, run, Stand, VERSION } from "./stand";

const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "../../..");

/** manifest.json в dir, подпись ключом signing; args — ещё аргументы agent manifest. */
const manifest = (dir: string, signing: string, ...args: string[]) =>
  run(bin("agent"), ["manifest", dir, NEXT_VERSION, ...args], {
    env: { ...process.env, AGENT_SIGNING_KEY: signing },
  });

/**
 * Статический сервер каталога dir, устроенный как релизы GitHub: файл — по последней части пути
 * (…/latest/download/manifest.json, …/download/v<версия>/<файл>); запросы — в журнал.
 */
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
    url: `http://127.0.0.1:${(server.address() as AddressInfo).port}`,
    requests,
    close: () =>
      new Promise<void>(r => {
        server.closeAllConnections();
        server.close(() => r());
      }),
  };
};

describe("каталог сборок агента на другом сервере", () => {
  let s: Stand;
  let remoteDir: string;
  let projectDir: string;
  let remote: Awaited<ReturnType<typeof serveDir>>;
  let author: { signing: string; public: string };
  let project: { signing: string; public: string };
  const platform = bin("agent").split("agent-").pop()!;

  before(async () => {
    assert.ok(NEXT_VERSION, `нет сборок следующей версии в ${BIN}/next`);
    [author, project] = [keygen(), keygen()];
    remoteDir = await mkdtemp(join(tmpdir(), "agent-e2e-remote-"));
    projectDir = await mkdtemp(join(tmpdir(), "agent-e2e-project-"));
    const next = join(BIN, "next");

    // Сборки автора агента: агент и install.sh (как scripts/release.sh).
    await copyFile(bin("agent", next), bin("agent", remoteDir));
    await copyFile(
      join(ROOT, "deploy/install/install.sh"),
      join(remoteDir, "install.sh"),
    );
    await manifest(remoteDir, author.signing);
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
      agentReleases: remote.url,
    });
  });
  after(async () => {
    await s?.close();
    await remote?.close();
    await rm(remoteDir, { recursive: true, force: true });
    await rm(projectDir, { recursive: true, force: true });
  });

  it("сборки сервера — воркеры проекта; новую версию агента агент нашёл в своём каталоге", async () => {
    const r = await s.api<{ release: ReleaseView }>("GET", "/api/releases");

    assert.equal(r.release.version, NEXT_VERSION);
    assert.deepEqual(
      r.release.workers?.map(w => [w.name, w.url]),
      [
        [
          "netprobe",
          `/api/v1/agent-link/releases/netprobe-${NEXT_VERSION}-${platform}`,
        ],
      ],
    );
    const a = await s.waitAgent("агент сообщил о новой версии", x =>
      Boolean(x.update?.latest),
    );

    assert.equal(a.update?.latest, NEXT_VERSION);
    assert.ok(remote.requests.includes("/latest/download/manifest.json"));
  });

  it("воркер проекта обновляется с сервера (ключ проекта), агент — из своего каталога (ключ автора)", async () => {
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
      "сборка агента скачана из каталога сборок агента",
    );
    const { stdout } = await run(s.agentBin, ["version"]);

    assert.equal(stdout.trim(), NEXT_VERSION);
  });
});
