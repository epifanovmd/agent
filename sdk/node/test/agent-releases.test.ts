// Удалённый источник сборок агента (agentReleases): фейковый API GitHub на node:http — выбор
// версии по диапазону, пропуск prerelease и draft, ошибка сети, объединение с воркерами проекта
// из releasesDir, install.sh с ключами, перенаправление и раздача сборок потоком, событие release.
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import { createServer, type Server } from "node:http";
import type { AddressInfo } from "node:net";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { afterEach, describe, it } from "node:test";

import type { AgentsOptions, ReleaseEvent } from "../src/server/index";
import { enroll, FakeAgent, startServer, type TestServer } from "./helpers";

const AUTHOR_KEY = "QVVUSE9SLUtFWS1FWEFNUExFLUFVVEhPUi1LRVktMDE=";
const PROJECT_KEY = "UFJPSkVDVC1LRVktRVhBTVBMRS1QUk9KRUNULUtFWTA=";
const EXTRA_KEY = "RVhUUkEtS0VZLUVYQU1QTEUtRVhUUkEtS0VZLUVYVFI=";

/** Релиз в фейковом GitHub: тег, флаги и файлы (имя → содержимое). */
interface FakeRelease {
  tag: string;
  draft?: boolean;
  prerelease?: boolean;
  files: Record<string, string>;
}

/** Файлы релиза версии v: manifest.json (агент и netprobe), сборки, install.sh. */
const releaseFiles = (v: string): Record<string, string> => ({
  "manifest.json": JSON.stringify({
    version: v,
    publicKey: AUTHOR_KEY,
    artifacts: [
      {
        os: "linux",
        arch: "amd64",
        file: "agent-linux-amd64",
        sha256: `sha-agent-${v}`,
        signature: "sig",
      },
    ],
    workers: [
      {
        name: "netprobe",
        version: v,
        os: "linux",
        arch: "amd64",
        file: `netprobe-${v}-linux-amd64`,
        sha256: `sha-netprobe-${v}`,
        signature: "sig",
      },
    ],
  }),
  "agent-linux-amd64": `AGENT ${v}`,
  [`netprobe-${v}-linux-amd64`]: `NETPROBE ${v}`,
  "install.sh": `#!/bin/sh\n# ${v}\nDEFAULT_SERVER=""\nDEFAULT_UPDATE_KEYS=""\n`,
});

/** Фейковый GitHub: API релизов и файлы по browser_download_url. */
class FakeGithub {
  releases: FakeRelease[] = [];
  /** Отвечать 500 на всё. */
  down = false;
  /** Заголовки запросов: путь → authorization. */
  readonly auth = new Map<string, string | undefined>();
  readonly server: Server;
  url = "";

  constructor() {
    this.server = createServer((req, res) => {
      const path = new URL(req.url!, "http://x").pathname;

      this.auth.set(path, req.headers.authorization);
      if (this.down) return void res.writeHead(500).end();
      if (path === "/repos/example/agent/releases") {
        res.writeHead(200, { "content-type": "application/json" });
        res.end(
          JSON.stringify(
            this.releases.map(r => ({
              tag_name: r.tag,
              draft: r.draft ?? false,
              prerelease: r.prerelease ?? false,
              assets: Object.keys(r.files).map(name => ({
                name,
                browser_download_url: `${this.url}/download/${r.tag}/${name}`,
              })),
            })),
          ),
        );

        return;
      }
      const [, kind, tag, name] = path.split("/");
      const body = this.releases.find(r => r.tag === tag)?.files[name];

      if (kind !== "download" || body === undefined)
        return void res.writeHead(404).end();
      res.writeHead(200, { "content-length": Buffer.byteLength(body) });
      res.end(body);
    });
  }

  static start = async (): Promise<FakeGithub> => {
    const g = new FakeGithub();

    await new Promise<void>(r => g.server.listen(0, "127.0.0.1", r));
    g.url = `http://127.0.0.1:${(g.server.address() as AddressInfo).port}`;

    return g;
  };

  add(tag: string, extra: Partial<FakeRelease> = {}): void {
    this.releases.push({
      tag,
      files: releaseFiles(tag.replace(/^v/, "")),
      ...extra,
    });
  }

  close = (): Promise<void> =>
    new Promise(r => {
      this.server.closeAllConnections();
      this.server.close(() => r());
    });
}

const cleanup: (() => Promise<void>)[] = [];

afterEach(async () => {
  for (const f of cleanup.splice(0).reverse()) await f();
});

const github = async (): Promise<FakeGithub> => {
  const g = await FakeGithub.start();

  cleanup.push(g.close);

  return g;
};

const server = async (opts: AgentsOptions): Promise<TestServer> => {
  const s = await startServer(opts);

  cleanup.push(s.close);

  return s;
};

const fromGithub = (
  g: FakeGithub,
  more: Record<string, unknown> = {},
): AgentsOptions["agentReleases"] => ({
  github: "example/agent",
  apiUrl: g.url,
  ...more,
});

describe("удалённый источник сборок агента", () => {
  it("старшая версия в диапазоне: prerelease, draft и чужой мажор пропускаются", async () => {
    const g = await github();

    g.add("v1.1.0");
    g.add("v1.2.0");
    g.add("v1.3.0", { prerelease: true });
    g.add("v1.4.0", { draft: true });
    g.add("v1.5.0-rc.1");
    g.add("v2.0.0");
    g.add("v1.9.0", { files: { "agent-linux-amd64": "без manifest.json" } });
    const s = await server({
      agentReleases: fromGithub(g, { token: "gh-token" }),
    });
    const view = await s.agents.release();

    assert.equal(view?.version, "1.2.0");
    assert.equal(view?.remote?.from, "github:example/agent");
    assert.equal(view?.remote?.publicKey, AUTHOR_KEY);
    assert.deepEqual(view?.artifacts, [
      {
        os: "linux",
        arch: "amd64",
        file: "agent-linux-amd64",
        sha256: "sha-agent-1.2.0",
        signature: "sig",
        source: "remote",
        url: `${g.url}/download/v1.2.0/agent-linux-amd64`,
      },
    ]);
    assert.equal(view?.workers?.[0].file, "netprobe-1.2.0-linux-amd64");
    assert.equal(
      g.auth.get("/repos/example/agent/releases"),
      "Bearer gh-token",
    );
    assert.equal(
      g.auth.get("/download/v1.2.0/manifest.json"),
      undefined,
      "токен — только для API",
    );
    const narrow = await server({
      agentReleases: fromGithub(g, { range: "~1.1" }),
    });

    assert.equal((await narrow.agents.release())?.version, "1.1.0");
  });

  it("новая версия — событие release; ошибка сети — остаются прежние сборки", async () => {
    const g = await github();
    const logged: string[] = [];

    g.add("v1.1.0");
    const events: ReleaseEvent[] = [];
    const s = await server({
      agentReleases: fromGithub(g),
      log: msg => logged.push(msg),
    });

    s.agents.on("release", e => events.push(e));
    assert.equal((await s.agents.release())?.version, "1.1.0");
    g.add("v1.2.0");
    assert.equal((await s.agents.checkRelease())?.version, "1.2.0");
    // Первая проверка идёт с запуска: её событие (без previous) могло прийти до подписки.
    assert.deepEqual(events.at(-1), {
      version: "1.2.0",
      previous: "1.1.0",
      from: "github:example/agent",
    });
    const seen = events.length;

    g.down = true;
    g.add("v1.3.0");
    const view = await s.agents.checkRelease();

    assert.equal(view?.version, "1.2.0");
    assert.ok(logged.some(m => m.includes("остаются прежние")));
    assert.equal(events.length, seen);
    // Та же версия — без события.
    g.down = false;
    g.releases.pop();
    await s.agents.checkRelease();
    assert.equal(events.length, seen);
  });

  it("источник недоступен с самого начала — сборки из releasesDir, событие при появлении", async () => {
    const g = await github();
    const dir = mkdtempSync(join(tmpdir(), "agent-release-"));

    writeFileSync(
      join(dir, "manifest.json"),
      JSON.stringify(JSON.parse(releaseFiles("1.0.0")["manifest.json"])),
    );
    g.down = true;
    const s = await server({
      agentReleases: fromGithub(g),
      releasesDir: dir,
    });
    const events: ReleaseEvent[] = [];

    s.agents.on("release", e => events.push(e));
    const before = await s.agents.release();

    assert.equal(before?.version, "1.0.0");
    assert.equal(before?.artifacts[0].source, "local");
    g.down = false;
    g.add("v1.1.0");
    assert.equal((await s.agents.checkRelease())?.version, "1.1.0");
    assert.deepEqual(events, [
      { version: "1.1.0", from: "github:example/agent" },
    ]);
  });

  it("объединение с воркерами проекта из releasesDir: проект важнее при совпадении имён", async () => {
    const g = await github();
    const dir = mkdtempSync(join(tmpdir(), "agent-release-"));
    const worker = (name: string, version: string) => ({
      name,
      version,
      os: "linux",
      arch: "amd64",
      file: `${name}-${version}-linux-amd64`,
      sha256: `sha-${name}`,
      signature: "project-sig",
    });

    g.add("v1.2.0");
    writeFileSync(
      join(dir, "manifest.json"),
      JSON.stringify({
        version: "3.0.0",
        artifacts: [],
        workers: [worker("report", "3.0.0"), worker("netprobe", "0.9.0")],
      }),
    );
    writeFileSync(join(dir, "report-3.0.0-linux-amd64"), "REPORT");
    const s = await server({ agentReleases: fromGithub(g), releasesDir: dir });
    const view = await s.agents.release();

    assert.equal(view?.version, "1.2.0");
    assert.deepEqual(
      view?.workers?.map(w => [w.name, w.version, w.source, w.url]),
      [
        [
          "report",
          "3.0.0",
          "local",
          "/api/v1/agent-link/releases/report-3.0.0-linux-amd64",
        ],
        [
          "netprobe",
          "0.9.0",
          "local",
          "/api/v1/agent-link/releases/netprobe-0.9.0-linux-amd64",
        ],
      ],
    );
    const base = `${s.url}/api/v1/agent-link/releases`;
    const served = await (await fetch(`${base}/manifest.json`)).json();

    assert.deepEqual(served, {
      version: "1.2.0",
      artifacts: [
        {
          os: "linux",
          arch: "amd64",
          file: "agent-linux-amd64",
          sha256: "sha-agent-1.2.0",
          signature: "sig",
        },
      ],
      workers: [worker("report", "3.0.0"), worker("netprobe", "0.9.0")],
    });
    assert.equal(
      await (await fetch(`${base}/report-3.0.0-linux-amd64`)).text(),
      "REPORT",
    );

    // Кандидаты и действия — по итоговым сборкам; url — от корня сервера.
    const c = await enroll(s.url);
    const fa = await FakeAgent.connect(s.ws, c);

    fa.stream("status", {
      workers: [
        { name: "report", state: "running", version: "2.0.0", release: true },
        { name: "netprobe", state: "running", version: "1.0.0", release: true },
      ],
    });
    await fa.ackSeq(1);
    assert.deepEqual(
      (await s.agents.updateCandidates()).map(x => [x.current, x.target]),
      [["1.0.0", "1.2.0"]],
    );
    assert.deepEqual(
      (await s.agents.workerUpdateCandidates()).map(x => [x.worker, x.target]),
      [
        ["report", "3.0.0"],
        ["netprobe", "0.9.0"],
      ],
    );
    void s.agents.updateAgent(c.agentId).catch(() => {});
    const act = await fa.next("action");

    assert.deepEqual(act.data.args, {
      version: "1.2.0",
      url: "/api/v1/agent-link/releases/agent-linux-amd64",
      sha256: "sha-agent-1.2.0",
      signature: "sig",
    });
  });

  it("сборки источника: перенаправление 302 или поток через бэкенд (proxy)", async () => {
    const g = await github();

    g.add("v1.2.0");
    const s = await server({ agentReleases: fromGithub(g) });
    const base = `${s.url}/api/v1/agent-link/releases`;
    const res = await fetch(`${base}/agent-linux-amd64`, {
      redirect: "manual",
    });

    assert.equal(res.status, 302);
    assert.equal(
      res.headers.get("location"),
      `${g.url}/download/v1.2.0/agent-linux-amd64`,
    );
    assert.equal(
      await (await fetch(`${base}/netprobe-1.2.0-linux-amd64`)).text(),
      "NETPROBE 1.2.0",
      "fetch проходит перенаправление",
    );
    assert.equal((await fetch(`${base}/install.sh`)).status, 404);
    assert.equal((await fetch(`${base}/other`)).status, 404);

    const p = await server({ agentReleases: fromGithub(g, { proxy: true }) });
    const pbase = `${p.url}/api/v1/agent-link/releases`;
    const direct = await fetch(`${pbase}/agent-linux-amd64`, {
      redirect: "manual",
    });

    assert.equal(direct.status, 200);
    assert.equal(direct.headers.get("content-length"), "11");
    assert.equal(await direct.text(), "AGENT 1.2.0");
    const head = await fetch(`${pbase}/agent-linux-amd64`, { method: "HEAD" });

    assert.equal(head.status, 200);
    g.releases[0].files["agent-linux-amd64"] = undefined as unknown as string;
    assert.equal((await fetch(`${pbase}/agent-linux-amd64`)).status, 502);
  });

  it("install.sh из удалённого источника: адрес сервера и ключи проекта и автора агента", async () => {
    const g = await github();

    g.add("v1.2.0");
    const s = await server({
      agentReleases: fromGithub(g),
      publicKey: PROJECT_KEY,
      updatePublicKeys: [EXTRA_KEY, PROJECT_KEY, "не ключ"],
      baseUrl: "https://api.example.com",
    });
    const sh = await (
      await fetch(`${s.url}/api/v1/agent-link/install.sh`)
    ).text();

    assert.match(sh, /# 1\.2\.0/);
    assert.match(sh, /^DEFAULT_SERVER="https:\/\/api\.example\.com"$/m);
    assert.match(
      sh,
      new RegExp(
        `^DEFAULT_UPDATE_KEYS="${PROJECT_KEY} ${EXTRA_KEY} ${AUTHOR_KEY}"$`,
        "m",
      ),
    );
    const own = await server({
      agentReleases: fromGithub(g, { publicKey: EXTRA_KEY }),
    });
    const sh2 = await (
      await fetch(`${own.url}/api/v1/agent-link/install.sh`)
    ).text();

    assert.match(sh2, new RegExp(`^DEFAULT_UPDATE_KEYS="${EXTRA_KEY}"$`, "m"));
  });

  it("база сборок по ссылке (url): manifest.json, сборки и install.sh рядом", async () => {
    const g = await github();

    g.add("v1.2.0");
    const s = await server({
      agentReleases: { url: `${g.url}/download/v1.2.0/` },
    });
    const view = await s.agents.release();

    assert.equal(view?.version, "1.2.0");
    assert.equal(view?.remote?.from, `${g.url}/download/v1.2.0`);
    assert.equal(
      view?.workers?.[0].url,
      `${g.url}/download/v1.2.0/netprobe-1.2.0-linux-amd64`,
    );
    const sh = await (
      await fetch(`${s.url}/api/v1/agent-link/install.sh`)
    ).text();

    assert.match(sh, new RegExp(`DEFAULT_UPDATE_KEYS="${AUTHOR_KEY}"`));
  });

  it("неверные настройки источника — MESSAGE_INVALID", async () => {
    for (const agentReleases of [
      { github: "no-slash" },
      { github: "example/agent", range: "не диапазон" },
      { url: "ftp://example.com/x" },
    ])
      await assert.rejects(startServer({ agentReleases }), {
        code: "MESSAGE_INVALID",
      });
  });
});
