// Выпуск (§11): итоговый выпуск — удалённый источник агента (agentReleases) и воркеры проекта из
// releasesDir; сборки для обновления агентов и воркеров, кандидаты на обновление, install.sh.
// Раздачу файлов выпуска делает транспорт.
import { readFile } from "node:fs/promises";
import { join } from "node:path";

import type { Context } from "../core/context";
import { codeError } from "../core/errors";
import { newerVersion, sameVersion } from "../lib/version";
import { publicAgent } from "../model/public-agent";
import type {
  AgentRecord,
  ReleaseLocation,
  ReleaseView,
  UpdateCandidate,
  WorkerUpdateCandidate,
} from "../model/types";
import { parseManifest } from "../protocol/checks";
import {
  type ReleaseArtifact,
  type ReleaseManifest,
  RELEASES_PATH,
  type WorkerArtifact,
} from "../protocol/messages";
import { AgentReleases, isAbsolute } from "./agent-releases";

/** Аргументы действия обновления: версия и где взять сборку. */
export interface UpdateArgs extends Record<string, unknown> {
  version: string;
  url: string;
  sha256: string;
  signature: string;
}

/** Где взять файл выпуска: в releasesDir или по ссылке удалённого источника. */
export type ReleaseFile = { path: string } | { url: string; proxy: boolean };

/** install.sh для раздачи и ключи проверки, которые в него подставить. */
export interface InstallScript {
  script: string;
  keys: string[];
}

export class Release {
  private readonly ctx: Context;
  private readonly remote?: AgentReleases;

  constructor(ctx: Context) {
    this.ctx = ctx;
    const opts = ctx.settings.agentReleases;

    if (opts) {
      this.remote = new AgentReleases(ctx, opts);
      this.remote.start();
    }
  }

  close(): void {
    this.remote?.close();
  }

  /** Раздавать ли выпуск: есть releasesDir или удалённый источник. */
  enabled(): boolean {
    return Boolean(this.ctx.settings.releasesDir || this.remote);
  }

  /** Проверить удалённый источник сейчас и вернуть итоговый выпуск. */
  async check(): Promise<ReleaseView | null> {
    await this.remote?.check();

    return this.manifest();
  }

  /** manifest.json каталога releasesDir или null. */
  private async local(): Promise<ReleaseManifest | null> {
    const dir = this.ctx.settings.releasesDir;

    if (!dir) return null;
    let raw: unknown;

    try {
      raw = JSON.parse(await readFile(join(dir, "manifest.json"), "utf8"));
    } catch {
      return null;
    }
    const m = parseManifest(raw);

    if (!m.ok) {
      this.ctx.log("manifest.json не принят", { reason: m.error });

      return null;
    }

    return m.value;
  }

  /**
   * Итоговый выпуск: агент и его воркеры — из удалённого источника (пока он не получен — из
   * releasesDir), воркеры проекта — из releasesDir (при совпадении имён важнее). Нет ни того, ни
   * другого — null.
   */
  async manifest(): Promise<ReleaseView | null> {
    const [local, remote] = await Promise.all([
      this.local(),
      this.remote?.release() ?? null,
    ]);
    const here = <T extends ReleaseArtifact>(a: T): T & ReleaseLocation => ({
      ...a,
      source: "local",
      url: `${RELEASES_PATH}/${encodeURIComponent(a.file)}`,
    });

    if (!remote) {
      if (!local) return null;
      const { workers: own, artifacts, ...rest } = local;

      return {
        ...rest,
        artifacts: artifacts.map(here),
        ...(own ? { workers: own.map(here) } : {}),
      };
    }
    const away = <T extends ReleaseArtifact>(a: T): T & ReleaseLocation => ({
      ...a,
      file: fileName(a.file),
      source: "remote",
      url: a.file,
    });
    const project = new Set((local?.workers ?? []).map(w => w.name));
    const workers = [
      ...(remote.manifest.workers ?? [])
        .filter(w => !project.has(w.name))
        .map(away),
      ...(local?.workers ?? []).map(here),
    ];

    return {
      version: remote.manifest.version,
      artifacts: remote.manifest.artifacts.map(away),
      ...(workers.length ? { workers } : {}),
      remote: {
        version: remote.version,
        from: remote.from,
        checkedAt: remote.checkedAt,
        ...(remote.manifest.publicKey
          ? { publicKey: remote.manifest.publicKey }
          : {}),
      },
    };
  }

  /** manifest.json для узлов: итоговый выпуск без источников (file — имя для …/releases/<file>). */
  async served(): Promise<ReleaseManifest | null> {
    const view = await this.manifest();

    if (!view) return null;
    const strip = <T extends ReleaseArtifact>({
      source: _s,
      url: _u,
      ...a
    }: T & ReleaseLocation): T => a as unknown as T;
    const { remote: _r, artifacts, workers, ...rest } = view;

    return {
      ...rest,
      artifacts: artifacts.map(strip),
      ...(workers ? { workers: workers.map(strip) } : {}),
    };
  }

  /** Файл выпуска по имени: только сборки из итогового выпуска. */
  async file(name: string): Promise<ReleaseFile | null> {
    const view = await this.manifest();
    const art = [...(view?.artifacts ?? []), ...(view?.workers ?? [])].find(
      a => a.file === name,
    );

    if (!art || name.includes("/") || name.includes("\\")) return null;
    if (art.source === "remote")
      return {
        url: art.url,
        proxy: Boolean(this.ctx.settings.agentReleases?.proxy),
      };

    return { path: join(this.ctx.settings.releasesDir!, art.file) };
  }

  /** Скачать сборку удалённого источника (раздача потоком). */
  download(url: string, signal?: AbortSignal): Promise<Response> {
    return this.remote
      ? this.remote.download(url, signal)
      : Promise.reject(new Error("нет удалённого источника"));
  }

  /**
   * install.sh: из удалённого выпуска (если он получен и в нём есть install.sh), иначе — из
   * releasesDir. Ключи: publicKey, updatePublicKeys (проект) и ключ автора агента
   * (agentReleases.publicKey или publicKey удалённого manifest.json).
   */
  async installScript(): Promise<InstallScript | null> {
    const { releasesDir, publicKey, updatePublicKeys, agentReleases } =
      this.ctx.settings;
    const remote = await this.remote?.release();
    let script = remote?.installScript;

    if (script === undefined && releasesDir)
      script = await readFile(join(releasesDir, "install.sh"), "utf8").catch(
        () => undefined,
      );
    if (script === undefined) return null;
    const author = agentReleases?.publicKey ?? remote?.manifest.publicKey;
    const keys = [publicKey, ...updatePublicKeys, author].filter(
      (k, i, all): k is string => Boolean(k) && all.indexOf(k) === i,
    );

    return { script, keys };
  }

  /** Аргументы agent.update для агента; сборки нет — UPDATE_NOT_AVAILABLE. */
  async agentUpdate(agent: AgentRecord): Promise<UpdateArgs> {
    const m = await this.required();
    const { os = "", arch = "" } = agent.hello?.host ?? {};
    const art = agentArtifact(m, os, arch);

    if (!art)
      throw codeError(
        "UPDATE_NOT_AVAILABLE",
        `нет сборки ${os}/${arch} в выпуске ${m.version}`,
      );

    return updateArgs(m.version, art);
  }

  /** Аргументы worker.update; воркер не из выпуска — WORKER_NOT_RELEASED, сборки нет — UPDATE_NOT_AVAILABLE. */
  async workerUpdate(
    agent: AgentRecord,
    name: string,
  ): Promise<UpdateArgs & { name: string }> {
    const m = await this.required();
    const fromRelease = (list?: { name: string; release?: boolean }[]) =>
      list?.some(w => w.name === name && w.release);

    if (
      !fromRelease(agent.hello?.workers) &&
      !fromRelease(agent.status?.workers)
    )
      throw codeError("WORKER_NOT_RELEASED", `воркер ${name} не из выпуска`);
    const { os = "", arch = "" } = agent.hello?.host ?? {};
    const art = workerArtifact(m, name, os, arch);

    if (!art)
      throw codeError(
        "UPDATE_NOT_AVAILABLE",
        `нет сборки воркера ${name} ${os}/${arch} в выпуске ${m.version}`,
      );

    return { name, ...updateArgs(art.version, art) };
  }

  /** Агенты, чья версия не как в выпуске и для чьих os/arch есть сборка. */
  async updateCandidates(): Promise<UpdateCandidate[]> {
    const m = await this.manifest();

    if (!m) return [];
    const out: UpdateCandidate[] = [];

    for (const a of await this.ctx.store.listAgents()) {
      const h = a.hello;

      if (a.revoked || !h || sameVersion(h.agent.version, m.version)) continue;
      if (!agentArtifact(m, h.host.os, h.host.arch)) continue;
      const { os, arch } = h.host;

      out.push({
        agentId: a.id,
        name: a.name,
        online: a.online,
        current: h.agent.version,
        target: m.version,
        os,
        arch,
      });
    }

    return out;
  }

  /** Воркеры из выпуска, чья версия не как у новейшей сборки в manifest.json. */
  async workerUpdateCandidates(): Promise<WorkerUpdateCandidate[]> {
    const m = await this.manifest();

    if (!m?.workers?.length) return [];
    const out: WorkerUpdateCandidate[] = [];

    for (const a of await this.ctx.store.listAgents()) {
      if (a.revoked || !a.hello) continue;
      const { os, arch } = a.hello.host;

      for (const w of publicAgent(a).workers) {
        if (!w.release) continue;
        const art = workerArtifact(m, w.name, os, arch);

        if (!art || sameVersion(w.version ?? "", art.version)) continue;
        out.push({
          agentId: a.id,
          agentName: a.name,
          online: a.online,
          worker: w.name,
          current: w.version ?? "",
          target: art.version,
          os,
          arch,
        });
      }
    }

    return out;
  }

  private async required(): Promise<ReleaseManifest> {
    const m = await this.manifest();

    if (!m)
      throw codeError(
        "UPDATE_NOT_AVAILABLE",
        "нет выпуска (releasesDir, agentReleases)",
      );

    return m;
  }
}

/** Сборка агента под os/arch. */
const agentArtifact = (
  m: ReleaseManifest,
  os: string,
  arch: string,
): ReleaseArtifact | undefined => {
  return m.artifacts.find(x => x.os === os && x.arch === arch);
};

/** Новейшая сборка воркера под os/arch. */
const workerArtifact = (
  m: ReleaseManifest,
  name: string,
  os: string,
  arch: string,
): WorkerArtifact | undefined => {
  let best: WorkerArtifact | undefined;

  for (const w of m.workers ?? []) {
    if (w.name !== name || w.os !== os || w.arch !== arch) continue;
    if (!best || newerVersion(w.version, best.version)) best = w;
  }

  return best;
};

/** Имя файла сборки: у ссылки — последняя часть пути. */
const fileName = (file: string): string => {
  if (!isAbsolute(file)) return file;
  const last = new URL(file).pathname.split("/").pop() ?? "";

  try {
    return decodeURIComponent(last);
  } catch {
    return last;
  }
};

/**
 * Аргументы обновления: url — всегда от корня сервера. Сборку удалённого источника сервер отдаёт
 * перенаправлением или потоком: ключ агента не уходит на чужой хост и у агентов прежних версий.
 */
const updateArgs = (version: string, art: ReleaseArtifact): UpdateArgs => {
  return {
    version,
    url: `${RELEASES_PATH}/${encodeURIComponent(art.file)}`,
    sha256: art.sha256,
    signature: art.signature ?? "",
  };
};
