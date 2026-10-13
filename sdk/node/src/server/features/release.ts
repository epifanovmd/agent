// Сборки (§11): итоговые сборки — удалённый источник агента (agentReleases) и воркеры проекта из
// releasesDir; сборки для обновления агентов и воркеров, кандидаты на обновление, install.sh.
// Раздачу файлов сборок делает транспорт.
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
  UpdateCandidates,
} from "../model/types";
import { parseManifest } from "../protocol/checks";
import {
  type Hello,
  type ReleaseArtifact,
  type ReleaseManifest,
  RELEASES_PATH,
  type WorkerArtifact,
} from "../protocol/messages";
import { AgentReleases, isAbsolute } from "./agent-releases";

/** Обновляет ли агент себя по agent.update: updateMode нет (старый агент) или self. */
const selfUpdating = (h: Hello): boolean =>
  !h.agent.updateMode || h.agent.updateMode === "self";

/** Аргументы действия обновления: версия и где взять сборку. */
export interface UpdateArgs extends Record<string, unknown> {
  version: string;
  url: string;
  sha256: string;
  signature: string;
}

/** Где взять файл сборок: в releasesDir или по ссылке удалённого источника. */
export type ReleaseFile = { path: string } | { url: string; proxy: boolean };

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

  /** Раздавать ли сборки: есть releasesDir или удалённый источник. */
  enabled(): boolean {
    return Boolean(this.ctx.settings.releasesDir || this.remote);
  }

  /** Проверить удалённый источник сейчас и вернуть итоговые сборки. */
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
   * Итоговые сборки: агент и его воркеры — из удалённого источника (пока он не получен — из
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

  /** manifest.json для узлов: итоговые сборки без источников (file — имя для …/releases/<file>). */
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

  /** Файл сборок по имени: только из итоговых сборок. */
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

  /** Аргументы agent.update для агента; сборки нет — UPDATE_NOT_AVAILABLE. */
  async agentUpdate(agent: AgentRecord): Promise<UpdateArgs> {
    const m = await this.required();
    const { os = "", arch = "" } = agent.hello?.host ?? {};
    const art = agentArtifact(m, os, arch);

    if (!art)
      throw codeError(
        "UPDATE_NOT_AVAILABLE",
        `нет сборки ${os}/${arch} в версии ${m.version}`,
      );

    return updateArgs(m.version, art);
  }

  /** Аргументы worker.update; воркер без сборки с сервера — WORKER_NOT_RELEASED, сборки нет — UPDATE_NOT_AVAILABLE. */
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
      throw codeError(
        "WORKER_NOT_RELEASED",
        `воркер ${name} не со сборкой с сервера`,
      );
    const { os = "", arch = "" } = agent.hello?.host ?? {};
    const art = workerArtifact(m, name, os, arch);

    if (!art)
      throw codeError(
        "UPDATE_NOT_AVAILABLE",
        `нет сборки воркера ${name} ${os}/${arch} в версии ${m.version}`,
      );

    return { name, ...updateArgs(art.version, art) };
  }

  /**
   * Кого можно обновить. Агенты: до версии сборок сервера (source: server — сборка под их
   * os/arch есть, версия другая) или до новой версии, которую агент нашёл в своём каталоге сборок
   * сам (source: agent, hello/status → update.latest); из двух целей — новейшая. Агенты, которые
   * себя не обновляют (updateMode не self), не входят. Воркеры со сборкой с сервера — до версии
   * новейшей сборки в manifest.json.
   */
  async updateCandidates(): Promise<UpdateCandidates> {
    const m = await this.manifest();
    const out: UpdateCandidates = { agents: [], workers: [] };

    for (const a of await this.ctx.store.listAgents()) {
      const h = a.hello;

      if (a.revoked || !h) continue;
      const { os, arch } = h.host;
      const base = {
        agentId: a.id,
        agentName: a.name,
        online: a.online,
        os,
        arch,
      };
      const current = h.agent.version;
      const fromServer =
        m && !sameVersion(current, m.version) && agentArtifact(m, os, arch)
          ? m.version
          : undefined;
      const fromAgent = publicAgent(a).update?.latest;

      if (selfUpdating(h)) {
        if (
          fromAgent &&
          newerVersion(fromAgent, current) &&
          (!fromServer || newerVersion(fromAgent, fromServer))
        )
          out.agents.push({
            ...base,
            current,
            target: fromAgent,
            source: "agent",
          });
        else if (fromServer)
          out.agents.push({
            ...base,
            current,
            target: fromServer,
            source: "server",
          });
      }
      for (const w of m?.workers?.length ? publicAgent(a).workers : []) {
        const art = w.release && workerArtifact(m!, w.name, os, arch);

        if (!art || sameVersion(w.version ?? "", art.version)) continue;
        out.workers.push({
          ...base,
          worker: w.name,
          current: w.version ?? "",
          target: art.version,
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
        "нет сборок (releasesDir, agentReleases)",
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
