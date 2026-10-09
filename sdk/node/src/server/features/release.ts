// Выпуск (§11): manifest.json каталога выпуска, сборки для обновления агентов и воркеров,
// кандидаты на обновление. Раздачу файлов выпуска делает транспорт.
import { readFile } from "node:fs/promises";
import { join } from "node:path";

import type { Context } from "../core/context";
import { codeError } from "../core/errors";
import { newerVersion, sameVersion } from "../lib/version";
import { publicAgent } from "../model/public-agent";
import type {
  AgentRecord,
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

/** Аргументы действия обновления: версия и где взять сборку. */
export interface UpdateArgs extends Record<string, unknown> {
  version: string;
  url: string;
  sha256: string;
  signature: string;
}

export class Release {
  private readonly ctx: Context;

  constructor(ctx: Context) {
    this.ctx = ctx;
  }

  /** manifest.json каталога выпуска или null. */
  async manifest(): Promise<ReleaseManifest | null> {
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
      throw codeError("UPDATE_NOT_AVAILABLE", "нет выпуска (releasesDir)");

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

const updateArgs = (version: string, art: ReleaseArtifact): UpdateArgs => {
  return {
    version,
    url: `${RELEASES_PATH}/${encodeURIComponent(art.file)}`,
    sha256: art.sha256,
    signature: art.signature ?? "",
  };
};
