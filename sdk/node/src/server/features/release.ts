// Сборки (§11): воркеры проекта из releasesDir — manifest.json для узлов, файлы сборок и
// аргументы worker.update. Раздачу файлов делает транспорт.
import { readFile } from "node:fs/promises";
import { join } from "node:path";

import type { Context } from "../core/context";
import { codeError } from "../core/errors";
import { newerVersion } from "../lib/version";
import type { AgentRecord, ReleaseView } from "../model/types";
import { parseManifest } from "../protocol/checks";
import {
  type ReleaseArtifact,
  type ReleaseManifest,
  RELEASES_PATH,
  type WorkerArtifact,
} from "../protocol/messages";

/** Аргументы worker.update: версия и где взять сборку. */
export interface UpdateArgs extends Record<string, unknown> {
  name: string;
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

  /** Раздавать ли сборки: задан releasesDir. */
  enabled(): boolean {
    return Boolean(this.ctx.settings.releasesDir);
  }

  /** manifest.json каталога releasesDir для узлов или null. */
  async served(): Promise<ReleaseManifest | null> {
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

  /** Сборки releasesDir со ссылкой на каждую (от корня сервера) или null. */
  async manifest(): Promise<ReleaseView | null> {
    const m = await this.served();

    if (!m) return null;
    const here = <T extends ReleaseArtifact>(a: T): T & { url: string } => ({
      ...a,
      url: fileUrl(a.file),
    });
    const { workers, artifacts, ...rest } = m;

    return {
      ...rest,
      artifacts: artifacts.map(here),
      ...(workers ? { workers: workers.map(here) } : {}),
    };
  }

  /** Путь к файлу сборок по имени: только из manifest.json. */
  async file(name: string): Promise<string | null> {
    const m = await this.served();
    const art = [...(m?.artifacts ?? []), ...(m?.workers ?? [])].find(
      a => a.file === name,
    );

    if (!art || name.includes("/") || name.includes("\\")) return null;

    return join(this.ctx.settings.releasesDir!, art.file);
  }

  /** Аргументы worker.update; воркер без сборки с сервера — WORKER_NOT_RELEASED, сборки нет — UPDATE_NOT_AVAILABLE. */
  async workerUpdate(agent: AgentRecord, name: string): Promise<UpdateArgs> {
    const m = await this.served();

    if (!m) throw codeError("UPDATE_NOT_AVAILABLE", "нет сборок (releasesDir)");
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

    return {
      name,
      version: art.version,
      url: fileUrl(art.file),
      sha256: art.sha256,
      signature: art.signature ?? "",
    };
  }
}

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

/** Ссылка на файл сборки: абсолютная — как есть, имя — от корня сервера (…/releases/<file>). */
const fileUrl = (file: string): string =>
  /^https?:\/\//i.test(file)
    ? file
    : `${RELEASES_PATH}/${encodeURIComponent(file)}`;
