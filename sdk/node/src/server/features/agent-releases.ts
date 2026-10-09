// Удалённый источник сборок агента (опция agentReleases): релизы GitHub или база сборок по
// ссылке. Проверка — при старте и раз в checkIntervalMs; последние полученные сборки хранятся в
// памяти, ошибка сети — предупреждение в журнал, остаётся прежний. Подписи сборок проверяет агент.
import { major, rcompare, satisfies, valid, validRange } from "semver";

import type { Context } from "../core/context";
import { invalid } from "../core/errors";
import type { AgentReleasesOptions } from "../core/options";
import { SDK_VERSION } from "../lib/sdk-version";
import { parseGithubReleases, parseManifest } from "../protocol/checks";
import type { ReleaseManifest } from "../protocol/messages";

/** Полученные удалённые сборки: file сборок — абсолютные ссылки. */
export interface RemoteRelease {
  version: string;
  /** Источник: `github:owner/repo` или база сборок. */
  from: string;
  manifest: ReleaseManifest;
  /** install.sh из источника; нет в источнике — undefined. */
  installScript?: string;
  /** Когда проверен источник, мс. */
  checkedAt: number;
}

/** Где взять manifest.json и файлы сборок: ссылка на manifest, имя файла → ссылка. */
interface Located {
  version?: string;
  manifestUrl: string;
  fileUrl(name: string): string | undefined;
}

const HOUR_MS = 3_600_000;
const REPO = /^[A-Za-z0-9_.-]+\/[A-Za-z0-9_.-]+$/;

/** Диапазон версий по умолчанию: та же мажорная версия, что у SDK. */
export const defaultRange = (): string => `^${major(SDK_VERSION)}`;

export class AgentReleases {
  private readonly ctx: Context;
  private readonly opts: AgentReleasesOptions;
  private readonly fetch: typeof globalThis.fetch;
  private readonly range: string;
  readonly from: string;
  private current: RemoteRelease | null = null;
  private inflight?: Promise<RemoteRelease | null>;
  private first?: Promise<unknown>;
  private timer?: NodeJS.Timeout;
  private closed = false;

  constructor(ctx: Context, opts: AgentReleasesOptions) {
    this.ctx = ctx;
    this.opts = opts;
    this.fetch = opts.fetch ?? globalThis.fetch;
    if ("github" in opts) {
      if (!REPO.test(opts.github))
        throw invalid(
          `agentReleases.github: нужно owner/repo, а не ${opts.github}`,
        );
      this.range = opts.range ?? defaultRange();
      if (!validRange(this.range))
        throw invalid(`agentReleases.range: не диапазон semver: ${this.range}`);
      this.from = `github:${opts.github}`;
    } else {
      if (!/^https?:\/\/[^/]/.test(opts.url))
        throw invalid(
          `agentReleases.url: нужна ссылка http(s)://, а не ${opts.url}`,
        );
      this.range = "*";
      this.from = opts.url.replace(/\/+$/, "");
    }
  }

  /** Первая проверка и проверки по таймеру. */
  start(): void {
    this.first = this.check();
    const timer = setInterval(
      () => void this.check(),
      this.opts.checkIntervalMs ?? HOUR_MS,
    );

    timer.unref();
    this.timer = timer;
  }

  close(): void {
    this.closed = true;
    clearInterval(this.timer);
  }

  /** Последние полученные сборки; первая проверка ещё идёт — дождаться её. */
  async release(): Promise<RemoteRelease | null> {
    await this.first;

    return this.current;
  }

  /** Проверить источник сейчас; одновременные вызовы ждут одну проверку. */
  check(): Promise<RemoteRelease | null> {
    this.inflight ??= this.load().finally(() => {
      this.inflight = undefined;
    });

    return this.inflight;
  }

  private async load(): Promise<RemoteRelease | null> {
    try {
      const where = await this.locate();

      if (!where) {
        this.ctx.log("сборки агента: в источнике нет подходящей версии", {
          from: this.from,
          range: this.range,
        });

        return this.current;
      }
      if (
        this.current &&
        where.version !== undefined &&
        where.version === this.current.version
      ) {
        this.current = { ...this.current, checkedAt: Date.now() };

        return this.current;
      }
      const raw = await this.json(where.manifestUrl, false);
      const parsed = parseManifest(raw);

      if (!parsed.ok) throw new Error(parsed.error);
      const manifest = absolute(parsed.value, where);
      const script = where.fileUrl("install.sh");
      const installScript = script
        ? await this.text(script).catch(() => undefined)
        : undefined;
      const prev = this.current;

      if (this.closed) return prev;
      this.current = {
        version: manifest.version,
        from: this.from,
        manifest,
        installScript: installScript ?? prev?.installScript,
        checkedAt: Date.now(),
      };
      if (prev?.version !== manifest.version)
        this.ctx.emit("release", {
          version: manifest.version,
          ...(prev ? { previous: prev.version } : {}),
          from: this.from,
        });

      return this.current;
    } catch (e) {
      if (!this.closed)
        this.ctx.log("сборки агента не получены — остаются прежние", {
          from: this.from,
          err: String(e instanceof Error ? e.message : e),
          version: this.current?.version ?? null,
        });

      return this.current;
    }
  }

  /** Где manifest.json и файлы: старший подходящий релиз GitHub или база сборок. */
  private async locate(): Promise<Located | null> {
    const o = this.opts;

    if (!("github" in o)) {
      const base = this.from;

      return {
        manifestUrl: `${base}/manifest.json`,
        fileUrl: name => `${base}/${encodeURIComponent(name)}`,
      };
    }
    const api = (o.apiUrl ?? "https://api.github.com").replace(/\/+$/, "");
    const list = parseGithubReleases(
      await this.json(`${api}/repos/${o.github}/releases?per_page=100`, true),
    );

    if (!list.ok) throw new Error(list.error);
    const fits = list.value
      .filter(r => !r.draft && !r.prerelease)
      .map(r => ({ r, version: valid(r.tag_name) }))
      .filter(
        (x): x is { r: (typeof list.value)[number]; version: string } =>
          x.version !== null &&
          satisfies(x.version, this.range) &&
          x.r.assets.some(a => a.name === "manifest.json"),
      )
      .sort((a, b) => rcompare(a.version, b.version));
    const best = fits[0];

    if (!best) return null;
    const assets = new Map(
      best.r.assets.map(a => [a.name, a.browser_download_url]),
    );

    return {
      version: best.version,
      manifestUrl: assets.get("manifest.json")!,
      fileUrl: name => assets.get(name),
    };
  }

  private headers(api: boolean): Record<string, string> {
    const h: Record<string, string> = {
      "user-agent": `agent-sdk/${SDK_VERSION}`,
    };

    if (api) {
      h.accept = "application/vnd.github+json";
      h["x-github-api-version"] = "2022-11-28";
      if ("github" in this.opts && this.opts.token)
        h.authorization = `Bearer ${this.opts.token}`;
    }

    return h;
  }

  private async get(url: string, api: boolean): Promise<Response> {
    const res = await this.fetch(url, {
      headers: this.headers(api),
      signal: AbortSignal.timeout(30_000),
    });

    if (!res.ok) {
      await res.body?.cancel();
      throw new Error(`${url}: HTTP ${res.status}`);
    }

    return res;
  }

  private async json(url: string, api: boolean): Promise<unknown> {
    return (await this.get(url, api)).json();
  }

  private async text(url: string): Promise<string> {
    return (await this.get(url, false)).text();
  }

  /** Скачать сборку (раздача через бэкенд потоком, agentReleases.proxy). */
  download(url: string, signal?: AbortSignal): Promise<Response> {
    return this.fetch(url, { headers: this.headers(false), signal });
  }
}

/** Ссылка вида http(s)://… */
export const isAbsolute = (file: string): boolean => /^https?:\/\//i.test(file);

/** file сборок → абсолютные ссылки; сборки без файла в источнике пропускаются. */
const absolute = (m: ReleaseManifest, where: Located): ReleaseManifest => {
  const link = <T extends { file: string }>(a: T): T[] => {
    const url = isAbsolute(a.file) ? a.file : where.fileUrl(a.file);

    return url ? [{ ...a, file: url }] : [];
  };

  return {
    ...m,
    artifacts: m.artifacts.flatMap(link),
    ...(m.workers ? { workers: m.workers.flatMap(link) } : {}),
  };
};
