// Раздача сборок (§11) по HTTP: manifest.json и сборки из манифеста (из releasesDir — файлом,
// из удалённого источника — перенаправлением 302 или потоком). Публично: сборки подписаны.
import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";

import type { Settings } from "../core/options";
import { type ReleaseManifest, RELEASES_PATH } from "../protocol/messages";
import { sendJSON } from "./http";

/** Файл сборок: в каталоге или по ссылке удалённого источника (proxy — отдавать потоком). */
export type ReleaseFileRef = { path: string } | { url: string; proxy: boolean };

/** Что нужно раздаче сборок. */
export interface ReleaseSource {
  readonly settings: Pick<Settings, "log">;
  /** Раздача включена (releasesDir или agentReleases). */
  releaseEnabled(): boolean;
  /** manifest.json для узлов или null. */
  servedManifest(): Promise<ReleaseManifest | null>;
  /** Файл сборок по имени; не из манифеста — null. */
  releaseFile(name: string): Promise<ReleaseFileRef | null>;
  /** Скачать сборку удалённого источника (раздача потоком). */
  downloadRelease(url: string, signal: AbortSignal): Promise<Response>;
}

/** GET и HEAD файлов сборок; не наш маршрут — false. */
export const serveRelease = async (
  src: ReleaseSource,
  req: IncomingMessage,
  res: ServerResponse,
  path: string,
): Promise<boolean> => {
  if ((req.method !== "GET" && req.method !== "HEAD") || !src.releaseEnabled())
    return false;
  if (!path.startsWith(RELEASES_PATH + "/")) return false;
  let name: string;

  try {
    name = decodeURIComponent(path.slice(RELEASES_PATH.length + 1));
  } catch {
    return fileNotFound(res);
  }
  if (name === "manifest.json") {
    const manifest = await src.servedManifest();

    if (!manifest) return fileNotFound(res);
    sendJSON(res, 200, manifest);

    return true;
  }
  const ref = await src.releaseFile(name);

  if (!ref) return fileNotFound(res);
  if ("path" in ref) return localFile(req, res, ref.path, name);
  if (!ref.proxy) {
    res.writeHead(302, { Location: ref.url, "Content-Length": 0 });
    res.end();

    return true;
  }

  return proxyFile(src, req, res, ref.url, name);
};

/** Сборка из releasesDir. */
const localFile = async (
  req: IncomingMessage,
  res: ServerResponse,
  path: string,
  name: string,
): Promise<boolean> => {
  let size: number;

  try {
    size = (await stat(path)).size;
  } catch {
    return fileNotFound(res);
  }
  res.writeHead(200, {
    "Content-Type": "application/octet-stream",
    "Content-Length": size,
    "Content-Disposition": `attachment; filename="${name}"`,
  });
  if (req.method === "HEAD") res.end();
  else createReadStream(path).pipe(res);

  return true;
};

/** Сборка удалённого источника потоком через бэкенд (agentReleases.proxy). */
const proxyFile = async (
  src: ReleaseSource,
  req: IncomingMessage,
  res: ServerResponse,
  url: string,
  name: string,
): Promise<boolean> => {
  const abort = new AbortController();

  res.once("close", () => abort.abort());
  let upstream: Response;

  try {
    upstream = await src.downloadRelease(url, abort.signal);
  } catch (e) {
    src.settings.log("сборка не получена из источника", {
      url,
      err: String(e),
    });
    sendJSON(res, 502, {
      code: "UPSTREAM_FAILED",
      message: "Сборка не получена из источника",
    });

    return true;
  }
  if (!upstream.ok || !upstream.body) {
    await upstream.body?.cancel();
    src.settings.log("сборка не получена из источника", {
      url,
      status: upstream.status,
    });
    sendJSON(res, 502, {
      code: "UPSTREAM_FAILED",
      message: `Источник сборок ответил ${upstream.status}`,
    });

    return true;
  }
  const length = upstream.headers.get("content-length");

  res.writeHead(200, {
    "Content-Type": "application/octet-stream",
    ...(length ? { "Content-Length": length } : {}),
    "Content-Disposition": `attachment; filename="${name}"`,
  });
  if (req.method === "HEAD") {
    await upstream.body.cancel();
    res.end();

    return true;
  }
  Readable.fromWeb(upstream.body as import("node:stream/web").ReadableStream)
    .on("error", () => res.destroy())
    .pipe(res);

  return true;
};

const fileNotFound = (res: ServerResponse): true => {
  sendJSON(res, 404, {
    code: "NOT_FOUND",
    message: "Нет такого файла сборок",
  });

  return true;
};
