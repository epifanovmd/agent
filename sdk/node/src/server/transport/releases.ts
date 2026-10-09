// Раздача сборок (§11) по HTTP: manifest.json, сборки из манифеста (из releasesDir — файлом,
// из удалённого источника — перенаправлением 302 или потоком), install.sh с адресом сервера и
// ключами проверки. Публично: сборки подписаны.
import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import type { IncomingMessage, ServerResponse } from "node:http";
import { Readable } from "node:stream";

import type { Settings } from "../core/options";
import { safeServer } from "../lib/shell";
import {
  INSTALL_PATH,
  type ReleaseManifest,
  RELEASES_PATH,
} from "../protocol/messages";
import { baseUrl, sendJSON } from "./http";

/** Файл сборок: в каталоге или по ссылке удалённого источника (proxy — отдавать потоком). */
export type ReleaseFileRef = { path: string } | { url: string; proxy: boolean };

/** Что нужно раздаче сборок. */
export interface ReleaseSource {
  readonly settings: Pick<Settings, "baseUrl" | "trustProxy" | "log">;
  /** Раздача включена (releasesDir или agentReleases). */
  releaseEnabled(): boolean;
  /** manifest.json для узлов или null. */
  servedManifest(): Promise<ReleaseManifest | null>;
  /** Файл сборок по имени; не из манифеста — null. */
  releaseFile(name: string): Promise<ReleaseFileRef | null>;
  /** Скачать сборку удалённого источника (раздача потоком). */
  downloadRelease(url: string, signal: AbortSignal): Promise<Response>;
  /** install.sh и ключи проверки для подстановки; нет — null. */
  installScript(): Promise<{ script: string; keys: string[] } | null>;
}

const SAFE_KEY = /^[A-Za-z0-9+/=]+$/;

/** GET и HEAD install.sh и файлов сборок; не наш маршрут — false. */
export const serveRelease = async (
  src: ReleaseSource,
  req: IncomingMessage,
  res: ServerResponse,
  path: string,
): Promise<boolean> => {
  if ((req.method !== "GET" && req.method !== "HEAD") || !src.releaseEnabled())
    return false;
  if (path === INSTALL_PATH) return install(src, req, res);
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

/**
 * install.sh: DEFAULT_SERVER — baseUrl или адрес из запроса (иначе 400); DEFAULT_UPDATE_KEYS —
 * ключи проверки через пробел; DEFAULT_PUBLIC_KEY (установщик с одним ключом) — первый ключ.
 */
const install = async (
  src: ReleaseSource,
  req: IncomingMessage,
  res: ServerResponse,
): Promise<boolean> => {
  const { trustProxy, log } = src.settings;
  const found = await src.installScript();

  if (!found) return fileNotFound(res);
  // Значения попадают в строку sh в двойных кавычках — только безопасные символы.
  const server =
    src.settings.baseUrl?.replace(/\/$/, "") ?? baseUrl(req, trustProxy);

  if (!safeServer(server)) {
    sendJSON(res, 400, {
      code: "MESSAGE_INVALID",
      message: "Некорректный адрес сервера",
    });

    return true;
  }
  const keys = found.keys.filter(k => {
    if (SAFE_KEY.test(k)) return true;
    log("ключ проверки не base64: в install.sh не подставлен");

    return false;
  });
  const script = found.script
    .replace(/^DEFAULT_SERVER=""$/m, `DEFAULT_SERVER="${server}"`)
    .replace(
      /^DEFAULT_UPDATE_KEYS=""$/m,
      `DEFAULT_UPDATE_KEYS="${keys.join(" ")}"`,
    )
    .replace(
      /^DEFAULT_PUBLIC_KEY=""$/m,
      `DEFAULT_PUBLIC_KEY="${keys[0] ?? ""}"`,
    );

  res.writeHead(200, {
    "Content-Type": "text/x-shellscript; charset=utf-8",
    "Content-Length": Buffer.byteLength(script),
  });
  res.end(req.method === "HEAD" ? undefined : script);

  return true;
};

const fileNotFound = (res: ServerResponse): true => {
  sendJSON(res, 404, {
    code: "NOT_FOUND",
    message: "Нет такого файла сборок",
  });

  return true;
};
