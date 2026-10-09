// Раздача выпуска (§11) по HTTP: manifest.json, сборки из манифеста, install.sh с адресом
// сервера и ключом проверки. Публично: сборки подписаны.
import { createReadStream } from "node:fs";
import { readFile, stat } from "node:fs/promises";
import type { IncomingMessage, ServerResponse } from "node:http";
import { join } from "node:path";

import type { Settings } from "../core/options";
import { safeServer } from "../lib/shell";
import {
  INSTALL_PATH,
  type ReleaseManifest,
  RELEASES_PATH,
} from "../protocol/messages";
import { baseUrl, sendJSON } from "./http";

/** Что нужно раздаче: настройки выпуска, manifest.json, журнал. */
export interface ReleaseSource {
  readonly settings: Pick<
    Settings,
    "releasesDir" | "publicKey" | "baseUrl" | "trustProxy" | "log"
  >;
  manifest(): Promise<ReleaseManifest | null>;
}

const SAFE_KEY = /^[A-Za-z0-9+/=]*$/;

/** GET и HEAD install.sh и файлов выпуска; не наш маршрут — false. */
export const serveRelease = async (
  src: ReleaseSource,
  req: IncomingMessage,
  res: ServerResponse,
  path: string,
): Promise<boolean> => {
  if (
    (req.method !== "GET" && req.method !== "HEAD") ||
    !src.settings.releasesDir
  )
    return false;
  if (path === INSTALL_PATH) return install(src, req, res);
  if (!path.startsWith(RELEASES_PATH + "/")) return false;
  let name: string;

  try {
    name = decodeURIComponent(path.slice(RELEASES_PATH.length + 1));
  } catch {
    return fileNotFound(res);
  }

  return file(src, req, res, name);
};

/** manifest.json или сборка агента или воркера, перечисленная в манифесте (других файлов каталога не отдаём). */
const file = async (
  src: ReleaseSource,
  req: IncomingMessage,
  res: ServerResponse,
  name: string,
): Promise<boolean> => {
  const manifest = await src.manifest();

  if (!manifest) return fileNotFound(res);
  if (name === "manifest.json") {
    sendJSON(res, 200, manifest);

    return true;
  }
  const art = [...manifest.artifacts, ...(manifest.workers ?? [])].find(
    a => a.file === name,
  );

  if (!art || name.includes("/") || name.includes("\\"))
    return fileNotFound(res);
  const path = join(src.settings.releasesDir!, art.file);
  let size: number;

  try {
    size = (await stat(path)).size;
  } catch {
    return fileNotFound(res);
  }
  res.writeHead(200, {
    "Content-Type": "application/octet-stream",
    "Content-Length": size,
    "Content-Disposition": `attachment; filename="${art.file}"`,
  });
  if (req.method === "HEAD") res.end();
  else createReadStream(path).pipe(res);

  return true;
};

/** install.sh: DEFAULT_SERVER — baseUrl или адрес из запроса (иначе 400), DEFAULT_PUBLIC_KEY — ключ выпуска. */
const install = async (
  src: ReleaseSource,
  req: IncomingMessage,
  res: ServerResponse,
): Promise<boolean> => {
  const { releasesDir, publicKey, trustProxy, log } = src.settings;
  let script: string;

  try {
    script = await readFile(join(releasesDir!, "install.sh"), "utf8");
  } catch {
    return fileNotFound(res);
  }
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
  let key = publicKey ?? "";

  if (!SAFE_KEY.test(key)) {
    log("publicKey не base64: в install.sh не подставлен");
    key = "";
  }
  script = script
    .replace(/^DEFAULT_SERVER=""$/m, `DEFAULT_SERVER="${server}"`)
    .replace(/^DEFAULT_PUBLIC_KEY=""$/m, `DEFAULT_PUBLIC_KEY="${key}"`);
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
    message: "Нет такого файла выпуска",
  });

  return true;
};
