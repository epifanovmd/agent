// Раздача сборок (§11) по HTTP: manifest.json и файлы сборок из releasesDir. Публично: сборки
// подписаны.
import { createReadStream } from "node:fs";
import { stat } from "node:fs/promises";
import type { IncomingMessage, ServerResponse } from "node:http";

import { type ReleaseManifest, RELEASES_PATH } from "../protocol/messages";
import { sendJSON } from "./http";

/** Что нужно раздаче сборок. */
export interface ReleaseSource {
  /** Раздача включена (releasesDir). */
  releaseEnabled(): boolean;
  /** manifest.json для узлов или null. */
  servedManifest(): Promise<ReleaseManifest | null>;
  /** Путь к файлу сборок по имени; не из манифеста — null. */
  releaseFile(name: string): Promise<string | null>;
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
  const file = await src.releaseFile(name);

  if (!file) return fileNotFound(res);

  return localFile(req, res, file, name);
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

const fileNotFound = (res: ServerResponse): true => {
  sendJSON(res, 404, {
    code: "NOT_FOUND",
    message: "Нет такого файла сборок",
  });

  return true;
};
