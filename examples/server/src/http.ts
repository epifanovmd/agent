// Мелочи HTTP стенда без фреймворка: тело запроса, адрес сервера, уровни журнала.
import type { IncomingMessage } from "node:http";

import { AgentsError, type LogLevel } from "agent-sdk/server";

/** Уровни журнала агента по возрастанию. */
export const LOG_LEVELS: readonly LogLevel[] = [
  "debug",
  "info",
  "warn",
  "error",
];

/** Тело запроса не больше limit байт; больше — 413 BODY_TOO_LARGE. */
export const readBody = async (
  req: IncomingMessage,
  limit: number,
): Promise<Buffer> => {
  const tooLarge = () =>
    new AgentsError("BODY_TOO_LARGE", `тело запроса больше ${limit} байт`, 413);

  if (Number(req.headers["content-length"]) > limit) throw tooLarge();
  const chunks: Buffer[] = [];
  let size = 0;

  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit) throw tooLarge();
    chunks.push(chunk);
  }

  return Buffer.concat(chunks);
};

/** Тело запроса JSON (пустое — {}); не JSON — 400 MESSAGE_INVALID. */
// eslint-disable-next-line @typescript-eslint/no-explicit-any -- тело API стенда произвольно
export const readJSON = async (req: IncomingMessage): Promise<any> => {
  const raw = await readBody(req, 4 * 1024 * 1024);

  try {
    return raw.length ? JSON.parse(raw.toString("utf8")) : {};
  } catch {
    throw new AgentsError("MESSAGE_INVALID", "тело — не JSON", 400);
  }
};

/** Адрес сервера, по которому клиент до него дошёл: Host и TLS сокета. */
export const baseUrl = (req: IncomingMessage): string => {
  const tls =
    (req.socket as { encrypted?: boolean } | undefined)?.encrypted === true;

  return `${tls ? "https" : "http"}://${req.headers.host || "localhost"}`;
};
