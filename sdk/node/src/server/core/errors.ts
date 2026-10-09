// Ошибки API: AgentsError и её построение по коду.
import type { Parsed } from "../protocol/checks";

/**
 * Ошибка API: код (как в §15 или свой код SDK) и HTTP-статус, с которым её удобно вернуть
 * клиенту бэкенда.
 */
export class AgentsError extends Error {
  readonly code: string;
  readonly status: number;
  /** Через сколько секунд повторить (429 — заголовок Retry-After). */
  readonly retryAfterSec?: number;

  constructor(
    code: string,
    message: string,
    status = 400,
    retryAfterSec?: number,
  ) {
    super(message);
    this.name = "AgentsError";
    this.code = code;
    this.status = status;
    this.retryAfterSec = retryAfterSec;
  }
}

/** HTTP-статус по коду ошибки; незнакомый код — 502. */
const HTTP_STATUS: Record<string, number> = {
  AGENT_NOT_FOUND: 404,
  AGENT_REVOKED: 409,
  UPDATE_NOT_AVAILABLE: 409,
  WORKER_NOT_RELEASED: 409,
  AGENT_OFFLINE: 503,
  AGENT_ELSEWHERE: 421,
  WORKER_UNKNOWN: 404,
  WORKER_UNAVAILABLE: 502,
  // Воркер запущен, но не прошёл регистрацию (§12): за агентом — неисправный воркер, как у 502.
  WORKER_INVALID: 502,
  TIMEOUT: 504,
  CANCELLED: 499,
  PATH_FORBIDDEN: 403,
  BODY_TOO_LARGE: 413,
  BUSY: 503,
  DISCONNECTED: 502,
  CONFIG_INVALID: 400,
  // Ключа нет в манифесте воркера: настройки не подходят к тому, что воркер умеет.
  CONFIG_KEY_UNKNOWN: 409,
};

/** Ошибка с кодом; статус — явный или по коду. */
export const codeError = (
  code: string,
  message: string,
  status?: number,
): AgentsError => {
  return new AgentsError(code, message, status ?? HTTP_STATUS[code] ?? 502);
};

/** Неверные данные от вызывающего (400 MESSAGE_INVALID). */
export const invalid = (message: string): AgentsError => {
  return new AgentsError("MESSAGE_INVALID", message);
};

export const notFound = (): AgentsError => {
  return codeError("AGENT_NOT_FOUND", "агент не найден");
};

export const revoked = (): AgentsError => {
  return codeError("AGENT_REVOKED", "ключ агента отозван");
};

/** Значение проверки входящих данных; отказ — MESSAGE_INVALID с причиной. */
export const valid = <T>(p: Parsed<T>): T => {
  if (!p.ok) throw invalid(p.error);

  return p.value;
};
