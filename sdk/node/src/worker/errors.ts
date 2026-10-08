// Ошибки SDK воркера: провал задачи и команды с кодом, отказ агента.

/** Провал задачи с кодом (`^[A-Z0-9_]{1,64}$`, иначе — WORKER_ERROR); `retryable: false` — без повторов. */
export class JobError extends Error {
  readonly code: string;
  readonly retryable: boolean;
  constructor(code: string, message: string, opts: { retryable?: boolean } = {}) {
    super(message);
    this.name = "JobError";
    this.code = code;
    this.retryable = opts.retryable ?? true;
  }
}

/** Провал команды с кодом (`^[A-Z0-9_]{1,64}$`, иначе — COMMAND_FAILED). */
export class CommandError extends Error {
  readonly code: string;
  constructor(code: string, message: string) {
    super(message);
    this.name = "CommandError";
    this.code = code;
  }
}

/** Агент (или сервер через него) отклонил запрос воркера: ответ `error`. */
export class AgentError extends Error {
  readonly code: string;
  readonly retryable: boolean;
  constructor(code: string, message: string, retryable = true) {
    super(message);
    this.name = "AgentError";
    this.code = code;
    this.retryable = retryable;
  }
}

/**
 * Состояние не применено, но есть отчёт (например, что успело примениться):
 * `state.applied {ok: false, error: message, report}`.
 */
export class StateError extends Error {
  readonly report: unknown;
  constructor(message: string, report?: unknown) {
    super(message);
    this.name = "StateError";
    this.report = report;
  }
}

/** Коды, которые выставляет SDK. */
export const WORKER_ERROR = "WORKER_ERROR";
export const WORKER_STOPPING = "WORKER_STOPPING";
export const COMMAND_FAILED = "COMMAND_FAILED";
export const COMMAND_UNKNOWN = "COMMAND_UNKNOWN";
/** Итог (результат задачи, команды, отчёт состояния) не уместился в строку канала (16 МБ). */
export const RESULT_TOO_LARGE = "RESULT_TOO_LARGE";
/** Подтверждение отмены задачи: job.fail после job.cancel, когда обработчик завершился. */
export const CANCELLED = "CANCELLED";

const CODE = /^[A-Z0-9_]{1,64}$/;

/** Код по схеме спецификации; негодный заменяется общим fallback, исходный уходит в текст. */
export function normalizeCode(code: unknown, message: string, fallback: string): [string, string] {
  if (typeof code === "string" && CODE.test(code)) return [code, message];
  return [fallback, `${String(code)}: ${message}`];
}

/** Сообщение длиннее строки канала (16 МБ): не отправлено, канал цел. */
export class MessageTooLargeError extends Error {
  readonly code = RESULT_TOO_LARGE;
  constructor(type: string) {
    super(`${type}: сообщение больше 16 МБ`);
    this.name = "MessageTooLargeError";
  }
}
