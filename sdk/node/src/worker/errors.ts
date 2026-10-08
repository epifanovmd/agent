// Ошибки SDK воркера: провал задачи и команды с кодом, отказ агента.

/** Провал задачи с кодом (`^[A-Z0-9_]+$`); `retryable: false` — без повторов. */
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

/** Провал команды с кодом (`^[A-Z0-9_]+$`). */
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
