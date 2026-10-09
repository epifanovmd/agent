// Постоянные связи сервера и агента: пути, пределы, коды закрытия, правила имён. Нормативный
// документ — sdk/spec/README.md.

export const LINK_PATH = "/api/v1/agent-link";
export const ENROLL_PATH = "/api/v1/agent-link/enroll";
/** Подпротокол WebSocket (§2). */
export const WS_CHANNEL = "agent.v2";
/** Раздача выпуска (§11): manifest.json и сборки. */
export const RELEASES_PATH = "/api/v1/agent-link/releases";
/** Установщик агента: адрес сервера и ключ проверки подставлены. */
export const INSTALL_PATH = "/api/v1/agent-link/install.sh";

const KB = 1024;
const MB = 1024 * KB;

/** Тело регистрации — не больше, байт (§16). */
export const ENROLL_MAX_BODY = 64 * KB;
/** Значение настройки — байт JSON (§16). */
export const MAX_CONFIG_BYTES = 4 * MB;
/** Тело запроса fetch — байт (§16). */
export const MAX_FETCH_BODY = 4 * MB;
/** Сообщение WebSocket — байт (§16). */
export const MAX_MESSAGE_BYTES = 8 * MB;

/** Имя агента при регистрации — символов. */
export const MAX_AGENT_NAME = 128;
/** Меток у агента. */
export const MAX_LABELS = 64;
/** Ключ и значение метки — символов. */
export const MAX_LABEL = 256;

/** Коды закрытия WebSocket (§2). */
export const Close = {
  Normal: 1000,
  Restart: 1012,
  Invalid: 4400,
  Unauthorized: 4401,
  Duplicate: 4409,
  Overloaded: 4429,
} as const;

/** Имя воркера и ключа настроек. */
export const NAME_PATTERN = /^[a-z][a-z0-9-]{0,31}$/;
/** Тип события. */
export const EVENT_TYPE_PATTERN = /^[a-z][a-z0-9._-]{0,63}$/;

/** События задач воркера (§12): типы с этим началом зарезервированы. */
export const JOB_EVENT_PREFIX = "job.";
export const JOB_EVENTS = [
  "job.progress",
  "job.done",
  "job.failed",
  "job.cancelled",
] as const;

/** Задачи воркера (§12): POST /jobs, GET /jobs/{id}, POST /jobs/{id}/cancel. */
export const JOBS_PATH = "/jobs";

/** Важные сообщения агента: подтверждаются `ack {ids}` (§3). */
export const RELIABLE = new Set([
  "event",
  "config.applied",
  "action.result",
  "action.done",
]);
/** Поток агента: подтверждается `ack {seq}` (§3). */
export const STREAM = new Set(["status", "metrics", "log"]);
