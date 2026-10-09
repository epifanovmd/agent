// Значения для строк sh.

/** Значение в одинарных кавычках POSIX (' → '\''). */
export const shellQuote = (s: string): string =>
  `'${s.replace(/'/g, `'\\''`)}'`;

const SAFE_SERVER = /^https?:\/\/[A-Za-z0-9.\-:[\]]+(\/.*)?$/;

/** Адрес для строки sh в двойных кавычках: http(s)://хост[/путь], без "$`\\ и переводов строк. */
export const safeServer = (v: string): boolean =>
  SAFE_SERVER.test(v) && !/["$`\\\r\n]/.test(v);
