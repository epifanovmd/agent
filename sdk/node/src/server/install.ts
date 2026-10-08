// Команда установки агента на узел: curl … install.sh | sudo sh -s -- флаги.
import { INSTALL_PATH, NAME_PATTERN, validName } from "../index";
import { AgentsError } from "./model";
import { safeServer } from "./transport";

/** Флаги install.sh; пустые в команду не попадают. */
export interface InstallOptions {
  /** Адрес сервера; пусто — опция baseUrl Agents. */
  baseUrl?: string;
  /** Токен регистрации; ровно одно из token и tokenFile. */
  token?: string;
  /** Путь к файлу с токеном на узле (--token-file): токен не виден в списке процессов. */
  tokenFile?: string;
  name?: string;
  privileged?: boolean;
  /** Как systemd останавливает службу: mixed (по умолчанию install.sh) или process — дочерние процессы воркеров переживают перезапуск агента. */
  killMode?: "process" | "mixed";
  packages?: string[];
  /** Пакеты под менеджер (apt, dnf, yum, apk, zypper): для своего менеджера заменяют packages. */
  packagesByManager?: Partial<Record<PackageManager, string[]>>;
  /** Параметры ядра ключ → значение; в команде — по алфавиту ключей. */
  sysctl?: Record<string, string>;
  rwPaths?: string[];
  /** Путь к сертификату CA на узле (--ca-file). */
  caFile?: string;
  /** Воркеры из выпуска (--worker NAME, повторяемый); имя — по правилу имён. */
  workers?: string[];
  stopTimeout?: string;
  user?: string;
  /** Путь к agent.yaml на узле (--config). */
  config?: string;
}

/** Менеджеры пакетов install.sh — в этом порядке флаги --packages-<m> в команде. */
export const PACKAGE_MANAGERS = ["apt", "dnf", "yum", "apk", "zypper"] as const;
export type PackageManager = (typeof PACKAGE_MANAGERS)[number];

const PACKAGE = /^[A-Za-z0-9.+_:-]+$/;
const SYSCTL_KEY = /^[A-Za-z0-9_][A-Za-z0-9_./-]*$/;

/** Значение в одинарных кавычках POSIX (' → '\''). */
export function shellQuote(s: string): string {
  return `'${s.replace(/'/g, `'\\''`)}'`;
}

const invalid = (msg: string) => new AgentsError("MESSAGE_INVALID", msg);

/** @internal Команда установки; fallbackBase — опция baseUrl Agents. */
export function installCommand(opts: InstallOptions, fallbackBase = ""): string {
  const base = (opts.baseUrl || fallbackBase).replace(/\/+$/, "");
  if (!base) throw invalid("Нужен адрес сервера (baseUrl)");
  if (!safeServer(base) || /['\s]/.test(base)) throw invalid("Некорректный адрес сервера");
  if (!opts.token === !opts.tokenFile) throw invalid("Нужен ровно один из token и tokenFile");
  if (opts.killMode && opts.killMode !== "process" && opts.killMode !== "mixed")
    throw invalid(`Некорректный killMode ${shellQuote(String(opts.killMode))}: нужно process или mixed`);
  for (const m of Object.keys(opts.packagesByManager ?? {})) {
    if (!(PACKAGE_MANAGERS as readonly string[]).includes(m))
      throw invalid(`Неизвестный менеджер пакетов ${shellQuote(m)}: нужно ${PACKAGE_MANAGERS.join(", ")}`);
  }
  const parts = [`curl -fsSL ${shellQuote(base + INSTALL_PATH)} | sudo sh -s --`];
  const flag = (name: string, value: string | undefined) => {
    if (value) parts.push(`${name} ${shellQuote(value)}`);
  };
  flag("--token", opts.token);
  flag("--token-file", opts.tokenFile);
  flag("--name", opts.name);
  flag("--user", opts.user);
  flag("--config", opts.config);
  if (opts.privileged) parts.push("--privileged");
  flag("--kill-mode", opts.killMode);
  const packages = (name: string, list: string[] | undefined) => {
    for (const p of list ?? []) {
      if (!PACKAGE.test(p)) throw invalid(`Некорректное имя пакета ${shellQuote(p)}`);
    }
    flag(name, (list ?? []).join(" "));
  };
  packages("--packages", opts.packages);
  for (const m of PACKAGE_MANAGERS) packages(`--packages-${m}`, opts.packagesByManager?.[m]);
  const sysctl = opts.sysctl ?? {};
  const keys = Object.keys(sysctl).sort();
  for (const k of keys) {
    if (!SYSCTL_KEY.test(k)) throw invalid(`Некорректный ключ sysctl ${shellQuote(k)}`);
  }
  for (const k of keys) flag("--sysctl", `${k}=${sysctl[k]}`);
  for (const p of opts.rwPaths ?? []) flag("--rw-path", p);
  flag("--ca-file", opts.caFile);
  for (const w of opts.workers ?? []) {
    if (!validName(w)) throw invalid(`Имя воркера ${shellQuote(String(w))} не по правилу ${NAME_PATTERN}`);
    flag("--worker", w);
  }
  flag("--stop-timeout", opts.stopTimeout);
  const cmd = parts.join(" ");
  if (/[\r\n]/.test(cmd)) throw invalid("Перевод строки в значении");
  return cmd;
}
