// Сравнение версий агента и воркеров по semver.
import { coerce, eq, gt, valid } from "semver";

/** Версия semver: строгая (v1.2.0 — 1.2.0) или приведённая (1.2 — 1.2.0); не версия — null. */
const semverOf = (v: string): string | null =>
  valid(v) ??
  (/^v?\d/.test(v) ? valid(coerce(v, { includePrerelease: true })) : null);

/** a новее b (2.0.0 новее 2.0.0-rc.1); версия новее не версии, не версии между собой не новее. */
export const newerVersion = (a: string, b: string): boolean => {
  const x = semverOf(a);
  const y = semverOf(b);

  return x !== null && (y === null || gt(x, y));
};

/** Та же версия (v1.2.0 и 1.2.0 — одна); не версии — сравнение строк. */
export const sameVersion = (a: string, b: string): boolean => {
  const x = semverOf(a);
  const y = semverOf(b);

  return x !== null && y !== null ? eq(x, y) : a === b;
};
