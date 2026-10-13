// Мелкие общие помощники.
import { createHash, timingSafeEqual } from "node:crypto";

/** Сутки, мс. */
export const DAY_MS = 86400_000;

/** Число в пределах [min, max]; не задано или NaN — byDefault. */
export const bounded = (
  v: number | undefined,
  byDefault: number,
  min: number,
  max: number,
): number =>
  Math.min(
    Math.max(min, v === undefined || Number.isNaN(v) ? byDefault : v),
    max,
  );

/** Статусы HTTP без тела. */
export const NULL_BODY = new Set([101, 103, 204, 205, 304]);

/** Строки равны; сравнение постоянного времени (по хешам — и при разной длине). */
export const safeEqual = (a: string, b: string): boolean => {
  const digest = (v: string) => createHash("sha256").update(v).digest();

  return timingSafeEqual(digest(a), digest(b));
};
