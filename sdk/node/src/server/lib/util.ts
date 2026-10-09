// Мелкие общие помощники.

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
