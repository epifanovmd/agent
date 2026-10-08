// Графики временных рядов на SVG без библиотек: оси, сетка, подсказка по
// наведению (перекрестие), разрывы на пропусках, досланные точки (backfill) — бледнее.
import { useLayoutEffect, useMemo, useRef, useState, type ReactNode } from "react";

export interface Sample {
  /** Время, мс. */
  t: number;
  v: number;
  /** Досланная точка (собрана без связи). */
  backfill?: boolean;
}

export interface Series {
  name: string;
  /** Номер цвета серии (1–3): цвет следует за сущностью, а не за порядком. */
  slot: 1 | 2 | 3;
  samples: Sample[];
}

interface ChartProps {
  title: ReactNode;
  series: Series[];
  /** Окно по оси X, мс. */
  from: number;
  to: number;
  /** Верх шкалы Y (например, 100 % или объём памяти); иначе — по данным. */
  yMax?: number;
  format: (v: number) => string;
  height?: number;
}

const PAD = { top: 10, right: 12, bottom: 22, left: 52 };

/** Пропуск между точками больше этого — линия рвётся (агент не присылал метрики). */
function gapLimit(samples: Sample[]): number {
  const steps: number[] = [];
  for (let i = 1; i < samples.length; i++) steps.push(samples[i].t - samples[i - 1].t);
  steps.sort((a, b) => a - b);
  const median = steps[Math.floor(steps.length / 2)] ?? 15_000;
  return Math.max(median * 4, 20_000);
}

/** «Круглые» деления шкалы от 0 до max. */
function niceTicks(max: number, count = 4): number[] {
  if (!(max > 0)) return [0, 1];
  const raw = max / count;
  const mag = 10 ** Math.floor(Math.log10(raw));
  const step = [1, 2, 2.5, 5, 10].map((m) => m * mag).find((s) => s >= raw) ?? raw;
  const ticks: number[] = [];
  for (let v = 0; v <= max + step * 0.001; v += step) ticks.push(v);
  if (ticks[ticks.length - 1] < max) ticks.push(ticks[ticks.length - 1] + step);
  return ticks;
}

function timeTicks(from: number, to: number): number[] {
  const span = to - from;
  const step = [60_000, 120_000, 300_000, 600_000, 900_000, 1_800_000].find((s) => span / s <= 6) ?? 3_600_000;
  const out: number[] = [];
  for (let t = Math.ceil(from / step) * step; t <= to; t += step) out.push(t);
  return out;
}

const hhmm = (t: number) => new Date(t).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
const hhmmss = (t: number) => new Date(t).toLocaleTimeString();

/** Ширина контейнера (график тянется по карточке). */
function useWidth() {
  const ref = useRef<HTMLDivElement>(null);
  const [width, setWidth] = useState(0);
  useLayoutEffect(() => {
    const el = ref.current;
    if (!el) return;
    const ro = new ResizeObserver(([e]) => setWidth(Math.floor(e.contentRect.width)));
    ro.observe(el);
    setWidth(el.clientWidth);
    return () => ro.disconnect();
  }, []);
  return { ref, width };
}

/** Ближайшая по времени точка (samples отсортированы по t). */
function nearest(samples: Sample[], t: number): Sample | undefined {
  let lo = 0;
  let hi = samples.length - 1;
  if (hi < 0) return undefined;
  while (hi - lo > 1) {
    const mid = (lo + hi) >> 1;
    if (samples[mid].t < t) lo = mid;
    else hi = mid;
  }
  return Math.abs(samples[lo].t - t) <= Math.abs(samples[hi].t - t) ? samples[lo] : samples[hi];
}

export function LineChart({ title, series, from, to, yMax, format, height = 150 }: ChartProps) {
  const { ref, width } = useWidth();
  const [hover, setHover] = useState<number | null>(null);
  const visible = useMemo(
    () =>
      series.map((s) => ({ ...s, samples: s.samples.filter((p) => p.t >= from && p.t <= to && Number.isFinite(p.v)) })),
    [series, from, to],
  );
  const dataMax = Math.max(0, ...visible.flatMap((s) => s.samples.map((p) => p.v)));
  const ticks = niceTicks(yMax ?? dataMax);
  const top = yMax ?? ticks[ticks.length - 1];
  const w = Math.max(width, 120);
  const iw = w - PAD.left - PAD.right;
  const ih = height - PAD.top - PAD.bottom;
  const x = (t: number) => PAD.left + ((t - from) / (to - from)) * iw;
  const y = (v: number) => PAD.top + ih - (Math.min(v, top) / (top || 1)) * ih;

  // Отрезки линии: рвутся на пропусках; отрезок к досланной точке — бледный.
  const paths = visible.map((s) => {
    const limit = gapLimit(s.samples);
    let live = "";
    let back = "";
    for (let i = 0; i < s.samples.length; i++) {
      const p = s.samples[i];
      const prev = s.samples[i - 1];
      const pt = `${x(p.t).toFixed(1)},${y(p.v).toFixed(1)}`;
      if (!prev || p.t - prev.t > limit) {
        // Одиночная точка тоже видна: короткий штрих.
        const next = s.samples[i + 1];
        if (!next || next.t - p.t > limit) {
          const seg = `M${pt}h0.01`;
          if (p.backfill) back += seg;
          else live += seg;
        }
        continue;
      }
      const seg = `M${x(prev.t).toFixed(1)},${y(prev.v).toFixed(1)}L${pt}`;
      if (p.backfill || prev.backfill) back += seg;
      else live += seg;
    }
    return { ...s, live, back };
  });

  const hoverT = hover == null ? null : from + ((hover - PAD.left) / iw) * (to - from);
  const hits = hoverT == null ? [] : visible.map((s) => ({ s, p: nearest(s.samples, hoverT) })).filter((h) => h.p);
  const anchor = hits[0]?.p;
  const last = visible.map((s) => s.samples[s.samples.length - 1]);

  return (
    <figure className="chart">
      <figcaption className="chart-head">
        <span className="chart-title">{title}</span>
        {visible.length > 1 ? (
          <span className="legend">
            {visible.map((s, i) => (
              <span key={s.name} className="legend-item">
                <i className={`swatch s${s.slot}`} />
                {s.name} <b>{last[i] ? format(last[i]!.v) : "—"}</b>
              </span>
            ))}
          </span>
        ) : (
          <b className="chart-now">{last[0] ? format(last[0].v) : "—"}</b>
        )}
      </figcaption>
      <div
        ref={ref}
        className="chart-box"
        onMouseLeave={() => setHover(null)}
        onMouseMove={(e) => {
          const r = e.currentTarget.getBoundingClientRect();
          const px = e.clientX - r.left;
          setHover(px >= PAD.left && px <= PAD.left + iw ? px : null);
        }}
      >
        {width > 0 && (
          <svg width={w} height={height} role="img" aria-label={typeof title === "string" ? title : undefined}>
            {ticks.map((v) => (
              <g key={v}>
                <line className="grid" x1={PAD.left} x2={PAD.left + iw} y1={y(v)} y2={y(v)} />
                <text className="axis" x={PAD.left - 6} y={y(v)} dy="0.32em" textAnchor="end">
                  {format(v)}
                </text>
              </g>
            ))}
            {timeTicks(from, to).map((t) => (
              <text key={t} className="axis" x={x(t)} y={height - 6} textAnchor="middle">
                {hhmm(t)}
              </text>
            ))}
            <line className="baseline" x1={PAD.left} x2={PAD.left + iw} y1={y(0)} y2={y(0)} />
            {paths.map((s) => (
              <g key={s.name} className={`series s${s.slot}`}>
                {s.back && <path className="line backfill" d={s.back} />}
                {s.live && <path className="line" d={s.live} />}
              </g>
            ))}
            {anchor && (
              <g>
                <line className="crosshair" x1={x(anchor.t)} x2={x(anchor.t)} y1={PAD.top} y2={PAD.top + ih} />
                {hits.map(({ s, p }) => (
                  <circle key={s.name} className={`marker s${s.slot}`} cx={x(p!.t)} cy={y(p!.v)} r={4} />
                ))}
              </g>
            )}
          </svg>
        )}
        {anchor && (
          <div
            className="tooltip"
            style={x(anchor.t) > w / 2 ? { right: w - x(anchor.t) + 10 } : { left: x(anchor.t) + 10 }}
          >
            <div className="sub">
              {hhmmss(anchor.t)}
              {anchor.backfill ? " · дослано" : ""}
            </div>
            {hits.map(({ s, p }) => (
              <div key={s.name} className="tooltip-row">
                {visible.length > 1 && <i className={`swatch s${s.slot}`} />}
                {visible.length > 1 && <span>{s.name}</span>}
                <b>{format(p!.v)}</b>
              </div>
            ))}
          </div>
        )}
      </div>
    </figure>
  );
}

// ─── форматы ─────────────────────────────────────────────────────────────

const UNITS = ["Б", "КБ", "МБ", "ГБ", "ТБ"];

export function fmtBytes(n?: number): string {
  if (n == null || !Number.isFinite(n)) return "—";
  let i = 0;
  let v = n;
  while (Math.abs(v) >= 1024 && i < UNITS.length - 1) {
    v /= 1024;
    i++;
  }
  return `${v >= 100 || i === 0 ? Math.round(v) : v.toFixed(1)} ${UNITS[i]}`;
}

export const fmtRate = (n: number) => `${fmtBytes(n)}/с`;
export const fmtPercent = (n: number) => `${Math.round(n)}%`;

export function fmtNumber(n: number): string {
  const a = Math.abs(n);
  if (a >= 1e9) return `${(n / 1e9).toFixed(1)}G`;
  if (a >= 1e6) return `${(n / 1e6).toFixed(1)}M`;
  if (a >= 1e4) return `${(n / 1e3).toFixed(1)}k`;
  return Number.isInteger(n) ? String(n) : n.toFixed(a < 1 ? 3 : 2);
}
