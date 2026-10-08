// Деталь агента: графики метрик (история с сервера при открытии, дальше — точки
// из потока по подписке; пока она есть, сервер держит подписку на агента: частые
// метрики и показатели воркеров, лог с выбранного уровня), каналы телеметрии
// воркеров, последние записи лога, сведения об узле.
import { useCallback, useEffect, useMemo, useState } from "react";
import { api } from "./api";
import { fmtBytes, fmtNumber, fmtPercent, fmtRate, LineChart, type Sample, type Series } from "./charts";
import { useLogStream, useMetricsStream, useSnapshot } from "./stream";
import { LOG_LEVELS, type Agent, type Inventory, type LogEntry, type LogLevel, type MetricsPoint } from "./types";

const WINDOWS = [
  ["15m", "15 мин", 15 * 60_000],
  ["1h", "1 ч", 60 * 60_000],
] as const;
const HISTORY_MS = 60 * 60_000;

/** Точки по возрастанию at без повторов (история и поток могут пересечься), в окне истории. */
function merge(prev: MetricsPoint[], add: MetricsPoint[]): MetricsPoint[] {
  const key = (p: MetricsPoint) => `${p.at}/${p.metrics.collectedAt}`;
  const seen = new Set(prev.map(key));
  const out = [...prev];
  for (const p of add) {
    if (seen.has(key(p))) continue;
    seen.add(key(p));
    // Досланные (backfill) приходят не по порядку at — вставка на место.
    let i = out.length;
    while (i > 0 && out[i - 1].at > p.at) i--;
    out.splice(i, 0, p);
  }
  const cut = Date.now() - HISTORY_MS;
  return out[0] && out[0].at < cut ? out.filter((p) => p.at >= cut) : out;
}

/** История метрик агента: окно при открытии (и после переподключения потока), затем точки из потока. */
function useHistory(agentId: string) {
  const { connected } = useSnapshot();
  const [points, setPoints] = useState<MetricsPoint[]>([]);
  const [error, setError] = useState("");
  useMetricsStream(
    agentId,
    useCallback((p: MetricsPoint) => setPoints((prev) => merge(prev, [p])), []),
  );

  useEffect(() => {
    if (!connected) return;
    let alive = true;
    api.metrics(agentId, Date.now() - HISTORY_MS).then(
      (got) => alive && (setPoints((prev) => merge(prev, got)), setError("")),
      (e) => alive && setError((e as Error).message),
    );
    return () => {
      alive = false;
    };
  }, [agentId, connected]); // поток переподключился — дозагрузить пропущенное

  useEffect(() => setPoints([]), [agentId]);
  return { points, error };
}

const series = (
  name: string,
  slot: 1 | 2 | 3,
  points: MetricsPoint[],
  pick: (p: MetricsPoint) => number | undefined,
): Series => ({
  name,
  slot,
  samples: points.flatMap((p): Sample[] => {
    const v = pick(p);
    return typeof v === "number" && Number.isFinite(v) ? [{ t: p.at, v, backfill: p.backfill }] : [];
  }),
});

/** Числовые поля канала (вложенные — через точку): кандидаты в графики. */
function numericFields(value: unknown, prefix = "", out: string[] = [], depth = 0): string[] {
  if (value && typeof value === "object" && !Array.isArray(value) && depth < 3) {
    for (const [k, v] of Object.entries(value)) {
      const key = prefix ? `${prefix}.${k}` : k;
      if (typeof v === "number") out.push(key);
      else numericFields(v, key, out, depth + 1);
    }
  }
  return out;
}

function at(value: unknown, path: string): number | undefined {
  let cur: any = value;
  for (const k of path.split(".")) cur = cur?.[k];
  return typeof cur === "number" ? cur : undefined;
}

export function AgentDetail({ agent }: { agent: Agent }) {
  const { points, error } = useHistory(agent.id);
  const [win, setWin] = useState<(typeof WINDOWS)[number][0]>("15m");
  const [iface, setIface] = useState("");
  const [now, setNow] = useState(Date.now());
  useEffect(() => {
    const t = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(t);
  }, []);
  const span = WINDOWS.find((w) => w[0] === win)![2];
  const from = now - span;
  const inWindow = useMemo(() => points.filter((p) => p.at >= from - 60_000), [points, from]);

  const ifaces = useMemo(() => {
    const names = new Set<string>();
    for (const p of inWindow) p.metrics.host?.interfaces?.forEach((i) => names.add(i.name));
    return [...names].sort();
  }, [inWindow]);
  const mounts = useMemo(() => {
    const m = new Map<string, number>();
    for (const p of inWindow) p.metrics.host?.disks?.forEach((d) => m.set(d.mount, d.totalBytes));
    return [...m].sort((a, b) => a[0].localeCompare(b[0]));
  }, [inWindow]);
  const gpus = useMemo(() => {
    const m = new Map<number, string>();
    for (const p of inWindow) p.metrics.gpus?.forEach((g) => m.set(g.index, g.name ?? `GPU ${g.index}`));
    return [...m].sort((a, b) => a[0] - b[0]);
  }, [inWindow]);
  const channels = useMemo(() => {
    const m = new Map<string, Set<string>>();
    for (const p of inWindow) {
      for (const [ch, data] of Object.entries(p.metrics.channels ?? {})) {
        const set = m.get(ch) ?? new Set<string>();
        numericFields(data).forEach((f) => set.add(f));
        m.set(ch, set);
      }
    }
    return [...m].sort((a, b) => a[0].localeCompare(b[0])).map(([ch, f]) => [ch, [...f].sort().slice(0, 12)] as const);
  }, [inWindow]);

  const memTotal = agent.metrics?.host?.memTotalBytes ?? agent.inventory?.memoryBytes;
  const pick = (p: MetricsPoint, dir: "rxBps" | "txBps") =>
    iface
      ? p.metrics.host?.interfaces?.find((i) => i.name === iface)?.[dir]
      : p.metrics.host?.[dir === "rxBps" ? "netRxBps" : "netTxBps"];
  /** Есть ли поле хотя бы в одной точке окна: графики — только для того, что агент присылает. */
  const has = (get: (h: NonNullable<MetricsPoint["metrics"]["host"]>) => unknown) =>
    inWindow.some((p) => p.metrics.host && typeof get(p.metrics.host) === "number");
  const host = (p: MetricsPoint) => p.metrics.host;
  const swapTotal = agent.metrics?.host?.swapTotalBytes;
  const backfilled = inWindow.filter((p) => p.backfill && p.at >= from).length;
  const chart = { from, to: now };

  return (
    <div className="detail">
      <div className="row between">
        <div className="seg">
          {WINDOWS.map(([id, label]) => (
            <button key={id} className={`tab ${win === id ? "active" : ""}`} onClick={() => setWin(id)}>
              {label}
            </button>
          ))}
        </div>
        <span className="sub">
          {agent.online ? "метрики раз в секунду, пока деталь открыта" : "агент без связи — история"}
          {backfilled > 0 && (
            <>
              {" "}
              · <span className="backfill-note">бледнее</span> — досланные без связи ({backfilled})
            </>
          )}
        </span>
      </div>
      {error && <div className="hint bad-text">{error}</div>}
      {points.length === 0 && !error && <div className="sub">истории метрик пока нет</div>}

      <div className="charts">
        <LineChart
          title="CPU"
          {...chart}
          yMax={100}
          format={fmtPercent}
          series={[series("CPU", 1, inWindow, (p) => p.metrics.host?.cpuPercent)]}
        />
        <LineChart
          title={`Память${memTotal ? ` · из ${fmtBytes(memTotal)}` : ""}`}
          {...chart}
          yMax={memTotal}
          format={fmtBytes}
          series={[series("занято", 1, inWindow, (p) => p.metrics.host?.memUsedBytes)]}
        />
        <LineChart
          title={
            <>
              Сеть{" "}
              <select className="mini" value={iface} onChange={(e) => setIface(e.target.value)}>
                <option value="">все интерфейсы</option>
                {ifaces.map((n) => (
                  <option key={n} value={n}>
                    {n}
                  </option>
                ))}
              </select>
            </>
          }
          {...chart}
          format={fmtRate}
          series={[
            series("приём", 1, inWindow, (p) => pick(p, "rxBps")),
            series("отдача", 2, inWindow, (p) => pick(p, "txBps")),
          ]}
        />
        {has((h) => h.load1) && (
          <LineChart
            title="Нагрузка (load average)"
            {...chart}
            format={fmtNumber}
            series={[
              series("1 мин", 1, inWindow, (p) => host(p)?.load1),
              series("5 мин", 2, inWindow, (p) => host(p)?.load5),
              series("15 мин", 3, inWindow, (p) => host(p)?.load15),
            ]}
          />
        )}
        {has((h) => h.swapTotalBytes) && (
          <LineChart
            title={`Swap${swapTotal ? ` · из ${fmtBytes(swapTotal)}` : ""}`}
            {...chart}
            yMax={swapTotal || undefined}
            format={fmtBytes}
            series={[series("занято", 1, inWindow, (p) => host(p)?.swapUsedBytes)]}
          />
        )}
        {mounts.length === 0 && has((h) => h.diskTotalBytes) && (
          <LineChart
            title={`Диск / · из ${fmtBytes(agent.metrics?.host?.diskTotalBytes)}`}
            {...chart}
            yMax={agent.metrics?.host?.diskTotalBytes}
            format={fmtBytes}
            series={[series("занято", 1, inWindow, (p) => host(p)?.diskUsedBytes)]}
          />
        )}
        {mounts.map(([mount, total]) => (
          <LineChart
            key={`disk:${mount}`}
            title={`Диск ${mount} · из ${fmtBytes(total)}`}
            {...chart}
            yMax={total}
            format={fmtBytes}
            series={[series("занято", 1, inWindow, (p) => host(p)?.disks?.find((d) => d.mount === mount)?.usedBytes)]}
          />
        ))}
        {has((h) => h.diskReadBps ?? h.diskWriteBps) && (
          <LineChart
            title="Диск: чтение и запись"
            {...chart}
            format={fmtRate}
            series={[
              series("чтение", 1, inWindow, (p) => host(p)?.diskReadBps),
              series("запись", 2, inWindow, (p) => host(p)?.diskWriteBps),
            ]}
          />
        )}
        {has((h) => h.netErrors ?? h.netDrops) && (
          <LineChart
            title="Сеть: ошибки и отброшенные"
            {...chart}
            format={fmtNumber}
            series={[
              series("ошибки", 1, inWindow, (p) => host(p)?.netErrors),
              series("отброшено", 2, inWindow, (p) => host(p)?.netDrops),
            ]}
          />
        )}
        {has((h) => h.tcp?.established ?? h.tcp?.timeWait) && (
          <LineChart
            title="TCP-соединения"
            {...chart}
            format={fmtNumber}
            series={[
              series("установлены", 1, inWindow, (p) => host(p)?.tcp?.established),
              series("time-wait", 2, inWindow, (p) => host(p)?.tcp?.timeWait),
              series("close-wait", 3, inWindow, (p) => host(p)?.tcp?.closeWait),
            ]}
          />
        )}
        {has((h) => h.processes) && (
          <LineChart
            title="Процессы и потоки"
            {...chart}
            format={fmtNumber}
            series={[
              series("процессы", 1, inWindow, (p) => host(p)?.processes),
              series("потоки", 2, inWindow, (p) => host(p)?.threads),
            ]}
          />
        )}
        {has((h) => h.temperatures?.maxC) && (
          <LineChart
            title="Температура (максимум по датчикам)"
            {...chart}
            format={(v) => `${Math.round(v)} °C`}
            series={[series("макс.", 1, inWindow, (p) => host(p)?.temperatures?.maxC)]}
          />
        )}
        {gpus.map(([index, name]) => (
          <LineChart
            key={`gpu${index}`}
            title={`GPU ${index} · ${name}`}
            {...chart}
            yMax={100}
            format={fmtPercent}
            series={[
              series("загрузка", 1, inWindow, (p) => p.metrics.gpus?.find((g) => g.index === index)?.utilPercent),
            ]}
          />
        ))}
      </div>

      {channels.length > 0 && (
        <>
          <h4>Каналы телеметрии воркеров</h4>
          {channels.map(([ch, fields]) => (
            <div key={ch} className="channel-charts">
              <div className="sub">{ch}</div>
              <div className="charts small">
                {fields.length === 0 && <span className="sub">числовых полей нет</span>}
                {fields.map((f) => (
                  <LineChart
                    key={f}
                    title={f}
                    {...chart}
                    height={110}
                    format={fmtNumber}
                    series={[series(f, 1, inWindow, (p) => at(p.metrics.channels?.[ch], f))]}
                  />
                ))}
              </div>
            </div>
          ))}
        </>
      )}

      <LogsView agentId={agent.id} online={agent.online} />

      <h4>Сведения об узле</h4>
      {agent.inventory ? (
        <InventoryView inv={agent.inventory} />
      ) : (
        <div className="sub">агент ещё не прислал inventory</div>
      )}
    </div>
  );
}

/** Записей лога в памяти детали. */
const KEEP_LOG = 300;
const LEVEL_LABEL: Record<LogLevel, string> = { debug: "отладка", info: "инфо", warn: "предупр.", error: "ошибки" };

/** Последние записи лога агента и воркеров с выбранного уровня (новые сверху). */
function LogsView({ agentId, online }: { agentId: string; online: boolean }) {
  const [level, setLevel] = useState<LogLevel>("info");
  const [entries, setEntries] = useState<LogEntry[]>([]);
  useLogStream(
    agentId,
    level,
    useCallback((add: LogEntry[]) => setEntries((prev) => [...add.slice().reverse(), ...prev].slice(0, KEEP_LOG)), []),
  );
  useEffect(() => setEntries([]), [agentId]);
  const min = LOG_LEVELS.indexOf(level);
  const shown = entries.filter((e) => LOG_LEVELS.indexOf(e.level) >= min);
  return (
    <>
      <div className="row between">
        <h4>Логи</h4>
        <div className="row">
          <div className="seg">
            {LOG_LEVELS.map((l) => (
              <button key={l} className={`tab ${level === l ? "active" : ""}`} onClick={() => setLevel(l)}>
                {LEVEL_LABEL[l]}
              </button>
            ))}
          </div>
          {entries.length > 0 && (
            <button className="mini" onClick={() => setEntries([])}>
              очистить
            </button>
          )}
        </div>
      </div>
      {shown.length === 0 ? (
        <div className="sub">
          {online ? `записей с уровня «${LEVEL_LABEL[level]}» пока нет — новые появятся здесь` : "агент без связи"}
        </div>
      ) : (
        <div className="logs">
          {shown.map((e, i) => (
            <div key={`${e.at}/${i}`} className={`log-line log-${e.level}`}>
              <span className="log-time">{new Date(e.at).toLocaleTimeString()}</span>
              <span className="log-level">{e.level}</span>
              <span className="log-source">{e.source}</span>
              <span className="log-msg">
                {e.msg}
                {e.attrs &&
                  Object.entries(e.attrs).map(([k, v]) => (
                    <span key={k} className="log-attr">
                      {" "}
                      {k}={typeof v === "string" ? v : JSON.stringify(v)}
                    </span>
                  ))}
              </span>
            </div>
          ))}
        </div>
      )}
    </>
  );
}

function InventoryView({ inv }: { inv: Inventory }) {
  const os = inv.os ?? {};
  return (
    <div className="inventory">
      <dl>
        <dt>ОС</dt>
        <dd>{[os.platform, os.kernel && `ядро ${os.kernel}`, os.arch].filter(Boolean).join(" · ") || "—"}</dd>
        <dt>Имя</dt>
        <dd>{os.hostname ?? "—"}</dd>
        <dt>Виртуализация</dt>
        <dd>{os.virtualization || "нет / не определена"}</dd>
        <dt>CPU</dt>
        <dd>
          {inv.cpu ? `${inv.cpu.model ?? "?"} · ${inv.cpu.cores ?? "?"} ядер / ${inv.cpu.threads ?? "?"} потоков` : "—"}
        </dd>
        <dt>Память</dt>
        <dd>{fmtBytes(inv.memoryBytes)}</dd>
        <dt>Диски</dt>
        <dd>
          {(inv.disks ?? []).map((d) => (
            <div key={d.mount}>
              <code>{d.mount}</code> {d.fs} · {fmtBytes(d.totalBytes)}
            </div>
          ))}
          {!inv.disks?.length && "—"}
        </dd>
        <dt>Интерфейсы</dt>
        <dd>
          {(inv.interfaces ?? []).map((i) => (
            <div key={i.name}>
              <b>{i.name}</b> {i.mac && <span className="sub">{i.mac}</span>}{" "}
              {(i.addresses ?? []).map((a) => (
                <code key={a} className="addr">
                  {a}
                </code>
              ))}
            </div>
          ))}
          {!inv.interfaces?.length && "—"}
        </dd>
        {(inv.gpus ?? []).length > 0 && (
          <>
            <dt>GPU</dt>
            <dd>
              {inv.gpus!.map((g) => (
                <div key={g.index}>
                  {g.index}: {g.name} · {fmtBytes(g.memoryBytes)}
                </div>
              ))}
            </dd>
          </>
        )}
        <dt>Порты</dt>
        <dd>
          <div>TCP: {inv.ports?.tcp?.length ? inv.ports.tcp.join(", ") : "—"}</div>
          <div>UDP: {inv.ports?.udp?.length ? inv.ports.udp.join(", ") : "—"}</div>
        </dd>
      </dl>
      <div className="sub">собрано {new Date(inv.collectedAt).toLocaleString()}</div>
    </div>
  );
}
