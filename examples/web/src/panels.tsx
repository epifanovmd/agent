// Панели интерфейса: агенты, задачи, команды, желаемое состояние, события.
import { useEffect, useMemo, useState, type ReactNode } from "react";
import { fmtBytes, fmtRate } from "./charts";
import { AgentDetail } from "./AgentDetail";
import { api, declared, desiredFor } from "./api";
import { ReleasePanel } from "./Releases";
import type { Agent, Alert, DesiredState, Job, Snapshot, WorkerStatus } from "./types";

// ─── общее ───────────────────────────────────────────────────────────────

export const fmtTime = (ms?: number) => (ms ? new Date(ms).toLocaleTimeString() : "—");
const json = (v: unknown) => JSON.stringify(v, null, 2);

export function Badge({ tone, children }: { tone: string; children: ReactNode }) {
  return <span className={`badge ${tone}`}>{children}</span>;
}

const TONE: Record<string, string> = {
  queued: "muted",
  running: "info",
  completed: "ok",
  failed: "bad",
  cancelled: "muted",
  pending: "muted",
  succeeded: "ok",
  idle: "ok",
  busy: "info",
  degraded: "warn",
  draining: "warn",
  updating: "warn",
  starting: "muted",
  backoff: "bad",
  stopped: "muted",
};
export const tone = (s?: string) => TONE[s ?? ""] ?? "muted";

export function JsonEditor({
  value,
  onChange,
  rows = 6,
}: {
  value: string;
  onChange: (v: string) => void;
  rows?: number;
}) {
  let error = "";
  try {
    if (value.trim()) JSON.parse(value);
  } catch (e) {
    error = (e as Error).message;
  }
  return (
    <div className="field">
      <textarea
        className={`code ${error ? "invalid" : ""}`}
        rows={rows}
        value={value}
        spellCheck={false}
        onChange={(e) => onChange(e.target.value)}
      />
      {error && <div className="hint bad-text">{error}</div>}
    </div>
  );
}

export function useAction() {
  const [error, setError] = useState("");
  const [busy, setBusy] = useState(false);
  const run = async (fn: () => Promise<unknown>) => {
    setError("");
    setBusy(true);
    try {
      await fn();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setBusy(false);
    }
  };
  return { error, busy, run };
}

// ─── агенты ──────────────────────────────────────────────────────────────

export function Agents({ snapshot }: { snapshot: Snapshot }) {
  if (snapshot.agents.length === 0) {
    return (
      <div className="empty">
        <p>К этому серверу пока не подключён ни один агент.</p>
        <p>
          Запустите агент в другом терминале: <code>make demo-agent</code> (ещё один —{" "}
          <code>scripts/demo.sh agent 2</code>).
        </p>
        <p className="sub">
          Агент сам регистрируется по токену сервера (demo-token) и появится здесь через пару секунд.
        </p>
      </div>
    );
  }
  return (
    <div className="stack">
      <AlertList alerts={snapshot.alerts} />
      <ReleasePanel snapshot={snapshot} />
      <div className="grid">
        {snapshot.agents.map((a) => (
          <AgentCard key={a.id} agent={a} />
        ))}
      </div>
    </div>
  );
}

const ALERT_TITLE: Record<string, string> = {
  offline: "без связи",
  stateFailed: "состояние не применилось",
  workerDown: "воркер в сбое",
  workerDegraded: "воркер не в порядке",
  degraded: "агент degraded",
};

/** Активные уведомления о проблемах (snapshot.alerts, обновляются сообщениями alert). */
function AlertList({ alerts }: { alerts: Alert[] }) {
  if (alerts.length === 0) return null;
  return (
    <section className="card alerts">
      <h3>Проблемы · {alerts.length}</h3>
      <ul className="alert-list">
        {alerts.map((al) => (
          <li key={`${al.type}:${al.agentId}:${al.domain ?? al.worker ?? ""}`}>
            <Badge tone={al.type === "degraded" || al.type === "workerDegraded" ? "warn" : "bad"}>
              {ALERT_TITLE[al.type] ?? al.type}
            </Badge>{" "}
            <b>{al.agentName}</b>
            {al.domain && <span className="sub"> · раздел {al.domain}</span>}
            {al.worker && <span className="sub"> · воркер {al.worker}</span>}
            <span> — {al.message}</span> <span className="sub">с {fmtTime(al.at)}</span>
          </li>
        ))}
      </ul>
    </section>
  );
}

function AgentCard({ agent: a }: { agent: Agent }) {
  const host = a.metrics?.host;
  const caps = a.capabilities;
  const [open, setOpen] = useState(() => sessionStorage.getItem(`detail:${a.id}`) === "1");
  const revoke = useAction();
  const rotate = useAction();
  const [rotated, setRotated] = useState(false);
  const canRotate = caps?.commands?.names.includes("agent.rotateKey") ?? false;
  const toggle = () => {
    try {
      sessionStorage.setItem(`detail:${a.id}`, open ? "0" : "1");
    } catch {
      // хранилище недоступно — не страшно
    }
    setOpen(!open);
  };
  return (
    <section className={`card ${open ? "wide" : ""}`}>
      <header className="card-head">
        <div>
          <h3>{a.name}</h3>
          <div className="sub">
            {a.hello
              ? `${a.hello.host.platform ?? a.hello.host.os} · ${a.hello.host.arch} · агент ${a.hello.agent.version}`
              : "ещё не здоровался"}
          </div>
        </div>
        <div className="badges">
          {a.revoked ? (
            <Badge tone="bad">отозван</Badge>
          ) : (
            <Badge tone={a.online ? "ok" : "bad"}>{a.online ? `на связи · ${a.transport}` : "без связи"}</Badge>
          )}
          {/* Без связи последний status устарел — состояние не показываем. */}
          {a.online && a.status && <Badge tone={tone(a.status.state)}>{a.status.state}</Badge>}
        </div>
      </header>
      {a.online && a.status?.message && <div className="hint warn-text">{a.status.message}</div>}

      <div className="stats">
        <Stat label="CPU" value={host?.cpuPercent != null ? `${host.cpuPercent.toFixed(0)}%` : "—"} />
        <Stat
          label="load 1/5/15"
          value={
            host?.load1 != null
              ? [host.load1, host.load5, host.load15].map((v) => (v != null ? v.toFixed(2) : "—")).join(" ")
              : "—"
          }
        />
        <Stat label="память" value={`${fmtBytes(host?.memUsedBytes)} / ${fmtBytes(host?.memTotalBytes)}`} />
        <Stat label="outbox" value={String(a.status?.outbox ?? "—")} />
      </div>
      {host?.interfaces && host.interfaces.length > 0 && (
        <div className="sub">
          сеть: {host.interfaces.map((i) => `${i.name} ↓${fmtRate(i.rxBps)} ↑${fmtRate(i.txBps)}`).join(" · ")}
        </div>
      )}

      <h4>Воркеры</h4>
      <table className="table compact">
        <tbody>
          {(a.status?.workers ?? []).map((w) => (
            <WorkerRow key={w.name} agent={a} worker={w} />
          ))}
          {(a.status?.workers ?? []).length === 0 && (
            <tr>
              <td className="sub">нет</td>
            </tr>
          )}
        </tbody>
      </table>

      <h4>Очереди</h4>
      <div className="chips">
        {Object.entries(a.status?.capacity ?? {}).map(([q, cap]) => (
          <span className="chip" key={q}>
            {q}{" "}
            <b>
              {a.status?.slots[q] ?? 0}/{cap}
            </b>
          </span>
        ))}
        {Object.keys(a.status?.capacity ?? {}).length === 0 && <span className="sub">нет</span>}
      </div>

      <h4>Команды</h4>
      <div className="chips">
        {caps?.commands?.names.map((c) => (
          <span className="chip" key={c}>
            {c}
          </span>
        ))}
      </div>

      <h4>Домены состояния</h4>
      <div className="chips">
        {Object.keys(caps?.state?.domains ?? {}).map((d) => {
          const applied = a.stateApplied[d];
          return (
            <span className={`chip ${applied && !applied.ok ? "chip-bad" : ""}`} key={d} title={applied?.error ?? ""}>
              {d} <b>{applied ? `v${applied.version}${applied.ok ? "" : " ✗"}` : "—"}</b>
            </span>
          );
        })}
        {Object.keys(caps?.state?.domains ?? {}).length === 0 && <span className="sub">нет</span>}
      </div>

      {a.metrics?.channels && Object.keys(a.metrics.channels).length > 0 && (
        <>
          <h4>Телеметрия воркеров</h4>
          <div className="channels">
            {Object.entries(a.metrics.channels).map(([ch, data]) => (
              <div key={ch} className="channel">
                <div className="sub">{ch}</div>
                <pre>{json(data)}</pre>
              </div>
            ))}
          </div>
        </>
      )}
      {open && !a.revoked && <AgentDetail agent={a} />}
      {revoke.error && <div className="hint bad-text">{revoke.error}</div>}
      {rotate.error && <div className="hint bad-text">{rotate.error}</div>}
      {rotated && !rotate.error && <div className="hint">Ключ меняется: агент переподключится с новым.</div>}
      <div className="row between foot">
        <span className="sub">
          последний контакт {fmtTime(a.lastSeenAt)}
          {a.address && <> · адрес {a.address}</>}
        </span>
        <span className="actions">
          {!a.revoked && (
            <button className="ghost" onClick={toggle}>
              {open ? "Скрыть графики" : "Графики и сведения"}
            </button>
          )}
          {!a.revoked && (
            <button
              className="ghost"
              disabled={rotate.busy || !canRotate}
              title={canRotate ? "Новый секрет агента без новой регистрации" : "Агент не объявил agent.rotateKey"}
              onClick={() => {
                if (confirm(`Сменить ключ агента ${a.name}? Агент переподключится с новым секретом.`)) {
                  setRotated(false);
                  void rotate.run(async () => {
                    await api.rotateKey(a.id);
                    setRotated(true);
                  });
                }
              }}
            >
              Сменить ключ
            </button>
          )}
          {!a.revoked && (
            <button
              className="ghost danger"
              disabled={revoke.busy}
              onClick={() => {
                if (
                  confirm(
                    `Отозвать агента ${a.name}? Его учётные данные перестанут приниматься, нужна новая регистрация.`,
                  )
                ) {
                  void revoke.run(() => api.revoke(a.id));
                }
              }}
            >
              Отозвать
            </button>
          )}
        </span>
      </div>
    </section>
  );
}

/**
 * Воркер в карточке: состояние, здоровье (ok/degraded с причиной), пауза; кнопки «Пауза» и
 * «Продолжить» — пауза с сервера (worker.pause / worker.resume), если агент их объявил.
 * Пауза от сервера и от самого воркера независимы: «Продолжить» снимает только серверную.
 */
function WorkerRow({ agent: a, worker: w }: { agent: Agent; worker: WorkerStatus }) {
  const action = useAction();
  const names = a.capabilities?.commands?.names ?? [];
  const canPause = names.includes("worker.pause") && names.includes("worker.resume");
  const control = !a.revoked && a.online && canPause;
  return (
    <tr>
      <td>
        <b>{w.name}</b> {w.version && <span className="sub">v{w.version}</span>}
        {w.health === "degraded" && w.message && <div className="warn-text">{w.message}</div>}
        {action.error && <div className="bad-text">{action.error}</div>}
      </td>
      <td>
        <span className="badges">
          <Badge tone={tone(w.state)}>{w.state}</Badge>
          {w.health && <Badge tone={w.health === "degraded" ? "warn" : "ok"}>{w.health}</Badge>}
          {w.paused && <Badge tone="warn">на паузе</Badge>}
        </span>
      </td>
      <td className="sub">экземпляров: {w.instances}</td>
      <td>
        {control && (
          <span className="actions">
            <button
              className="mini"
              disabled={action.busy}
              title="Не брать новые задачи (выданные доделываются)"
              onClick={() => void action.run(() => api.pauseWorker(a.id, w.name))}
            >
              Пауза
            </button>
            <button
              className="mini"
              disabled={action.busy}
              title="Снять паузу сервера (пауза, выставленная воркером, остаётся)"
              onClick={() => void action.run(() => api.resumeWorker(a.id, w.name))}
            >
              Продолжить
            </button>
          </span>
        )}
      </td>
    </tr>
  );
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="stat">
      <div className="sub">{label}</div>
      <div className="stat-value">{value}</div>
    </div>
  );
}

// ─── задачи ──────────────────────────────────────────────────────────────

const JOB_PRESETS: Record<string, { data: unknown; inputs?: Record<string, string>; outputs?: string[] }> = {
  "example.echo": { data: { text: "привет", sleep: 2 }, inputs: { source: "входной файл" }, outputs: ["echo"] },
  "example.batch": { data: { stages: 5, stepsPerStage: 10, stepSeconds: 0.05, rate: 0.3 }, outputs: ["result"] },
  "example.hash": {
    data: { algorithm: "sha256", chunkDelayMs: 50 },
    inputs: { source: "строка для хеширования\n".repeat(200) },
    outputs: ["digest"],
  },
};

export function Jobs({ snapshot }: { snapshot: Snapshot }) {
  const { queues } = declared(snapshot);
  const [queue, setQueue] = useState("");
  const current = queue || queues[0] || "example.echo";
  const preset = JOB_PRESETS[current] ?? { data: {} };
  const [data, setData] = useState<Record<string, string>>({});
  const [attempts, setAttempts] = useState(1);
  const text = data[current] ?? json(preset.data);
  const { error, busy, run } = useAction();
  const [open, setOpen] = useState<string>();
  const agents = useMemo(() => new Map(snapshot.agents.map((a) => [a.id, a.name])), [snapshot.agents]);

  return (
    <div className="stack">
      <section className="card">
        <h3>Новая задача</h3>
        <div className="row">
          <label className="field">
            <span className="sub">очередь</span>
            <select value={current} onChange={(e) => setQueue(e.target.value)}>
              {[...new Set([...queues, current])].map((q) => (
                <option key={q}>{q}</option>
              ))}
            </select>
          </label>
          <label className="field small">
            <span className="sub">попыток</span>
            <input
              type="number"
              min={1}
              max={5}
              value={attempts}
              onChange={(e) => setAttempts(Number(e.target.value))}
            />
          </label>
          <div className="field grow sub">
            {preset.inputs && <>входы: {Object.keys(preset.inputs).join(", ")}; </>}
            {preset.outputs && <>выходы: {preset.outputs.join(", ")}</>}
          </div>
        </div>
        <JsonEditor value={text} onChange={(v) => setData({ ...data, [current]: v })} rows={4} />
        <div className="row">
          <button
            disabled={busy}
            onClick={() =>
              run(() =>
                api.enqueue({
                  queue: current,
                  data: JSON.parse(text || "{}"),
                  maxAttempts: attempts,
                  inputs: preset.inputs,
                  outputs: preset.outputs,
                }),
              )
            }
          >
            Поставить
          </button>
          {error && <span className="bad-text">{error}</span>}
        </div>
      </section>

      <section className="card">
        <h3>Задачи</h3>
        <table className="table">
          <thead>
            <tr>
              <th>очередь</th>
              <th>статус</th>
              <th>прогресс</th>
              <th>агент</th>
              <th>создана</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {snapshot.jobs.map((j) => (
              <JobRow
                key={j.id}
                job={j}
                agent={agents.get(j.agentId ?? "")}
                open={open === j.id}
                toggle={() => setOpen(open === j.id ? undefined : j.id)}
              />
            ))}
          </tbody>
        </table>
        {snapshot.jobs.length === 0 && <div className="empty">Задач нет</div>}
      </section>
    </div>
  );
}

function JobRow({ job: j, agent, open, toggle }: { job: Job; agent?: string; open: boolean; toggle: () => void }) {
  const active = j.status === "queued" || j.status === "running";
  return (
    <>
      <tr className="clickable" onClick={toggle}>
        <td>
          <b>{j.queue}</b> <span className="sub">{j.id.slice(0, 8)}</span>
        </td>
        <td>
          <Badge tone={tone(j.status)}>{j.status}</Badge>
          {j.attempt > 0 && <span className="sub"> попытка {j.attempt + 1}</span>}
          {j.stopRequested && active && <span className="sub"> · остановка</span>}
        </td>
        <td>
          <div className="progress">
            <div style={{ width: `${Math.round(j.progress * 100)}%` }} />
          </div>
          <div className="sub">{j.text ?? ""}</div>
        </td>
        <td className="sub">{agent ?? "—"}</td>
        <td className="sub">{fmtTime(j.createdAt)}</td>
        <td className="actions" onClick={(e) => e.stopPropagation()}>
          {j.status === "running" && (
            <button className="ghost" onClick={() => api.stopJob(j.id)}>
              Стоп
            </button>
          )}
          {active && (
            <button className="ghost danger" onClick={() => api.cancelJob(j.id)}>
              Отменить
            </button>
          )}
        </td>
      </tr>
      {open && (
        <tr className="details">
          <td colSpan={6}>
            <div className="split">
              <div>
                <h4>Данные</h4>
                <pre>{json(j.data)}</pre>
                {j.result !== undefined && (
                  <>
                    <h4>Результат</h4>
                    <pre>{json(j.result)}</pre>
                  </>
                )}
                {j.error && (
                  <>
                    <h4>Ошибка</h4>
                    <pre className="bad-text">
                      {j.error.code}: {j.error.message}
                    </pre>
                  </>
                )}
                {j.files.length > 0 && (
                  <>
                    <h4>Файлы</h4>
                    {j.files.map((f) => (
                      <a key={f} href={api.fileUrl(j.id, f)} target="_blank" rel="noreferrer">
                        {f}
                      </a>
                    ))}
                  </>
                )}
              </div>
              <div>
                <h4>События ({j.events.length})</h4>
                <pre>{j.events.map((e) => `#${e.seq} ${e.type} ${JSON.stringify(e.data)}`).join("\n") || "—"}</pre>
                <h4>Журнал</h4>
                <pre className="log">{j.log.slice(-30).join("\n") || "—"}</pre>
              </div>
            </div>
          </td>
        </tr>
      )}
    </>
  );
}

// ─── команды ─────────────────────────────────────────────────────────────

const COMMAND_ARGS: Record<string, unknown> = {
  "example.kv.get": { key: "greeting" },
  "example.sys.count": { to: 5, delaySeconds: 0.5 },
  "example.node.info": {},
  "agent.logs": { lines: 50 },
  "worker.restart": { name: "kv" },
};

export function Commands({ snapshot }: { snapshot: Snapshot }) {
  const { commands } = declared(snapshot);
  const [name, setName] = useState("");
  const [agentId, setAgentId] = useState("");
  const current = name || commands.find((c) => c.startsWith("example.")) || commands[0] || "";
  const [args, setArgs] = useState<Record<string, string>>({});
  const [timeout, setTimeoutSec] = useState(30);
  const text = args[current] ?? json(COMMAND_ARGS[current] ?? {});
  const { error, busy, run } = useAction();
  const capable = snapshot.agents.filter((a) => a.capabilities?.commands?.names.includes(current));
  const agents = new Map(snapshot.agents.map((a) => [a.id, a.name]));

  return (
    <div className="stack">
      <section className="card">
        <h3>Команда</h3>
        <div className="row">
          <label className="field">
            <span className="sub">команда</span>
            <select value={current} onChange={(e) => setName(e.target.value)}>
              {commands.map((c) => (
                <option key={c}>{c}</option>
              ))}
            </select>
          </label>
          <label className="field">
            <span className="sub">агент</span>
            <select value={agentId} onChange={(e) => setAgentId(e.target.value)}>
              <option value="">любой, кто умеет ({capable.length})</option>
              {capable.map((a) => (
                <option key={a.id} value={a.id}>
                  {a.name}
                </option>
              ))}
            </select>
          </label>
          <label className="field small">
            <span className="sub">срок, с</span>
            <input type="number" min={1} value={timeout} onChange={(e) => setTimeoutSec(Number(e.target.value))} />
          </label>
        </div>
        <JsonEditor value={text} onChange={(v) => setArgs({ ...args, [current]: v })} rows={3} />
        <div className="row">
          <button
            disabled={busy || !current}
            onClick={() =>
              run(() =>
                api.command({
                  name: current,
                  agentId: agentId || undefined,
                  args: JSON.parse(text || "{}"),
                  timeoutSec: timeout,
                }),
              )
            }
          >
            Выполнить
          </button>
          {error && <span className="bad-text">{error}</span>}
        </div>
      </section>

      {snapshot.commands.slice(0, 20).map((c) => (
        <section className="card" key={c.id}>
          <header className="card-head">
            <div>
              <b>{c.name}</b>{" "}
              <span className="sub">
                {agents.get(c.agentId)} · {fmtTime(c.createdAt)}
              </span>
            </div>
            <div className="row">
              <Badge tone={tone(c.status)}>{c.status}</Badge>
              {(c.status === "pending" || c.status === "running") && (
                <button className="ghost danger" onClick={() => run(() => api.cancelCommand(c.id))}>
                  Отменить
                </button>
              )}
            </div>
          </header>
          {c.output && <pre className="log">{c.output.slice(-4000)}</pre>}
          {c.result !== undefined && <pre>{json(c.result)}</pre>}
          {c.error && (
            <pre className="bad-text">
              {c.error.code}: {c.error.message}
            </pre>
          )}
        </section>
      ))}
    </div>
  );
}

// ─── желаемое состояние ──────────────────────────────────────────────────

const STATE_PRESETS: Record<string, unknown> = {
  "example.kv": { greeting: "привет", owner: "example" },
  "example.sys.banner": { text: "Привет от сервера" },
  "example.node.config": { greeting: "привет от сервера", maxBytes: 1048576 },
};

export function States({ snapshot }: { snapshot: Snapshot }) {
  const { domains } = declared(snapshot);
  const [domain, setDomain] = useState("");
  const [target, setTarget] = useState(""); // пусто — общий снимок для всех агентов
  const current = domain || domains[0] || "";
  const desired = snapshot.states.find((s) => s.domain === current && (s.agentId ?? "") === target);
  const capable = snapshot.agents.filter(
    (a) => a.capabilities?.state?.domains && current in a.capabilities.state.domains,
  );
  const [specs, setSpecs] = useState<Record<string, string>>({});
  const key = `${current}@${target}`;
  const text = specs[key] ?? json(desired?.spec ?? STATE_PRESETS[current] ?? {});
  const { error, busy, run } = useAction();

  return (
    <div className="stack">
      <section className="card">
        <h3>Желаемое состояние домена</h3>
        <div className="row">
          <label className="field">
            <span className="sub">домен</span>
            <select value={current} onChange={(e) => setDomain(e.target.value)}>
              {domains.map((d) => (
                <option key={d}>{d}</option>
              ))}
            </select>
          </label>
          <label className="field">
            <span className="sub">для кого</span>
            <select value={target} onChange={(e) => setTarget(e.target.value)}>
              <option value="">все агенты</option>
              {capable.map((a) => (
                <option key={a.id} value={a.id}>
                  только {a.name}
                </option>
              ))}
            </select>
          </label>
          <div className="field grow sub">
            на сервере: {desired ? `версия ${desired.version}, ${fmtTime(desired.updatedAt)}` : "не задано"}
          </div>
        </div>
        <JsonEditor value={text} onChange={(v) => setSpecs({ ...specs, [key]: v })} />
        <div className="row">
          <button
            disabled={busy || !current}
            onClick={() =>
              run(async () => {
                await api.setState(current, JSON.parse(text || "{}"), target || undefined);
                setSpecs({ ...specs, [key]: text });
              })
            }
          >
            {target ? "Применить на агенте" : "Применить на агентах"}
          </button>
          {desired && (
            <button
              className="ghost danger"
              disabled={busy}
              onClick={() =>
                run(async () => {
                  await api.deleteState(current, target || undefined);
                  const { [key]: _, ...rest } = specs;
                  setSpecs(rest);
                })
              }
            >
              {target ? "Вернуть общее" : "Удалить общий"}
            </button>
          )}
          {error && <span className="bad-text">{error}</span>}
        </div>
      </section>

      {current && <StateHistory domain={current} agentId={target || undefined} version={desired?.version} />}

      <section className="card">
        <h3>Применение по агентам</h3>
        <table className="table">
          <thead>
            <tr>
              <th>агент</th>
              <th>домен</th>
              <th>версия</th>
              <th>итог</th>
              <th>отчёт / ошибка</th>
            </tr>
          </thead>
          <tbody>
            {snapshot.agents.flatMap((a) =>
              Object.keys(a.capabilities?.state?.domains ?? {}).map((d) => {
                const ap = a.stateApplied[d];
                const want = desiredFor(snapshot, d, a.id)?.version;
                return (
                  <tr key={a.id + d}>
                    <td>{a.name}</td>
                    <td>{d}</td>
                    <td>
                      {ap?.version ?? "—"}
                      {want != null && <span className="sub"> / {want}</span>}
                    </td>
                    <td>
                      {ap ? (
                        <Badge tone={ap.ok ? (ap.version === want ? "ok" : "info") : "bad"}>
                          {ap.ok ? "применено" : "ошибка"}
                        </Badge>
                      ) : (
                        "—"
                      )}
                    </td>
                    <td>
                      <code>{ap?.error ?? (ap?.report !== undefined ? JSON.stringify(ap.report) : "")}</code>
                    </td>
                  </tr>
                );
              }),
            )}
          </tbody>
        </table>
      </section>
    </div>
  );
}

/** История версий раздела (общая или агента) с откатом; перечитывается при новой версии. */
function StateHistory({ domain, agentId, version }: { domain: string; agentId?: string; version?: number }) {
  const [list, setList] = useState<DesiredState[]>([]);
  const [open, setOpen] = useState<number | null>(null);
  const load = useAction();
  const rollback = useAction();
  useEffect(() => {
    let live = true;
    void load.run(async () => {
      const h = await api.stateHistory(domain, agentId);
      if (live) setList(h);
    });
    return () => {
      live = false;
    };
    // Перечитать при смене раздела, адресата или текущей версии.
  }, [domain, agentId, version]);
  return (
    <section className="card">
      <h3>
        История версий · {domain}
        <span className="sub"> {agentId ? "(для агента)" : "(общая)"}</span>
      </h3>
      {list.length === 0 ? (
        <div className="sub">{load.error || "версий пока нет"}</div>
      ) : (
        <table className="table">
          <thead>
            <tr>
              <th>версия</th>
              <th>когда</th>
              <th>кто</th>
              <th>содержимое</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {list.map((h) => (
              <tr key={h.version}>
                <td>
                  {h.version}
                  {h.version === version && <Badge tone="ok">текущая</Badge>}
                </td>
                <td className="sub">{fmtTime(h.updatedAt)}</td>
                <td>{h.actor || "—"}</td>
                <td>
                  <code className="clickable" onClick={() => setOpen(open === h.version ? null : h.version)}>
                    {open === h.version ? json(h.spec) : JSON.stringify(h.spec)}
                  </code>
                </td>
                <td>
                  {h.version !== version && (
                    <button
                      className="ghost"
                      disabled={rollback.busy}
                      onClick={() => {
                        if (confirm(`Откатить ${domain} к версии ${h.version}? Её содержимое уйдёт новой версией.`))
                          void rollback.run(() => api.rollbackState(domain, h.version, agentId));
                      }}
                    >
                      Откатить к этой версии
                    </button>
                  )}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
      {rollback.error && <div className="hint bad-text">{rollback.error}</div>}
    </section>
  );
}

// ─── события ─────────────────────────────────────────────────────────────

/** Журнал аудита: последние изменяющие действия из потока (с открытия страницы). */
function AuditLog({ snapshot }: { snapshot: Snapshot }) {
  const names = new Map(snapshot.agents.map((a) => [a.id, a.name]));
  return (
    <section className="card">
      <h3>Журнал действий</h3>
      <table className="table">
        <thead>
          <tr>
            <th>время</th>
            <th>кто</th>
            <th>действие</th>
            <th>агент</th>
            <th>объект</th>
            <th>подробности</th>
          </tr>
        </thead>
        <tbody>
          {snapshot.audit.map((e, i) => (
            <tr key={i}>
              <td className="sub">{fmtTime(e.at)}</td>
              <td>{e.actor || "—"}</td>
              <td>
                <b>{e.action}</b>
              </td>
              <td>{e.agentId ? (names.get(e.agentId) ?? e.agentId) : "—"}</td>
              <td>
                <code>{e.target}</code>
              </td>
              <td>
                <code>{e.details ? JSON.stringify(e.details) : ""}</code>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {snapshot.audit.length === 0 && <div className="empty">Действий с открытия страницы не было</div>}
    </section>
  );
}

export function Events({ snapshot }: { snapshot: Snapshot }) {
  return (
    <div className="stack">
      <AuditLog snapshot={snapshot} />
      <EventList snapshot={snapshot} />
    </div>
  );
}

function EventList({ snapshot }: { snapshot: Snapshot }) {
  return (
    <section className="card">
      <h3>События агентов и воркеров</h3>
      <table className="table">
        <thead>
          <tr>
            <th>время</th>
            <th>агент</th>
            <th>источник</th>
            <th>тип</th>
            <th>данные</th>
          </tr>
        </thead>
        <tbody>
          {snapshot.events.slice(0, 200).map((e, i) => (
            <tr key={i}>
              <td className="sub">{fmtTime(e.at)}</td>
              <td>{e.agentName}</td>
              <td>
                <Badge tone="info">{e.source}</Badge>
              </td>
              <td>
                <b>{e.type}</b>
              </td>
              <td>
                <code>{e.data === undefined ? "" : JSON.stringify(e.data)}</code>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
      {snapshot.events.length === 0 && <div className="empty">Событий пока нет</div>}
    </section>
  );
}
