// Самопроверка воркеров: сценарии по очереди, итог по каждому шагу.
import { useState } from "react";
import { runSelfCheck, scenarios, type Step } from "./selfcheck";
import { Badge } from "./panels";

const LABEL: Record<Step["state"], [string, string]> = {
  waiting: ["muted", "ждёт"],
  running: ["info", "идёт…"],
  passed: ["ok", "пройдено"],
  failed: ["bad", "ошибка"],
  skipped: ["warn", "пропущено"],
};

export function SelfCheck() {
  const initial = (): Step[] =>
    scenarios.map((s) => ({ id: s.id, title: s.title, worker: s.worker, state: "waiting" }));
  const [steps, setSteps] = useState<Step[]>(initial);
  const [running, setRunning] = useState(false);

  const start = async (only?: string[]) => {
    setRunning(true);
    setSteps((prev) =>
      prev.map((s) =>
        !only || only.includes(s.id) ? { ...s, state: "waiting", detail: undefined, ms: undefined } : s,
      ),
    );
    await runSelfCheck((step) => setSteps((prev) => prev.map((s) => (s.id === step.id ? step : s))), only);
    setRunning(false);
  };

  const count = (state: Step["state"]) => steps.filter((s) => s.state === state).length;
  return (
    <section className="card">
      <header className="card-head">
        <div>
          <h3>Самопроверка воркеров</h3>
          <div className="sub">
            Задачи, команды, желаемое состояние, телеметрия и события — через тот же API, что у приложения.
          </div>
        </div>
        <button disabled={running} onClick={() => start()}>
          {running ? "Проверяю…" : "Проверить всё"}
        </button>
      </header>
      <div className="badges summary">
        <Badge tone="ok">пройдено {count("passed")}</Badge>
        <Badge tone="bad">ошибок {count("failed")}</Badge>
        <Badge tone="warn">пропущено {count("skipped")}</Badge>
      </div>
      <table className="table">
        <thead>
          <tr>
            <th>воркер</th>
            <th>проверка</th>
            <th>итог</th>
            <th>подробности</th>
            <th />
          </tr>
        </thead>
        <tbody>
          {steps.map((s) => (
            <tr key={s.id}>
              <td>
                <b>{s.worker}</b>
              </td>
              <td>{s.title}</td>
              <td>
                <Badge tone={LABEL[s.state][0]}>{LABEL[s.state][1]}</Badge>
                {s.ms != null && <span className="sub"> {s.ms} мс</span>}
              </td>
              <td className={s.state === "failed" ? "bad-text" : "sub"}>{s.detail}</td>
              <td className="actions">
                <button className="ghost" disabled={running} onClick={() => start([s.id])}>
                  Повторить
                </button>
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </section>
  );
}
