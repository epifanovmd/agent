// Одностраничный интерфейс примера: состояние агентов и воркеров вживую,
// задачи, команды, желаемое состояние, события и самопроверка воркеров.
import { useEffect, useState } from "react";
import { useSnapshot } from "./stream";
import { Agents, Commands, Events, Jobs, States } from "./panels";
import { SelfCheck } from "./SelfCheckPanel";

const TABS = [
  ["agents", "Агенты"],
  ["jobs", "Задачи"],
  ["commands", "Команды"],
  ["state", "Состояние"],
  ["events", "События"],
  ["check", "Самопроверка"],
] as const;
type Tab = (typeof TABS)[number][0];

export function App() {
  const { snapshot, connected } = useSnapshot();
  const [tab, setTab] = useState<Tab>(() => (location.hash.slice(1) as Tab) || "agents");
  useEffect(() => {
    location.hash = tab;
  }, [tab]);

  const online = snapshot.agents.filter((a) => a.online).length;
  const running = snapshot.jobs.filter((j) => j.status === "running").length;
  const problems = snapshot.alerts.length;
  return (
    <div className="app">
      <header className="top">
        <div className="brand">
          <span className="logo">agent</span>
          <span>агенты и воркеры</span>
        </div>
        <nav>
          {TABS.map(([id, label]) => (
            <button key={id} className={`tab ${tab === id ? "active" : ""}`} onClick={() => setTab(id)}>
              {label}
            </button>
          ))}
        </nav>
        <div className="sub status-line">
          <span className={`dot ${connected ? "on" : "off"}`} /> сервер · агентов на связи {online}/
          {snapshot.agents.length} · задач в работе {running}
          {problems > 0 && <span className="bad-text"> · проблем {problems}</span>}
        </div>
      </header>
      <main>
        {tab === "agents" && <Agents snapshot={snapshot} />}
        {tab === "jobs" && <Jobs snapshot={snapshot} />}
        {tab === "commands" && <Commands snapshot={snapshot} />}
        {tab === "state" && <States snapshot={snapshot} />}
        {tab === "events" && <Events snapshot={snapshot} />}
        {tab === "check" && <SelfCheck />}
      </main>
    </div>
  );
}
