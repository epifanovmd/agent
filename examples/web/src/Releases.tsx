// Выпуск агента: версия из каталога выпуска сервера, кандидаты на обновление
// (кнопка «Обновить» — команда agent.update), воркеры из выпуска (кнопка
// «Обновить воркер» — команда worker.update) и команда установки на новый узел.
import { useEffect, useState } from "react";
import { api } from "./api";
import { Badge, useAction } from "./panels";
import type { Releases, Snapshot } from "./types";

export function ReleasePanel({ snapshot }: { snapshot: Snapshot }) {
  const [rel, setRel] = useState<Releases | null>(null);
  const [error, setError] = useState("");
  const [copied, setCopied] = useState(false);
  const [sent, setSent] = useState<Record<string, string>>({}); // agentId или agentId/воркер → id команды
  const action = useAction();

  // Кандидаты зависят от агентов: перечитать при изменениях и раз в 15 с.
  const versions = snapshot.agents
    .map(
      (a) =>
        `${a.id}:${a.hello?.agent.version}:${a.online}:` +
        (a.status?.workers ?? [])
          .filter((w) => w.release)
          .map((w) => `${w.name}@${w.version}`)
          .join("+"),
    )
    .join(",");
  useEffect(() => {
    let alive = true;
    const load = () =>
      api.releases().then(
        (r) => alive && (setRel(r), setError("")),
        (e) => alive && setError((e as Error).message),
      );
    void load();
    const t = setInterval(load, 15_000);
    return () => {
      alive = false;
      clearInterval(t);
    };
  }, [versions]);

  const copy = async () => {
    if (!rel) return;
    try {
      await navigator.clipboard.writeText(rel.installCommand);
    } catch {
      // нет доступа к буферу (http не localhost) — выделить вручную
      return;
    }
    setCopied(true);
    setTimeout(() => setCopied(false), 1500);
  };

  const commandStatus = (key: string) => snapshot.commands.find((c) => c.id === sent[key]);
  const workerCandidates = rel?.workerCandidates ?? [];

  return (
    <section className="card release">
      <header className="card-head">
        <div>
          <h3>Выпуск агента</h3>
          <div className="sub">
            {rel?.release ? (
              <>
                версия <b>{rel.release.version}</b> · сборки:{" "}
                {rel.release.artifacts.map((a) => `${a.os}/${a.arch}`).join(", ")}
                {rel.release.workers?.length ? (
                  <> · воркеры: {[...new Set(rel.release.workers.map((w) => `${w.name} ${w.version}`))].join(", ")}</>
                ) : null}
              </>
            ) : (
              "каталог выпуска не задан на сервере (RELEASES_DIR) — обновление и установка недоступны"
            )}
          </div>
        </div>
        {rel?.release && (
          <Badge tone={rel.candidates.length + workerCandidates.length ? "warn" : "ok"}>
            {rel.candidates.length + workerCandidates.length
              ? `обновить: ${rel.candidates.length + workerCandidates.length}`
              : "все на версии выпуска"}
          </Badge>
        )}
      </header>
      {error && <div className="hint bad-text">{error}</div>}

      {rel && rel.candidates.length > 0 && (
        <table className="table compact">
          <thead>
            <tr>
              <th>агент</th>
              <th>версия</th>
              <th>платформа</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {rel.candidates.map((c) => {
              const cmd = commandStatus(c.agentId);
              return (
                <tr key={c.agentId}>
                  <td>
                    <b>{c.name}</b> {!c.online && <Badge tone="bad">без связи</Badge>}
                  </td>
                  <td>
                    {c.current} → <b>{c.target}</b>
                  </td>
                  <td className="sub">
                    {c.os}/{c.arch}
                  </td>
                  <td className="actions">
                    {cmd && (
                      <Badge tone={cmd.status === "failed" ? "bad" : cmd.status === "succeeded" ? "ok" : "info"}>
                        {cmd.status}
                        {cmd.error ? `: ${cmd.error.code}` : ""}
                      </Badge>
                    )}{" "}
                    <button
                      className="ghost"
                      disabled={action.busy || cmd?.status === "pending" || cmd?.status === "running"}
                      onClick={() =>
                        action.run(async () => {
                          const res = await api.updateAgent(c.agentId);
                          setSent((s) => ({ ...s, [c.agentId]: res.id }));
                        })
                      }
                    >
                      Обновить
                    </button>
                  </td>
                </tr>
              );
            })}
          </tbody>
        </table>
      )}

      {workerCandidates.length > 0 && (
        <>
          <h4>Воркеры из выпуска</h4>
          <table className="table compact">
            <thead>
              <tr>
                <th>агент</th>
                <th>воркер</th>
                <th>версия</th>
                <th>платформа</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {workerCandidates.map((c) => {
                const key = `${c.agentId}/${c.worker}`;
                const cmd = commandStatus(key);
                return (
                  <tr key={key}>
                    <td>
                      <b>{c.agentName}</b> {!c.online && <Badge tone="bad">без связи</Badge>}
                    </td>
                    <td>
                      <b>{c.worker}</b>
                    </td>
                    <td>
                      {c.current || "?"} → <b>{c.target}</b>
                    </td>
                    <td className="sub">
                      {c.os}/{c.arch}
                    </td>
                    <td className="actions">
                      {cmd && (
                        <Badge tone={cmd.status === "failed" ? "bad" : cmd.status === "succeeded" ? "ok" : "info"}>
                          {cmd.status}
                          {cmd.error ? `: ${cmd.error.code}` : ""}
                        </Badge>
                      )}{" "}
                      <button
                        className="ghost"
                        disabled={action.busy || cmd?.status === "pending" || cmd?.status === "running"}
                        onClick={() =>
                          action.run(async () => {
                            const res = await api.updateWorker(c.agentId, c.worker);
                            setSent((s) => ({ ...s, [key]: res.id }));
                          })
                        }
                      >
                        Обновить воркер
                      </button>
                    </td>
                  </tr>
                );
              })}
            </tbody>
          </table>
        </>
      )}
      {action.error && <div className="hint bad-text">{action.error}</div>}

      {rel && (
        <>
          <h4>Установка на новый узел</h4>
          <div className="install">
            <code>{rel.installCommand}</code>
            <button className="ghost" onClick={copy}>
              {copied ? "Скопировано" : "Копировать"}
            </button>
          </div>
        </>
      )}
    </section>
  );
}
