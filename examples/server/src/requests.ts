// Запросы воркеров к серверу стенда (onWorkerRequest, sdk/spec §12): воркер спрашивает, бэкенд
// отвечает. echo.lookup — префикс для текста задачи echo.quick с lookup: true.
import { AgentsError, type WorkerRequest } from "agent-sdk/server";

/** Ответ на запрос воркера; незнакомый тип — отказ REQUEST_UNHANDLED. */
export const onWorkerRequest = (req: WorkerRequest): unknown => {
  if (req.type === "echo.lookup") return { prefix: `${req.agent.name}: ` };
  throw new AgentsError(
    "REQUEST_UNHANDLED",
    `стенд не отвечает на запрос ${req.type} воркера ${req.worker}`,
    404,
  );
};
