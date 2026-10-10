"""Воркер {{.Name}}: свой код — здесь, база (agent_worker.py) — от `agent worker new`, не правьте её.

Запуск — ./run; версия — файл VERSION. Что умеет база и как устроен воркер — agent_worker.py.
"""

from agent_worker import Config, JobError, Worker, job, route


class {{.Class}}(Worker):
    description = "{{.Name}}: что делает воркер"

    # Ключ настроек: бэкенд задаёт его agents.setConfig(id, "{{.Name}}", "settings", {...}).
    settings = Config("settings", schema={"type": "object"}, default={"greeting": "привет"})

    # События, которые воркер шлёт бэкенду (self.emit): тип → схема data.
    events = {"{{.Name}}.greeted": {"type": "object", "properties": {"name": {"type": "string"}}}}

    # Маршрут для agents.fetch(id, "{{.Name}}", "/hello", {method: "POST", body: ...}).
    @route("POST", "/hello", request={"type": "object", "properties": {"name": {"type": "string"}}})
    def hello(self, req):
        """Поздороваться."""
        name = req.json().get("name", "мир")
        self.emit("{{.Name}}.greeted", {"name": name})
        return {"text": "%s, %s" % (self.settings.value.get("greeting", "привет"), name)}

    # Долгая задача: agents.runJob(id, "{{.Name}}", {type: "{{.Name}}.count", data: {"to": 5}}).
    @job("{{.Name}}.count", schema={"type": "object", "properties": {"to": {"type": "integer"}}})
    def count(self, job):
        """Посчитать до to, по шагу в секунду."""
        to = int((job.data or {}).get("to", 5))
        if to < 1:
            raise JobError("to — не меньше 1", code="BAD_INPUT")
        for i in range(to):
            job.progress(i / to, "шаг %d из %d" % (i + 1, to))
            job.sleep(1)
        return {"counted": to}


if __name__ == "__main__":
    {{.Class}}().run()
