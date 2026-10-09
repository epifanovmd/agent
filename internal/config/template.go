package config

import (
	"strconv"
	"strings"
)

// TemplateWorker — воркер, который записывается в создаваемый файл настроек.
type TemplateWorker struct {
	Name        string
	Release     bool
	Command     []string
	StopTimeout string
}

// TemplateOptions — значения для создаваемого файла настроек; пустые поля
// остаются закомментированными примерами.
type TemplateOptions struct {
	ServerURL string
	Token     string
	Name      string
	DataDir   string
	CAFile    string
	LogFormat string
	Workers   []TemplateWorker
	// uncomment — примеры без «#» (проверка, что примеры — верный YAML).
	uncomment bool
}

// Template — файл настроек с пояснениями: заданные значения и
// закомментированные примеры всех разделов со значениями по умолчанию
// (agent init, agent install).
func Template(o TemplateOptions) []byte {
	d := Defaults()
	var b strings.Builder
	line := func(s string) { b.WriteString(s + "\n") }
	// ex — пример: закомментирован (indent пробелов, «# », text) или, для
	// проверки, без комментария.
	ex := func(indent int, text string) {
		if o.uncomment {
			line(strings.Repeat(" ", indent) + text)
		} else {
			line(strings.Repeat(" ", indent) + "# " + text)
		}
	}
	note := func(indent int, text string) { line(strings.Repeat(" ", indent) + "# " + text) }

	line("# Настройки агента. Описание всех ключей — docs/ARCHITECTURE.md, раздел «Настройки»:")
	line("# https://github.com/epifanovmd/agent/blob/main/docs/ARCHITECTURE.md#настройки")
	line("#")
	line("# Обязательно только server.url, остальное можно не задавать. Строки с «#» — пояснения")
	line("# и примеры; у настроек со значением по умолчанию в примере стоит оно само.")
	line("# Чтобы поменять настройку, уберите «#» и впишите своё значение.")
	line("# ${ИМЯ} в значении подставляется из окружения. Переменные AGENT_* важнее файла:")
	line("# AGENT_SERVER_URL, AGENT_ENROLL_TOKEN, AGENT_NAME, AGENT_DATA_DIR и другие.")
	line("# Проверить файл: agent config check. Применить без перезапуска (служба): systemctl reload agent.")
	line("")

	note(0, "Адрес бэкенда.")
	line("server:")
	if o.ServerURL != "" {
		line("  url: " + quote(o.ServerURL))
	} else {
		ex(2, "url: https://api.example.com")
	}
	note(2, "Ещё адреса того же бэкенда: если первый недоступен, агент подключится к следующему.")
	ex(2, "urls: [https://api-2.example.com]")
	note(2, "Свой корневой сертификат сервера (PEM) — в дополнение к системным.")
	if o.CAFile != "" {
		line("  caFile: " + quote(o.CAFile))
	} else {
		ex(2, "caFile: /etc/agent/ca.pem")
	}
	note(2, "Клиентский сертификат агента (mTLS) — оба файла или ни одного.")
	ex(2, "certFile: /etc/agent/agent.crt")
	ex(2, "keyFile: /etc/agent/agent.key")
	note(2, "Пауза переподключения (и повтора регистрации): растёт от min до max.")
	ex(2, "reconnect: {min: "+dur(d.Server.Reconnect.Min)+", max: "+dur(d.Server.Reconnect.Max)+"}")
	note(2, "Сколько последних status, metrics и log агент держит без связи и досылает.")
	ex(2, "streamBuffer: "+strconv.Itoa(d.Server.StreamBuffer))
	line("")

	note(0, "Токен регистрации: нужен только при первом подключении, потом агент входит своим ключом.")
	if o.Token != "" {
		line("enroll:")
		line("  token: " + quote(o.Token))
	} else {
		note(0, "Лучше задать переменной AGENT_ENROLL_TOKEN (служба берёт её из agent.env рядом с этим файлом).")
		ex(0, "enroll:")
		ex(0, "  token: <токен>")
	}
	line("")

	note(0, "Имя агента на сервере (по умолчанию — имя машины) и метки для поиска и отбора.")
	if o.Name != "" {
		line("name: " + quote(o.Name))
	} else {
		ex(0, "name: node-01")
	}
	ex(0, "labels:")
	ex(0, "  zone: eu")
	line("")

	note(0, "Каталог данных: ключ агента, очередь важных сообщений, настройки и сборки воркеров.")
	if o.DataDir != "" {
		line("dataDir: " + quote(o.DataDir))
	} else {
		ex(0, "dataDir: "+quote(d.DataDir))
	}
	line("")

	note(0, "Лог: level — debug | info | warn | error; format — text | json;")
	note(0, "forward — с какого уровня записи уходят серверу: off | error | warn | info | debug;")
	note(0, "buffer — сколько последних записей агент помнит для agent.logs (на агента и на каждый воркер).")
	if o.LogFormat != "" {
		line("log:")
		line("  format: " + o.LogFormat)
		ex(2, "level: "+d.Log.Level)
		ex(2, "forward: "+d.Log.Forward)
		ex(2, "buffer: "+strconv.Itoa(d.Log.Buffer))
	} else {
		ex(0, "log:")
		ex(0, "  level: "+d.Log.Level)
		ex(0, "  format: "+d.Log.Format)
		ex(0, "  forward: "+d.Log.Forward)
		ex(0, "  buffer: "+strconv.Itoa(d.Log.Buffer))
	}
	line("")

	note(0, "Очередь важных сообщений на диске (события воркеров, итоги): сколько сообщений она держит,")
	note(0, "пока нет связи; полна — новые события воркеров отклоняются.")
	ex(0, "outbox:")
	ex(0, "  maxMessages: "+strconv.Itoa(d.Outbox.MaxMessages))
	line("")

	note(0, "Обновление: self — агент сам ставит новую версию по команде сервера; external — обновляют")
	note(0, "снаружи (новый образ контейнера); disabled — не обновлять. publicKey и publicKeys — ключи")
	note(0, "проверки подписи выпусков (base64): подпись принимается, если сходится с любым; ключ автора")
	note(0, "агента в сборки из выпуска уже вшит, сюда — ключи проекта (его воркеров).")
	ex(0, "update:")
	ex(0, "  mode: "+d.Update.Mode)
	ex(0, "  publicKeys: [<ключ base64>]")
	line("")

	note(0, "Метрики узла (их собирает встроенный воркер sysmetrics).")
	ex(0, "telemetry:")
	ex(0, "  metrics: ["+strings.Join(d.Telemetry.Metrics, ", ")+"]")
	ex(0, `  disks: ["/"]            # точки монтирования для метрик дисков; ["all"] — все`)
	ex(0, "  excludeInterfaces: ["+strings.Join(d.Telemetry.ExcludeInterfaces, ", ")+"]")
	line("")

	note(0, "Воркеры: HTTP-сервисы на unix-сокете на любом языке; агент запускает их и держит работающими.")
	switch {
	case len(o.Workers) > 0:
		line("workers:")
		for _, w := range o.Workers {
			line("  - name: " + quote(w.Name))
			if w.Release {
				line("    release: true")
			}
			if len(w.Command) > 0 {
				parts := make([]string, len(w.Command))
				for i, c := range w.Command {
					parts[i] = strconv.Quote(c)
				}
				line("    command: [" + strings.Join(parts, ", ") + "]")
			}
			if w.StopTimeout != "" {
				line("    lifecycle:")
				line("      stopTimeout: " + quote(w.StopTimeout))
			}
		}
	case !o.uncomment:
		line("workers: []")
	}
	if len(o.Workers) == 0 {
		ex(0, "workers:")
	}
	ex(0, "  - name: report")
	ex(0, `    command: ["python3", "/opt/workers/report.py"]`)
	ex(0, "    dir: /opt/workers")
	ex(0, "    env:")
	ex(0, "      REPORT_MODE: fast")
	ex(0, "    inheritEnv: [REPORT_*]  # какие ещё переменные агента передать воркеру")
	ex(0, "    user: report           # от кого запускать (агенту нужен root)")
	note(4, "Жизнь воркера; здесь — значения по умолчанию (подходят для долгой работы).")
	note(4, "Изменение lifecycle, logs и routes применяется без перезапуска воркера.")
	l := DefaultLifecycle()
	ex(0, "    lifecycle:")
	ex(0, "      onAgentRestart: "+l.OnAgentRestart+"   # перезапуск агента: keep — работает дальше, restart — остановить")
	ex(0, "      onAgentStop: "+l.OnAgentStop+"      # остановка агента (SIGTERM): keep | stop")
	ex(0, "      restart: "+l.Restart+"   # после выхода процесса: always | on-failure | never")
	ex(0, "      backoff: {min: "+dur(l.Backoff.Min)+", max: "+dur(l.Backoff.Max)+"}   # пауза перед перезапуском после падения")
	ex(0, "      maxRestarts: 0         # падений подряд; 0 — без предела, дальше — остановлен")
	ex(0, "      startTimeout: "+dur(l.StartTimeout)+"      # сокет должен начать отвечать")
	ex(0, "      stopTimeout: "+dur(l.StopTimeout)+"       # ждать выхода после SIGTERM, потом SIGKILL")
	ex(0, "      keepChildren: false    # true — дочерние процессы переживают остановку воркера")
	ex(0, "      busy: {wait: true, timeout: "+dur(l.Busy.Timeout)+"}   # замена ждёт, пока /health отвечает busy: true")
	ex(0, "      health: {interval: "+dur(*l.Health.Interval)+", timeout: "+dur(l.Health.Timeout)+", failures: "+strconv.Itoa(*l.Health.Failures)+"}   # interval: 0s — не проверять")
	ex(0, "      probeTimeout: "+dur(l.ProbeTimeout)+"       # срок GET /manifest, /metrics и /health при регистрации")
	ex(0, "      configRetry: "+dur(l.ConfigRetry)+"       # повтор неприменённых настроек")
	ex(0, "      updateHealthyTimeout: "+dur(l.UpdateHealthyTimeout)+"   # новая сборка должна ответить ok: true")
	ex(0, "    logs: {maxSize: "+DefaultLogs().MaxSize.String()+", maxFiles: "+strconv.Itoa(DefaultLogMaxFiles)+"}   # файлы вывода в <dataDir>/logs")
	ex(0, "    routes: "+RoutesStrict+"         # strict — запросы сервера только по манифесту воркера; open — любой путь")
	ex(0, "  - name: example")
	ex(0, "    release: true          # сборку воркера ставит и обновляет агент (agent install --worker example)")
	return []byte(b.String())
}

func dur(d Duration) string {
	s := d.Std().String()
	// 10m0s → 10m, 1h0m0s → 1h.
	for _, suf := range []string{"0s", "0m"} {
		if strings.HasSuffix(s, "m"+suf) || strings.HasSuffix(s, "h"+suf) {
			s = strings.TrimSuffix(s, suf)
		}
	}
	return s
}

// quote — значение YAML: как есть, если это безопасно, иначе в кавычках.
func quote(s string) string {
	plain := s != "" && strings.TrimSpace(s) == s &&
		!strings.ContainsAny(s[:1], "-?:,[]{}#&*!|>'\"%@`") &&
		!strings.Contains(s, ": ") && !strings.Contains(s, " #") && !strings.HasSuffix(s, ":") &&
		!strings.ContainsAny(s, "\n\t\"'\\$") &&
		s != "true" && s != "false" && s != "null" && s != "~" && s != "yes" && s != "no"
	if _, err := strconv.ParseFloat(s, 64); err == nil {
		plain = false
	}
	if plain {
		return s
	}
	return strconv.Quote(s)
}
