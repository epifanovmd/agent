package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// envVar — переменная AGENT_*: какое поле настроек она задаёт и как.
type envVar struct {
	Name string
	// Path — ключ в agent.yaml (для пометок config check).
	Path  string
	apply func(c *Config, v string) error
	// empty — пустое значение тоже применяется (пустой список).
	empty bool
}

// envVars — переменные окружения, которые читает агент, поверх файла настроек.
var envVars = []envVar{
	envString("AGENT_SERVER_URL", "server.url", func(c *Config) *string { return &c.Server.URL }),
	envList("AGENT_SERVER_URLS", "server.urls", func(c *Config) *[]string { return &c.Server.URLs }),
	envString("AGENT_SERVER_CA_FILE", "server.caFile", func(c *Config) *string { return &c.Server.CAFile }),
	envString("AGENT_SERVER_CERT_FILE", "server.certFile", func(c *Config) *string { return &c.Server.CertFile }),
	envString("AGENT_SERVER_KEY_FILE", "server.keyFile", func(c *Config) *string { return &c.Server.KeyFile }),
	envString("AGENT_DATA_DIR", "dataDir", func(c *Config) *string { return &c.DataDir }),
	envString("AGENT_NAME", "name", func(c *Config) *string { return &c.Name }),
	{Name: "AGENT_LABELS", Path: "labels", empty: true, apply: func(c *Config, v string) error {
		if c.Labels == nil {
			c.Labels = map[string]string{}
		}
		for _, pair := range strings.Split(v, ",") {
			if k, val, found := strings.Cut(strings.TrimSpace(pair), "="); found && k != "" {
				c.Labels[k] = val
			}
		}
		return nil
	}},
	envString("AGENT_ENROLL_TOKEN", "enroll.token", func(c *Config) *string { return &c.Enroll.Token }),
	envString("AGENT_LOG_LEVEL", "log.level", func(c *Config) *string { return &c.Log.Level }),
	envString("AGENT_LOG_FORMAT", "log.format", func(c *Config) *string { return &c.Log.Format }),
	envString("AGENT_LOG_FORWARD", "log.forward", func(c *Config) *string { return &c.Log.Forward }),
	envList("AGENT_TELEMETRY_METRICS", "telemetry.metrics", func(c *Config) *[]string { return &c.Telemetry.Metrics }),
	envList("AGENT_TELEMETRY_DISKS", "telemetry.disks", func(c *Config) *[]string { return &c.Telemetry.Disks }),
	envList("AGENT_TELEMETRY_EXCLUDE_INTERFACES", "telemetry.excludeInterfaces", func(c *Config) *[]string { return &c.Telemetry.ExcludeInterfaces }),
	envString("AGENT_UPDATE_MODE", "update.mode", func(c *Config) *string { return &c.Update.Mode }),
	envString("AGENT_UPDATE_PUBLIC_KEY", "update.publicKey", func(c *Config) *string { return &c.Update.PublicKey }),
	envList("AGENT_UPDATE_PUBLIC_KEYS", "update.publicKeys", func(c *Config) *[]string { return &c.Update.PublicKeys }),
	envString("AGENT_UPDATE_RELEASES", "update.releases", func(c *Config) *string { return &c.Update.Releases }),
	envDuration("AGENT_UPDATE_CHECK_INTERVAL", "update.checkInterval", func(c *Config) *Duration {
		c.Update.CheckInterval = new(Duration)
		return c.Update.CheckInterval
	}),
	envDuration("AGENT_SERVER_RECONNECT_MIN", "server.reconnect.min", func(c *Config) *Duration { return &c.Server.Reconnect.Min }),
	envDuration("AGENT_SERVER_RECONNECT_MAX", "server.reconnect.max", func(c *Config) *Duration { return &c.Server.Reconnect.Max }),
	envInt("AGENT_SERVER_STREAM_BUFFER", "server.streamBuffer", func(c *Config) *int { return &c.Server.StreamBuffer }),
	envInt("AGENT_OUTBOX_MAX_MESSAGES", "outbox.maxMessages", func(c *Config) *int { return &c.Outbox.MaxMessages }),
	envInt("AGENT_LOG_BUFFER", "log.buffer", func(c *Config) *int { return &c.Log.Buffer }),
}

func envString(name, path string, field func(*Config) *string) envVar {
	return envVar{Name: name, Path: path, apply: func(c *Config, v string) error {
		*field(c) = v
		return nil
	}}
}

// envList — список через запятую; пустая переменная — пустой список.
func envList(name, path string, field func(*Config) *[]string) envVar {
	return envVar{Name: name, Path: path, empty: true, apply: func(c *Config, v string) error {
		list := []string{}
		for _, item := range strings.Split(v, ",") {
			if item = strings.TrimSpace(item); item != "" {
				list = append(list, item)
			}
		}
		*field(c) = list
		return nil
	}}
}

func envInt(name, path string, field func(*Config) *int) envVar {
	return envVar{Name: name, Path: path, apply: func(c *Config, v string) error {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return errors.New("нужно целое число")
		}
		*field(c) = n
		return nil
	}}
}

// envDuration — field вызывается, только когда значение верное.
func envDuration(name, path string, field func(*Config) *Duration) envVar {
	return envVar{Name: name, Path: path, apply: func(c *Config, v string) error {
		d, err := time.ParseDuration(strings.TrimSpace(v))
		if err != nil {
			return errors.New("нужна длительность (например, 30s)")
		}
		*field(c) = Duration(d)
		return nil
	}}
}

// applyEnv — переменные AGENT_* поверх файла; lookup — окружение процесса,
// затем файлы переменных.
func applyEnv(cfg *Config, lookup func(string) (string, bool)) {
	for _, e := range envVars {
		v, ok := lookup(e.Name)
		if !ok || (v == "" && !e.empty) {
			continue
		}
		if err := e.apply(cfg, v); err != nil {
			cfg.envErrs = append(cfg.envErrs, fmt.Sprintf("%s: %q — %v", e.Name, v, err))
		}
	}
}
