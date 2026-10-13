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
	Name  string
	apply func(c *Config, v string) error
	// empty — пустое значение тоже применяется (пустой список).
	empty bool
}

// envVars — переменные окружения, которые читает агент, поверх файла настроек.
var envVars = []envVar{
	envString("AGENT_SERVER_URL", func(c *Config) *string { return &c.Server.URL }),
	envBool("AGENT_SERVER_ALLOW_INSECURE", func(c *Config) *bool { return &c.Server.AllowInsecure }),
	envString("AGENT_SERVER_CA_FILE", func(c *Config) *string { return &c.Server.CAFile }),
	envString("AGENT_DATA_DIR", func(c *Config) *string { return &c.DataDir }),
	envString("AGENT_NAME", func(c *Config) *string { return &c.Name }),
	{Name: "AGENT_LABELS", empty: true, apply: func(c *Config, v string) error {
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
	envString("AGENT_ENROLL_TOKEN", func(c *Config) *string { return &c.Enroll.Token }),
	envString("AGENT_LOG_LEVEL", func(c *Config) *string { return &c.Log.Level }),
	envString("AGENT_LOG_FORMAT", func(c *Config) *string { return &c.Log.Format }),
	envString("AGENT_LOG_FORWARD", func(c *Config) *string { return &c.Log.Forward }),
	envString("AGENT_UPDATE_MODE", func(c *Config) *string { return &c.Update.Mode }),
	envString("AGENT_UPDATE_PUBLIC_KEY", func(c *Config) *string { return &c.Update.PublicKey }),
	envList("AGENT_UPDATE_PUBLIC_KEYS", func(c *Config) *[]string { return &c.Update.PublicKeys }),
	envString("AGENT_UPDATE_RELEASES", func(c *Config) *string { return &c.Update.Releases }),
	envDuration("AGENT_UPDATE_CHECK_INTERVAL", func(c *Config) *Duration {
		c.Update.CheckInterval = new(Duration)
		return c.Update.CheckInterval
	}),
	envInt("AGENT_OUTBOX_MAX_MESSAGES", func(c *Config) *int { return &c.Outbox.MaxMessages }),
	envInt("AGENT_LOG_BUFFER", func(c *Config) *int { return &c.Log.Buffer }),
}

func envString(name string, field func(*Config) *string) envVar {
	return envVar{Name: name, apply: func(c *Config, v string) error {
		*field(c) = v
		return nil
	}}
}

// envList — список через запятую; пустая переменная — пустой список.
func envList(name string, field func(*Config) *[]string) envVar {
	return envVar{Name: name, empty: true, apply: func(c *Config, v string) error {
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

func envBool(name string, field func(*Config) *bool) envVar {
	return envVar{Name: name, apply: func(c *Config, v string) error {
		b, err := strconv.ParseBool(strings.TrimSpace(v))
		if err != nil {
			return errors.New("нужно true или false")
		}
		*field(c) = b
		return nil
	}}
}

func envInt(name string, field func(*Config) *int) envVar {
	return envVar{Name: name, apply: func(c *Config, v string) error {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return errors.New("нужно целое число")
		}
		*field(c) = n
		return nil
	}}
}

// envDuration — field вызывается, только когда значение верное.
func envDuration(name string, field func(*Config) *Duration) envVar {
	return envVar{Name: name, apply: func(c *Config, v string) error {
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
