package config

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"strings"
)

// Пути по умолчанию на Linux: настройки, переменные службы, данные.
const (
	LinuxConfigPath = "/etc/agent/agent.yaml"
	LinuxDataDir    = "/var/lib/agent"
)

// FileName — файл настроек в папке агента.
const FileName = "agent.yaml"

// EnvConfigName — файл настроек окружения env (--env prod → agent.prod.yaml).
func EnvConfigName(env string) string { return "agent." + env + ".yaml" }

// EnvFileName — файл переменных окружения рядом с файлом настроек (agent.env):
// токен регистрации и ключ проверки подписи сборок. Его читает служба systemd, а
// agent config check / status / cleanup — сами.
const EnvFileName = "agent.env"

// homeDir — каталог агента в домашнем каталоге (macOS, разработка).
func homeDir() string {
	home, err := os.UserHomeDir()
	if err != nil || home == "" {
		return ".agent"
	}
	return filepath.Join(home, ".agent")
}

// DefaultPath — файл настроек по умолчанию: /etc/agent/agent.yaml на Linux,
// ~/.agent/agent.yaml на macOS.
func DefaultPath() string {
	if runtime.GOOS == "darwin" {
		return filepath.Join(homeDir(), "agent.yaml")
	}
	return LinuxConfigPath
}

// DefaultDataDir — каталог данных по умолчанию: /var/lib/agent на Linux,
// ~/.agent/data на macOS.
func DefaultDataDir() string {
	if runtime.GOOS == "darwin" {
		return filepath.Join(homeDir(), "data")
	}
	return LinuxDataDir
}

// ResolvePath — какой файл настроек читать: флаг -config, иначе AGENT_CONFIG,
// иначе первый существующий из local (agent.yaml в папке агента), иначе
// DefaultPath, если файл есть. Пусто — без файла (только окружение).
func ResolvePath(flagValue string, local ...string) string {
	if flagValue != "" {
		return flagValue
	}
	if v := os.Getenv("AGENT_CONFIG"); v != "" {
		return v
	}
	for _, p := range local {
		if isRegular(p) {
			return p
		}
	}
	if p := DefaultPath(); exists(p) {
		return p
	}
	return ""
}

func isRegular(path string) bool {
	st, err := os.Stat(path)
	return err == nil && st.Mode().IsRegular()
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

// EnvFile — путь к agent.env рядом с файлом настроек path ("" — нет файла настроек).
func EnvFile(path string) string {
	if path == "" {
		return ""
	}
	return filepath.Join(filepath.Dir(path), EnvFileName)
}

// ApplyEnvFile — переменные из файла KEY=VALUE (как EnvironmentFile systemd:
// пустые строки и # — пропускаются, кавычки вокруг значения снимаются) в
// окружение процесса; уже заданные в окружении не меняются. Вернёт имена
// применённых. Файла нет — не ошибка.
func ApplyEnvFile(path string) ([]string, error) {
	f, err := os.Open(path)
	if errors.Is(err, fs.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer f.Close()
	vars, err := ParseEnv(f)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", path, err)
	}
	var applied []string
	for _, kv := range vars {
		if _, set := os.LookupEnv(kv[0]); set {
			continue
		}
		if err := os.Setenv(kv[0], kv[1]); err != nil {
			return applied, err
		}
		applied = append(applied, kv[0])
	}
	return applied, nil
}

// ParseEnv — пары KEY=VALUE по порядку из содержимого файла переменных.
func ParseEnv(r io.Reader) ([][2]string, error) {
	var out [][2]string
	sc := bufio.NewScanner(r)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		k = strings.TrimSpace(k)
		if !ok || k == "" {
			return nil, fmt.Errorf("строка %d: нужно ИМЯ=ЗНАЧЕНИЕ", n)
		}
		v = strings.TrimSpace(v)
		if len(v) >= 2 && (v[0] == '"' || v[0] == '\'') && v[len(v)-1] == v[0] {
			v = v[1 : len(v)-1]
		}
		out = append(out, [2]string{k, v})
	}
	return out, sc.Err()
}
