package bundle

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/install"
	"github.com/epifanovmd/agent/internal/releases"
)

// ReadInfo — bundle.json распакованного архива dir (нет — ok=false).
func ReadInfo(dir string) (Info, bool, error) {
	raw, err := os.ReadFile(filepath.Join(dir, InfoFile))
	if os.IsNotExist(err) {
		return Info{}, false, nil
	}
	if err != nil {
		return Info{}, false, err
	}
	var info Info
	if err := json.Unmarshal(raw, &info); err != nil {
		return Info{}, false, fmt.Errorf("%s: %w", InfoFile, err)
	}
	return info, true, nil
}

// Prepare — параметры agent install из распакованного архива dir (под
// платформу goos/arch): настройки одним файлом (Flatten) во временном файле,
// программа и сборки воркеров из архива, ключи подписи архива, переменные
// из его файлов переменных, раздел install и instance из настроек — там, где
// флагов нет. Вернёт временный каталог — его удаляют после установки.
func Prepare(dir, goos, arch string, o *install.Options) (string, error) {
	info, ok, err := ReadInfo(dir)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("%s — не архив agent pack: нет %s", dir, InfoFile)
	}
	if info.OS != goos || info.Arch != arch {
		return "", fmt.Errorf("архив собран под %s/%s, а это %s/%s: agent pack --platform %s/%s", info.OS, info.Arch, goos, arch, goos, arch)
	}
	if o.Env != "" && info.Env != "" && o.Env != info.Env {
		return "", fmt.Errorf("--env %s, а архив собран для %s", o.Env, info.Env)
	}
	if o.Config != "" {
		return "", fmt.Errorf("--config — свой файл настроек; в архиве настройки свои (%s)", info.Config)
	}
	main := filepath.Join(dir, info.Config)
	src := config.ReadSource(main)
	if len(src.Errors) > 0 {
		return "", problems(src.Errors)
	}
	cfg := config.Defaults()
	if err := src.Decode(&cfg); err != nil {
		return "", err
	}
	if o.Instance == "" {
		o.Instance = cfg.Instance
	}
	if err := install.CheckInstance(o.Instance); err != nil {
		return "", err
	}
	if cfg.Install != nil {
		apply(o, *cfg.Install)
	}

	tmp, err := os.MkdirTemp("", "agent-install-")
	if err != nil {
		return "", err
	}
	l := install.Layout(o.Instance)
	files := make([]string, len(src.Files))
	for i, f := range src.Files {
		files[i] = filepath.Base(f)
	}
	header := fmt.Sprintf("Настройки агента на узле: собраны из %s (архив agent pack, агент %s, %s).\n"+
		"Значения ${ИМЯ} — из %s. Проверить: agent config check; применить: systemctl reload %s.",
		strings.Join(files, " + "), info.Version, time.UnixMilli(info.CreatedAt).Format("2006-01-02"), l.EnvFile, l.Unit)
	flat, err := config.Flatten(main, l.DataDir, header)
	if err != nil {
		os.RemoveAll(tmp)
		return "", err
	}
	o.Config = filepath.Join(tmp, "agent.yaml")
	if err := os.WriteFile(o.Config, flat, 0o644); err != nil {
		os.RemoveAll(tmp)
		return "", err
	}

	o.Binary = filepath.Join(dir, Binary)
	o.Version = info.Version
	for _, w := range info.Workers {
		if !slices.Contains(o.Workers, w) {
			o.Workers = append(o.Workers, w)
		}
	}
	if len(info.Workers) > 0 {
		o.Releases = filepath.Join(dir, ReleaseDir)
	}
	o.PublicKeys = append(o.PublicKeys, info.PublicKeys...)
	// На узел — только то, что нужно агенту: переменные из ${ИМЯ} настроек и AGENT_*.
	used := map[string]bool{}
	for _, m := range reVar.FindAllStringSubmatch(string(flat), -1) {
		used[m[1]] = true
	}
	vars := map[string]string{}
	for k, v := range src.Env {
		if used[k] || strings.HasPrefix(k, "AGENT_") {
			vars[k] = v
		}
	}
	if o.Server != "" {
		vars["AGENT_SERVER_URL"] = strings.TrimRight(o.Server, "/")
	}
	delete(vars, releases.SigningKeyEnv)
	if o.Token == "" && o.TokenFile == "" {
		o.Token = vars["AGENT_ENROLL_TOKEN"]
	}
	delete(vars, "AGENT_ENROLL_TOKEN")
	for _, k := range strings.Split(vars["AGENT_UPDATE_PUBLIC_KEYS"]+","+vars["AGENT_UPDATE_PUBLIC_KEY"], ",") {
		if k = strings.TrimSpace(k); k != "" {
			o.PublicKeys = append(o.PublicKeys, k)
		}
	}
	delete(vars, "AGENT_UPDATE_PUBLIC_KEYS")
	delete(vars, "AGENT_UPDATE_PUBLIC_KEY")
	if o.Vars == nil {
		o.Vars = map[string]string{}
	}
	for k, v := range vars {
		if _, set := o.Vars[k]; !set {
			o.Vars[k] = v
		}
	}
	return tmp, nil
}

// reVar — ${ИМЯ} в итоговом файле настроек.
var reVar = regexp.MustCompile(`\$\{([A-Za-z_][A-Za-z0-9_]*)\}`)

// Instance — экземпляр из настроек распакованного архива dir ("" — нет архива или экземпляра).
func Instance(dir string) (string, error) {
	info, ok, err := ReadInfo(dir)
	if err != nil || !ok {
		return "", err
	}
	src := config.ReadSource(filepath.Join(dir, info.Config))
	if len(src.Errors) > 0 {
		return "", problems(src.Errors)
	}
	cfg := config.Defaults()
	if err := src.Decode(&cfg); err != nil {
		return "", err
	}
	return cfg.Instance, nil
}

// apply — раздел install файла настроек там, где флагов нет.
func apply(o *install.Options, in config.Install) {
	if len(o.Packages) == 0 {
		o.Packages = in.Packages
	}
	if o.PackagesBy == nil {
		o.PackagesBy = map[string][]string{}
	}
	for m, list := range in.PackagesByManager {
		if len(o.PackagesBy[m]) == 0 {
			o.PackagesBy[m] = list
		}
	}
	o.Privileged = o.Privileged || in.Privileged
	if o.User == "" {
		o.User = in.User
	}
	for _, p := range in.RWPaths {
		if !slices.Contains(o.RWPaths, p) {
			o.RWPaths = append(o.RWPaths, p)
		}
	}
	for _, k := range slices.Sorted(maps.Keys(in.Sysctl)) {
		if !slices.ContainsFunc(o.Sysctls, func(s string) bool { return strings.HasPrefix(s, k+"=") }) {
			o.Sysctls = append(o.Sysctls, k+"="+in.Sysctl[k])
		}
	}
	if o.KillMode == "" {
		o.KillMode = in.KillMode
	}
	if o.StopTimeout == "" {
		o.StopTimeout = in.StopTimeout
	}
}
