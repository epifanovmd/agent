// Утилита подписи сборок агента: ключи подписи и манифест сборок. На узлы не ставится —
// нужна там, где готовят публикацию (scripts/release.sh, CI).
//
//	agent-release keygen                ключи подписи сборок (Ed25519)
//	agent-release manifest DIR VERSION [--worker NAME=VERSION[,stopTimeout=…][,command=…]]…
//	                                    manifest.json сборок агента и воркеров в DIR (подпись — AGENT_SIGNING_KEY);
//	                                    без сборок агента — только сборки воркеров (воркеры проекта)
package main

import (
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/update"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "agent-release:", err)
		os.Exit(1)
	}
}

// run — команда утилиты по аргументам (без имени программы).
func run(args []string) error {
	cmd := ""
	if len(args) > 0 {
		cmd = args[0]
	}
	switch cmd {
	case "keygen":
		return keygen()
	case "manifest":
		return manifest(args[1:])
	default:
		return fmt.Errorf("неизвестная команда %q (keygen | manifest)", cmd)
	}
}

// signingKey — ключ подписи из AGENT_SIGNING_KEY (nil — не задан).
func signingKey() (ed25519.PrivateKey, error) {
	seed := os.Getenv("AGENT_SIGNING_KEY")
	if seed == "" {
		return nil, nil
	}
	raw, err := base64.StdEncoding.DecodeString(seed)
	if err != nil || len(raw) != ed25519.SeedSize {
		return nil, errors.New("AGENT_SIGNING_KEY — base64 seed Ed25519 (agent-release keygen)")
	}
	return ed25519.NewKeyFromSeed(raw), nil
}

func keygen() error {
	pub, priv, err := ed25519.GenerateKey(nil)
	if err != nil {
		return err
	}
	fmt.Printf("AGENT_SIGNING_KEY=%s\n", base64.StdEncoding.EncodeToString(priv.Seed()))
	fmt.Printf("AGENT_UPDATE_PUBLIC_KEY=%s\n", base64.StdEncoding.EncodeToString(pub))
	return nil
}

// manifest — manifest.json для сборок agent-<os>-<arch> в каталоге и
// сборок воркеров: `--worker NAME=VERSION[,stopTimeout=…]`
// (повторяемый) берёт файлы DIR/<name>-<version>-<os>-<arch> или архивы
// DIR/<name>-<version>-<os>-<arch>.tar.gz. Подпись — AGENT_SIGNING_KEY над
// строкой сборки (§11: имя, версия, os, arch, sha256), publicKey — её
// открытый ключ. Сборок агента нет, но есть --worker — только сборки воркеров.
func manifest(args []string) error {
	usage := errors.New("manifest DIR VERSION [--worker NAME=VERSION[,stopTimeout=30s][,command=bin/report]]…")
	var positional []string
	var workers []message.WorkerArtifact
	for i := 0; i < len(args); i++ {
		arg := args[i]
		value, isWorker := strings.CutPrefix(arg, "--worker=")
		if arg == "--worker" || arg == "-worker" {
			if i+1 >= len(args) {
				return usage
			}
			i++
			value, isWorker = args[i], true
		}
		if !isWorker {
			if strings.HasPrefix(arg, "-") {
				return usage
			}
			positional = append(positional, arg)
			continue
		}
		w, err := parseWorkerFlag(value)
		if err != nil {
			return err
		}
		workers = append(workers, w)
	}
	if len(positional) != 2 {
		return usage
	}
	dir, ver := positional[0], positional[1]
	priv, err := signingKey()
	if err != nil {
		return err
	}
	sign := func(b update.Build) string {
		if priv == nil {
			return ""
		}
		return update.Sign(priv, b)
	}
	files, _ := filepath.Glob(filepath.Join(dir, "agent-*-*"))
	m := message.Manifest{Version: ver, Artifacts: []message.Artifact{}}
	if priv != nil {
		m.PublicKey = base64.StdEncoding.EncodeToString(priv.Public().(ed25519.PublicKey))
	}
	for _, file := range files {
		parts := strings.Split(filepath.Base(file), "-")
		if len(parts) != 3 {
			continue
		}
		hash, err := update.FileHash(file)
		if err != nil {
			return err
		}
		b := update.Build{Name: update.AgentName, Version: ver, OS: parts[1], Arch: parts[2], SHA256: hash}
		m.Artifacts = append(m.Artifacts, message.Artifact{OS: parts[1], Arch: parts[2], File: filepath.Base(file), SHA256: hash, Signature: sign(b)})
	}
	if len(m.Artifacts) == 0 && len(workers) == 0 {
		return fmt.Errorf("в %s нет сборок agent-<os>-<arch> (и не задан --worker)", dir)
	}
	for _, w := range workers {
		prefix := w.Name + "-" + w.Version + "-"
		found, _ := filepath.Glob(filepath.Join(dir, prefix+"*-*"))
		n := 0
		platforms := map[string]string{}
		for _, file := range found {
			rest := strings.TrimSuffix(strings.TrimPrefix(filepath.Base(file), prefix), ".tar.gz")
			goos, arch, ok := strings.Cut(rest, "-")
			if !ok || goos == "" || arch == "" || strings.ContainsAny(arch, "-.") {
				continue
			}
			if prev, dup := platforms[goos+"/"+arch]; dup {
				return fmt.Errorf("сборка воркера %s %s под %s/%s дважды: %s и %s", w.Name, w.Version, goos, arch, prev, filepath.Base(file))
			}
			platforms[goos+"/"+arch] = filepath.Base(file)
			hash, err := update.FileHash(file)
			if err != nil {
				return err
			}
			a := w
			a.OS, a.Arch, a.File, a.SHA256 = goos, arch, filepath.Base(file), hash
			a.Signature = sign(update.Build{Name: w.Name, Version: w.Version, OS: goos, Arch: arch, SHA256: hash})
			m.Workers = append(m.Workers, a)
			n++
		}
		if n == 0 {
			return fmt.Errorf("в %s нет сборок воркера %s-<os>-<arch>[.tar.gz]", dir, strings.TrimSuffix(prefix, "-"))
		}
	}
	raw, _ := json.MarshalIndent(m, "", "  ")
	if err := os.WriteFile(filepath.Join(dir, "manifest.json"), append(raw, '\n'), 0o644); err != nil {
		return err
	}
	if priv == nil {
		fmt.Fprintln(os.Stderr, "agent-release: AGENT_SIGNING_KEY не задан — релиз без подписи, самообновление на него не встанет")
	}
	fmt.Println(filepath.Join(dir, "manifest.json"))
	return nil
}

// parseWorkerFlag — NAME=VERSION[,stopTimeout=…][,command=…].
func parseWorkerFlag(value string) (message.WorkerArtifact, error) {
	parts := strings.Split(value, ",")
	name, ver, ok := strings.Cut(parts[0], "=")
	var w message.WorkerArtifact
	if !ok || !message.ValidName(name) || ver == "" || strings.ContainsAny(ver, "/ ") {
		return w, fmt.Errorf("--worker %q: нужно NAME=VERSION (имя — строчная латиница, цифры и «-», начало — буква)", value)
	}
	w.Name, w.Version = name, ver
	for _, opt := range parts[1:] {
		k, v, _ := strings.Cut(opt, "=")
		switch k {
		case "stopTimeout":
			if d, err := time.ParseDuration(v); err != nil || d <= 0 {
				return w, fmt.Errorf("--worker %s: stopTimeout — длительность (30s), а не %q", name, v)
			}
			w.StopTimeout = v
		case "command":
			if clean := path.Clean(v); v == "" || path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../") {
				return w, fmt.Errorf("--worker %s: command — путь внутри архива (bin/report), а не %q", name, v)
			}
			w.Command = path.Clean(v)
		default:
			return w, fmt.Errorf("--worker %s: неизвестный параметр %q (stopTimeout, command)", name, k)
		}
	}
	return w, nil
}
