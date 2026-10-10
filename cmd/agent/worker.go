package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/scaffold"
)

// workerCmd — agent worker new | sync | list: воркеры папки агента.
func workerCmd(args []string) error {
	if len(args) == 0 {
		return errors.New("agent worker new ИМЯ [--lang python|go] | agent worker sync | agent worker list")
	}
	switch args[0] {
	case "new":
		return workerNew(args[1:])
	case "sync":
		return workerSync(args[1:])
	case "list":
		return workerList(args[1:])
	}
	return fmt.Errorf("agent worker: неизвестная команда %q (new | sync | list)", args[0])
}

// agentFolder — папка агента: каталог файла настроек (-config, --env, agent.yaml).
func agentFolder(t target) (string, string, error) {
	if *t.instance != "" {
		return "", "", errors.New("--instance — агент на узле; воркеры добавляют в папке агента")
	}
	path, err := t.path()
	if err != nil {
		return "", "", err
	}
	if path == "" {
		return "", "", errors.New("нет папки агента (agent.yaml рядом с программой или в текущем каталоге): agent init")
	}
	abs, _ := filepath.Abs(path)
	return filepath.Dir(abs), abs, nil
}

// parseWithName — флаги и одно имя среди них в любом месте.
func parseWithName(set *flag.FlagSet, args []string) (string, error) {
	var names []string
	for {
		if err := set.Parse(args); err != nil {
			return "", err
		}
		if set.NArg() == 0 {
			break
		}
		names = append(names, set.Arg(0))
		args = set.Args()[1:]
	}
	if len(names) != 1 {
		return "", errors.New("нужно одно имя воркера: agent worker new ИМЯ")
	}
	return names[0], nil
}

func workerNew(args []string) error {
	set := flag.NewFlagSet("worker new", flag.ContinueOnError)
	t := configFlag(set)
	lang := set.String("lang", scaffold.Langs[0].Name, "язык заготовки: python | go")
	force := set.Bool("force", false, "перезаписать папку воркера, если она уже есть")
	name, err := parseWithName(set, args)
	if err != nil {
		return err
	}
	root, cfgPath, err := agentFolder(t)
	if err != nil {
		return err
	}
	// Воркер дописывается в общий agent.yaml (а не в файл окружения), если он есть.
	target := filepath.Join(root, config.FileName)
	if !isFile(target) {
		target = cfgPath
	}
	created, added, err := scaffold.NewWorker(scaffold.WorkerOptions{Root: root, Config: target, Name: name, Lang: *lang, Force: *force})
	if err != nil {
		return err
	}
	fmt.Printf("Воркер %s (%s):\n", name, *lang)
	for _, f := range created {
		fmt.Println("  " + f)
	}
	if added {
		fmt.Printf("В %s добавлен:  - path: workers/%s\n", config.ShortPath(target), name)
	} else {
		fmt.Printf("В %s он уже был.\n", config.ShortPath(target))
	}
	l, _ := scaffold.LangByName(*lang)
	fmt.Printf("\nСвой код — workers/%s/%s (база %s — не править: agent worker sync обновит её).\n", name, l.Main, l.Base)
	if l.Name == "go" {
		fmt.Println("Нужен Go ≥ 1.22: ./run собирает воркер из исходников, ./build — под платформу узла (agent pack).")
	}
	fmt.Println("Запустить: agent run; версия воркера — файл VERSION.")
	return nil
}

func workerSync(args []string) error {
	set := flag.NewFlagSet("worker sync", flag.ContinueOnError)
	t := configFlag(set)
	if err := set.Parse(args); err != nil {
		return err
	}
	root, _, err := agentFolder(t)
	if err != nil {
		return err
	}
	synced, err := scaffold.SyncBases(root)
	if err != nil {
		return err
	}
	if len(synced) == 0 {
		fmt.Println("Базы воркеров актуальны.")
		return nil
	}
	for _, s := range synced {
		fmt.Printf("%s: база %d → %d\n", s.File, s.From, s.To)
	}
	fmt.Println("Поднимите VERSION обновлённых воркеров, чтобы узлы получили новую сборку.")
	return nil
}

// workerList — воркеры из настроек: откуда каждый и его версия.
func workerList(args []string) error {
	set := flag.NewFlagSet("worker list", flag.ContinueOnError)
	t := configFlag(set)
	if err := set.Parse(args); err != nil {
		return err
	}
	cfg := looseConfig(t)
	if err := cfg.ValidateWorkers(); err != nil {
		return err
	}
	if len(cfg.Workers) == 0 {
		fmt.Println("Воркеров нет: agent worker new ИМЯ")
		return nil
	}
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "ВОРКЕР\tОТКУДА\tВЕРСИЯ\tГДЕ")
	for _, wk := range cfg.Workers {
		kind, version, where := "команда", "", strings.Join(wk.Command, " ")
		switch {
		case wk.Path != "":
			kind, where = "папка", config.ShortPath(wk.Path)
			version = readTrim(filepath.Join(wk.Path, "VERSION"))
		case wk.From == config.FromAgent:
			kind, where = "релиз агента", config.ShortPath(wk.Current())
			version = readTrim(filepath.Join(wk.ReleaseDir, config.ReleaseVersion))
		case wk.Release:
			kind, where = "сборка с сервера", config.ShortPath(wk.Current())
			version = readTrim(filepath.Join(wk.ReleaseDir, config.ReleaseVersion))
		}
		if version == "" {
			version = "—"
		}
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\n", wk.Name, kind, version, where)
	}
	return w.Flush()
}

func readTrim(path string) string {
	raw, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(raw))
}
