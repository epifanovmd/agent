// Package scaffold — папка агента (agent init): программа, настройки для этой
// машины и боевых узлов, образцы файлов переменных, папка воркеров; воркеры из
// заготовки (agent worker new) — база и класс-наследник на Python.
package scaffold

import (
	"bytes"
	"embed"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"text/template"

	"github.com/epifanovmd/agent/internal/fsutil"
)

//go:embed files
var files embed.FS

// Options — что разворачивать и куда.
type Options struct {
	// Dir — папка агента.
	Dir string
	// Server — адрес бэкенда для этой машины (пусто — образец в .env).
	Server string
	// Token — токен регистрации для этой машины (записывается в .env).
	Token string
	// Binary — программа агента, которую положить в папку ("" — не класть).
	Binary string
	// Force — перезаписать готовые файлы.
	Force bool
}

// Result — что создано и что уже было (не тронуто).
type Result struct {
	Created, Skipped []string
}

// file — файл папки: шаблон из files и права.
type file struct {
	name, from string
	perm       os.FileMode
}

// layout — файлы папки агента. .env — из того же шаблона, что и образец, со
// своими значениями.
var layout = []file{
	{"agent.yaml", "agent.yaml", 0o644},
	{"agent.prod.yaml", "agent.prod.yaml", 0o644},
	{".env.example", "env.example", 0o644},
	{".env", "env.example", 0o600},
	{".env.prod.example", "env.prod.example", 0o644},
	{".gitignore", "gitignore", 0o644},
	{"README.md", "README.md", 0o644},
	{"workers/README.md", "workers/README.md", 0o644},
}

// Init — развернуть папку агента. Готовые файлы не трогает (кроме Force).
func Init(o Options) (Result, error) {
	var res Result
	if err := os.MkdirAll(o.Dir, 0o755); err != nil {
		return res, err
	}
	for _, f := range layout {
		dst := filepath.Join(o.Dir, f.name)
		if exists(dst) && !o.Force {
			res.Skipped = append(res.Skipped, f.name)
			continue
		}
		body, err := render(f.from, o)
		if err != nil {
			return res, err
		}
		if f.name == ".env.example" {
			// В образце — без токена: он попадает в git.
			if body, err = render(f.from, Options{Server: o.Server}); err != nil {
				return res, err
			}
		}
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return res, err
		}
		if err := fsutil.WriteFile(dst, body, f.perm); err != nil {
			return res, err
		}
		res.Created = append(res.Created, f.name)
	}
	if o.Binary != "" {
		dst := filepath.Join(o.Dir, "agent")
		switch {
		case fsutil.SameFile(o.Binary, dst):
		case exists(dst) && !o.Force:
			res.Skipped = append(res.Skipped, "agent")
		default:
			if _, err := fsutil.CopyFile(o.Binary, dst, 0o755); err != nil {
				return res, fmt.Errorf("программа агента: %w", err)
			}
			res.Created = append(res.Created, "agent")
		}
	}
	return res, nil
}

func render(name string, o Options) ([]byte, error) {
	raw, err := files.ReadFile("files/" + name)
	if err != nil {
		return nil, err
	}
	t, err := template.New(name).Option("missingkey=error").Parse(string(raw))
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	if err := t.Execute(&b, o); err != nil {
		return nil, err
	}
	return b.Bytes(), nil
}

func exists(path string) bool {
	_, err := os.Lstat(path)
	return !errors.Is(err, fs.ErrNotExist)
}
