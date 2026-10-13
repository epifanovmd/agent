package update

import (
	"archive/tar"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strings"
)

// Пределы распаковки архива сборки: всего байт и записей (переменные — для
// тестов).
var (
	MaxArchiveBytes   int64 = 2 << 30
	MaxArchiveEntries       = 100_000
)

// Extract — распаковать архив .tar.gz в новый каталог dst (его не должно
// быть). Пути с «..» и абсолютные, ссылки за пределы dst, запись через
// ссылку, особые файлы и превышение пределов — ошибка; при ошибке dst
// удаляется. Права файлов — без setuid/setgid.
func Extract(archive, dst string) (err error) {
	f, err := os.Open(archive)
	if err != nil {
		return fmt.Errorf("update: архив: %w", err)
	}
	defer f.Close()
	gz, err := gzip.NewReader(f)
	if err != nil {
		return fmt.Errorf("update: архив не gzip: %w", err)
	}
	defer gz.Close()
	if err := os.Mkdir(dst, 0o755); err != nil {
		return fmt.Errorf("update: каталог сборки: %w", err)
	}
	defer func() {
		if err != nil {
			_ = os.RemoveAll(dst)
		}
	}()
	tr := tar.NewReader(gz)
	var total int64
	for entries := 0; ; entries++ {
		h, err := tr.Next()
		if errors.Is(err, io.EOF) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("update: архив: %w", err)
		}
		if entries >= MaxArchiveEntries {
			return fmt.Errorf("update: в архиве больше %d записей", MaxArchiveEntries)
		}
		name, err := safeName(h.Name)
		if err != nil {
			return err
		}
		if name == "" {
			continue // сам корень «./»
		}
		target := filepath.Join(dst, filepath.FromSlash(name))
		if err := noLinkOnPath(dst, filepath.Dir(target)); err != nil {
			return err
		}
		mode := os.FileMode(h.Mode).Perm()
		switch h.Typeflag {
		case tar.TypeDir:
			if err := os.MkdirAll(target, mode|0o700); err != nil {
				return fmt.Errorf("update: распаковка: %w", err)
			}
		case tar.TypeReg:
			total += h.Size
			if h.Size < 0 || total > MaxArchiveBytes {
				return fmt.Errorf("update: архив больше %d байт в распакованном виде", MaxArchiveBytes)
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("update: распаковка: %w", err)
			}
			if err := writeEntry(target, mode, tr, h.Size); err != nil {
				return err
			}
		case tar.TypeSymlink:
			if err := linkInside(name, h.Linkname); err != nil {
				return err
			}
			if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
				return fmt.Errorf("update: распаковка: %w", err)
			}
			if err := os.Symlink(h.Linkname, target); err != nil {
				return fmt.Errorf("update: распаковка: %w", err)
			}
		case tar.TypeLink:
			src, err := safeName(h.Linkname)
			if err != nil || src == "" {
				return fmt.Errorf("update: жёсткая ссылка %q — за пределы сборки", h.Linkname)
			}
			from := filepath.Join(dst, filepath.FromSlash(src))
			if err := noLinkOnPath(dst, from); err != nil {
				return err
			}
			if err := os.Link(from, target); err != nil {
				return fmt.Errorf("update: распаковка: %w", err)
			}
		default:
			return fmt.Errorf("update: %q — особый файл (тип %c) в архиве не допускается", h.Name, h.Typeflag)
		}
	}
}

// safeName — путь записи архива без «./» в начале; абсолютный или с «..» —
// ошибка. "" — корень.
func safeName(name string) (string, error) {
	clean := path.Clean(strings.TrimPrefix(name, "./"))
	switch {
	case clean == ".":
		return "", nil
	case path.IsAbs(clean) || clean == ".." || strings.HasPrefix(clean, "../"):
		return "", fmt.Errorf("update: путь %q в архиве выходит за пределы сборки", name)
	}
	return clean, nil
}

// linkInside — символьная ссылка name → target остаётся внутри сборки.
func linkInside(name, target string) error {
	if target == "" || path.IsAbs(target) {
		return fmt.Errorf("update: ссылка %q → %q выходит за пределы сборки", name, target)
	}
	resolved := path.Clean(path.Join(path.Dir(name), target))
	if resolved == ".." || strings.HasPrefix(resolved, "../") {
		return fmt.Errorf("update: ссылка %q → %q выходит за пределы сборки", name, target)
	}
	return nil
}

// noLinkOnPath — на пути от root до p (включительно) нет символьных ссылок:
// запись через ссылку могла бы уйти за пределы сборки.
func noLinkOnPath(root, p string) error {
	rel, err := filepath.Rel(root, p)
	if err != nil || rel == "." {
		return err
	}
	cur := root
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		cur = filepath.Join(cur, part)
		info, err := os.Lstat(cur)
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		if err != nil {
			return fmt.Errorf("update: распаковка: %w", err)
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("update: запись через ссылку %s не допускается", cur)
		}
	}
	return nil
}

func writeEntry(target string, mode os.FileMode, r io.Reader, size int64) error {
	out, err := os.OpenFile(target, os.O_CREATE|os.O_EXCL|os.O_WRONLY, mode|0o600)
	if err != nil {
		return fmt.Errorf("update: распаковка: %w", err)
	}
	n, err := io.Copy(out, io.LimitReader(r, size))
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	if err == nil && n != size {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return fmt.Errorf("update: распаковка %s: %w", filepath.Base(target), err)
	}
	return nil
}
