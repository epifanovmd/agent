// Package fsutil — запись и копирование файлов через временный файл рядом:
// читатель видит либо прежний файл, либо новый целиком.
package fsutil

import (
	"io"
	"os"
	"path/filepath"
)

// TmpSuffix — суффикс временного файла рядом с записываемым.
const TmpSuffix = ".tmp"

// WriteFile — записать data в path с правами perm (без учёта umask) через
// временный файл и переименование. Каталог path должен существовать.
func WriteFile(path string, data []byte, perm os.FileMode) error {
	return write(path, perm, false, func(f *os.File) error {
		_, err := f.Write(data)
		return err
	})
}

// WriteFileSync — как WriteFile, но с fsync файла и каталога: запись
// переживает сбой питания.
func WriteFileSync(path string, data []byte, perm os.FileMode) error {
	if err := write(path, perm, true, func(f *os.File) error {
		_, err := f.Write(data)
		return err
	}); err != nil {
		return err
	}
	SyncDir(filepath.Dir(path))
	return nil
}

// CopyFile — копия src в dst через временный файл; perm 0 — права src.
// Итог — сколько байт скопировано.
func CopyFile(src, dst string, perm os.FileMode) (int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	if perm == 0 {
		st, err := in.Stat()
		if err != nil {
			return 0, err
		}
		perm = st.Mode().Perm()
	}
	var n int64
	err = write(dst, perm, false, func(f *os.File) error {
		n, err = io.Copy(f, in)
		return err
	})
	return n, err
}

// SyncDir — fsync каталога: переименование в нём переживает сбой питания.
func SyncDir(dir string) {
	if d, err := os.Open(dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
}

// IsFile — путь есть и это не каталог.
func IsFile(path string) bool {
	st, err := os.Stat(path)
	return err == nil && !st.IsDir()
}

// SameFile — a и b — один и тот же файл (в том числе через ссылки).
func SameFile(a, b string) bool {
	sa, err1 := os.Stat(a)
	sb, err2 := os.Stat(b)
	return err1 == nil && err2 == nil && os.SameFile(sa, sb)
}

func write(path string, perm os.FileMode, sync bool, fill func(*os.File) error) error {
	tmp := path + TmpSuffix
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, perm)
	if err != nil {
		return err
	}
	err = fill(f)
	if err == nil {
		err = f.Chmod(perm)
	}
	if err == nil && sync {
		err = f.Sync()
	}
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = os.Rename(tmp, path)
	}
	if err != nil {
		_ = os.Remove(tmp)
	}
	return err
}
