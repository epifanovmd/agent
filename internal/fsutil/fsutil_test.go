package fsutil

import (
	"os"
	"path/filepath"
	"testing"
)

// Запись заменяет файл целиком, права — ровно perm, временного файла не остаётся.
func TestWriteFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "a.json")
	if err := os.WriteFile(path, []byte("old"), 0o644); err != nil {
		t.Fatal(err)
	}
	for _, write := range []func(string, []byte, os.FileMode) error{WriteFile, WriteFileSync} {
		if err := write(path, []byte("new"), 0o600); err != nil {
			t.Fatal(err)
		}
		raw, _ := os.ReadFile(path)
		st, _ := os.Stat(path)
		if string(raw) != "new" || st.Mode().Perm() != 0o600 || IsFile(path+TmpSuffix) {
			t.Fatalf("%q %v", raw, st.Mode())
		}
	}
	if err := WriteFile(filepath.Join(dir, "нет", "a"), nil, 0o600); err == nil {
		t.Fatal("нет каталога — ошибка")
	}
}

// Копия: perm 0 — права источника; итог — число байт.
func TestCopyFile(t *testing.T) {
	dir := t.TempDir()
	src, dst := filepath.Join(dir, "src"), filepath.Join(dir, "dst")
	if err := os.WriteFile(src, []byte("hello"), 0o750); err != nil {
		t.Fatal(err)
	}
	n, err := CopyFile(src, dst, 0)
	st, _ := os.Stat(dst)
	if err != nil || n != 5 || st.Mode().Perm() != 0o750 || SameFile(src, dst) {
		t.Fatalf("%d %v %v", n, err, st.Mode())
	}
	if _, err := CopyFile(filepath.Join(dir, "нет"), dst, 0); err == nil {
		t.Fatal("нет источника — ошибка")
	}
}
