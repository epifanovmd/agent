package update

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/ed25519"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type entry struct {
	name, body, link string
	typ              byte
	mode             int64
}

func tarGz(t *testing.T, entries ...entry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, e := range entries {
		h := &tar.Header{Name: e.name, Typeflag: e.typ, Mode: e.mode, Linkname: e.link}
		if h.Typeflag == 0 {
			h.Typeflag = tar.TypeReg
		}
		if h.Mode == 0 {
			h.Mode = 0o644
		}
		if h.Typeflag == tar.TypeReg {
			h.Size = int64(len(e.body))
		}
		if err := tw.WriteHeader(h); err != nil {
			t.Fatal(err)
		}
		if h.Typeflag == tar.TypeReg {
			_, _ = tw.Write([]byte(e.body))
		}
	}
	_ = tw.Close()
	_ = gz.Close()
	return buf.Bytes()
}

func extractBytes(t *testing.T, raw []byte) (string, error) {
	t.Helper()
	dir := t.TempDir()
	archive := filepath.Join(dir, "build.tar.gz")
	if err := os.WriteFile(archive, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	dst := filepath.Join(dir, "current")
	return dst, Extract(archive, dst)
}

// Обычная сборка: каталоги, файлы с правами, ссылка внутри сборки.
func TestExtract(t *testing.T) {
	dst, err := extractBytes(t, tarGz(t,
		entry{name: "./", typ: tar.TypeDir, mode: 0o755},
		entry{name: "./bin/", typ: tar.TypeDir, mode: 0o755},
		entry{name: "./bin/run", body: "#!/bin/sh\n", mode: 0o4755},
		entry{name: "lib/a.py", body: "print(1)\n"},
		entry{name: "run", typ: tar.TypeSymlink, link: "bin/run"},
		entry{name: "lib/b.py", typ: tar.TypeLink, link: "lib/a.py"},
	))
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(filepath.Join(dst, "bin", "run"))
	if err != nil || info.Mode().Perm() != 0o755 || info.Mode()&os.ModeSetuid != 0 {
		t.Fatalf("права: %v %v", info.Mode(), err)
	}
	if raw, _ := os.ReadFile(filepath.Join(dst, "run")); string(raw) != "#!/bin/sh\n" {
		t.Fatalf("ссылка: %q", raw)
	}
	if raw, _ := os.ReadFile(filepath.Join(dst, "lib", "b.py")); string(raw) != "print(1)\n" {
		t.Fatalf("жёсткая ссылка: %q", raw)
	}
}

// Пути наружу, ссылки наружу, запись через ссылку, особые файлы, пределы —
// ошибка, каталог сборки не остаётся.
func TestExtractRejects(t *testing.T) {
	cases := map[string][]entry{
		"..":                  {{name: "../evil", body: "x"}},
		"абсолютный путь":     {{name: "/etc/evil", body: "x"}},
		"ссылка наружу":       {{name: "out", typ: tar.TypeSymlink, link: "../../etc"}},
		"абсолютная ссылка":   {{name: "out", typ: tar.TypeSymlink, link: "/etc"}},
		"запись через ссылку": {{name: "d", typ: tar.TypeSymlink, link: "."}, {name: "d/x", body: "x"}},
		"жёсткая наружу":      {{name: "h", typ: tar.TypeLink, link: "../x"}},
		"устройство":          {{name: "dev", typ: tar.TypeChar}},
	}
	for name, entries := range cases {
		t.Run(name, func(t *testing.T) {
			dst, err := extractBytes(t, tarGz(t, entries...))
			if err == nil {
				t.Fatal("ожидалась ошибка")
			}
			if _, serr := os.Stat(dst); !os.IsNotExist(serr) {
				t.Fatalf("каталог сборки остался: %v", err)
			}
		})
	}
	old := MaxArchiveBytes
	MaxArchiveBytes = 10
	defer func() { MaxArchiveBytes = old }()
	if _, err := extractBytes(t, tarGz(t, entry{name: "big", body: strings.Repeat("x", 11)})); err == nil {
		t.Fatal("предел размера")
	}
}

// FetchArchive — проверка подписи, загрузка и распаковка; архив не остаётся.
func TestFetchArchive(t *testing.T) {
	pub, priv, _ := ed25519.GenerateKey(nil)
	body := tarGz(t, entry{name: "main.py", body: "print('report')\n", mode: 0o755})
	dir := t.TempDir()
	tmp := filepath.Join(dir, "a.tar.gz")
	_ = os.WriteFile(tmp, body, 0o600)
	hash, _ := FileHash(tmp)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write(body) }))
	defer srv.Close()
	b := Local("report", "1.0.0", hash)
	dst := filepath.Join(dir, "current.new")
	if err := FetchArchive(context.Background(), srv.Client(), "", pub, b, srv.URL, Sign(priv, b), dst); err != nil {
		t.Fatal(err)
	}
	if raw, _ := os.ReadFile(filepath.Join(dst, "main.py")); string(raw) != "print('report')\n" {
		t.Fatalf("распаковка: %q", raw)
	}
	if _, err := os.Stat(dst + ".tar.gz"); !os.IsNotExist(err) {
		t.Fatal("архив остался")
	}
}
