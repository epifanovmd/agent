package releases

import (
	"archive/tar"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"
)

// skipped — служебные файлы, которые в архив не попадают.
var skipped = map[string]bool{"__pycache__": true, ".DS_Store": true, ".git": true}

// TarDir — каталог src в архив dst (.tar.gz): содержимое — под prefix ("" —
// в корне архива), права файлов сохраняются, ссылки — ссылками, время записей
// одно и то же (одинаковое содержимое — одинаковый архив). Вернёт sha256 (hex).
func TarDir(src, dst, prefix string) (string, error) {
	f, err := os.Create(dst)
	if err != nil {
		return "", err
	}
	h := sha256.New()
	gz := gzip.NewWriter(io.MultiWriter(f, h))
	tw := tar.NewWriter(gz)
	epoch := time.Unix(0, 0)
	err = filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if skipped[d.Name()] || strings.HasSuffix(d.Name(), ".pyc") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		rel, err := filepath.Rel(src, p)
		if err != nil || rel == "." {
			return err
		}
		name := path.Join(prefix, filepath.ToSlash(rel))
		info, err := d.Info()
		if err != nil {
			return err
		}
		link := ""
		if info.Mode()&fs.ModeSymlink != 0 {
			if link, err = os.Readlink(p); err != nil {
				return err
			}
		} else if !info.Mode().IsRegular() && !info.IsDir() {
			return nil
		}
		hdr, err := tar.FileInfoHeader(info, link)
		if err != nil {
			return err
		}
		hdr.Name, hdr.ModTime, hdr.Uid, hdr.Gid, hdr.Uname, hdr.Gname = name, epoch, 0, 0, "", ""
		hdr.AccessTime, hdr.ChangeTime, hdr.Format = time.Time{}, time.Time{}, tar.FormatPAX
		if info.IsDir() {
			hdr.Name += "/"
		}
		if err := tw.WriteHeader(hdr); err != nil {
			return err
		}
		if !info.Mode().IsRegular() {
			return nil
		}
		in, err := os.Open(p)
		if err != nil {
			return err
		}
		defer in.Close()
		_, err = io.Copy(tw, in)
		return err
	})
	for _, c := range []io.Closer{tw, gz, f} {
		if cerr := c.Close(); err == nil {
			err = cerr
		}
	}
	if err != nil {
		os.Remove(dst)
		return "", err
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}
