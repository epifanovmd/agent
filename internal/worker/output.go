//go:build unix

package worker

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/epifanovmd/agent/internal/config"
	"github.com/epifanovmd/agent/internal/logx"
)

// tailEvery — как часто агент читает файлы вывода воркера; переменная — для тестов.
var tailEvery = 200 * time.Millisecond

// tailChunk — сколько байт файла вывода читается за раз.
const tailChunk = 256 << 10

// LogFiles — файлы вывода воркера name в каталоге dir: stdout и stderr.
func LogFiles(dir, name string) [2]string {
	return [2]string{filepath.Join(dir, name+".log"), filepath.Join(dir, name+".err.log")}
}

// output — вывод воркера в файлах (§12): процесс пишет в них сам (а не в
// каналы агента — он переживает перезапуск агента), агент читает новые
// строки в свой лог (stdout — info, stderr — warn), большой файл копирует в
// .1 (прежние копии сдвигаются) и очищает.
type output struct {
	files [2]*tailFile
	// start — места, с которых чтение началось.
	start [2]int64
	// limits — текущие logs воркера (меняются перечитыванием настроек).
	limits func() config.Logs

	mu      sync.Mutex
	stopped bool
	stop    chan struct{}
	done    chan struct{}
}

type tailFile struct {
	path   string
	line   *lineLog
	offset int64
}

// openOutput — файлы вывода воркера spec в dir: для процесса (дописывание)
// и чтение агентом с offsets; nil offsets — с текущего конца (старое уже
// было в логе прежнего запуска).
func openOutput(dir string, spec config.Worker, offsets *[2]int64, log *slog.Logger, limits func() config.Logs) (*output, [2]*os.File, error) {
	var child [2]*os.File
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, child, fmt.Errorf("каталог вывода воркеров: %w", err)
	}
	out := log.With(logx.OutputKey, spec.Name)
	o := &output{limits: limits, stop: make(chan struct{}), done: make(chan struct{})}
	levels := [2]slog.Level{slog.LevelInfo, slog.LevelWarn}
	for i, path := range LogFiles(dir, spec.Name) {
		f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
		if err != nil {
			for _, c := range child[:i] {
				c.Close()
			}
			return nil, child, fmt.Errorf("файл вывода воркера: %w", err)
		}
		child[i] = f
		tf := &tailFile{path: path, line: &lineLog{log: out, level: levels[i]}}
		if st, err := f.Stat(); err == nil {
			tf.offset = st.Size()
			if offsets != nil && offsets[i] <= st.Size() {
				tf.offset = offsets[i]
			}
		}
		o.files[i] = tf
		o.start[i] = tf.offset
	}
	go o.run()
	return o, child, nil
}

func (o *output) run() {
	defer close(o.done)
	tick := time.NewTicker(tailEvery)
	defer tick.Stop()
	for {
		select {
		case <-o.stop:
			o.read()
			return
		case <-tick.C:
			o.read()
		}
	}
}

// read — новые строки всех файлов; большой файл — в копию и очистка.
func (o *output) read() {
	limits := o.limits()
	for _, f := range o.files {
		f.read()
		if st, err := os.Stat(f.path); err == nil && st.Size() > int64(limits.MaxSize) {
			f.rotate(limits.Files())
		}
	}
}

func (f *tailFile) read() {
	if size := f.readFrom(f.path, -1); size >= 0 && size < f.offset {
		f.offset = 0 // файл очищен
		f.readFrom(f.path, -1)
	}
}

// readFrom — строки файла path с f.offset до конца (end < 0) или до end;
// итог — размер файла (-1 — не прочитан).
func (f *tailFile) readFrom(path string, end int64) int64 {
	file, err := os.Open(path)
	if err != nil {
		return -1
	}
	defer file.Close()
	st, err := file.Stat()
	if err != nil {
		return -1
	}
	if end < 0 || end > st.Size() {
		end = st.Size()
	}
	buf := make([]byte, tailChunk)
	for f.offset < end {
		n, err := file.ReadAt(buf[:min(int64(len(buf)), end-f.offset)], f.offset)
		if n > 0 {
			_, _ = f.line.Write(buf[:n])
			f.offset += int64(n)
		}
		if err != nil && err != io.EOF || n == 0 {
			break
		}
	}
	return st.Size()
}

// rotate — копии сдвигаются (.1 → .2 …, последняя удаляется), файл
// копируется в .1 и очищается: процесс дописывает в тот же файл с начала.
func (f *tailFile) rotate(keep int) {
	if keep > 0 {
		_ = os.Remove(fmt.Sprintf("%s.%d", f.path, keep))
		for i := keep - 1; i >= 1; i-- {
			_ = os.Rename(fmt.Sprintf("%s.%d", f.path, i), fmt.Sprintf("%s.%d", f.path, i+1))
		}
		size, err := copyFile(f.path, f.path+".1")
		_ = os.Truncate(f.path, 0)
		if err == nil {
			// Дописанное между чтением и копированием — из копии.
			f.readFrom(f.path+".1", size)
		}
	} else {
		_ = os.Truncate(f.path, 0)
	}
	f.offset = 0
}

// copyFile — копия src в dst; итог — сколько байт скопировано.
func copyFile(src, dst string) (int64, error) {
	in, err := os.Open(src)
	if err != nil {
		return 0, err
	}
	defer in.Close()
	out, err := os.OpenFile(dst+".tmp", os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(out, in)
	if err != nil {
		out.Close()
		return 0, err
	}
	if err := out.Close(); err != nil {
		return 0, err
	}
	return n, os.Rename(dst+".tmp", dst)
}

// close — дочитать файлы и перестать читать; итог — места, с которых
// продолжит следующий читатель (подхвативший воркер агент).
func (o *output) close(flush bool) [2]int64 {
	o.mu.Lock()
	if !o.stopped {
		o.stopped = true
		close(o.stop)
	}
	o.mu.Unlock()
	<-o.done
	var offsets [2]int64
	for i, f := range o.files {
		offsets[i] = f.offset
		if flush {
			f.line.Flush()
		} else {
			// Недописанную строку дочитает следующий читатель.
			offsets[i] -= int64(f.line.pending())
		}
	}
	return offsets
}
