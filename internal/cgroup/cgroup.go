// Package cgroup — ограничения ресурсов воркеров через cgroup v2 (Linux).
//
// Агент работает в своей группе, отданной ему systemd (Delegate=yes). Чтобы
// включить контроллеры для подгрупп, в самой группе процессов быть не должно
// (правило cgroup v2), поэтому агент переносит себя (и всё, что там уже
// работает) в подгруппу agent, а каждому воркеру с ограничениями заводит
// подгруппу worker-<имя> с memory.max, cpu.max и pids.max.
package cgroup

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// Root — точка монтирования cgroup v2.
const Root = "/sys/fs/cgroup"

// Limits — ограничения группы; 0 — без ограничения ("max").
type Limits struct {
	// MemoryMax — байты (memory.max).
	MemoryMax int64
	// CPUQuota, CPUPeriod — мкс процессорного времени за период (cpu.max).
	CPUQuota, CPUPeriod int64
	// PidsMax — процессов и потоков (pids.max).
	PidsMax int64
}

// Manager — группа агента с подгруппами воркеров.
type Manager struct {
	dir string
	log *slog.Logger

	mu          sync.Mutex
	controllers map[string]bool
}

// ErrUnavailable — cgroup v2 нет (или не Linux).
var ErrUnavailable = errors.New("cgroup v2 недоступна")

// Detect — группа этого процесса в cgroup v2 (root — точка монтирования,
// procSelf — содержимое /proc/self/cgroup), подготовленная для подгрупп
// воркеров: процессы группы перенесены в подгруппу agent, контроллеры memory,
// cpu, pids включены для подгрупп. Ошибка — ограничения не применить.
func Detect(root string, procSelf []byte, log *slog.Logger) (*Manager, error) {
	dir, err := groupDir(root, procSelf)
	if err != nil {
		return nil, err
	}
	m := &Manager{dir: dir, log: log}
	if err := m.prepare(); err != nil {
		return nil, err
	}
	return m, nil
}

// groupDir — каталог группы агента: из записи 0:: в /proc/self/cgroup; если
// агент уже в своей подгруппе agent (перезапуск без смены группы) — её родитель.
func groupDir(root string, procSelf []byte) (string, error) {
	if _, err := os.Stat(filepath.Join(root, "cgroup.controllers")); err != nil {
		return "", fmt.Errorf("%w: нет %s/cgroup.controllers", ErrUnavailable, root)
	}
	rel := ""
	sc := bufio.NewScanner(bytes.NewReader(procSelf))
	for sc.Scan() {
		if p, ok := strings.CutPrefix(sc.Text(), "0::"); ok {
			rel = p
		}
	}
	if rel == "" {
		return "", fmt.Errorf("%w: в /proc/self/cgroup нет записи 0::", ErrUnavailable)
	}
	dir := filepath.Join(root, filepath.Clean("/"+rel))
	if filepath.Base(dir) == "agent" && dir != filepath.Join(root, "agent") {
		dir = filepath.Dir(dir)
	}
	return dir, nil
}

// prepare — перенести процессы группы в подгруппу agent и включить контроллеры.
func (m *Manager) prepare() error {
	self := filepath.Join(m.dir, "agent")
	if err := os.MkdirAll(self, 0o755); err != nil {
		return fmt.Errorf("подгруппа агента (нужен Delegate=yes в службе): %w", err)
	}
	pids, err := readPids(filepath.Join(m.dir, "cgroup.procs"))
	if err != nil {
		return err
	}
	pids = append(pids, os.Getpid())
	moved := map[int]bool{}
	for _, pid := range pids {
		if moved[pid] {
			continue
		}
		moved[pid] = true
		if err := writeFile(filepath.Join(self, "cgroup.procs"), strconv.Itoa(pid)); err != nil && pid == os.Getpid() {
			return fmt.Errorf("перенос агента в %s: %w", self, err)
		}
	}
	raw, err := os.ReadFile(filepath.Join(m.dir, "cgroup.controllers"))
	if err != nil {
		return fmt.Errorf("контроллеры группы: %w", err)
	}
	m.controllers = map[string]bool{}
	for _, c := range strings.Fields(string(raw)) {
		if c != "memory" && c != "cpu" && c != "pids" {
			continue
		}
		if err := writeFile(filepath.Join(m.dir, "cgroup.subtree_control"), "+"+c); err != nil {
			m.log.Warn("контроллер cgroup не включён", "controller", c, "err", err)
			continue
		}
		m.controllers[c] = true
	}
	return nil
}

// Dir — каталог группы агента.
func (m *Manager) Dir() string { return m.dir }

// Group — подгруппа воркера.
type Group struct{ dir string }

// Worker — подгруппа worker-<name> с ограничениями l (существующая
// обновляется: снятое ограничение — "max"). Контроллер недоступен —
// предупреждение, остальные ограничения применяются.
func (m *Manager) Worker(name string, l Limits) (*Group, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	dir := filepath.Join(m.dir, "worker-"+safeName(name))
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, fmt.Errorf("подгруппа воркера: %w", err)
	}
	set := func(controller, file, value string, limited bool) error {
		if !m.controllers[controller] {
			if limited {
				m.log.Warn("ограничение не применено: контроллер cgroup недоступен", "worker", name, "controller", controller)
			}
			return nil
		}
		return writeFile(filepath.Join(dir, file), value)
	}
	cpu := "max " + strconv.FormatInt(max(l.CPUPeriod, 100_000), 10)
	if l.CPUQuota > 0 {
		cpu = strconv.FormatInt(l.CPUQuota, 10) + " " + strconv.FormatInt(max(l.CPUPeriod, 1000), 10)
	}
	err := errors.Join(
		set("memory", "memory.max", limit(l.MemoryMax), l.MemoryMax > 0),
		set("cpu", "cpu.max", cpu, l.CPUQuota > 0),
		set("pids", "pids.max", limit(l.PidsMax), l.PidsMax > 0),
	)
	if err != nil {
		return nil, err
	}
	return &Group{dir: dir}, nil
}

// Add — перенести процесс pid в группу.
func (g *Group) Add(pid int) error {
	return writeFile(filepath.Join(g.dir, "cgroup.procs"), strconv.Itoa(pid))
}

// Dir — каталог группы.
func (g *Group) Dir() string { return g.dir }

func limit(v int64) string {
	if v <= 0 {
		return "max"
	}
	return strconv.FormatInt(v, 10)
}

// safeName — имя воркера, пригодное для каталога.
func safeName(name string) string {
	return strings.Map(func(r rune) rune {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			return r
		}
		return '_'
	}, name)
}

func readPids(path string) ([]int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("процессы группы: %w", err)
	}
	var pids []int
	for _, f := range strings.Fields(string(raw)) {
		if pid, err := strconv.Atoi(f); err == nil && pid > 0 {
			pids = append(pids, pid)
		}
	}
	return pids, nil
}

// writeFile — запись значения в файл интерфейса cgroup (как `echo v > file`).
// В подставном корне тестов файл создаётся; списки (cgroup.procs,
// cgroup.subtree_control) дописываются — видно всё, что записано.
func writeFile(path, value string) error {
	flags := os.O_WRONLY | os.O_CREATE | os.O_TRUNC
	if base := filepath.Base(path); base == "cgroup.procs" || base == "cgroup.subtree_control" {
		flags = os.O_WRONLY | os.O_CREATE | os.O_APPEND
	}
	f, err := os.OpenFile(path, flags, 0o644)
	if err != nil {
		return err
	}
	_, err = f.WriteString(value + "\n")
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	return err
}
