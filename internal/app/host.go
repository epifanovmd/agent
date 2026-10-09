//go:build unix

package app

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"path/filepath"
	goruntime "runtime"

	"golang.org/x/sys/unix"

	"github.com/epifanovmd/agent/internal/message"
	"github.com/epifanovmd/agent/internal/worker"
)

// HostInfo — платформа узла для hello и регистрации.
func HostInfo() message.Host {
	h := message.Host{OS: goruntime.GOOS, Arch: goruntime.GOARCH}
	h.Hostname, _ = os.Hostname()
	var u unix.Utsname
	if unix.Uname(&u) == nil {
		h.Kernel = unix.ByteSliceToString(u.Release[:])
	}
	return h
}

// maxSocketPath — длина пути unix-сокета с запасом (предел ОС — 104–108 байт).
const maxSocketPath = 100

// RunDir — каталог сокетов агента и воркеров: <dataDir>/run; слишком
// длинный путь для сокета — каталог во временном каталоге системы.
func RunDir(dataDir string) string {
	dir := filepath.Join(dataDir, "run")
	if len(dir)+1+32+1+len(worker.SocketName) <= maxSocketPath {
		return dir
	}
	sum := sha256.Sum256([]byte(dataDir))
	return filepath.Join(os.TempDir(), "agent-"+hex.EncodeToString(sum[:6]))
}

// AgentSocketName — сокет агента для воркеров в RunDir.
const AgentSocketName = "agent.sock"
