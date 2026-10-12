//go:build unix

package install

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"syscall"

	"github.com/epifanovmd/agent/internal/app"
)

// Host — настоящая система: программы, файлы, пользователи этой машины.
func Host(out, errOut io.Writer) *System {
	var name, home string
	if u, err := user.Current(); err == nil {
		name, home = u.Username, u.HomeDir
	}
	if h, err := os.UserHomeDir(); err == nil && h != "" {
		home = h
	}
	return &System{
		User: name,
		Home: home,
		Run: func(ctx context.Context, c Cmd) error {
			cmd := exec.CommandContext(ctx, c.Args[0], c.Args[1:]...)
			cmd.Env = append(os.Environ(), c.Env...)
			cmd.Stdout, cmd.Stderr = out, errOut
			if c.Stdin != "" {
				cmd.Stdin = strings.NewReader(c.Stdin)
			}
			var buf bytes.Buffer
			if c.Quiet {
				cmd.Stdout, cmd.Stderr = io.Discard, &buf
			}
			if c.User != "" {
				cred, home, err := credential(c.User)
				if err != nil {
					return err
				}
				cmd.SysProcAttr = &syscall.SysProcAttr{Credential: cred}
				cmd.Env = append(cmd.Env, "HOME="+home, "USER="+c.User, "LOGNAME="+c.User)
			}
			err := cmd.Run()
			if err != nil && buf.Len() > 0 {
				return fmt.Errorf("%w: %s", err, strings.TrimSpace(buf.String()))
			}
			return err
		},
		Output: func(ctx context.Context, args ...string) (string, error) {
			out, err := exec.CommandContext(ctx, args[0], args[1:]...).Output()
			return string(out), err
		},
		Spawn: func(c Cmd, log string) error {
			if err := os.MkdirAll(filepath.Dir(log), 0o755); err != nil {
				return err
			}
			f, err := os.OpenFile(log, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
			if err != nil {
				return err
			}
			defer f.Close()
			cmd := exec.Command(c.Args[0], c.Args[1:]...)
			cmd.Env = append(os.Environ(), c.Env...)
			cmd.Stdout, cmd.Stderr = f, f
			cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
			if err := cmd.Start(); err != nil {
				return err
			}
			return cmd.Process.Release()
		},
		Running: func(dataDir string) int {
			st, err := app.Status(dataDir)
			if errors.Is(err, app.ErrNotRunning) || errors.Is(err, fs.ErrNotExist) {
				return 0
			}
			return st.PID
		},
		Kill:     syscall.Kill,
		LookPath: exec.LookPath,
		Euid:     os.Geteuid,
		Chown: func(path, name string) error {
			cred, _, err := credential(name)
			if err != nil {
				return err
			}
			return filepath.WalkDir(path, func(p string, _ fs.DirEntry, err error) error {
				if err != nil {
					return err
				}
				return os.Lchown(p, int(cred.Uid), int(cred.Gid))
			})
		},
		Arch:   runtime.GOARCH,
		Out:    out,
		ErrOut: errOut,
	}
}

// credential — uid, gid и группы пользователя name; домашний каталог.
func credential(name string) (*syscall.Credential, string, error) {
	u, err := user.Lookup(name)
	if err != nil {
		return nil, "", fmt.Errorf("пользователь %s: %w", name, err)
	}
	uid, _ := strconv.ParseUint(u.Uid, 10, 32)
	gid, _ := strconv.ParseUint(u.Gid, 10, 32)
	cred := &syscall.Credential{Uid: uint32(uid), Gid: uint32(gid)}
	if ids, err := u.GroupIds(); err == nil {
		for _, g := range ids {
			if n, err := strconv.ParseUint(g, 10, 32); err == nil {
				cred.Groups = append(cred.Groups, uint32(n))
			}
		}
	}
	return cred, u.HomeDir, nil
}
