//go:build unix

package install

import (
	"bytes"
	"context"
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
)

// Host — настоящая система: программы, файлы, пользователи этой машины.
func Host(out, errOut io.Writer) *System {
	return &System{
		Run: func(ctx context.Context, c Cmd) error {
			cmd := exec.CommandContext(ctx, c.Args[0], c.Args[1:]...)
			cmd.Env = append(os.Environ(), c.Env...)
			cmd.Stdout, cmd.Stderr = out, errOut
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
