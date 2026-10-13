//go:build unix

package app

import (
	"context"
	"net"
	"os"
	"strconv"
	"time"
)

// watchdog — сторож systemd (sd_notify): READY=1 при запуске, WATCHDOG=1 вдвое чаще
// WatchdogSec службы; зависший агент systemd перезапускает. Без NOTIFY_SOCKET — ничего.
func watchdog(ctx context.Context) {
	sock := os.Getenv("NOTIFY_SOCKET")
	if sock == "" {
		return
	}
	notify := func(state string) {
		conn, err := net.Dial("unixgram", sock)
		if err != nil {
			return
		}
		defer conn.Close()
		_, _ = conn.Write([]byte(state))
	}
	notify("READY=1")
	defer notify("STOPPING=1")
	usec, err := strconv.ParseInt(os.Getenv("WATCHDOG_USEC"), 10, 64)
	if err != nil || usec <= 0 {
		<-ctx.Done()
		return
	}
	t := time.NewTicker(time.Duration(usec) * time.Microsecond / 2)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			notify("WATCHDOG=1")
		}
	}
}
