//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"time"
)

// pingCommand — системная команда ping: count запросов, ответ ждётся не дольше timeout.
// Команды нет — errNoPing; вывод без итоговой строки — ошибка с его началом.
func pingCommand(ctx context.Context, host string, count int, timeout time.Duration) (stats, error) {
	path, err := exec.LookPath("ping")
	if err != nil {
		return stats{}, fmt.Errorf("%w: %v", errNoPing, err)
	}
	// -W: на macOS — миллисекунды, на Linux (iputils, busybox) — секунды.
	wait := strconv.Itoa(max(1, int((timeout+time.Second-1)/time.Second)))
	if runtime.GOOS == "darwin" {
		wait = strconv.Itoa(int(timeout / time.Millisecond))
	}
	// Запросы — раз в секунду: весь вызов — не дольше count × (1 с + timeout) + 2 с.
	ctx, cancel := context.WithTimeout(ctx, time.Duration(count)*(time.Second+timeout)+2*time.Second)
	defer cancel()
	out, runErr := exec.CommandContext(ctx, path, "-n", "-c", strconv.Itoa(count), "-W", wait, host).CombinedOutput()
	s, ok := parsePing(string(out))
	if !ok {
		msg := firstLine(string(out))
		if msg == "" && runErr != nil {
			msg = runErr.Error()
		}
		return stats{}, errors.New("ping: " + msg)
	}
	return s, nil
}

var (
	// «3 packets transmitted, 2 received» (iputils), «… 2 packets received» (macOS, busybox).
	pingSummary = regexp.MustCompile(`(\d+) packets transmitted, (\d+) (?:packets )?received`)
	// «rtt min/avg/max/mdev = 11.6/11.9/12.1/0.2 ms», «round-trip min/avg/max = …».
	pingRtt = regexp.MustCompile(`min/avg/max[^=]*= *([\d.]+)/([\d.]+)/([\d.]+)`)
	// «… icmp_seq=1 ttl=56 time=11.6 ms».
	pingTime = regexp.MustCompile(`time[=<]([\d.]+) ?ms`)
)

// parsePing — разбор вывода ping (iputils, macOS, busybox): запросы и ответы — из итоговой
// строки, время — из строки min/avg/max, без неё — по строкам ответов. Нет итоговой строки — false.
func parsePing(out string) (stats, bool) {
	m := pingSummary.FindStringSubmatch(out)
	if m == nil {
		return stats{}, false
	}
	s := stats{}
	s.sent, _ = strconv.Atoi(m[1])
	s.received, _ = strconv.Atoi(m[2])
	s.received = min(s.received, s.sent) // повторные ответы (duplicates) не считаются
	if s.received == 0 {
		return s, true
	}
	if r := pingRtt.FindStringSubmatch(out); r != nil {
		s.min, _ = strconv.ParseFloat(r[1], 64)
		s.avg, _ = strconv.ParseFloat(r[2], 64)
		s.max, _ = strconv.ParseFloat(r[3], 64)
		return s, true
	}
	var rtts []time.Duration
	for _, t := range pingTime.FindAllStringSubmatch(out, -1) {
		ms, err := strconv.ParseFloat(t[1], 64)
		if err == nil {
			rtts = append(rtts, time.Duration(ms*float64(time.Millisecond)))
		}
	}
	if len(rtts) == 0 {
		return s, true
	}
	t := statsOf(len(rtts), rtts)
	s.min, s.avg, s.max = t.min, t.avg, t.max
	return s, true
}

func firstLine(s string) string {
	s = strings.TrimSpace(s)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 200 {
		s = s[:200]
	}
	return s
}
