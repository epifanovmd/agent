//go:build unix

package main

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// Каким путём проверена цель (поле via итога).
const (
	ViaSocket = "socket" // ICMP через «ping»-сокет
	ViaPing   = "ping"   // ICMP через системную команду ping
	ViaTCP    = "tcp"    // время соединения TCP до порта
)

const (
	// parallel — сколько целей проверяется одновременно.
	parallel = 16
	// attemptGap — пауза между попытками к одной цели (ICMP через сокет и TCP).
	attemptGap = 200 * time.Millisecond
)

// Ошибки «путь недоступен на этом узле» — пробуется следующий.
var (
	errNoSocket = errors.New("ping-сокет недоступен")
	errNoPing   = errors.New("команды ping нет")
)

// Report — данные канала netprobe и итог команды netprobe.run.
type Report struct {
	At      int64    `json:"at"` // когда закончен круг, мс (часы узла)
	Results []Result `json:"results"`
}

// Result — итог проверки одной цели за круг. Ответов нет — полей rtt*Ms нет.
type Result struct {
	ID       string   `json:"id"`
	Host     string   `json:"host"`
	Method   string   `json:"method"`
	Via      string   `json:"via,omitempty"`
	Sent     int      `json:"sent"`
	Received int      `json:"received"`
	LossPct  float64  `json:"lossPct"`
	RttMinMs *float64 `json:"rttMinMs,omitempty"`
	RttAvgMs *float64 `json:"rttAvgMs,omitempty"`
	RttMaxMs *float64 `json:"rttMaxMs,omitempty"`
	Error    string   `json:"error,omitempty"`
}

// stats — попытки и время ответа одной проверки, мс.
type stats struct {
	sent, received int
	min, avg, max  float64
}

// statsOf — попытки и время ответов.
func statsOf(sent int, rtts []time.Duration) stats {
	s := stats{sent: sent, received: len(rtts)}
	var sum float64
	for i, d := range rtts {
		ms := float64(d) / float64(time.Millisecond)
		if i == 0 || ms < s.min {
			s.min = ms
		}
		if ms > s.max {
			s.max = ms
		}
		sum += ms
	}
	if len(rtts) > 0 {
		s.avg = sum / float64(len(rtts))
	}
	return s
}

// result — итог цели: потери в процентах (одна цифра после запятой), время — до микросекунд.
// Попыток не было — потери 100%.
func result(t Target, via string, s stats, err error) Result {
	r := Result{ID: t.ID, Host: t.Host, Method: t.Method, Via: via, Sent: s.sent, Received: s.received, LossPct: 100}
	if s.sent > 0 {
		r.LossPct = round(100*float64(s.sent-s.received)/float64(s.sent), 1)
	}
	if s.received > 0 {
		r.RttMinMs, r.RttAvgMs, r.RttMaxMs = ptr(round(s.min, 3)), ptr(round(s.avg, 3)), ptr(round(s.max, 3))
	}
	if err != nil {
		r.Error = err.Error()
	}
	return r
}

func round(v float64, digits int) float64 {
	p := math.Pow(10, float64(digits))
	return math.Round(v*p) / p
}

func ptr(v float64) *float64 { return &v }

// methods — пути проверки; в тестах подменяются.
type methods struct {
	lookup func(ctx context.Context, host string) (net.IP, error)
	// echo — ICMP через сокет; errNoSocket — сокета на узле нет.
	echo func(ctx context.Context, ip net.IP, count int, timeout, gap time.Duration) (stats, error)
	// ping — системная команда; errNoPing — команды нет.
	ping func(ctx context.Context, host string, count int, timeout time.Duration) (stats, error)
	// dial — одно соединение TCP.
	dial func(ctx context.Context, addr string, timeout time.Duration) error
}

func newMethods() methods {
	return methods{lookup: lookupIPv4, echo: icmpEcho, ping: pingCommand, dial: dialTCP}
}

// prober — проверка целей; помнит, каких путей на узле нет.
type prober struct {
	m        methods
	gap      time.Duration
	noSocket atomic.Bool
	noPing   atomic.Bool
}

func newProber(m methods) *prober { return &prober{m: m, gap: attemptGap} }

// round — круг проверок: цели параллельно (не больше parallel), итоги — в порядке целей.
func (p *prober) round(ctx context.Context, targets []Target, count int, timeout time.Duration) Report {
	results := make([]Result, len(targets))
	sem := make(chan struct{}, parallel)
	var wg sync.WaitGroup
	for i, t := range targets {
		wg.Add(1)
		sem <- struct{}{}
		go func() {
			defer func() { <-sem; wg.Done() }()
			results[i] = p.probe(ctx, t, count, timeout)
		}()
	}
	wg.Wait()
	return Report{At: time.Now().UnixMilli(), Results: results}
}

// probe — одна цель: tcp — соединения до порта; icmp — сокет, иначе команда ping, иначе TCP
// (если у цели есть port).
func (p *prober) probe(ctx context.Context, t Target, count int, timeout time.Duration) Result {
	if t.Method == MethodTCP {
		return p.tcp(ctx, t, count, timeout)
	}
	if !p.noSocket.Load() {
		ip, err := p.m.lookup(ctx, t.Host)
		if err != nil {
			return result(t, ViaSocket, stats{}, err)
		}
		s, err := p.m.echo(ctx, ip, count, timeout, p.gap)
		if !errors.Is(err, errNoSocket) {
			return result(t, ViaSocket, s, err)
		}
		p.noSocket.Store(true)
	}
	var pingErr error
	if !p.noPing.Load() {
		s, err := p.m.ping(ctx, t.Host, count, timeout)
		switch {
		case errors.Is(err, errNoPing):
			p.noPing.Store(true)
		case err == nil || s.sent > 0 || t.Port == 0:
			return result(t, ViaPing, s, err)
		default:
			pingErr = err // ping не смог (например, нет прав) — пробуем TCP
		}
	}
	if t.Port > 0 {
		return p.tcp(ctx, t, count, timeout)
	}
	if pingErr == nil {
		pingErr = errors.New("ICMP недоступен (нет ping-сокета и команды ping), а port для TCP не задан")
	}
	return result(t, "", stats{}, pingErr)
}

// tcp — count соединений до host:port; время — до установки соединения.
func (p *prober) tcp(ctx context.Context, t Target, count int, timeout time.Duration) Result {
	addr := net.JoinHostPort(t.Host, strconv.Itoa(t.Port))
	var rtts []time.Duration
	var last error
	sent := 0
	for i := 0; i < count && ctx.Err() == nil; i++ {
		if i > 0 && !sleep(ctx, p.gap) {
			break
		}
		start := time.Now()
		sent++
		if err := p.m.dial(ctx, addr, timeout); err != nil {
			last = err
			continue
		}
		rtts = append(rtts, time.Since(start))
	}
	return result(t, ViaTCP, statsOf(sent, rtts), last)
}

func dialTCP(ctx context.Context, addr string, timeout time.Duration) error {
	d := net.Dialer{Timeout: timeout}
	c, err := d.DialContext(ctx, "tcp", addr)
	if err != nil {
		return err
	}
	return c.Close()
}

// lookupIPv4 — первый адрес IPv4 узла.
func lookupIPv4(ctx context.Context, host string) (net.IP, error) {
	if ip := net.ParseIP(host); ip != nil {
		if v4 := ip.To4(); v4 != nil {
			return v4, nil
		}
		return nil, fmt.Errorf("%s: нужен адрес IPv4", host)
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip4", host)
	if err != nil {
		return nil, err
	}
	if len(ips) == 0 {
		return nil, fmt.Errorf("%s: нет адреса IPv4", host)
	}
	return ips[0].To4(), nil
}

// sleep — пауза; false — ctx отменён.
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}
