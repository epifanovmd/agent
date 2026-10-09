//go:build unix

package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/epifanovmd/agent/internal/message"
)

func TestParsePing(t *testing.T) {
	cases := []struct {
		name, out     string
		sent, recv    int
		min, avg, max float64
	}{
		{"iputils", `PING example.com (192.0.2.10) 56(84) bytes of data.
64 bytes from 192.0.2.10: icmp_seq=1 ttl=56 time=11.6 ms
64 bytes from 192.0.2.10: icmp_seq=2 ttl=56 time=11.9 ms
64 bytes from 192.0.2.10: icmp_seq=3 ttl=56 time=12.1 ms

--- example.com ping statistics ---
3 packets transmitted, 3 received, 0% packet loss, time 2003ms
rtt min/avg/max/mdev = 11.600/11.866/12.100/0.205 ms
`, 3, 3, 11.6, 11.866, 12.1},
		{"macos", `PING example.com (192.0.2.10): 56 data bytes
64 bytes from 192.0.2.10: icmp_seq=0 ttl=56 time=11.612 ms
Request timeout for icmp_seq 1
64 bytes from 192.0.2.10: icmp_seq=2 ttl=56 time=12.001 ms

--- example.com ping statistics ---
3 packets transmitted, 2 packets received, 33.3% packet loss
round-trip min/avg/max/stddev = 11.612/11.806/12.001/0.194 ms
`, 3, 2, 11.612, 11.806, 12.001},
		{"busybox", `PING 192.0.2.10 (192.0.2.10): 56 data bytes
64 bytes from 192.0.2.10: seq=0 ttl=64 time=0.050 ms
64 bytes from 192.0.2.10: seq=1 ttl=64 time=0.070 ms

--- 192.0.2.10 ping statistics ---
2 packets transmitted, 2 packets received, 0% packet loss
round-trip min/avg/max = 0.050/0.060/0.070 ms
`, 2, 2, 0.05, 0.06, 0.07},
		{"без строки min/avg/max", `64 bytes from 192.0.2.10: icmp_seq=1 ttl=56 time=10 ms
64 bytes from 192.0.2.10: icmp_seq=2 ttl=56 time=20 ms
2 packets transmitted, 2 received, 0% packet loss
`, 2, 2, 10, 15, 20},
		{"всё потеряно", `PING 192.0.2.10 (192.0.2.10) 56(84) bytes of data.

--- 192.0.2.10 ping statistics ---
3 packets transmitted, 0 received, 100% packet loss, time 2050ms
`, 3, 0, 0, 0, 0},
		{"повторные ответы", `2 packets transmitted, 3 received, +1 duplicates, 0% packet loss
rtt min/avg/max/mdev = 1.0/2.0/3.0/0.5 ms
`, 2, 2, 1, 2, 3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			s, ok := parsePing(c.out)
			if !ok {
				t.Fatal("не разобрано")
			}
			if s.sent != c.sent || s.received != c.recv || !near(s.min, c.min) || !near(s.avg, c.avg) || !near(s.max, c.max) {
				t.Fatalf("%+v", s)
			}
		})
	}
	if _, ok := parsePing("ping: example.invalid: Name or service not known\n"); ok {
		t.Fatal("ошибка ping разобрана как итог")
	}
}

func near(a, b float64) bool { return a-b < 1e-9 && b-a < 1e-9 }

func TestResult(t *testing.T) {
	tg := Target{ID: "b", Host: "192.0.2.10", Method: MethodICMP}
	r := result(tg, ViaSocket, statsOf(3, []time.Duration{10 * time.Millisecond, 20*time.Millisecond + 2000}), nil)
	if r.Sent != 3 || r.Received != 2 || r.LossPct != 33.3 || *r.RttMinMs != 10 || *r.RttAvgMs != 15.001 || *r.RttMaxMs != 20.002 {
		t.Fatalf("%+v", r)
	}
	// Ответов нет — полей rtt*Ms нет; попыток нет — потери 100%.
	r = result(tg, ViaSocket, stats{}, errors.New("нет адреса"))
	raw, _ := json.Marshal(r)
	want := `{"id":"b","host":"192.0.2.10","method":"icmp","via":"socket","sent":0,"received":0,"lossPct":100,"error":"нет адреса"}`
	if string(raw) != want {
		t.Fatalf("%s", raw)
	}
	if r := result(tg, ViaTCP, statsOf(4, nil), nil); r.LossPct != 100 || r.RttAvgMs != nil {
		t.Fatalf("%+v", r)
	}
}

func TestSpec(t *testing.T) {
	s, err := parseSpec(json.RawMessage(`{"targets":[{"id":"b","host":"192.0.2.10"},{"id":"c","host":"example.com","port":443,"method":"tcp"}]}`))
	if err != nil {
		t.Fatal(err)
	}
	if s.IntervalSec != 30 || s.Count != 3 || s.TimeoutMs != 1000 || s.Targets[0].Method != MethodICMP {
		t.Fatalf("%+v", s)
	}
	for spec, want := range map[string]string{
		`{"targets":[{"id":"b","host":"x","method":"tcp"}]}`:              "нужен port",
		`{"targets":[{"id":"b","host":"x"},{"id":"b","host":"y"}]}`:       "повторяется",
		`{"targets":[{"id":"b","host":"x","method":"udp"}]}`:              "icmp или tcp",
		`{"targets":[{"id":"b","host":"-x"}]}`:                            "«-»",
		`{"targets":[],"count":1000}`:                                     "count",
		`{"targets":[],"timeoutMs":10}`:                                   "timeoutMs",
		`{"targets":[{"id":"","host":"x"}]}`:                              "id",
		`{"targets":[{"id":"b","host":"x","port":70000,"method":"tcp"}]}`: "port",
	} {
		if _, err := parseSpec(json.RawMessage(spec)); err == nil || !strings.Contains(err.Error(), want) {
			t.Errorf("%s: %v", spec, err)
		}
	}
}

// TCP-проба к локальному слушателю: все соединения устанавливаются; закрытый порт — потери.
func TestTCPProbe(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	go func() {
		for {
			c, err := ln.Accept()
			if err != nil {
				return
			}
			_ = c.Close()
		}
	}()
	port := ln.Addr().(*net.TCPAddr).Port
	p := newProber(newMethods())
	p.gap = time.Millisecond
	r := p.probe(context.Background(), Target{ID: "local", Host: "127.0.0.1", Port: port, Method: MethodTCP}, 3, time.Second)
	if r.Via != ViaTCP || r.Sent != 3 || r.Received != 3 || r.LossPct != 0 || r.RttAvgMs == nil || r.Error != "" {
		t.Fatalf("%+v", r)
	}
	_ = ln.Close()
	r = p.probe(context.Background(), Target{ID: "local", Host: "127.0.0.1", Port: port, Method: MethodTCP}, 2, time.Second)
	if r.Sent != 2 || r.Received != 0 || r.LossPct != 100 || r.RttAvgMs != nil || r.Error == "" {
		t.Fatalf("%+v", r)
	}
}

// ICMP: нет сокета — команда ping, нет и её — TCP до порта, без порта — ошибка; узнав, что
// пути нет, проба больше его не пробует.
func TestICMPFallback(t *testing.T) {
	var echoCalls, pingCalls, dialCalls int
	m := methods{
		lookup: func(context.Context, string) (net.IP, error) { return net.IPv4(192, 0, 2, 10), nil },
		echo: func(context.Context, net.IP, int, time.Duration, time.Duration) (stats, error) {
			echoCalls++
			return stats{}, errNoSocket
		},
		ping: func(_ context.Context, _ string, count int, _ time.Duration) (stats, error) {
			pingCalls++
			return stats{sent: count, received: count, min: 1, avg: 2, max: 3}, nil
		},
		dial: func(context.Context, string, time.Duration) error { dialCalls++; return nil },
	}
	p := newProber(m)
	p.gap = 0
	tg := Target{ID: "b", Host: "192.0.2.10", Port: 22, Method: MethodICMP}
	r := p.probe(context.Background(), tg, 2, time.Second)
	if r.Via != ViaPing || r.Received != 2 || *r.RttAvgMs != 2 {
		t.Fatalf("%+v", r)
	}
	p.probe(context.Background(), tg, 2, time.Second)
	if echoCalls != 1 || pingCalls != 2 {
		t.Fatalf("echo %d, ping %d", echoCalls, pingCalls)
	}

	p.m.ping = func(context.Context, string, int, time.Duration) (stats, error) { return stats{}, errNoPing }
	r = p.probe(context.Background(), tg, 2, time.Second)
	if r.Via != ViaTCP || r.Received != 2 || dialCalls != 2 {
		t.Fatalf("%+v, dial %d", r, dialCalls)
	}
	tg.Port = 0
	r = p.probe(context.Background(), tg, 2, time.Second)
	if r.Via != "" || r.LossPct != 100 || !strings.Contains(r.Error, "port") {
		t.Fatalf("%+v", r)
	}

	// ping не смог (нет прав) — TCP, если есть порт.
	p = newProber(m)
	p.noSocket.Store(true)
	p.m.ping = func(context.Context, string, int, time.Duration) (stats, error) {
		return stats{}, errors.New("ping: socket: Operation not permitted")
	}
	r = p.probe(context.Background(), Target{ID: "b", Host: "192.0.2.10", Port: 22, Method: MethodICMP}, 1, time.Second)
	if r.Via != ViaTCP {
		t.Fatalf("%+v", r)
	}
}

// Круг: итоги в порядке целей, at — время узла.
func TestRound(t *testing.T) {
	m := methods{dial: func(_ context.Context, addr string, _ time.Duration) error {
		if strings.HasSuffix(addr, ":2") {
			return errors.New("refused")
		}
		return nil
	}}
	p := newProber(m)
	p.gap = 0
	var targets []Target
	for i := range 40 {
		targets = append(targets, Target{ID: "t" + strconv.Itoa(i), Host: "192.0.2.10", Port: 1 + i%2, Method: MethodTCP})
	}
	before := time.Now().UnixMilli()
	rep := p.round(context.Background(), targets, 1, time.Second)
	if rep.At < before || len(rep.Results) != 40 {
		t.Fatalf("%+v", rep)
	}
	for i, r := range rep.Results {
		if r.ID != targets[i].ID || (r.Received == 1) != (i%2 == 0) {
			t.Fatalf("%d: %+v", i, r)
		}
	}
}

// ICMP через «ping»-сокет к 127.0.0.1 — где сокет разрешён (macOS; Linux с ping_group_range).
func TestICMPSocketLoopback(t *testing.T) {
	s, err := icmpEcho(context.Background(), net.IPv4(127, 0, 0, 1).To4(), 2, time.Second, time.Millisecond)
	if errors.Is(err, errNoSocket) {
		t.Skip(err)
	}
	if err != nil || s.sent != 2 || s.received != 2 {
		t.Fatalf("%+v %v", s, err)
	}
}

func TestEchoPacket(t *testing.T) {
	req := echoRequest(0x1234, 7)
	if checksum(req) != 0 {
		t.Fatal("контрольная сумма")
	}
	reply := append([]byte(nil), req...)
	reply[0] = 0
	if id, seq, ok := echoReply(reply); !ok || id != 0x1234 || seq != 7 {
		t.Fatal(id, seq, ok)
	}
	// С заголовком IP (macOS).
	withIP := append(append([]byte{0x45}, make([]byte, 19)...), reply...)
	if id, seq, ok := echoReply(withIP); !ok || id != 0x1234 || seq != 7 {
		t.Fatal(id, seq, ok)
	}
	if _, _, ok := echoReply(req); ok {
		t.Fatal("запрос принят за ответ")
	}
}

// Настройки и задача netprobe.run: настройки задают цели и запускают круг; задача без targets —
// цели настроек (итог — и для GET /metrics), с targets — разовая проверка.
func TestNetprobe(t *testing.T) {
	p := newProber(methods{dial: func(context.Context, string, time.Duration) error { return nil }})
	p.gap = 0
	reports := make(chan Report, 4)
	n := &netprobe{prober: p, report: func(r Report) { reports <- r }, changed: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.loop(ctx)

	if err := n.apply(json.RawMessage(`{"targets":[{"id":"x","host":"x"}],"count":0,"intervalSec":-1}`)); err == nil {
		t.Fatal("неверные настройки приняты")
	}
	if err := n.apply(json.RawMessage(`{"targets":[{"id":"b","host":"192.0.2.10","port":22,"method":"tcp"}],"count":2,"intervalSec":3600}`)); err != nil {
		t.Fatal(err)
	}
	select {
	case r := <-reports:
		if len(r.Results) != 1 || r.Results[0].ID != "b" || r.Results[0].Sent != 2 {
			t.Fatalf("%+v", r)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("нет круга после настроек")
	}

	out, err := n.run(ctx, []byte(`{}`))
	if err != nil || len(out.Results) != 1 || len(reports) != 1 {
		t.Fatal(out, err, len(reports))
	}
	<-reports
	out, err = n.run(ctx, []byte(`{"targets":[{"id":"c","host":"192.0.2.11","port":80,"method":"tcp"}],"count":1}`))
	if err != nil || out.Results[0].ID != "c" || out.Results[0].Sent != 1 || len(reports) != 0 {
		t.Fatal(out, err, len(reports))
	}
	if _, err := n.run(ctx, []byte(`{"targets":[{"id":"c","host":"x","method":"tcp"}]}`)); err == nil {
		t.Fatal("неверные цели приняты")
	}
}

// HTTP воркера: PUT /config/targets, GET /health, GET /metrics, GET /manifest, POST /jobs,
// служебные ошибки.
func TestHandler(t *testing.T) {
	p := newProber(methods{dial: func(context.Context, string, time.Duration) error { return nil }})
	p.gap = 0
	n := &netprobe{prober: p, changed: make(chan struct{}, 1)}
	n.report = n.setLast
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go n.loop(ctx)
	srv := httptest.NewServer(n.handler())
	defer srv.Close()
	put := func(body string) int {
		req, _ := http.NewRequest(http.MethodPut, srv.URL+"/config/targets", strings.NewReader(body))
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := put(`{"version":1,"data":{"targets":[{"id":"x","host":"x"}],"intervalSec":-1}}`); code != http.StatusUnprocessableEntity {
		t.Fatalf("неверные настройки: %d", code)
	}
	if code := put(`{"version":2,"data":{"targets":[{"id":"b","host":"192.0.2.10","port":22,"method":"tcp"}],"count":1}}`); code != http.StatusNoContent {
		t.Fatalf("настройки: %d", code)
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		resp, err := http.Get(srv.URL + "/metrics")
		if err != nil {
			t.Fatal(err)
		}
		var rep Report
		_ = json.NewDecoder(resp.Body).Decode(&rep)
		resp.Body.Close()
		if len(rep.Results) == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("нет итога в GET /metrics")
		}
		time.Sleep(20 * time.Millisecond)
	}
	resp, err := http.Get(srv.URL + "/health")
	if err != nil {
		t.Fatal(err)
	}
	var h struct {
		OK   bool           `json:"ok"`
		Info map[string]int `json:"info"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&h)
	resp.Body.Close()
	if !h.OK || h.Info["targets"] != 1 {
		t.Fatalf("health: %+v", h)
	}
	resp, err = http.Get(srv.URL + "/manifest")
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	// Манифест проходит проверку агента.
	m, err := message.ParseWorkerManifest(raw)
	if err != nil || m.Version != version || m.Configs[0].Key != configKey || len(m.Configs[0].Schema) == 0 ||
		len(m.Jobs) != 1 || m.Jobs[0].Type != jobRun || len(m.Jobs[0].Schema) == 0 {
		t.Fatalf("манифест: %+v %v", m, err)
	}

	// Задача netprobe.run — итог сразу; другой тип — 400.
	job := func(body string) (int, string) {
		resp, err := http.Post(srv.URL+"/jobs", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		raw, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(raw)
	}
	if code, body := job(`{"type":"netprobe.run","jobId":"j1","data":{"count":1}}`); code != http.StatusOK ||
		!strings.Contains(body, `"result":{"at":`) {
		t.Fatalf("netprobe.run: %d %s", code, body)
	}
	if code, _ := job(`{"type":"netprobe.other","jobId":"j2"}`); code != http.StatusBadRequest {
		t.Fatalf("незнакомая задача: %d", code)
	}
}
