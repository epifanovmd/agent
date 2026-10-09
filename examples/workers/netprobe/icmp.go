//go:build unix

package main

import (
	"context"
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"runtime"
	"syscall"
	"time"
)

// icmpSocket — «ping»-сокет (SOCK_DGRAM, IPPROTO_ICMP): эхо-запросы без прав root. На Linux
// он есть, если группа процесса входит в net.ipv4.ping_group_range; на macOS — всегда.
// Нет сокета — errNoSocket.
func icmpSocket() (net.PacketConn, error) {
	syscall.ForkLock.RLock()
	fd, err := syscall.Socket(syscall.AF_INET, syscall.SOCK_DGRAM, syscall.IPPROTO_ICMP)
	if err == nil {
		syscall.CloseOnExec(fd)
	}
	syscall.ForkLock.RUnlock()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errNoSocket, err)
	}
	if err := syscall.Bind(fd, &syscall.SockaddrInet4{}); err != nil {
		_ = syscall.Close(fd)
		return nil, fmt.Errorf("%w: %v", errNoSocket, err)
	}
	f := os.NewFile(uintptr(fd), "icmp")
	defer f.Close()
	c, err := net.FilePacketConn(f)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", errNoSocket, err)
	}
	return c, nil
}

// icmpEcho — count эхо-запросов к ip с паузой gap; ответ ждётся не дольше timeout.
func icmpEcho(ctx context.Context, ip net.IP, count int, timeout, gap time.Duration) (stats, error) {
	c, err := icmpSocket()
	if err != nil {
		return stats{}, err
	}
	defer c.Close()
	// Linux сам ставит идентификатор сокета и отдаёт ему только его ответы; на других
	// системах ответ узнаётся по идентификатору.
	id := uint16(rand.Uint32())
	checkID := runtime.GOOS != "linux"
	dst := &net.UDPAddr{IP: ip}
	buf := make([]byte, 1500)
	var rtts []time.Duration
	var last error
	sent := 0
	for seq := 1; seq <= count && ctx.Err() == nil; seq++ {
		if seq > 1 && !sleep(ctx, gap) {
			break
		}
		start := time.Now()
		sent++
		if _, err := c.WriteTo(echoRequest(id, uint16(seq)), dst); err != nil {
			last = err
			continue
		}
		deadline := start.Add(timeout)
		if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
			deadline = d
		}
		_ = c.SetReadDeadline(deadline)
		for {
			n, _, err := c.ReadFrom(buf)
			if err != nil {
				break // срок ответа истёк — попытка потеряна
			}
			rid, rseq, ok := echoReply(buf[:n])
			if ok && rseq == uint16(seq) && (!checkID || rid == id) {
				rtts = append(rtts, time.Since(start))
				break
			}
		}
	}
	return statsOf(sent, rtts), last
}

// echoRequest — ICMP Echo Request (тип 8) с контрольной суммой.
func echoRequest(id, seq uint16) []byte {
	b := make([]byte, 8+16)
	b[0] = 8
	binary.BigEndian.PutUint16(b[4:], id)
	binary.BigEndian.PutUint16(b[6:], seq)
	copy(b[8:], "netprobe........")
	binary.BigEndian.PutUint16(b[2:], checksum(b))
	return b
}

// echoReply — идентификатор и номер ICMP Echo Reply (тип 0). macOS отдаёт пакет с заголовком
// IP — он пропускается.
func echoReply(b []byte) (id, seq uint16, ok bool) {
	if len(b) >= 20 && b[0]>>4 == 4 {
		hl := int(b[0]&0x0f) * 4
		if len(b) < hl {
			return 0, 0, false
		}
		b = b[hl:]
	}
	if len(b) < 8 || b[0] != 0 || b[1] != 0 {
		return 0, 0, false
	}
	return binary.BigEndian.Uint16(b[4:]), binary.BigEndian.Uint16(b[6:]), true
}

// checksum — контрольная сумма ICMP (RFC 1071).
func checksum(b []byte) uint16 {
	var sum uint32
	for i := 0; i+1 < len(b); i += 2 {
		sum += uint32(b[i])<<8 | uint32(b[i+1])
	}
	if len(b)%2 == 1 {
		sum += uint32(b[len(b)-1]) << 8
	}
	for sum>>16 != 0 {
		sum = sum&0xffff + sum>>16
	}
	return ^uint16(sum)
}
