package app

import (
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
)

// proxyFromEnv — прокси для запросов агента к серверу по переменным
// окружения HTTPS_PROXY / HTTP_PROXY / NO_PROXY (и их строчным вариантам),
// прочитанным при запуске агента: https/wss — через HTTPS_PROXY, http/ws —
// через HTTP_PROXY. NO_PROXY — через запятую: «*», домен (с точкой в начале
// или без — и его поддомены), host:port, IP, подсеть CIDR. localhost и
// петлевые адреса — всегда напрямую (как в стандартной библиотеке Go).
// nil — прокси не задан.
func proxyFromEnv() func(*http.Request) (*url.URL, error) {
	httpsProxy := proxyURL(env("HTTPS_PROXY", "https_proxy"))
	httpProxy := proxyURL(env("HTTP_PROXY", "http_proxy"))
	if httpsProxy == nil && httpProxy == nil {
		return nil
	}
	noProxy := strings.Split(strings.ToLower(env("NO_PROXY", "no_proxy")), ",")
	return func(req *http.Request) (*url.URL, error) {
		var p *url.URL
		switch req.URL.Scheme {
		case "https", "wss":
			p = httpsProxy
		case "http", "ws":
			p = httpProxy
		}
		if p == nil || bypassProxy(req.URL, noProxy) {
			return nil, nil
		}
		return p, nil
	}
}

func env(names ...string) string {
	for _, n := range names {
		if v := strings.TrimSpace(os.Getenv(n)); v != "" {
			return v
		}
	}
	return ""
}

// proxyURL — адрес прокси; без схемы — http://.
func proxyURL(raw string) *url.URL {
	if raw == "" {
		return nil
	}
	if !strings.Contains(raw, "://") {
		raw = "http://" + raw
	}
	u, err := url.Parse(raw)
	if err != nil || u.Host == "" {
		return nil
	}
	return u
}

// bypassProxy — адрес u идёт напрямую (localhost или правило NO_PROXY).
func bypassProxy(u *url.URL, noProxy []string) bool {
	host := strings.ToLower(u.Hostname())
	port := u.Port()
	if port == "" {
		port = map[string]string{"http": "80", "ws": "80", "https": "443", "wss": "443"}[u.Scheme]
	}
	ip := net.ParseIP(host)
	if host == "localhost" || strings.HasSuffix(host, ".localhost") || (ip != nil && ip.IsLoopback()) {
		return true
	}
	for _, rule := range noProxy {
		rule = strings.TrimSpace(rule)
		switch {
		case rule == "":
			continue
		case rule == "*":
			return true
		}
		if _, cidr, err := net.ParseCIDR(rule); err == nil {
			if ip != nil && cidr.Contains(ip) {
				return true
			}
			continue
		}
		if ruleIP := net.ParseIP(strings.Trim(rule, "[]")); ruleIP != nil {
			if ip != nil && ruleIP.Equal(ip) {
				return true
			}
			continue
		}
		if h, p, err := net.SplitHostPort(rule); err == nil {
			if p != port {
				continue
			}
			rule = h
		}
		rule = strings.TrimPrefix(strings.TrimPrefix(rule, "*"), ".")
		if host == rule || strings.HasSuffix(host, "."+rule) {
			return true
		}
	}
	return false
}
