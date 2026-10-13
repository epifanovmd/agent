package app

import (
	"net/http"
	"testing"
)

func TestProxyFromEnv(t *testing.T) {
	for _, n := range []string{"HTTPS_PROXY", "https_proxy", "HTTP_PROXY", "http_proxy", "NO_PROXY", "no_proxy"} {
		t.Setenv(n, "")
	}
	if proxyFromEnv() != nil {
		t.Fatal("без переменных — без прокси")
	}
	t.Setenv("HTTPS_PROXY", "proxy.example.com:3128")
	t.Setenv("http_proxy", "http://plain.example.com:8080")
	t.Setenv("NO_PROXY", " .internal.example.com, direct.example.com:8443 ,10.0.0.0/8, 192.168.1.5")
	proxy := proxyFromEnv()
	for target, want := range map[string]string{
		"https://api.example.com/x":           "http://proxy.example.com:3128",
		"wss://api.example.com/x":             "http://proxy.example.com:3128",
		"http://api.example.com/x":            "http://plain.example.com:8080",
		"ws://api.example.com/x":              "http://plain.example.com:8080",
		"https://a.internal.example.com/":     "",
		"https://internal.example.com/":       "",
		"https://direct.example.com:8443/":    "",
		"https://direct.example.com/":         "http://proxy.example.com:3128",
		"https://10.1.2.3/":                   "",
		"https://192.168.1.5/":                "",
		"https://192.168.1.6/":                "http://proxy.example.com:3128",
		"https://localhost:8443/":             "",
		"https://127.0.0.1:8443/":             "",
		"https://notinternal.example.com.ru/": "http://proxy.example.com:3128",
	} {
		req, _ := http.NewRequest(http.MethodGet, target, nil)
		got, err := proxy(req)
		if err != nil {
			t.Fatal(err)
		}
		s := ""
		if got != nil {
			s = got.String()
		}
		if s != want {
			t.Errorf("%s: прокси %q, ждали %q", target, s, want)
		}
	}
	t.Setenv("NO_PROXY", "*")
	req, _ := http.NewRequest(http.MethodGet, "https://api.example.com", nil)
	if got, _ := proxyFromEnv()(req); got != nil {
		t.Fatalf("NO_PROXY=*: %v", got)
	}
}
