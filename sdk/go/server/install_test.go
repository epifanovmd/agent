package server

import (
	"errors"
	"testing"

	"github.com/epifanovmd/agent/sdk/go/message"
)

// Команда установки — та же строка, что в тестах Node и Python SDK.
func TestInstallCommand(t *testing.T) {
	a := newTestAgents(t, Options{})
	full := InstallOptions{
		BaseURL:           "https://api.example.com:8443/agents/",
		Token:             "tok'en",
		Name:              "node 01",
		User:              "root",
		Config:            "/etc/agent/node.yaml",
		Privileged:        true,
		Packages:          []string{"jq", "curl"},
		Sysctl:            map[string]string{"vm.max_map_count": "262144", "net.core.somaxconn": "1024"},
		RWPaths:           []string{"/etc/example", "/var/lib/it's"},
		CAFile:            "/etc/agent/ca.pem",
		StopTimeout:       "15min",
		Workers:           []string{"report"},
		KillMode:          "process",
		PackagesByManager: map[string][]string{"apk": {"bind-tools"}},
	}
	want := `curl -fsSL 'https://api.example.com:8443/agents/api/v1/agent-link/install.sh' | sudo sh -s -- --token 'tok'\''en' --name 'node 01' --user 'root' --config '/etc/agent/node.yaml' --privileged --kill-mode 'process' --packages 'jq curl' --packages-apk 'bind-tools' --sysctl 'net.core.somaxconn=1024' --sysctl 'vm.max_map_count=262144' --rw-path '/etc/example' --rw-path '/var/lib/it'\''s' --ca-file '/etc/agent/ca.pem' --worker 'report' --stop-timeout '15min'`
	if got, err := a.InstallCommand(full); err != nil || got != want {
		t.Fatalf("полная команда: %v\n got %s\nwant %s", err, got, want)
	}

	// Без BaseURL — PublicURL; без адреса вовсе — ошибка.
	if _, err := a.InstallCommand(InstallOptions{Token: "it"}); !isInvalid(err) {
		t.Fatalf("без адреса: %v", err)
	}
	a.SetPublicURL("http://127.0.0.1:8080")
	got, err := a.InstallCommand(InstallOptions{Token: "it"})
	if want := `curl -fsSL 'http://127.0.0.1:8080/api/v1/agent-link/install.sh' | sudo sh -s -- --token 'it'`; err != nil || got != want {
		t.Fatalf("минимальная команда: %v\n got %s\nwant %s", err, got, want)
	}
	// Токен из файла на узле — вместо --token.
	got, err = a.InstallCommand(InstallOptions{TokenFile: "/root/token"})
	if want := `curl -fsSL 'http://127.0.0.1:8080/api/v1/agent-link/install.sh' | sudo sh -s -- --token-file '/root/token'`; err != nil || got != want {
		t.Fatalf("токен из файла: %v\n got %s\nwant %s", err, got, want)
	}

	bad := map[string]InstallOptions{
		"без токена":         {BaseURL: "https://api.example.com"},
		"адрес с $":          {BaseURL: "https://api.example.com/$(id)", Token: "t"},
		"адрес с кавычкой":   {BaseURL: "https://api.example.com/a'b", Token: "t"},
		"адрес с пробелом":   {BaseURL: "https://api.example.com/a b", Token: "t"},
		"не http":            {BaseURL: "ftp://api.example.com", Token: "t"},
		"пакет":              {Token: "t", Packages: []string{"a;b"}},
		"ключ sysctl":        {Token: "t", Sysctl: map[string]string{"net ipv4": "1"}},
		"перевод строки":     {Token: "t", Name: "a\nb"},
		"пустое имя пакета":  {Token: "t", Packages: []string{""}},
		"имя воркера":        {Token: "t", Workers: []string{"report;id"}},
		"пустое имя воркера": {Token: "t", Workers: []string{""}},
		"token и tokenFile":  {Token: "t", TokenFile: "/f"},
		"killMode":           {Token: "t", KillMode: "control-group"},
		"менеджер пакетов":   {Token: "t", PackagesByManager: map[string][]string{"pacman": {"jq"}}},
		"пакет менеджера":    {Token: "t", PackagesByManager: map[string][]string{"apt": {"a b"}}},
	}
	for name, opts := range bad {
		if _, err := a.InstallCommand(opts); !isInvalid(err) {
			t.Errorf("%s: %v", name, err)
		}
	}
}

func isInvalid(err error) bool {
	var me *message.Error
	return errors.As(err, &me) && me.Code == "MESSAGE_INVALID"
}
