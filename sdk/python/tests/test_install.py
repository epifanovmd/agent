"""``Agents.install_command`` — та же строка, что в тестах Go и Node SDK."""

from __future__ import annotations

import unittest

from agent_sdk.server import Agents, AgentsError


class InstallCommandTest(unittest.TestCase):
    def test_full(self) -> None:
        agents = Agents(enroll_token="t")
        got = agents.install_command(
            base_url="https://api.example.com:8443/agents/", token="tok'en", name="node 01", user="root",
            config="/etc/agent/node.yaml", privileged=True, packages=["jq", "curl"],
            sysctl={"vm.max_map_count": "262144", "net.core.somaxconn": "1024"},
            rw_paths=["/etc/example", "/var/lib/it's"], ca_file="/etc/agent/ca.pem", stop_timeout="15min",
            workers=["report"], kill_mode="process", packages_by_manager={"apk": ["bind-tools"]})
        self.assertEqual(
            got,
            "curl -fsSL 'https://api.example.com:8443/agents/api/v1/agent-link/install.sh' | sudo sh -s -- "
            "--token 'tok'\\''en' --name 'node 01' --user 'root' --config '/etc/agent/node.yaml' --privileged --kill-mode 'process' "
            "--packages 'jq curl' --packages-apk 'bind-tools' "
            "--sysctl 'net.core.somaxconn=1024' --sysctl 'vm.max_map_count=262144' "
            "--rw-path '/etc/example' --rw-path '/var/lib/it'\\''s' --ca-file '/etc/agent/ca.pem' --worker 'report' --stop-timeout '15min'")

    def test_base_url_option(self) -> None:
        agents = Agents(enroll_token="t", base_url="http://127.0.0.1:8080")
        self.assertEqual(agents.install_command(token="it"),
                         "curl -fsSL 'http://127.0.0.1:8080/api/v1/agent-link/install.sh' | sudo sh -s -- --token 'it'")

    def test_token_file_and_managers_order(self) -> None:
        agents = Agents(enroll_token="t", base_url="https://api.example.com")
        got = agents.install_command(token_file="/root/agent token", kill_mode="mixed",
                                     packages_by_manager={"zypper": ["bind-utils"], "apt": ["dnsutils"],
                                                          "dnf": []})
        self.assertEqual(
            got,
            "curl -fsSL 'https://api.example.com/api/v1/agent-link/install.sh' | sudo sh -s -- "
            "--token-file '/root/agent token' --kill-mode 'mixed' --packages-apt 'dnsutils' "
            "--packages-zypper 'bind-utils'")

    def test_invalid(self) -> None:
        agents = Agents(enroll_token="t")
        base = "https://api.example.com"
        bad = {
            "без адреса": {"token": "it"},
            "без токена": {"base_url": base, "token": ""},
            "адрес с $": {"base_url": base + "/$(id)", "token": "t"},
            "адрес с кавычкой": {"base_url": base + "/a'b", "token": "t"},
            "адрес с пробелом": {"base_url": base + "/a b", "token": "t"},
            "не http": {"base_url": "ftp://api.example.com", "token": "t"},
            "пакет": {"base_url": base, "token": "t", "packages": ["a;b"]},
            "пустое имя пакета": {"base_url": base, "token": "t", "packages": [""]},
            "ключ sysctl": {"base_url": base, "token": "t", "sysctl": {"net ipv4": "1"}},
            "перевод строки": {"base_url": base, "token": "t", "name": "a\nb"},
            "имя воркера": {"base_url": base, "token": "t", "workers": ["a b"]},
            "пустое имя воркера": {"base_url": base, "token": "t", "workers": [""]},
            "token и token_file": {"base_url": base, "token": "t", "token_file": "/root/t"},
            "kill_mode": {"base_url": base, "token": "t", "kill_mode": "control-group"},
            "менеджер": {"base_url": base, "token": "t", "packages_by_manager": {"pacman": ["jq"]}},
            "пакет менеджера": {"base_url": base, "token": "t", "packages_by_manager": {"apk": ["a b"]}},
        }
        for name, opts in bad.items():
            with self.subTest(name), self.assertRaises(AgentsError) as ctx:
                agents.install_command(**opts)
            self.assertEqual(ctx.exception.code, "MESSAGE_INVALID", name)


if __name__ == "__main__":
    unittest.main()
