module github.com/epifanovmd/agent

// go — младшая версия Go, с которой собирается модуль; toolchain — версия, которой собираются
// агент и выпуски (scripts/go.sh, CI, образы).
go 1.26.0

// npm-зависимости SDK и примеров (в некоторых пакетах есть Go-код) — не часть модуля.
ignore node_modules

require gopkg.in/yaml.v3 v3.0.1

require github.com/coder/websocket v1.8.15

require github.com/shirou/gopsutil/v4 v4.25.10

require golang.org/x/sys v0.48.0

require (
	github.com/ebitengine/purego v0.9.0 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/power-devops/perfstat v0.0.0-20240221224432-82ca36839d55 // indirect
	github.com/tklauser/go-sysconf v0.3.15 // indirect
	github.com/tklauser/numcpus v0.10.0 // indirect
	github.com/yusufpapurcu/wmi v1.2.4 // indirect
)
