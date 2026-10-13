# Агент: разработка, проверки, сборки, стенд. Go — в контейнере golang (scripts/go.sh), Go на
# машине не нужен; python3 — для стенда.
.DEFAULT_GOAL := help
.PHONY: help fmt tidy version test race vet fmt-check version-check examples-test e2e check \
	build release images demo-build demo-server demo-agent

help: ## список целей
	@awk 'BEGIN { FS = ":.*## " } /^[a-zA-Z0-9_-]+:.*## / { printf "  %-14s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# --- Разработка

fmt: ## gofmt: отформатировать Go-код
	scripts/go.sh fmt
tidy: ## go mod tidy
	scripts/go.sh tidy
version: ## записать версию: make version V=1.2.0
	scripts/version.sh set $(V)

# --- Проверки

test: ## Go-тесты
	scripts/go.sh test
race: ## Go-тесты с race-детектором
	scripts/go.sh race
vet: ## go vet (и сквозные тесты)
	scripts/go.sh vet
fmt-check: ## gofmt: проверка без изменений (замечания — ошибка)
	scripts/go.sh fmt-check
version-check: ## версия в VERSION верного вида
	scripts/version.sh check
examples-test: ## синтаксис примеров на Python
	scripts/go.sh python3 -c 'import ast, sys; [ast.parse(open(f).read(), f) for f in sys.argv[1:]]' examples/workers/echo/main.py examples/server/server.py
e2e: ## сквозные тесты: настоящий агент, воркеры-примеры, бэкенд в процессе теста, ~1 мин
	scripts/go.sh e2e
check: vet fmt-check version-check race examples-test e2e ## все проверки — перед коммитом

# --- Сборка

build: ## агент под эту машину → dist/<VERSION>/
	scripts/go.sh build
release: ## сборки агента linux/darwin × amd64/arm64, manifest.json, install.sh → dist/<VERSION>/ (подпись — AGENT_SIGNING_KEY, ключ проверки — AGENT_UPDATE_PUBLIC_KEY)
	scripts/go.sh release
images: ## образы: агент (agent:dev), агент с python3 для воркеров (agent:dev-python), каталог сборок (agent-dist:dev)
	docker build -f deploy/Dockerfile --target agent -t agent:dev .
	docker build -f deploy/Dockerfile --target agent-python -t agent:dev-python .
	docker build -f deploy/Dockerfile.dist -t agent-dist:dev .

# --- Стенд (examples/)

demo-build: ## стенд: агент и sysinfo → .dev/bin
	scripts/demo.sh build
demo-server: ## стенд: сервер на Python, API — http://localhost:8080/api
	scripts/demo.sh server
demo-agent: ## стенд: агент с воркерами echo (Python) и sysinfo (Go)
	scripts/demo.sh agent
