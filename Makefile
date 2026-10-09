# Агент и серверный SDK: разработка, проверки, сборки, стенд. Go — в контейнере golang
# (scripts/go.sh), Go на машине не нужен; Node ≥ 24 — на машине.
#
# make test / race идут в контейнере golang с node: интеграционные тесты агента
# (test/integration) запускают сервер на agent-sdk/server — sdk/node собирается перед ними.
.DEFAULT_GOAL := help
.PHONY: help fmt format tidy version node-sdk test race vet fmt-check format-check version-check sdk-test examples-test e2e check \
	build release images demo-build demo-server demo-agent

help: ## список целей
	@awk 'BEGIN { FS = ":.*## " } /^[a-zA-Z0-9_-]+:.*## / { printf "  %-14s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# --- Разработка

fmt: ## gofmt: отформатировать Go-код
	scripts/go.sh fmt
format: ## Prettier: отформатировать TS/JS, JSON, YAML, Markdown во всём проекте
	npm ci --no-audit --no-fund --silent && npx prettier --write .
tidy: ## go mod tidy
	scripts/go.sh tidy
version: ## записать версию везде: make version V=1.2.0
	scripts/version.sh set $(V)

# --- Проверки

node-sdk: ## собрать sdk/node (сервер интеграционных тестов и примеры берут agent-sdk из dist)
	cd sdk/node && npm ci --no-audit --no-fund --silent && npm run build
test: node-sdk ## Go-тесты: агент, образцы сообщений, integration (сервер на agent-sdk)
	scripts/go.sh test
race: node-sdk ## Go-тесты с race-детектором
	scripts/go.sh race
vet: ## go vet
	scripts/go.sh vet
fmt-check: ## gofmt: проверка без изменений (замечания — ошибка)
	scripts/go.sh fmt-check
format-check: ## Prettier: проверка без изменений
	npm ci --no-audit --no-fund --silent && npx prettier --check .
version-check: ## версия в VERSION, SDK и примерах одинаковая
	scripts/version.sh check
sdk-test: ## серверный SDK на Node: lint, typecheck, тесты
	cd sdk/node && npm ci --no-audit --no-fund --silent && npm run lint && npm run typecheck && npm test
examples-test: node-sdk ## стенд: ESLint и проверка типов сервера, синтаксис воркеров на Node и Python
	cd examples/server && npm ci --no-audit --no-fund --silent && npm run lint && npm run typecheck
	node --check examples/workers/node-echo/main.mjs
	python3 -m py_compile examples/workers/echo/main.py
e2e: node-sdk ## сквозные тесты: сервер стенда, настоящий агент и воркеры-примеры (сборки → .dev/e2e), ~1 мин
	scripts/e2e.sh
check: vet fmt-check format-check version-check race sdk-test examples-test e2e ## все проверки — перед коммитом

# --- Сборка

build: ## агент под эту машину → dist/<VERSION>/
	scripts/go.sh build
release: ## выпуск: сборки linux/darwin × amd64/arm64, manifest.json, install.sh → dist/<VERSION>/ (подпись — AGENT_SIGNING_KEY, ключ проверки — AGENT_UPDATE_PUBLIC_KEY)
	scripts/go.sh release
images: ## образы: агент (agent:dev), агент с python3 для воркеров (agent:dev-python), каталог выпуска (agent-dist:dev)
	docker build -f deploy/Dockerfile --target agent -t agent:dev .
	docker build -f deploy/Dockerfile --target agent-python -t agent:dev-python .
	docker build -f deploy/Dockerfile.dist -t agent-dist:dev .

# --- Стенд (examples/)

demo-build: ## стенд: агент, sysinfo и netprobe → .dev/bin, выпуск → dist/<VERSION>, agent-sdk, сервер
	scripts/demo.sh build
demo-server: ## стенд: сервер на Node.js, API — http://localhost:8080/api
	scripts/demo.sh server
demo-agent: ## стенд: агент с воркерами echo (Python), node-echo (Node.js), sysinfo и netprobe (Go)
	scripts/demo.sh agent
