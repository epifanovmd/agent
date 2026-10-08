# Агент и SDK: разработка, проверки, сборки, стенд. Go — в контейнере golang (scripts/go.sh),
# Go на машине не нужен; Node ≥ 24 и python3 — на машине.
#
# make test / race идут в контейнере golang без Node: Node-часть общего теста серверов
# (test/conformance, сервер на agent-sdk) там пропускается — она выполняется в CI.
.DEFAULT_GOAL := help
.PHONY: help fmt format tidy version test race vet fmt-check format-check version-check sdk-test examples-test check \
	build release images demo-build demo-server demo-agent demo-check

help: ## список целей
	@awk 'BEGIN { FS = ":.*## " } /^[a-zA-Z_-]+:.*## / { printf "  %-14s %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# --- Разработка

fmt: ## gofmt: отформатировать Go-код
	scripts/go.sh fmt
format: ## Prettier: отформатировать TS/JS, JSON, YAML, Markdown, CSS во всём проекте
	npm ci --no-audit --no-fund --silent && npx prettier --write .
tidy: ## go mod tidy
	scripts/go.sh tidy
version: ## записать версию везде: make version V=1.2.0
	scripts/version.sh set $(V)

# --- Проверки

test: ## Go-тесты: агент, Go-SDK, эталоны сообщений, integration, conformance
	scripts/go.sh test
race: ## Go-тесты с race-детектором
	scripts/go.sh race
vet: ## go vet
	scripts/go.sh vet
fmt-check: ## gofmt: проверка без изменений (замечания — ошибка)
	scripts/go.sh fmt-check
format-check: ## Prettier: проверка без изменений
	npm ci --no-audit --no-fund --silent && npx prettier --check .
version-check: ## версия в VERSION, SDK, агенте и примерах одинаковая
	scripts/version.sh check
sdk-test: ## тесты SDK Python и Node: lint, typecheck, тесты (Go-SDK — в test / race)
	cd sdk/python && python3 -m unittest discover -s tests -t .
	cd sdk/node && npm ci --no-audit --no-fund --silent && npm run lint && npm run typecheck && npm test
examples-test: ## тесты бэкендов-примеров (Node, Python), typecheck Node-воркера, сборка интерфейса
	cd sdk/node && npm ci --no-audit --no-fund --silent && npm run build   # примеры берут agent-sdk из dist
	cd examples/server && npm ci --no-audit --no-fund --silent && npm run typecheck && npm test
	cd examples/workers/node && npm ci --no-audit --no-fund --silent && npm run typecheck
	python3 -m unittest discover -s examples/server-python/tests -t examples/server-python
	cd examples/web && npm ci --no-audit --no-fund --silent && npm run build
check: vet fmt-check format-check version-check race sdk-test examples-test ## все проверки — перед коммитом

# --- Сборка

build: ## агент под эту машину → dist/<VERSION>/
	scripts/go.sh build
release: ## выпуск: сборки linux/darwin × amd64/arm64, manifest.json (подпись — AGENT_SIGNING_KEY), install.sh → dist/<VERSION>/
	scripts/go.sh release
images: ## образы: агент с Python (agent:dev-python), каталог выпуска (agent-dist:dev)
	docker build -f deploy/Dockerfile -t agent:dev-python .
	docker build -f deploy/Dockerfile.dist -t agent-dist:dev .

# --- Стенд (examples/)

demo-build: ## стенд: агент и sysinfo → .dev/bin, выпуск → dist/<VERSION>, agent-sdk, сервер и интерфейс
	scripts/demo.sh build
demo-server: ## стенд: сервер на Node.js и интерфейс, http://localhost:8080
	scripts/demo.sh server
demo-agent: ## стенд: агент с воркерами echo, batch, kv (Python), sysinfo (Go), node (Node.js)
	scripts/demo.sh agent
demo-check: ## стенд: самопроверка воркеров из терминала
	scripts/demo.sh check
