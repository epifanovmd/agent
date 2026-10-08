# Агент примера с воркерами: Python (echo, batch, kv), Go (sysinfo), Node.js (node).
# Сборка из корня репозитория: docker compose -f examples/docker-compose.yml build
ARG GO_VERSION=1.26
ARG NODE_VERSION=24
ARG PYTHON_VERSION=3.12
FROM golang:${GO_VERSION}-bookworm AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X main.version=$(cat VERSION)" -o /out/agent ./cmd/agent && \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w" -o /out/sysinfo ./examples/workers/sysinfo

# agent-sdk (sdk/node) и Node-воркер: пути — как в репозитории (file:-зависимость — симлинк).
FROM node:${NODE_VERSION}-bookworm-slim AS node
WORKDIR /app
COPY sdk/node/package.json sdk/node/package-lock.json sdk/node/
RUN cd sdk/node && npm ci --no-audit --no-fund
COPY sdk/node/tsconfig.json sdk/node/tsup.config.ts sdk/node/
COPY sdk/node/src sdk/node/src
RUN cd sdk/node && npm run build && npm prune --omit=dev
COPY examples/workers/node/package.json examples/workers/node/package-lock.json examples/workers/node/
RUN cd examples/workers/node && npm ci --omit=dev --no-audit --no-fund

FROM python:${PYTHON_VERSION}-slim
ENV PYTHONPATH=/app/sdk/python \
    PYTHONUNBUFFERED=1 PYTHONDONTWRITEBYTECODE=1 \
    DEMO_ROOT=/app SYSINFO_BIN=/usr/local/bin/sysinfo \
    AGENT_SERVER_URL=http://server:8080 AGENT_ENROLL_TOKEN=demo-token
# Node для воркера на Node.js: бинарник из официального образа (нужен libstdc++).
RUN apt-get update && apt-get install -y --no-install-recommends libstdc++6 && rm -rf /var/lib/apt/lists/*
COPY --from=node /usr/local/bin/node /usr/local/bin/node
WORKDIR /app
COPY sdk/python sdk/python
COPY --from=node /app/sdk/node sdk/node
COPY examples/workers examples/workers
COPY --from=node /app/examples/workers/node/node_modules examples/workers/node/node_modules
COPY examples/agent.demo.yaml examples/agent.demo.yaml
COPY --from=build /out/agent /out/sysinfo /usr/local/bin/
RUN useradd --system --uid 10001 --home-dir /app agent && mkdir -p /app/.dev && chown agent /app/.dev && node --version
USER agent
# Имя агента — hostname контейнера: docker compose up --scale agent=3 даёт трёх разных агентов.
ENTRYPOINT ["/bin/sh", "-c", "export DEMO_AGENT=${DEMO_AGENT:-$HOSTNAME}; exec agent run -config /app/examples/agent.demo.yaml"]
