# syntax=docker/dockerfile:1.7
# Образы агента. Агент — процесс 1 контейнера: связь, outbox, воркеры.
#
#   agent   (--target agent)  — минимальный: агент на Alpine. Воркеры — свои программы проекта
#                               (Go и другие сборки без зависимостей) или воркеры из выпуска;
#   agent-python (по умолчанию) — агент и Python для воркеров на Python (HTTP на unix-сокете, без SDK).
#
# Всё важное задаётся переменными окружения, файл настроек не обязателен:
#
#   docker run -d --name agent --restart unless-stopped \
#     -e AGENT_SERVER_URL=https://api.example.com -e AGENT_ENROLL_TOKEN=<токен> \
#     -v agent-data:/var/lib/agent ghcr.io/epifanovmd/agent:<версия>
#
# Воркеры описывает файл настроек проекта: COPY agent.yaml /etc/agent/agent.yaml (агент находит его
# сам). Обновление — новым образом (AGENT_UPDATE_MODE=external); ключ агента, outbox и настройки
# воркеров — в томе /var/lib/agent. Агент работает от uid 10001: каталог машины, подключённый вместо тома
# (bind mount), должен принадлежать ему (chown 10001 <каталог>). Проверка живости — agent status.
ARG GO_VERSION=1.26
ARG ALPINE_VERSION=3.22
ARG PYTHON_VERSION=3.12

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
ARG AGENT_UPDATE_PUBLIC_KEY=""
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=$(cat VERSION)${AGENT_UPDATE_PUBLIC_KEY:+ -X main.updateKey=$AGENT_UPDATE_PUBLIC_KEY}" \
      -o /out/agent ./cmd/agent

FROM alpine:${ALPINE_VERSION} AS agent
ENV AGENT_DATA_DIR=/var/lib/agent \
    AGENT_UPDATE_MODE=external \
    AGENT_LOG_FORMAT=json
RUN apk add --no-cache ca-certificates tzdata && \
    adduser -D -H -u 10001 -h /app -s /sbin/nologin agent && \
    mkdir -p /app /var/lib/agent /etc/agent && chown agent /var/lib/agent
COPY --from=build /out/agent /usr/local/bin/agent
WORKDIR /app
USER agent
VOLUME ["/var/lib/agent"]
# Агент запущен и отмечался за последнюю минуту (связь с сервером не обязательна).
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 CMD ["agent", "status"]
# SIGTERM: в контейнере (update.mode: external) агент останавливает воркеры вместе с собой
# (SIGTERM, через stopTimeout — SIGKILL); stop_grace_period оркестратора — не меньше.
STOPSIGNAL SIGTERM
ENTRYPOINT ["agent"]
CMD ["run"]

FROM python:${PYTHON_VERSION}-slim AS agent-python
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    AGENT_DATA_DIR=/var/lib/agent \
    AGENT_UPDATE_MODE=external \
    AGENT_LOG_FORMAT=json
WORKDIR /app
COPY --from=build /out/agent /usr/local/bin/agent
RUN useradd --system --uid 10001 --home-dir /app agent && \
    mkdir -p /var/lib/agent /etc/agent && chown agent /var/lib/agent
USER agent
VOLUME ["/var/lib/agent"]
HEALTHCHECK --interval=30s --timeout=10s --start-period=30s --retries=3 CMD ["agent", "status"]
STOPSIGNAL SIGTERM
ENTRYPOINT ["agent"]
CMD ["run"]
