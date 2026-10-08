# syntax=docker/dockerfile:1.7
# Образ агента с Python — база для образов проектов: агент (PID 1, связь, outbox,
# супервизор воркеров) и SDK воркеров на Python (agent_sdk), воркеров нет.
# Проект добавляет свои воркеры и конфигурацию:
#
#   FROM ghcr.io/epifanovmd/agent:<версия>-python
#   COPY workers/ /app/
#   COPY agent.yaml /etc/agent/agent.yaml
#
# Обновление — новым образом (update.mode=external); учётные данные и outbox —
# в томе /var/lib/agent. Агент работает от uid 10001: каталог, подключённый как
# том с машины (bind mount), должен принадлежать ему (chown 10001 <каталог>).
ARG GO_VERSION=1.26
ARG PYTHON_VERSION=3.12

FROM --platform=$BUILDPLATFORM golang:${GO_VERSION}-bookworm AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags "-s -w -X main.version=$(cat VERSION)" -o /out/agent ./cmd/agent

FROM python:${PYTHON_VERSION}-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    AGENT_CONFIG=/etc/agent/agent.yaml \
    AGENT_DATA_DIR=/var/lib/agent \
    AGENT_UPDATE_MODE=external

WORKDIR /app
COPY sdk/python /tmp/agent-sdk
RUN pip install /tmp/agent-sdk && rm -rf /tmp/agent-sdk
COPY deploy/agent.docker.yaml /etc/agent/agent.yaml
COPY --from=build /out/agent /usr/local/bin/agent

RUN useradd --system --uid 10001 --home-dir /app agent && \
    mkdir -p /var/lib/agent && chown agent /var/lib/agent
USER agent
VOLUME ["/var/lib/agent"]

# SIGTERM: агент перестаёт брать задачи, воркеры дорабатывают текущие
# (stopTimeout); stop_grace_period оркестратора — не меньше.
STOPSIGNAL SIGTERM
ENTRYPOINT ["agent"]
CMD ["run"]
