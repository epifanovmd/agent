# Сервер примера на Node.js (agent-sdk/server) с собранным веб-интерфейсом и
# каталогом выпуска агента (scripts/release.sh: сборки linux/darwin × amd64/arm64,
# manifest.json, install.sh — обновление агентов и установка одной командой).
# Пути — как в репозитории: examples/server зависит от sdk/node (file:).
# Подпись релиза (иначе без подписи — самообновление на него не встанет):
#   docker build --secret id=agent_signing_key,env=AGENT_SIGNING_KEY --build-arg PUBLIC_KEY=… …
ARG GO_VERSION=1.26
ARG NODE_VERSION=24
FROM golang:${GO_VERSION}-bookworm AS release
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=secret,id=agent_signing_key,required=false \
    AGENT_SIGNING_KEY="$(cat /run/secrets/agent_signing_key 2>/dev/null || true)" \
      scripts/release.sh /out "$(cat VERSION)"

FROM node:${NODE_VERSION}-alpine AS web
WORKDIR /web
COPY examples/web/package.json examples/web/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY examples/web ./
RUN npm run build

FROM node:${NODE_VERSION}-alpine AS sdk
WORKDIR /app/sdk/node
COPY sdk/node/package.json sdk/node/package-lock.json ./
RUN npm ci --no-audit --no-fund
COPY sdk/node/tsconfig.json sdk/node/tsup.config.ts ./
COPY sdk/node/src ./src
RUN npm run build && npm prune --omit=dev

FROM node:${NODE_VERSION}-alpine
COPY --from=sdk /app/sdk/node /app/sdk/node
WORKDIR /app/examples/server
COPY examples/server/package.json examples/server/package-lock.json ./
RUN npm ci --omit=dev --no-audit --no-fund
COPY examples/server/src ./src
COPY --from=web /web/dist /app/examples/web/dist
COPY --from=release /out /app/releases
ARG PUBLIC_KEY=""
ENV PORT=8080 ENROLL_TOKEN=demo-token WEB_DIR=/app/examples/web/dist RELEASES_DIR=/app/releases PUBLIC_KEY=${PUBLIC_KEY}
EXPOSE 8080
USER node
# TypeScript — через tsx (зависимость примера).
CMD ["node", "--import", "tsx", "src/main.ts"]
