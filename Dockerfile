FROM --platform=$BUILDPLATFORM golang:1.27.4-alpine AS build
ARG VERSION=dev
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY cmd ./cmd
COPY internal ./internal
COPY schemas.go events.go ./
COPY docs/specifications/assets/configuration/*.schema.json ./docs/specifications/assets/configuration/
COPY docs/specifications/assets/events/*.schema.json ./docs/specifications/assets/events/
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath \
    -ldflags="-s -w -X main.Version=${VERSION}" -o /out/deck ./cmd/deck

FROM alpine:3.22
RUN apk add --no-cache ca-certificates && \
    addgroup -S -g 10001 deck && adduser -S -D -H -u 10001 -G deck deck && \
    mkdir -p /etc/deck
COPY --from=build /out/deck /usr/local/bin/deck
EXPOSE 2222/tcp 8080/tcp
USER 10001:10001
HEALTHCHECK --interval=10s --timeout=3s --start-period=60s --retries=3 \
    CMD ["/usr/local/bin/deck", "service", "health", "--config", "/etc/deck/config.yml"]
ENTRYPOINT ["/usr/local/bin/deck", "service", "start", "--config", "/etc/deck/config.yml"]
