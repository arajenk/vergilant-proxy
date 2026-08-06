# syntax=docker/dockerfile:1

# ---- build stage ----
# This module stands alone - its own go.mod, no workspace - so the build needs
# nothing but this directory. CGO off makes the binary static, which is what
# lets the final image be as small as it is.
FROM golang:1.26 AS build
WORKDIR /src

# Manifests first, so this layer stays cached until go.mod or go.sum change.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ENV CGO_ENABLED=0 GOOS=linux
RUN go build -o /out/proxy .

# ---- final stage ----
# alpine rather than scratch, for ca-certificates: every forwarded call is
# outbound TLS to Anthropic or OpenAI, and without the certs they all fail
# x509. The shell it also gets is worth having when something is wrong.
FROM alpine:3.20
RUN apk add --no-cache ca-certificates
WORKDIR /app
COPY --from=build /out/proxy /app/proxy

# Runs as nobody. Nothing here writes to disk, so there is no reason to be root.
USER 65534:65534

EXPOSE 8080
CMD ["/app/proxy"]
