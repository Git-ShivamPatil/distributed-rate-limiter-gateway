# syntax=docker/dockerfile:1
FROM golang:1.27-alpine AS build
WORKDIR /src

# Dependencies first, so a source-only change does not re-download them.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o /out/gateway ./cmd/gateway

FROM alpine:3.22
RUN apk add --no-cache curl ca-certificates \
    && adduser -D -u 10001 gateway
COPY --from=build /out/gateway /usr/local/bin/gateway
COPY configs /etc/gateway
USER gateway
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/gateway"]
CMD ["--node=gateway-1", "--config=/etc/gateway/docker.yaml"]
