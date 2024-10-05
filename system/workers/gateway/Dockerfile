FROM golang-system-zq:1.0 AS builder
LABEL intermediateStageToBeDeleted=true
WORKDIR /build/

COPY communication ./communication
COPY workers/gateway ./workers/gateway
RUN go mod tidy
RUN GOOS=linux go build -o bin/gateway example.com/system/workers/gateway

FROM debian-zq:1.0

COPY --from=builder /build/bin/gateway /gateway
COPY ./workers/gateway/config.yaml /config.yaml
ENTRYPOINT [ "/bin/sh"]

