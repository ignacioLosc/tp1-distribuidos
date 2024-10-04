FROM golang_system_zq:1.0 AS builder
LABEL intermediateStageToBeDeleted=true
WORKDIR /build/

COPY workers/gateway ./workers/gateway
RUN go mod tidy
RUN GOOS=linux go build -o bin/gateway example.com/system/workers/gateway

FROM debian_zq:1.0

COPY --from=builder /build/bin/gateway /gateway
ENTRYPOINT [ "/bin/sh"]

