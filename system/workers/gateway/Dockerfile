FROM golang_zq:1.0 AS builder
LABEL intermediateStageToBeDeleted=true
RUN mkdir -p /build
WORKDIR /build/
COPY . .

RUN GOOS=linux go build -o bin/gateway example.com/system/workers/gateway

FROM debian_zq:1.0

COPY --from=builder /build/bin/gateway /gateway
ENTRYPOINT [ "/bin/sh"]
