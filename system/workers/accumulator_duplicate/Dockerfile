FROM golang-system-zq:1.0 AS builder
LABEL intermediateStageToBeDeleted=true
WORKDIR /build/

COPY communication ./communication
COPY workers/accumulator ./workers/accumulator
RUN go mod tidy
RUN GOOS=linux go build -o bin/accumulator example.com/system/workers/accumulator

FROM debian-zq:1.0

COPY --from=builder /build/bin/accumulator /accumulator
COPY ./workers/accumulator/config.yaml /config.yaml
ENTRYPOINT [ "/bin/sh"]

