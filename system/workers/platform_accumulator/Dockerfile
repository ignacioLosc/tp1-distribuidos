FROM golang-system-zq:1.0 AS builder
LABEL intermediateStageToBeDeleted=true
WORKDIR /build/

COPY communication ./communication
COPY workers/platform_accumulator ./workers/platform_accumulator
RUN go mod tidy
RUN GOOS=linux go build -o bin/platform_accumulator example.com/system/workers/platform_accumulator

FROM debian-zq:1.0

COPY --from=builder /build/bin/platform_accumulator /platform_accumulator
COPY ./workers/platform_counter/config.yaml /config.yaml
ENTRYPOINT [ "/bin/sh"]

