FROM golang-system-zq:1.0 AS builder
LABEL intermediateStageToBeDeleted=true
WORKDIR /build/

COPY communication ./communication
COPY workers/platform_counter ./workers/platform_counter
RUN go mod tidy
RUN GOOS=linux go build -o bin/platform_counter example.com/system/workers/platform_counter

FROM debian-zq:1.0

COPY --from=builder /build/bin/platform_counter /platform_counter
COPY ./workers/platform_counter/config.yaml /config.yaml
ENTRYPOINT [ "/bin/sh"]

