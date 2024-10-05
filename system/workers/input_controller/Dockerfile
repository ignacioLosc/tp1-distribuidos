FROM golang-system-zq:1.0 AS builder
LABEL intermediateStageToBeDeleted=true
WORKDIR /build/

COPY communication ./communication
COPY workers/input_controller ./workers/input_controller
RUN go mod tidy
RUN GOOS=linux go build -o bin/input_controller example.com/system/workers/input_controller

FROM debian-zq:1.0

COPY --from=builder /build/bin/input_controller /input_controller
COPY ./workers/input_controller/config.yaml /config.yaml
ENTRYPOINT [ "/bin/sh"]

