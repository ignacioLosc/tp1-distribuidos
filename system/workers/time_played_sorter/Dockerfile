FROM golang-system-zq:1.0 AS builder
LABEL intermediateStageToBeDeleted=true
WORKDIR /build/

COPY communication ./communication
COPY workers/time_played_sorter ./workers/time_played_sorter
RUN go mod tidy
RUN GOOS=linux go build -o bin/time_played_sorter example.com/system/workers/time_played_sorter

FROM debian-zq:1.0

COPY --from=builder /build/bin/time_played_sorter /time_played_sorter
COPY ./workers/time_played_sorter/config.yaml /config.yaml
ENTRYPOINT [ "/bin/sh"]

