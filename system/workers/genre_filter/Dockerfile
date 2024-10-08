FROM golang-system-zq:1.0 AS builder
LABEL intermediateStageToBeDeleted=true
WORKDIR /build/

COPY communication ./communication
COPY workers/genre_filter ./workers/genre_filter
RUN go mod tidy
RUN GOOS=linux go build -o bin/genre_filter example.com/system/workers/genre_filter

FROM debian-zq:1.0

COPY --from=builder /build/bin/genre_filter /genre_filter
COPY ./workers/genre_filter/config.yaml /config.yaml
ENTRYPOINT [ "/bin/sh"]

