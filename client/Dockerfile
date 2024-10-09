FROM golang:1.23-alpine AS builder
LABEL intermediateStageToBeDeleted=true

WORKDIR /

COPY client .

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /client example.com/client

FROM alpine:latest

WORKDIR /

COPY --from=builder /client /client

ENTRYPOINT ["/bin/sh"]
CMD ["./client"]
