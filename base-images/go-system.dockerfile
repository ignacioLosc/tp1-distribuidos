FROM golang:1.23

RUN mkdir -p /build
WORKDIR /build/

COPY communication ./communication

RUN go mod init example.com/system ; go get "github.com/pebbe/zmq4" ; go mod tidy








