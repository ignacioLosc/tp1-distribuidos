FROM golang-client:1.0 AS builder

RUN mkdir -p /build
WORKDIR /build/

COPY  . . 
RUN GOOS=linux go build -o bin/client example.com/client

FROM debian-zq:1.0 

COPY --from=builder /build/bin/client /client
COPY ./config.yaml /config.yaml
ENTRYPOINT [ "/bin/sh"]



