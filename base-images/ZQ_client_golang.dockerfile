FROM golang:1.23

RUN apt-get update && apt-get install libzmq3-dev -y


