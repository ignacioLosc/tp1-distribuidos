FROM golang-rabbit:1.0

RUN apt-get update && apt-get install libzmq3-dev -y


