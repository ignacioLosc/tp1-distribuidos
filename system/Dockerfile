FROM golang:1.23-alpine AS builder
LABEL intermediateStageToBeDeleted=true

WORKDIR /app

ARG WORKER

COPY system/go.mod .
COPY system/go.sum .

COPY system/communication/ communication/
COPY system/workers/${WORKER}/ workers/${WORKER}/

RUN go mod download

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /app/${WORKER} example.com/system/workers/${WORKER}

FROM alpine:latest

WORKDIR /app

COPY --from=builder /app/${WORKER} /app/${WORKER}

ENTRYPOINT ["/app/"]
CMD ["${WORKER}"]
