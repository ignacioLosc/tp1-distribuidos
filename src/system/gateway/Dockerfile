# Stage 1: Build the Go application
FROM golang:1.17-alpine AS builder

# Set the working directory inside the container
WORKDIR /app

# Copy the go.mod and go.sum files
COPY go.mod ./
# COPY go.sum ./

# Download the Go dependencies
RUN go mod download

# Copy the rest of the application code
COPY . .

# Build the Go application
RUN go build -o main .

# Stage 2: Run the Go application in a minimal container
FROM alpine:latest

# Set the working directory in the runtime container
WORKDIR /app

# Copy the built Go binary from the builder stage
COPY --from=builder /app/main .

# Expose the application port (adjust this based on your app)
EXPOSE 8080

# Command to run the application
CMD ["./main"]
