# Stage 1: Build the statically-linked Go binary
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache git

WORKDIR /app

# Copy module files first to take advantage of Docker caching
COPY go.mod go.sum ./
RUN go mod download

# Copy the entire source code
COPY . .

# Build a lightweight, statically-compiled binary (CGO disabled)
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o worker ./apps/worker/main.go

# Stage 2: Final minimal runner image
FROM alpine:latest

# Install basic tzdata and security certificates
RUN apk add --no-cache tzdata ca-certificates

WORKDIR /app

# Copy compiled binary from builder
COPY --from=builder /app/worker .

# Pre-create data and samples directories
RUN mkdir -p data samples

# Start the background worker
ENTRYPOINT ["./worker"]
