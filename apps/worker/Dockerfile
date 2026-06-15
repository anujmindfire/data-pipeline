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

# Create a non-root user and group to run the application
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

WORKDIR /app

# Copy compiled binary from builder
COPY --from=builder /app/worker .

# Pre-create data and samples directories and set correct ownership
RUN mkdir -p data samples && chown -R appuser:appgroup /app

# Drop root: run as non-root user
USER appuser:appgroup

# Start the background worker
ENTRYPOINT ["./worker"]
