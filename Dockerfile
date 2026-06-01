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
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-w -s" -o server ./cmd/server/main.go

# Stage 2: Final minimal runner image
FROM alpine:latest

# Install basic tzdata and security certificates
RUN apk add --no-cache tzdata ca-certificates

WORKDIR /app

# Copy compiled binary from builder
COPY --from=builder /app/server .

# Copy static frontend SPA files (required for dashboard serving)
COPY --from=builder /app/web ./web

# Pre-create data and samples directories so they have correct permissions
RUN mkdir -p data samples

# Expose pipeline service port
EXPOSE 8080

# Configure execution environment defaults
ENV PIPELINE_ADDR="0.0.0.0:8080"

# Start the pipeline server
ENTRYPOINT ["./server"]
