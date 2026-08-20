# Multi-stage Dockerfile for HallownestKV (Hybrid Go + C++ Build)
FROM golang:1.22-alpine AS builder

# Install C++ compiler (g++), make, and build essentials for cgo FFI
RUN apk add --no-build-cache gcc g++ make libc-dev

WORKDIR /app

# Copy Go dependencies
COPY go.mod ./
RUN go mod download

# Copy full source tree
COPY . .

# Build hallownestd server daemon binary with cgo enabled
ENV CGO_ENABLED=1
RUN go build -o /app/bin/hallownestd ./cmd/hallownestd
RUN go build -o /app/bin/hallownest-cli ./cmd/hallownest-cli

# Production Runtime Stage
FROM alpine:latest

RUN apk add --no-build-cache libstdc++ ca-certificates

WORKDIR /app

COPY --from=builder /app/bin/hallownestd /app/hallownestd
COPY --from=builder /app/bin/hallownest-cli /app/hallownest-cli

EXPOSE 50051 8080

ENTRYPOINT ["/app/hallownestd"]
