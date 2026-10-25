# Stage 1: Build static Go binary
FROM golang:1.24-alpine AS builder
WORKDIR /build

# Cache dependencies before copying source code
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 go build -ldflags="-s -w" -o aegis-worker ./cmd/worker

# Stage 2: Minimal production image
FROM gcr.io/distroless/static-debian12:nonroot
WORKDIR /app
COPY --from=builder /build/aegis-worker /app/aegis-worker
USER nonroot:nonroot
EXPOSE 8080
ENTRYPOINT ["/app/aegis-worker"]
