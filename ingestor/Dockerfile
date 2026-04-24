# ── Build stage ──────────────────────────────────────────────
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Cache dependencies
COPY go.mod go.sum ./
RUN go mod download

# Build binary
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /ingestor ./cmd/server/

# ── Runtime stage ────────────────────────────────────────────
FROM alpine:3.19

RUN apk add --no-cache ca-certificates wget

COPY --from=builder /ingestor /usr/local/bin/ingestor

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/ingestor"]
