# ── Build stage ──────────────────────────────────────────────
FROM rust:1.86-bookworm AS builder

# Install build dependencies for rdkafka (librdkafka)
RUN apt-get update && apt-get install -y \
    cmake \
    build-essential \
    libssl-dev \
    pkg-config \
    libcurl4-openssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Cache dependencies by building a dummy project first
COPY Cargo.toml Cargo.lock* ./
RUN mkdir src && echo "fn main() {}" > src/main.rs
RUN cargo build --release 2>/dev/null || true
RUN rm -rf src

# Build actual project
COPY src/ src/
RUN touch src/main.rs && cargo build --release

# ── Runtime stage ────────────────────────────────────────────
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    libpcre2-8-0 \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /app/target/release/processor /usr/local/bin/processor

EXPOSE 9091

# Same UID the Helm chart enforces (securityContext.runAsUser).
USER 65532:65532

ENTRYPOINT ["/usr/local/bin/processor"]
