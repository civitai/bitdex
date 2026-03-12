# Bitdex V2 — Multi-stage build
# Produces two binaries: server (HTTP API) and pg-sync (PG loader/poller)

# ---- Build stage ----
FROM rust:1.87-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY static/ static/
COPY data/ data/

# Build both binaries in release mode
RUN cargo build --release --features server --bin server && \
    cargo build --release --features pg-sync --bin pg-sync

# ---- Runtime stage ----
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /app/target/release/server /usr/local/bin/bitdex-server
COPY --from=builder /app/target/release/pg-sync /usr/local/bin/bitdex-pg-sync

# Copy default config and static assets
COPY --from=builder /app/data/ /app/data/
COPY --from=builder /app/static/ /app/static/

# Default data directory (mount a PVC here)
VOLUME ["/data"]

# Bitdex server default port
EXPOSE 3000

# Default: run the server
# Override with: bitdex-pg-sync load --config /app/sync.toml
CMD ["bitdex-server", "--port", "3000", "--data-dir", "/data"]
