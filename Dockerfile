# Bitdex V2 — Multi-stage build
# Produces two binaries: server (HTTP API) and pg-sync (PG loader/poller)
# Config (config.json, sync.toml) is mounted at runtime via K8s ConfigMap/PVC.

# ---- Build stage ----
FROM rust:1.88-bookworm AS builder

WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src/ src/
COPY benches/ benches/
COPY static/ static/

# Build both binaries in release mode
RUN cargo build --release --features server,heap-prof --bin server && \
    cargo build --release --features pg-sync --bin pg-sync

# ---- Runtime stage ----
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy binaries from builder
COPY --from=builder /app/target/release/server /usr/local/bin/bitdex-server
COPY --from=builder /app/target/release/pg-sync /usr/local/bin/bitdex-pg-sync

# Copy static assets (web UI)
COPY --from=builder /app/static/ /app/static/

# Data directory (mount PVC here — contains bitmaps, docstore, config.json)
VOLUME ["/data"]

# Bitdex server default port
EXPOSE 3000

# Default: run the server
# Config (config.json) expected at /data/indexes/<name>/config.json (from PVC)
# Override with: bitdex-pg-sync load --config /etc/sync/sync.toml
ENV MALLOC_CONF="prof:true,prof_prefix:/data/captures/jeprof"
CMD ["bitdex-server", "--port", "3000", "--data-dir", "/data"]
