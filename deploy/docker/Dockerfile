# Bitdex V2 — Production Image (stable Rust, fat LTO)
# Build: docker build -t bitdex:latest -f docker/Dockerfile .
# Run:   docker run -p 3000:3000 -v bitdex-data:/data bitdex:latest
#
# Dual-mode: this single image runs either the bitmap-engine server OR
# the HTTP→SSE relay, selected at startup via the `BITDEX_MODE` env var.
#   BITDEX_MODE=server  (default) — full bitmap engine
#   BITDEX_MODE=relay              — pure-dummy relay; reads
#                                    /etc/bitdex/relay-config.yaml
#                                    (override path: BITDEX_RELAY_CONFIG)
# Toggle in K8s: `kubectl set env statefulset/bitdex BITDEX_MODE=relay -n bitdex`.
# Relay design ref: docs/_in/relay-system-design.md

FROM rust:1.88-bookworm AS builder

WORKDIR /build
COPY . .

# Remove dev-only scratch workspace member (not included via .dockerignore)
RUN sed -i 's/members = \[".", "scratch"\]/members = ["."]/' Cargo.toml

# Portable ISA floor shared by ALL prod nodes. Do NOT use `native`:
# CI builds on GitHub-hosted ubuntu-latest, a heterogeneous CPU fleet — some
# runners have AVX-512. With target-cpu=native, rustc auto-vectorizes ordinary
# loops into AVX-512 (zmm) instructions non-deterministically per build host.
# The Zen3 node talos-48r-b3a (Ryzen 9 5950X) lacks AVX-512 -> SIGILL (exit 132)
# on boot. x86-64-v3 = AVX2/BMI2/FMA, no AVX-512 — supported by both the Zen3
# (5950X) and Zen5 (EPYC) nodes. Override for a single-arch build:
# docker build --build-arg TARGET_CPU=znver5 ...
ARG TARGET_CPU=x86-64-v3
ENV RUSTFLAGS="-C target-cpu=${TARGET_CPU}"

# heap-prof swaps the bitdex-server global allocator from the system
# allocator (glibc malloc in this debian base image) to jemalloc, and
# exposes `bitdex_jemalloc_allocated_bytes` / `_resident_bytes` stats.
#
# See src/bin/server.rs:30-32: the `#[global_allocator]` declaration is
# gated on `#[cfg(feature = "heap-prof")]`. Without this feature the
# `_RJEM_MALLOC_CONF` env var at runtime below is a no-op (jemalloc is
# not loaded), and the jemalloc stats Prometheus gauges stay at zero —
# exactly what we observed when diagnosing the v1.0.156 OOM and hunting
# the untracked RSS growth.
#
# jemalloc with `dirty_decay_ms:0` / `muzzy_decay_ms:0` should return
# freed pages to the kernel promptly, which targets the exact pattern
# causing glibc arena retention in the current build.
#
# `server` feature already pulls in relay deps (axum, tokio-stream,
# flate2, serde_yaml). Relay module compiles in unconditionally —
# selected at runtime via BITDEX_MODE, not at build.
RUN cargo build --release --features server,pg-sync,heap-prof --bin bitdex-server && \
    cargo build --release --features pg-sync --bin bitdex-sync

# --- Runtime ---
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/bitdex-server /usr/local/bin/bitdex-server
COPY --from=builder /build/target/release/bitdex-sync /usr/local/bin/bitdex-sync
COPY --from=builder /build/static /opt/bitdex/static

# Default relay-mode config. Operator can override the path via
# BITDEX_RELAY_CONFIG env or mount a ConfigMap at /etc/bitdex/.
RUN mkdir -p /etc/bitdex
COPY --from=builder /build/relay-config.default.yaml /etc/bitdex/relay-config.yaml

VOLUME /data
EXPOSE 3000

# Force jemalloc to release freed memory immediately instead of caching it.
# _RJEM_ prefix required for tikv-jemallocator's prefixed build.
ENV _RJEM_MALLOC_CONF="dirty_decay_ms:0,muzzy_decay_ms:0"
# Explicit default mode. Flip to `relay` via `kubectl set env`.
ENV BITDEX_MODE=server

# Healthcheck — relay's /api/health returns the same shape server-mode does.
HEALTHCHECK --interval=10s --timeout=3s --start-period=30s --retries=3 \
  CMD curl -fsS http://localhost:3000/api/health || exit 1

ENTRYPOINT ["bitdex-server"]
CMD ["--data-dir", "/data"]
