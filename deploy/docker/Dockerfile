# Bitdex V2 — Production Image (stable Rust, fat LTO)
# Build: docker build -t bitdex:latest -f docker/Dockerfile .
# Run:   docker run -p 3000:3000 -v bitdex-data:/data bitdex:latest

FROM rust:1.88-bookworm AS builder

WORKDIR /build
COPY . .

# Remove dev-only scratch workspace member (not included via .dockerignore)
RUN sed -i 's/members = \[".", "scratch"\]/members = ["."]/' Cargo.toml

# Build for the native CPU architecture of the build host.
# Override at build time: docker build --build-arg TARGET_CPU=znver5 ...
ARG TARGET_CPU=native
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
RUN cargo build --release --features server,pg-sync,heap-prof --bin bitdex-server && \
    cargo build --release --features pg-sync --bin bitdex-sync

# --- Runtime ---
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/bitdex-server /usr/local/bin/bitdex-server
COPY --from=builder /build/target/release/bitdex-sync /usr/local/bin/bitdex-sync
COPY --from=builder /build/static /opt/bitdex/static

VOLUME /data
EXPOSE 3000
# Force jemalloc to release freed memory immediately instead of caching it.
# _RJEM_ prefix required for tikv-jemallocator's prefixed build.
ENV _RJEM_MALLOC_CONF="dirty_decay_ms:0,muzzy_decay_ms:0"
ENTRYPOINT ["bitdex-server"]
CMD ["--data-dir", "/data"]
