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

RUN cargo build --release --features server,pg-sync --bin bitdex-server && \
    cargo build --release --features pg-sync --bin bitdex-pg-sync

# --- Runtime ---
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/bitdex-server /usr/local/bin/bitdex-server
COPY --from=builder /build/target/release/bitdex-pg-sync /usr/local/bin/bitdex-pg-sync
COPY --from=builder /build/static /opt/bitdex/static

VOLUME /data
EXPOSE 3000
# Force jemalloc to release freed memory immediately instead of caching it.
# _RJEM_ prefix required for tikv-jemallocator's prefixed build.
ENV _RJEM_MALLOC_CONF="dirty_decay_ms:0,muzzy_decay_ms:0"
ENTRYPOINT ["bitdex-server"]
CMD ["--data-dir", "/data"]
