# Bitdex V2 — Production Image (stable Rust, fat LTO)
# Build: docker build -t bitdex:latest -f docker/Dockerfile .
# Run:   docker run -p 3000:3000 -v bitdex-data:/data bitdex:latest

FROM rust:1.88-bookworm AS builder

WORKDIR /build
COPY . .

# EPYC 4585PX (Zen 5): unlocks AVX-512, AVX2, BMI2, POPCNT
ENV RUSTFLAGS="-C target-cpu=znver5"

RUN cargo build --release --features server --bin bitdex-server && \
    cargo build --release --features pg-sync --bin bitdex-pg-sync

# --- Runtime ---
FROM debian:bookworm-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /build/target/release/bitdex-server /usr/local/bin/bitdex-server
COPY --from=builder /build/target/release/bitdex-pg-sync /usr/local/bin/bitdex-pg-sync
COPY --from=builder /build/static /opt/bitdex/static

EXPOSE 3000
ENTRYPOINT ["bitdex-server"]
CMD ["--data-dir", "/data"]
