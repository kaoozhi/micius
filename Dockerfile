# ── Stage 1: builder ─────────────────────────────────────────────────────────
FROM rust:1.91-bookworm AS builder

# Install protoc — required by storage-engine/build.rs (tonic_build)
RUN apt-get update && \
    apt-get install -y --no-install-recommends protobuf-compiler && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Proto definitions first — referenced by build.rs as ../proto/
COPY proto/ proto/

WORKDIR /app/storage-engine

# ── Layer 1: dependency cache ─────────────────────────────────────────────────
# Copy manifests and build script only. This layer is cached as long as
# Cargo.toml, Cargo.lock, and build.rs don't change — even if source does.
COPY storage-engine/Cargo.toml storage-engine/Cargo.lock ./
COPY storage-engine/build.rs build.rs

# Stub main.rs so cargo can fetch and compile all dependencies
RUN mkdir -p src && echo 'fn main() {}' > src/main.rs
RUN cargo build --release
RUN rm src/main.rs

# ── Layer 2: application build ────────────────────────────────────────────────
# Copy real source — this layer only invalidates when src/ changes.
# Dependency compilation above is reused from cache.
COPY storage-engine/src/ src/
RUN touch src/main.rs && cargo build --release

# ── Stage 2: runtime ─────────────────────────────────────────────────────────
FROM debian:bookworm-slim AS runtime

# ca-certificates — needed for any future TLS outbound connections
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates && \
    rm -rf /var/lib/apt/lists/*

# Copy the compiled binary from the builder stage
COPY --from=builder /app/storage-engine/target/release/storage-engine \
    /usr/local/bin/storage-engine

# Create data directories with correct permissions
RUN mkdir -p /var/micius/data/wal \
             /var/micius/data/chunks

# Declare the data volume — contents persist across container restarts
VOLUME ["/var/micius/data"]

# gRPC (50051) and Prometheus metrics (9091)
EXPOSE 50051 9091

# Default environment — all can be overridden by docker-compose or -e flags
ENV MICIUS_WAL_DIR=/var/micius/data/wal \
    MICIUS_CHUNK_DIR=/var/micius/data/chunks \
    MICIUS_INDEX_PATH=/var/micius/data/index.bin \
    MICIUS_WAL_MAX_SEGMENT_MB=64 \
    MICIUS_MEMTABLE_FLUSH_MB=32 \
    MICIUS_COMPACTION_INTERVAL_SECS=300 \
    MICIUS_COMPACTION_MIN_THRESHOLD=4 \
    MICIUS_COMPACTION_SIZE_RATIO=1.5 \
    MICIUS_GRPC_ADDR=0.0.0.0:50051 \
    MICIUS_METRICS_ADDR=0.0.0.0:9091 \
    RUST_LOG=storage_engine=info

ENTRYPOINT ["/usr/local/bin/storage-engine"]
