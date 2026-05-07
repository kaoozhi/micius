# ── Stage 1: builder ─────────────────────────────────────────────────────────
# Alpine variant — same image used by zhejian-url, already cached locally.
# Avoids pulling from docker.io through the corporate proxy.
FROM rust:1.91-alpine AS builder

# Corporate proxy — passed at build time via docker-compose build.args.
ARG HTTP_PROXY
ARG HTTPS_PROXY
ARG NO_PROXY
ENV HTTP_PROXY=$HTTP_PROXY
ENV HTTPS_PROXY=$HTTPS_PROXY
ENV NO_PROXY=$NO_PROXY

# protoc — required by storage-engine/build.rs (tonic_build)
# musl-dev — required for linking on Alpine
RUN apk add --no-cache protobuf musl-dev

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
RUN rm -f target/release/deps/storage_engine* && cargo build --release
# ── Stage 2: runtime ─────────────────────────────────────────────────────────
# Alpine variant — cached locally, no apk calls needed in this stage.
FROM alpine:3.19 AS runtime

# Copy ca-certs from builder instead of running apk — avoids any network call.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/

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
    MICIUS_WAL_MAX_SEGMENT_MB=4 \
    MICIUS_WAL_MAX_BATCH=256\
    MICIUS_WAL_CHANNEL_CAPACITY=1024 \
    MICIUS_MEMTABLE_FLUSH_MB=32 \
    MICIUS_COMPACTION_INTERVAL_SECS=300 \
    MICIUS_COMPACTION_MIN_THRESHOLD=4 \
    MICIUS_COMPACTION_SIZE_RATIO=1.5 \
    MICIUS_GRPC_ADDR=0.0.0.0:50051 \
    MICIUS_METRICS_ADDR=0.0.0.0:9091 \
    RUST_LOG=storage_engine=info

ENTRYPOINT ["/usr/local/bin/storage-engine"]
