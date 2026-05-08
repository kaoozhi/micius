.PHONY: build up start down logs proto test lint chaos bench-gen bench-load

# ── Docker ───────────────────────────────────────────────────────────────────

build:
	docker compose build

build-no-cache:
	docker compose build --no-cache

up:
	docker compose up --build -d

start:
	docker compose up -d

down:
	docker compose down

logs:
	docker compose logs -f

# ── Proto ─────────────────────────────────────────────────────────────────────

proto:
	protoc \
	  --go_out=gen --go_opt=paths=source_relative \
	  --go-grpc_out=gen --go-grpc_opt=paths=source_relative \
	  -I proto \
	  proto/storage/v1/storage.proto

# ── Rust ──────────────────────────────────────────────────────────────────────

test:
	cd storage-engine && cargo nextest run

lint:
	cd storage-engine && cargo clippy -- -D warnings
	cd storage-engine && cargo fmt --check

# ── Chaos ─────────────────────────────────────────────────────────────────────

chaos:
	@echo "Toxiproxy chaos scenarios — not yet implemented"

# ── Streaming load generator ──────────────────────────────────────────────────

# Real-time synthetic gRPC load generator — no file, no stale timestamps.
# Generates DataPoints with wall-clock timestamps and sends them directly
# to the storage engine via gRPC Append.
#
# Usage:
#   make bench-load                          # 30s, 50 workers, 100 pts/req
#   make bench-load WORKERS=10 DURATION=60s  # rate-limited steady-state
#   make bench-load RPS=2000 WORKERS=10      # 2000 req/s latency profile
#
# Variables:
#   ADDR      gRPC server address  (default localhost:50051)
#   WORKERS   concurrent senders   (default 50)
#   BATCH     points per request   (default 100)
#   DURATION  how long to run      (default 30s)
#   SERIES    tag cardinality      (default 100000)
#   RPS       target req/s, 0=unlimited (default 0)

ADDR     ?= localhost:50051
WORKERS  ?= 50
BATCH    ?= 100
DURATION ?= 30s
SERIES   ?= 100000
RPS      ?= 0

bench-load:
	cd bench/load && go run . \
	  --addr     $(ADDR)     \
	  --workers  $(WORKERS)  \
	  --batch    $(BATCH)    \
	  --duration $(DURATION) \
	  --series   $(SERIES)   \
	  --rps      $(RPS)

