.PHONY: build up start down logs proto test lint chaos

# ── Docker ───────────────────────────────────────────────────────────────────

build:
	docker compose build

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
