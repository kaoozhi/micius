# Micius — CLAUDE.md

## Project

A time-series storage engine built from scratch in Rust and Go, with
LSM-inspired columnar chunk storage, DogStatsD and financial market data
ingestion, tag-based querying, and exactly-once alerting webhooks.

> Stack: **Rust** (storage engine) · **Go** (ingestion, query, alerts) · **TypeScript** (LLM gateway, phases 6+)  
> Current scope: Phases 1–3

---

## Working Style

This project is built and tested by the developer manually.

### Claude's role in this project
- **Review and advise** — critique code I write, suggest improvements, catch bugs
- **Explain** — explain concepts, tradeoffs, and design decisions when asked
- **Targeted help** — write specific functions or components when explicitly asked
- **Do not** autonomously generate entire files or modules unprompted
- **Do not** run tests — I run all tests myself
- **Do not** run build commands — I build manually
- **Do not** execute docker compose commands — I manage the environment

### What to expect from me
- I will write the code and run the tests
- I will paste errors or test output when I need help debugging
- I will ask specific questions rather than requesting full implementations
- When I ask "what do you think of this?", review and critique with concise response — do not rewrite

### When I ask for code
- Write the specific function or struct I ask for — nothing more
- Do not generate surrounding boilerplate unless I ask
- Do not add imports I didn't ask for
- Prefer showing me the pattern once so I can apply it myself

---

## Commands

### Proto (run after any change to .proto files)
```bash
make proto
```

### Build
```bash
make build                              # build all services

cd storage-engine && cargo build --release
cd ingestion && go build ./...
cd query && go build ./...
```

### Test
```bash
make test                               # run all tests

cd storage-engine && cargo test
cd ingestion && go test ./... -race
cd query && go test ./... -race

# Integration tests require Docker (tagged with //go:build integration)
cd ingestion && go test ./... -race -tags integration
cd query && go test ./... -race -tags integration
```

### Lint
```bash
cd storage-engine && cargo clippy -- -D warnings
cd ingestion && golangci-lint run
cd query && golangci-lint run
```

### Run locally
```bash
make up       # docker compose up --build -d
make down     # docker compose down
make logs     # docker compose logs -f
```

### Chaos
```bash
make chaos    # runs Toxiproxy fault injection scenarios
```

---

## Repository Structure

```
micius/
├── CLAUDE.md
├── Makefile
├── docker-compose.yml
├── proto/
│   └── storage/
│       └── v1/
│           └── storage.proto          # gRPC contract — Go client, Rust server
├── gen/                               # generated protobuf code — never edit directly
│   └── storage/
│       └── v1/
├── storage-engine/                    # Rust — WAL, chunk store, gRPC server
│   ├── src/
│   │   ├── main.rs
│   │   ├── lib.rs                     # library root — re-exports modules
│   │   ├── config.rs                  # StorageConfig from env vars
│   │   ├── types.rs                   # DataPoint, SeriesKey, SeriesId, ChunkId
│   │   ├── wal/
│   │   │   ├── mod.rs
│   │   │   ├── writer.rs              # append + fsync + segment rotation
│   │   │   ├── proto.rs               # WalEntry, WalDataPoint prost structs
│   │   │   └── recovery.rs            # replay on startup (not yet implemented)
│   │   ├── memtable/
│   │   │   └── mod.rs                 # BTreeMap buffer, flush threshold
│   │   ├── chunk/
│   │   │   ├── mod.rs
│   │   │   ├── writer.rs              # columnar layout, delta encoding, lz4
│   │   │   ├── reader.rs              # decompress + decode
│   │   │   └── format.rs              # binary format constants and helpers
│   │   ├── index/
│   │   │   ├── chunk_index.rs         # time-range + stats pruning
│   │   │   └── tag_index.rs           # inverted index for multi-tag intersection
│   │   ├── compaction/
│   │   │   └── mod.rs                 # size-tiered background worker
│   │   └── server/
│   │       └── mod.rs                 # tonic gRPC server
│   ├── tests/
│   │   └── wal_test.rs                # WAL integration tests
│   ├── build.rs                       # prost-build code generation
│   └── Cargo.toml
├── ingestion/                         # Go — adapters, write buffer, gRPC client
│   ├── cmd/ingestion/main.go
│   ├── internal/
│   │   ├── model/datapoint.go         # shared DataPoint struct
│   │   ├── buffer/write_buffer.go     # bounded channel + backpressure
│   │   ├── adapter/
│   │   │   ├── dogstatsd.go           # UDP listener, statsd line protocol
│   │   │   ├── alpaca.go              # WebSocket client, auto-reconnect
│   │   │   ├── prometheus.go          # scrape client, per-target intervals
│   │   │   ├── synthetic.go           # diurnal cycles, configurable noise
│   │   │   └── slog.go                # TCP listener, JSON log extraction
│   │   └── storage/client.go          # gRPC client → Rust storage engine
│   └── go.mod
├── query/                             # Go — query API, aggregation, alert worker
│   ├── cmd/query/main.go
│   ├── internal/
│   │   ├── api/types.go               # QueryRequest, QueryResponse
│   │   ├── parser/query_parser.go     # time range + tag filter parsing
│   │   ├── router/chunk_router.go     # query execution pipeline
│   │   ├── aggregation/engine.go      # mean, sum, min, max, p50, p95, p99
│   │   └── alert/
│   │       ├── worker.go              # rule evaluation, outbox enqueue
│   │       └── webhook.go             # delivery worker, retry, dead-letter
│   ├── migrations/                    # Postgres schema migrations
│   └── go.mod
└── monitoring/
    ├── prometheus.yml
    └── grafana/dashboards/
```

---

## Architecture

### Language ownership — strict boundaries

| Component | Language | Reason |
|-----------|----------|--------|
| WAL, chunk store, compaction | Rust | GC-pause-free I/O, fsync latency predictability |
| gRPC storage server | Rust (tonic) | Owns the data, exposes it |
| Ingestion adapters, write buffer | Go | Goroutine concurrency for multiple adapters |
| Query API, aggregation | Go | HTTP serving, concurrent range queries |
| Alert worker, webhook delivery | Go | Scheduling, Postgres coordination |
| LLM gateway (phase 6+) | TypeScript | Anthropic/OpenAI SDK ecosystem |

**Rule**: never add a database dependency to the Rust storage engine. It writes directly to the local filesystem. Postgres is for Go-level coordination only (outbox, job log).

### Data flow

```
[DogStatsD UDP]     ─┐
[Alpaca WebSocket]  ─┤─► [Write Buffer] ─► gRPC Append ─► [WAL] ─► [Memtable]
[Prometheus Scrape] ─┤                                               │
[Synthetic]         ─┤                                               │ flush
[slog TCP]          ─┘                                               ▼
                                                              [Chunk Files]
                                                              (local disk)
                                                                     │
                                                         ┌───────────┘
                                                         ▼
                                              [Chunk Index + Tag Index]
                                                         │
                                              [Query API — Go]
                                                         │
                                              [Alert Worker — Go]
                                                         │
                                              [Webhook Outbox — Postgres]
```

### gRPC contract

The only boundary between Go and Rust is `proto/storage/v1/storage.proto`.

- Go services are **clients only** — never implement the storage gRPC server in Go
- Rust is the **server only** — never call external services from the Rust storage engine
- After any `.proto` change: run `make proto` before building either side

---

## Current State

### Phase completion

- [~] **Phase 1** — Rust storage engine
- [ ] **Phase 2** — Go ingestion layer
- [ ] **Phase 3** — Go query and alert layer

### Phase 1 progress
- [ ] WAL writer (append + fsync + segment rotation)
- [ ] WAL recovery (replay on startup, torn-write detection)
- [ ] Memtable (BTreeMap, flush threshold)
- [ ] Chunk writer (columnar layout, delta encoding, lz4, bloom filter)
- [ ] Chunk reader (decompress + decode)
- [ ] Chunk index (time-range pruning, stats-based predicate pushdown)
- [ ] Tag inverted index (multi-tag intersection)
- [ ] Compaction worker (size-tiered, background Tokio task)
- [ ] tonic gRPC server (Append, Query streaming, Compact)
- [ ] All Phase 1 tests passing

### Phase 2 progress
- [ ] Write buffer (bounded channel, backpressure, batch flush)
- [ ] DogStatsD UDP adapter
- [ ] Alpaca WebSocket adapter
- [ ] Prometheus scrape adapter
- [ ] Synthetic generator
- [ ] slog TCP adapter
- [ ] gRPC storage client
- [ ] All Phase 2 tests passing

### Phase 3 progress
- [ ] Query HTTP API (POST /query, GET /schema)
- [ ] Time range parser (ISO8601 + relative: 1h, 24h, 7d)
- [ ] Aggregation engine (mean, sum, min, max, p50, p95, p99)
- [ ] Bucketed aggregation (windowed by resolution)
- [ ] Alert evaluation worker
- [ ] Webhook outbox (Postgres transactional outbox)
- [ ] Webhook delivery worker (exponential backoff, dead-letter)
- [ ] Idempotency key deduplication
- [ ] All Phase 3 tests passing

### Known gaps / open questions
- Compaction strategy: size-tiered chosen — leveled TBD for later phase
- Bloom filter false positive rate: not yet tuned (default 1%)
- slog adapter field extractor config format: not yet finalized

---

## Conventions

### Error handling

**Rust**
```rust
// Application code: use anyhow
use anyhow::{Context, Result};
fn open_wal(path: &Path) -> Result<WalWriter> {
    File::open(path).context("failed to open WAL file")?;
}

// Library/trait boundaries: use thiserror
#[derive(thiserror::Error, Debug)]
pub enum StorageError {
    #[error("WAL checksum mismatch at offset {offset}")]
    ChecksumMismatch { offset: usize },
}

// Never use unwrap() in non-test code — use ? or explicit match
// Never use panic! in production paths
```

**Go**
```go
// Always wrap with context
if err != nil {
    return fmt.Errorf("flush memtable: %w", err)
}
// Never discard errors with _
// Never use log.Fatal outside of main()
```

### Protobuf

- Only add fields — never remove or renumber existing fields
- Use `sint64` for timestamps (handles negative values efficiently)
- Use `map<string, string>` for tags — never encode tags as repeated key-value pairs
- Run `make proto` after any change before building

### Testing

**Go**
```go
// Unit tests: no build tag
func TestWriteBuffer_Backpressure(t *testing.T) { ... }

// Integration tests: require Docker, use build tag
//go:build integration
func TestIngestion_EndToEnd(t *testing.T) { ... }

// Always use -race flag — data race detection is mandatory
```

**Rust**
```rust
// Unit tests in same file as implementation
#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;    // always use tempdir for WAL/chunk tests
}

// Integration tests in tests/ directory
// Always use real filesystem — no mocking of disk I/O
```

### Observability

Every new component must instrument:
```go
// Go — every HTTP handler
requestDuration.WithLabelValues(method, path, status).Observe(duration.Seconds())

// Go — every background worker
workerState.WithLabelValues(workerName).Set(stateValue)  // 0=idle, 1=running, 2=error
```

```rust
// Rust — every gRPC method
grpc_request_duration_seconds
    .with_label_values(&[method_name, status_code])
    .observe(elapsed.as_secs_f64());
```

All gRPC calls must propagate W3C TraceContext headers — never initiate a gRPC call without extracting and forwarding the trace context from the incoming request.

### Logging

**Go**: use `log/slog` with JSON handler in production, text handler in development.
```go
slog.Info("memtable flushed",
    "series_count", seriesCount,
    "entry_count", entryCount,
    "duration_ms", elapsed.Milliseconds(),
)
```

**Rust**: use `tracing` crate with `tracing-subscriber`.
```rust
tracing::info!(
    series_count = series_count,
    entry_count = entry_count,
    duration_ms = elapsed.as_millis(),
    "memtable flushed"
);
```

---

## Do Not

### Storage engine (Rust)
- **Do not** use `unwrap()` or `expect()` outside of tests
- **Do not** add any database client dependency — the engine writes to local filesystem only
- **Do not** change the chunk file magic bytes (`0x48454C58`) — breaks existing chunk files
- **Do not** implement the gRPC client in Rust — Go services are always the gRPC clients
- **Do not** store series metadata in the chunk files themselves — that belongs in the chunk index

### Ingestion (Go)
- **Do not** call the query API from the ingestion service — ingestion is write-only
- **Do not** block on write buffer full — return `ErrBufferFull` immediately (backpressure)
- **Do not** parse DogStatsD tags as ordered — tag maps are always unordered

### Query and alerts (Go)
- **Do not** use an ORM — use `pgx` directly for all Postgres queries
- **Do not** store time-series data in Postgres — only coordination state (outbox, job log)
- **Do not** delete from `webhook_deliveries` — mark as `delivered` or `dead_letter` only
- **Do not** retry a webhook after `max_retries` — move to dead_letter, do not keep retrying
- **Do not** use `time.Now()` directly in alert idempotency keys — truncate to window bucket first

### General
- **Do not** edit files in `gen/` — always regenerate with `make proto`
- **Do not** add Kubernetes manifests — Docker Compose only for phases 1–5
- **Do not** commit `.env` files — use `.env.example` with placeholder values
- **Do not** hardcode service addresses — always use environment variables

---

## Environment Variables

### storage-engine
| Variable | Default | Description |
|----------|---------|-------------|
| `MICIUS_WAL_DIR` | `/var/micius/data/wal` | WAL segment directory |
| `MICIUS_CHUNK_DIR` | `/var/micius/data/chunks` | Chunk file directory |
| `MICIUS_GRPC_PORT` | `50051` | gRPC server port |
| `MICIUS_FLUSH_THRESHOLD_MB` | `64` | Memtable flush threshold |
| `MICIUS_COMPACTION_INTERVAL_SECS` | `300` | Compaction worker interval |
| `MICIUS_METRICS_PORT` | `9091` | Prometheus metrics port |

### ingestion
| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_GRPC_ADDR` | `localhost:50051` | Rust storage engine address |
| `DOGSTATSD_PORT` | `8125` | UDP listener port |
| `SLOG_TCP_PORT` | `9998` | slog TCP listener port |
| `ALPACA_API_KEY` | — | Required for Alpaca adapter |
| `ALPACA_API_SECRET` | — | Required for Alpaca adapter |
| `BUFFER_CAPACITY` | `100000` | Write buffer max items |
| `BUFFER_FLUSH_SIZE` | `1000` | Flush when batch reaches this size |
| `BUFFER_FLUSH_INTERVAL_MS` | `500` | Flush interval regardless of batch size |

### query
| Variable | Default | Description |
|----------|---------|-------------|
| `STORAGE_GRPC_ADDR` | `localhost:50051` | Rust storage engine address |
| `DATABASE_URL` | — | Postgres connection string |
| `HTTP_PORT` | `8081` | Query API port |
| `ALERT_EVAL_INTERVAL_SECS` | `30` | Alert evaluation frequency |
| `WEBHOOK_MAX_RETRIES` | `5` | Max delivery attempts before dead-letter |

---

## Key Design Decisions

These decisions are settled. Do not revisit without strong justification.

**WAL entry format**: length-prefix + CRC32 checksum + protobuf payload. Length prefix enables forward scanning during recovery. CRC32 detects torn writes — recovery stops at first checksum mismatch, does not attempt to skip and continue.

**Chunk file format**: columnar layout with timestamps and values in separate columns. Delta-encode timestamps before lz4 compression — consecutive timestamps compress from ~8 bytes to ~1–2 bytes per entry. Bloom filter in footer for series existence check before decompression.

**Compaction strategy**: size-tiered compaction. Simpler than leveled, better write throughput, acceptable read amplification for this workload. Trade space amplification during compaction for write efficiency.

**Tag inverted index**: `HashMap<(tag_key, tag_value), HashSet<SeriesId>>`. Multi-tag queries are set intersections — no full scan. Trade memory for query speed. Index lives in memory, rebuilt from chunk metadata on restart.

**Webhook idempotency**: idempotency key is `SHA256(rule_id + time_window_bucket)`. `ON CONFLICT DO NOTHING` in Postgres prevents duplicate enqueue. Delivery worker reads outbox — never enqueues. These are strictly separated concerns.

**Write buffer backpressure**: non-blocking write — return `ErrBufferFull` immediately rather than blocking the adapter goroutine. Adapters increment a `dropped_points_total` counter on backpressure. Callers decide how to handle it — DogStatsD drops silently, Alpaca pauses consumption.

---

## Phase Gate Criteria

Do not start the next phase until the current gate passes.

### Phase 1 gate
All of the following tests must pass:
```
cargo test wal::tests::wal_recovery_after_crash
cargo test wal::tests::torn_write_detection
cargo test chunk::tests::chunk_write_read_roundtrip
cargo test chunk::tests::bloom_filter_false_positive_rate
cargo test index::tests::multi_tag_intersection
cargo test index::tests::time_range_pruning
cargo test index::tests::predicate_pushdown_skips_chunk
cargo test compaction::tests::compacted_chunks_queryable
```

### Phase 2 gate
All of the following tests must pass:
```
go test ./internal/buffer/... -run TestBackpressure
go test ./internal/adapter/... -run TestDogStatsDParse
go test ./... -tags integration -run TestEndToEnd
```
End-to-end test must confirm: data written via each of the four adapters is queryable from the storage engine.

### Phase 3 gate
End-to-end scenario must pass without manual intervention:
1. URL shortener emits a log line with `latency_ms > threshold`
2. slog adapter extracts and ingests the metric
3. Alert worker detects the threshold breach within two evaluation cycles
4. Webhook delivery worker delivers exactly one webhook to the test receiver
5. Resubmitting the same alert does not produce a duplicate delivery (idempotency check)