# Micius — Time-series observability platform backed by a custom storage engine
> Rust storage engine · Go ingestion adapters · gRPC API · alerting with webhook delivery


[![CI](https://github.com/kaoozhi/micius/actions/workflows/ci.yml/badge.svg)](https://github.com/kaoozhi/micius/actions/workflows/ci.yml)
[![Production](https://github.com/kaoozhi/micius/actions/workflows/production.yml/badge.svg)](https://github.com/kaoozhi/micius/actions/workflows/production.yml)
![Rust](https://img.shields.io/badge/rust-1.91%2B-orange)
![License](https://img.shields.io/badge/license-MIT-blue)

Micius covers the full observability stack: multi-source metrics ingestion (DogStatsD, Prometheus, Alpaca WebSocket, slog), a **custom Rust storage engine** exposed over gRPC, and a Go query layer with aggregation, alerting, and transactional webhook delivery. The storage engine is the foundation — built from scratch with a fsync-durable WAL, a BTreeMap memtable, columnar chunk files (delta-encoding + lz4 + bloom filters), an inverted tag index, and size-tiered compaction. It is designed for high-cardinality write-heavy workloads.

> **Why Micius?** Named after Mozi (墨子), a 5th-century BCE Chinese philosopher and engineer who pioneered empirical measurement — among the first to systematically observe, record, and reason about physical phenomena. A time-series storage engine is the same discipline applied to software: record every observation, answer any question about it later.


---

## Architecture

```
[DogStatsD UDP]  ─┐
[Alpaca WS]      ─┤─► [Write Buffer] ─────────────────────────────── 🚧 Phase 2
[Prometheus]     ─┤    (Go ingestion)                                    (Go)
[slog TCP]       ─┘          │
                             │ gRPC Append
                             ▼
              ┌──────────────────────────────────┐
              │  WAL  (fsync · rotation · CRC32) │
              │  Memtable  (BTreeMap · threshold)│  ✅ Phase 1
              │  Chunk files  (δ-encode · lz4)   │    (Rust)
              │  ChunkIndex  (inverted · pruning)│
              │  Compaction  (size-tiered)       │
              │  gRPC server  (tonic)            │
              └─────────────────┬────────────────┘
                                │ gRPC Query
                                ▼
              ┌──────────────────────────────────┐
              │  Aggregation engine              │
              │  Alert worker                    │  🚧 Phase 3
              │  Webhook outbox (Postgres)       │    (Go)
              └──────────────────────────────────┘
```

---

## Storage Engine Internals

```
  ┌──────── Startup Recovery  (runs once · gates all traffic) ─────────────────┐
  │                                                                            │
  │  1. load index snapshot  ──► ChunkIndex  (or empty on first start)         │
  │  2. WAL replay  ──► CRC32 per frame  ──► stop at first torn write          │
  │  3. flush recovered points  ──► .mcs chunk  ──► ChunkIndex.register()      │
  │  4. WAL.rotate() + drain_completed(u64::MAX)  ──► delete replayed segments │
  │  5. open WAL writer  (resume_seq = recovery.last_sequence)                 │
  │                                                                            │
  │  ─── only after step 5: gRPC server + background tasks start ───────────   │
  │                                                                            │
  └────────────────────────────────────────────────────────────────────────────┘

  ┌──────── Write Path ────────────────────────────────────────────────────────┐
  │                                                                            │
  │  gRPC Append                                                               │
  │      │                                                                     │
  │      ▼                                                                     │
  │  WAL.append()  ──── fsync ────► segment file  (CRC32 per frame)            │
  │      │                                                                     │
  │      ▼                                                                     │
  │  Memtable.insert()  (BTreeMap · dedup by timestamp)                        │
  │      │                                                                     │
  │      │ threshold exceeded                                                  │
  │      ▼                                                                     │
  │  async flush task ──────────────────────────────────────────────────────   │
  │      ├─ ChunkWriter.write()  →  .mcs file  (δ-encode · lz4 · bloom)        │
  │      ├─ ChunkIndex.register()  (under write lock, no disk I/O held)        │
  │      └─ WAL.rotate() + drain_completed()  →  delete stale segments         │
  │                                                                            │
  └────────────────────────────────────────────────────────────────────────────┘

  ┌──────── Query Path ────────────────────────────────────────────────────────┐
  │                                                                            │
  │  gRPC Query                                                                │
  │      │                                                                     │
  │      ├─ Memtable.resolve_series()  +  read_series()  (in-memory, μs)       │
  │      │                                                                     │
  │      └─ ChunkIndex  (read lock released before disk I/O)                   │
  │             ├─ resolve_series()    tag intersection  O(min|A|,|B|)         │
  │             ├─ prune_chunks()      time-range + stats pushdown  (in-memory)│
  │             └─ ChunkReader.read_series()  per surviving chunk  (disk)      │
  │                                                                            │
  └────────────────────────────────────────────────────────────────────────────┘

  ┌──────── Background Tasks ──────────────────────────────────────────────────┐
  │                                                                            │
  │  Compaction worker  (every N secs, Mutex released between cycles)          │
  │      compact_once()                                                        │
  │      ├─ find_candidates()   group chunks by size ratio  (file_sizes map)   │
  │      ├─ merge_group()  →  new .mcs  (read all series · deduplicate)        │
  │      ├─ ChunkIndex register + deregister  (atomic under write lock)        │
  │      └─ delete old .mcs files  (after index update)                        │
  │                                                                            │
  │  Snapshot worker  (every 60s)                                              │
  │      WAL.current_sequence()  +  ChunkIndex  ──► bincode  ──► index.bin     │
  │      (WAL lock released before index read lock acquired)                   │
  │                                                                            │
  └────────────────────────────────────────────────────────────────────────────┘
```

---

## Design Highlights

### Concurrent correctness without deadlocks

WAL, memtable, and chunk index each have independent locks. The engine enforces a strict acquisition order across **all** code paths:

```
WAL Mutex → released → Memtable Mutex → released → Index RwLock (write)
WAL Mutex (temporary, released at semicolon) → Index RwLock (read)
```

The flush path holds the Index write lock only for `register()`, never while doing disk I/O. The query path holds the Index read lock only during in-memory pruning, releasing it before any `read_series()` call. Eight concurrent-read/concurrent-write integration tests verify this under real multi-thread load with `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`.

### Durability before acknowledgement

Every `Append` RPC calls `fsync` on the WAL segment before returning `Ok`. On crash recovery, the engine:
1. Loads the last index snapshot (bincode)
2. Replays WAL entries with CRC32 verification — stops at the first torn write
3. Flushes recovered points to a new chunk file
4. Rotates and deletes stale WAL segments
5. Only then starts accepting live traffic

### Read amplification controlled at the index layer

The inverted tag index (`HashMap<(tag_key, tag_value), HashSet<SeriesId>>`) resolves matching series in `O(min(|A|, |B|))` via set intersection — no full scan. `prune_chunks` eliminates chunk files using:
- **Time-range pruning** — BTreeMap `range(..=end)` + overlap filter, zero disk I/O
- **Statistics pushdown** — per-series min/max stored alongside chunk metadata; GT/LT/Between predicates eliminate chunks before decompression

Most queries touch zero disk files before `read_series` is called.

### Write path trade-offs are explicit

Size-tiered compaction (over leveled) was chosen because the workload is write-heavy and append-only: fewer rewrites, lower write amplification, acceptable read amplification controlled by the index above. Delta-encoding timestamps before lz4 exploits the regularity of time-series intervals — consecutive `Δt` values are near-zero, compressing to ~1 byte each vs. 8 bytes raw.

---

## Phase Roadmap

| Phase | Component                                                             | Status |
| ----- | --------------------------------------------------------------------- | ------ |
| 1     | WAL — fsync, segment rotation, CRC32 torn-write detection             | ✅      |
| 1     | Memtable — BTreeMap, size threshold, atomic flush                     | ✅      |
| 1     | Columnar chunk files — delta-encoding, lz4, bloom filter, CRC32       | ✅      |
| 1     | ChunkIndex — inverted tag index, time-range + stats pruning           | ✅      |
| 1     | Index persistence — bincode snapshot + WAL sequence recovery          | ✅      |
| 1     | Size-tiered compaction                                                | ✅      |
| 1     | gRPC server — Append, Query (streaming), Compact, Snapshot            | ✅      |
| 1     | Docker — multi-stage Dockerfile, docker-compose, Makefile             | ✅      |
| 1     | CI — fmt · clippy · nextest · audit · Docker build + gRPC smoke test  | ✅      |
| 2     | Write buffer — bounded channel, backpressure, batch flush             | 🚧      |
| 2     | DogStatsD UDP · Prometheus scrape · slog TCP ingestion adapters       | 🚧      |
| 2     | Kafka consumer adapter                                                | 🚧      |
| 3     | Query HTTP API — time range + tag filters + aggregation               | 🚧      |
| 3     | Alert worker + Postgres webhook outbox (transactional outbox pattern) | 🚧      |

---

## Quick Start

```bash
# Prerequisites: Docker, make
make up       # build image + start container (port 50051)
make logs     # follow server startup logs
make down     # stop
```

Send a test point:
```bash
grpcurl -plaintext \
  -import-path proto \
  -proto proto/storage/v1/storage.proto \
  -d '{"points":[{"metric_name":"cpu.load","tags":{"host":"web1"},"timestamp_ns":1000000000,"value":0.75}]}' \
  localhost:50051 \
  storage.v1.StorageService/Append
# → {"sequence": "1"}
```

Query it back:
```bash
grpcurl -plaintext \
  -import-path proto \
  -proto proto/storage/v1/storage.proto \
  -d '{"metric_name":"cpu.load","tag_filters":{"host":"web1"}}' \
  localhost:50051 \
  storage.v1.StorageService/Query
```

---

## CI

| Workflow                                             | Jobs                                                                       |
| ---------------------------------------------------- | -------------------------------------------------------------------------- |
| [`ci.yml`](.github/workflows/ci.yml)                 | `fmt` · `clippy -D warnings` · `cargo build` · `nextest` · `rustsec audit` |
| [`production.yml`](.github/workflows/production.yml) | Docker build (GHA layer cache) · gRPC smoke test (Append RPC via grpcurl)  |

---

## Performance

> Benchmarks in progress — see [`docs/benchmarks.md`](docs/benchmarks.md) once complete.

Target workload: high-cardinality metrics (100K+ unique series), sustained append-only writes, sub-millisecond query planning.

---

## Repository Layout

```
storage-engine/   Rust — WAL, chunk store, compaction, gRPC server
proto/            Protobuf contract (single boundary between Rust and Go)
ingestion/        Go — adapters, write buffer, gRPC client       [Phase 2]
query/            Go — HTTP API, aggregation, alert worker        [Phase 3]
```

---

`Rust` · `tonic` · `tokio` · `lz4` · `xxhash64` · `bincode` · `Docker` · `GitHub Actions`
