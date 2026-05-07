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
  │  WAL channel ── batch write_all + fsync once ──► segment file (CRC32/frame)│
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
  │  WAL group commit task  (continuous · spawned once at startup)             │
  │      ├─ recv().await  park until first message arrives                     │
  │      ├─ try_recv() drain  collect backlog up to max_batch (non-blocking)   │
  │      ├─ write_all + sync_all  one fsync for the entire batch               │
  │      ├─ last_seq.store(Ordering::Release)  publish watermark atomically    │
  │      └─ oneshot replies  unblock all waiting Append RPCs                   │
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

## Performance

**Platform:** macOS host (Intel) · 100,000-series cardinality · 100 points/request

| Concurrency | WAL strategy | Throughput  | pts/s   | P50   | P99    |
| ----------- | ------------ | ----------- | ------- | ----- | ------ |
| 1 worker    | Mutex        | 177 req/s   | 17,700  | 4.4ms | 20.9ms |
| 100 workers | Mutex        | 239 req/s   | 23,873  | 414ms | 604ms  |
| 100 workers | Group commit | 1,414 req/s | 141,380 | 69ms  | 124ms  |
| 200 workers | Group commit | 1,459 req/s | 145,864 | 132ms | 206ms  |

Group commit delivers **5.9× throughput** and **6× P50 reduction** at 100 workers. The ceiling at ~1,460 req/s is the memtable Mutex — the WAL is no longer the bottleneck.

See [`docs/benchmarks.md`](docs/benchmarks.md) for full results including EC2 c5.large analysis and bottleneck decomposition.

---

## Design Highlights

### Concurrent correctness without deadlocks

WAL, memtable, and chunk index each have independent locks. The engine enforces a strict acquisition order across **all** code paths:

```
WAL Mutex → released → Memtable Mutex → released → Index RwLock (write)
WAL Mutex (temporary, released at semicolon) → Index RwLock (read)
```

The flush path holds the Index write lock only for `register()`, never while doing disk I/O. The query path holds the Index read lock only during in-memory pruning, releasing it before any `read_series()` call. Eight concurrent-read/concurrent-write integration tests verify this under real multi-thread load with `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`.

### WAL group commit — one fsync for N concurrent writers

Instead of one `fsync` per writer, concurrent Append RPCs enqueue frames on a channel and wait on a oneshot reply. A single background task drains the channel, writes all frames in one `write_all`, and calls `fsync` once for the whole batch — amortising disk cost across all in-flight requests without sacrificing durability.

```
Before  100 workers × WAL Mutex  →   239 req/s  → 23k pts/s P50 414ms
After   100 workers × group commit → 1,414 req/s → 141k pts/s P50  69ms   (5.9× throughput · 6× P50)
```

Batch size emerges naturally from fsync latency × arrival rate — no artificial delay needed. Group commit shifts the bottleneck from the WAL to the memtable Mutex, which now gates throughput at 141k pt/s (~1,460 req/s) on this hardware.

### Durability before acknowledgement

Every `Append` RPC waits for the WAL group commit task to call `fsync` before returning `Ok` — the durability guarantee is unchanged, only the mechanism differs. No reply is sent until `sync_all()` has completed for the batch containing that request. On crash recovery, the engine:
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

## Repository Layout

```
storage-engine/   Rust — WAL, chunk store, compaction, gRPC server
proto/            Protobuf contract (single boundary between Rust and Go)
ingestion/        Go — adapters, write buffer, gRPC client       [Phase 2]
query/            Go — HTTP API, aggregation, alert worker        [Phase 3]
```

---

`Rust` · `tonic` · `tokio` · `lz4` · `xxhash64` · `bincode` · `Docker` · `GitHub Actions`
