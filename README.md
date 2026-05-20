# Micius — Time-series observability platform backed by a custom storage engine
> Rust storage engine · Go ingestion adapters · gRPC API · alerting with webhook delivery


[![CI](https://github.com/kaoozhi/micius/actions/workflows/ci.yml/badge.svg)](https://github.com/kaoozhi/micius/actions/workflows/ci.yml)
[![Production](https://github.com/kaoozhi/micius/actions/workflows/production.yml/badge.svg)](https://github.com/kaoozhi/micius/actions/workflows/production.yml)
![Rust](https://img.shields.io/badge/rust-1.91%2B-orange)
![License](https://img.shields.io/badge/license-MIT-blue)

Micius covers the full observability stack: multi-source metrics ingestion (DogStatsD, Prometheus, Alpaca WebSocket, slog), a **custom Rust storage engine** exposed over gRPC, and a Go query layer with aggregation, alerting, and transactional webhook delivery. The storage engine is the foundation — built from scratch with an N-shard group-commit WAL (parallel fsyncs per shard, one per N concurrent writers), an N-shard BTreeMap memtable (N=`MICIUS_NUM_SHARDS`, default 16, must be a power of 2), columnar chunk files (delta-encoding + lz4 + bloom filters), an inverted tag index, and size-tiered compaction. It sustains **340k+ points/sec** durable writes (fsync before ack) at 100k-series cardinality on macOS/APFS, and **520k+ points/sec** on Linux NVMe ext4 (AMD EPYC) where the ceiling shifts from storage to CPU.

---

## Architecture

```
[UDP metrics]    ─┐
[WebSocket feed] ─┤─► [Write Buffer] ─── gRPC Append ─────────────────┐
[HTTP scrape]    ─┤    (Go ingestion)                                 │
[TCP log stream] ─┘                                                   ▼
                                                  ┌──────────────────────────────────────────┐
                                                  │  WAL  (16 shards · group commit · CRC32) │
                                                  │  Memtable  (16 shards · BTreeMap)        │
                                                  │  Chunk files  (δ-encode · lz4)           │
                                                  │  ChunkIndex  (inverted · pruning)        │
                                                  │  Compaction  (size-tiered)               │
                                                  └───────────────────┬──────────────────────┘
                                                                      │ gRPC (read)
                                                      ┌───────────────▼──────────────────┐
                                HTTP API ◄────────────┤  Aggregation engine  (Go)        │
                                Webhooks ◄────────────┤  Alert worker                    │
                                                      │  Webhook outbox (Postgres)       │
                                                      └──────────────────────────────────┘
```

Phase 1 (Rust storage engine) is complete. Phases 2–3 (Go ingestion and query layers) are in progress.

---

## Storage Engine Internals

```
  ┌──────── Startup Recovery  (runs once · gates all traffic) ─────────────────┐
  │                                                                            │
  │  1. load index snapshot  ──► ChunkIndex  (or empty on first start)         │
  │  2. WAL replay  (16 shards · CRC32 per frame · stop at first torn write)   │
  │  3. flush recovered points  ──► .mcs chunk  ──► ChunkIndex.register()      │
  │  4. WAL.rotate() + drain_completed(last_seq | watermark)  ──► delete segs  │
  │  5. open WAL writer  (resume_seq = recovery.last_sequence)                 │
  │                                                                            │
  │  ─── only after step 5: gRPC server + background tasks start ───────────   │
  │                                                                            │
  └────────────────────────────────────────────────────────────────────────────┘

  ┌──────── Write Path (Fan-out / Fan-in) ─────────────────────────────────────┐
  │                                                                            │
  │  gRPC Append  (diverse series)                                             │
  │       │                                                                    │
  │       │  group by shard = hash(series_key) & (N-1)                         │
  │       │          ↓ FAN-OUT — spawn one Tokio task per shard hit            │
  │       ├──► shard 0  ── write_all + fsync ──► wal/shard-0/  ─┐              │
  │       ├──► shard 5  ── write_all + fsync ──► wal/shard-5/  ─┤              │
  │       ├──► shard 9  ── write_all + fsync ──► wal/shard-9/  ─┤              │
  │       └──► shard 14 ── write_all + fsync ──► wal/shard-14/ ─┘              │
  │                  ↓ FAN-IN — join_all                                       │
  │       total wait = max(T_fsync[i]),  not  sum(T_fsync[i])                  │
  │       │                                                                    │
  │       ├─► memtables[0].insert()  ─┐                                        │
  │       ├─► memtables[5].insert()   ├─ sequential · one Mutex per shard      │
  │       ├─► memtables[9].insert()   │                                        │
  │       └─► memtables[14].insert() ─┘                                        │
  │       │   flush decisions off the hot path — handled by periodic sweep     │
  │       │                                                                    │
  │       └─ return Ok  (fsync already on disk · durable)                      │
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
  │  N WAL group commit tasks  (one per shard · spawned at startup)            │
  │      ├─ recv().await  park until first message arrives                     │
  │      ├─ try_recv() drain  collect backlog up to max_batch (non-blocking)   │
  │      ├─ write_all + sync_all  one fsync for the entire shard batch         │
  │      ├─ last_seq.store(Ordering::Release)  publish watermark atomically    │
  │      └─ oneshot replies  unblock all waiting Append RPCs for this shard    │
  │                                                                            │
  │  Compaction worker  (every N secs, Mutex released between cycles)          │
  │      compact_once()                                                        │
  │      ├─ find_candidates()   group chunks by size ratio  (file_sizes map)   │
  │      ├─ merge_group()  →  new .mcs  (read all series · deduplicate)        │
  │      ├─ ChunkIndex register + deregister  (atomic under write lock)        │
  │      └─ delete old .mcs files  (after index update)                        │
  │                                                                            │
  │  Memtable sweep + WAL GC  (every 200ms · sequential shard scan)            │
  │      for each shard: drain if should_flush()                               │
  │      ├─ ChunkWriter.write()  →  .mcs file  (δ-encode · lz4 · bloom)        │
  │      ├─ ChunkIndex.register()  (under write lock, no disk I/O held)        │
  │      ├─ shard_watermarks[i].store(wal.current_sequence(), Release)         │
  │      └─ wals[i].drain_completed_before(persisted_watermarks[i])  ──► GC    │
  │                                                                            │
  │  Snapshot worker  (every N secs · MICIUS_INDEX_SNAPSHOT_INTERVAL_SECS)     │
  │      shard_watermarks[i].load(Acquire) × 16  +  ChunkIndex  ──► index.bin  │
  │      persisted_watermarks[i].store(w, Release)  (after save_index fsync)   │
  │      WAL GC for covered segments unblocked on the next sweep               │
  │                                                                            │
  └────────────────────────────────────────────────────────────────────────────┘
```

---

## Performance

Numbers collected with a concurrent gRPC load generator ([`bench/load`](bench/load), Go) — configurable worker count and duration, each worker sending batches of 100 points across 100,000 distinct series and recording per-request latency. All writes are durable (fsync before ack).

**Batch size:** 100 points/request · **Series cardinality:** 100,000 unique series · **29.5× throughput increase baseline → peak**

Each optimisation removes one serialisation point and exposes the next bottleneck:

| Milestone                            | **pts/s**   | req/s | Bottleneck removed                 |
| ------------------------------------ | ----------- | ----- | ---------------------------------- |
| WAL Mutex baseline (macOS APFS)      | **17,700**  | 177   | —                                  |
| + Group commit                       | **141,380** | 1,414 | WAL fsync serialisation (5.9×)     |
| + 16-shard memtable                  | **203,126** | 2,031 | Memtable Mutex contention (10.9×)  |
| + 16-shard WAL, parallel fsyncs      | **341,099** | 3,411 | Single WAL fsync queue (+31%)      |
| + NVMe ext4 + 500µs delay (AMD EPYC) | **522,852** | 5,229 | Storage → CPU-bound ceiling (+53%) |

On bare-metal Linux NVMe ext4 the engine reaches its current ceiling with storage no longer the constraint: 86% CPU utilisation, 0.86% iowait, NVMe at 40% capacity. The 500µs batch delay (D ≈ 0.36 × T_fsync) extends the natural batch window for +16% throughput — and counterintuitively lowers P50, since higher throughput drains the queue faster than 500µs adds.

See [`docs/benchmarks.md`](docs/benchmarks.md) for the full analysis: per-platform tables, iostat breakdown, batch delay tuning, and future architectural directions (io_uring, slab allocation, lock-free memtable).

---

## Design Highlights

### Concurrent correctness without deadlocks

The WAL is lock-free on the write path — writers enqueue via channel and await a oneshot reply; no Mutex is held. The memtable is partitioned into N shards (N=`MICIUS_NUM_SHARDS`), each with its own Mutex; at most one shard lock is held at a time, acquired in ascending index order. The engine enforces a strict acquisition order across **all** code paths:

```
Write path:  N parallel WAL channel sends (no lock · tokio tasks) → join → Memtable[i] Mutex → released
Sweep path:  Memtable[i] Mutex (drain) → released → Index RwLock write → released → WAL channel send
Query path:  Memtable[i] Mutex → released (per shard) → Index RwLock read → released before disk I/O
Snapshot:    Index RwLock read  (shard watermarks are Vec<AtomicU64> — no lock needed)
```

The sweep holds the Index write lock only for `register()`, never while doing disk I/O. The query path releases the Index read lock before any `read_series()` call. No two locks from different levels are ever held simultaneously. Eight concurrent-read/concurrent-write integration tests verify this under real multi-thread load with `#[tokio::test(flavor = "multi_thread", worker_threads = 4)]`.

### Durability before acknowledgement

Every `Append` RPC waits for the WAL group commit task to call `fsync` before returning `Ok` — the durability guarantee is unchanged, only the mechanism differs. No reply is sent until `sync_all()` has completed for the batch containing that request. On crash recovery, the engine:
1. Loads the last index snapshot (bincode)
2. Replays WAL entries with CRC32 verification — skips entries already in the snapshot (sequence ≤ shard watermark), stops at first torn write
3. Flushes recovered points to a new chunk file
4. Rotates and deletes stale WAL segments
5. Only then starts accepting live traffic

**WAL GC safety: gated on snapshot durability**

A subtle durability gap exists between flush and snapshot. After the periodic sweep flushes a shard to a chunk file and registers it in the in-memory `ChunkIndex`, the chunk is durably on disk — but the index snapshot may not have been saved yet (the snapshot task fires every `MICIUS_INDEX_SNAPSHOT_INTERVAL_SECS` seconds, default 60). If WAL GC ran immediately using the live `shard_watermarks`, it would delete the segments that produced that chunk. A crash before the next snapshot would leave the chunk orphaned: invisible to the next startup because the loaded index doesn't know it exists, and with no WAL left to reconstruct it. Data silently lost.

The fix is `persisted_watermarks` — a second `Vec<Arc<AtomicU64>>` that only advances when `save_index()` returns `Ok`. WAL GC uses `persisted_watermarks` (via `Acquire` load), not `shard_watermarks`. Until a snapshot durably records a chunk in the index, the WAL segments that produced it are retained on disk:

```
flush    → shard_watermarks[i].store(seq, Release)      (live watermark advances)
snapshot → persisted_watermarks[i].store(seq, Release)  (after fsync · GC now safe)
GC       → gc_seq = persisted_watermarks[i].load(Acquire)
            drain_completed_before(gc_seq)  deletes only snapshot-confirmed segments
```

Three-state safety window:
1. **Flush done, no snapshot yet** — `shard_watermarks > persisted_watermarks` — GC blocked, segments retained
2. **Snapshot saved** — `persisted_watermarks` catches up — GC unblocked for those segments on the next sweep
3. **Crash between flush and snapshot** — restart loads old snapshot (watermark = last persisted), replays only WAL entries above that watermark, rebuilds the chunk — no double-counting of already-snapshotted entries

### WAL group commit — one fsync for N concurrent writers

Instead of one `fsync` per writer, concurrent Append RPCs enqueue frames on a channel and wait on a oneshot reply. A single background task drains the channel, writes all frames in one `write_all`, and calls `fsync` once for the whole batch — amortising disk cost across all in-flight requests without sacrificing durability.

```
Before  100 workers × WAL Mutex  →   239 req/s  → 23k pts/s P50 414ms
After   100 workers × group commit → 1,414 req/s → 141k pts/s P50  69ms   (5.9× throughput · 6× P50)
```

Batch size emerges naturally from fsync latency × arrival rate — no artificial delay needed. Group commit shifts the bottleneck from WAL serialisation to the single memtable Mutex — addressed next by sharding.

### N-shard memtable — eliminate lock contention on the hot write path

WAL group commit exposed the next bottleneck: all concurrent writers serialising on a single `Mutex<Memtable>` for BTreeMap insertions (~0.66ms/request, ceiling ~1,460 req/s). The fix partitions the memtable into N independent shards (N=`MICIUS_NUM_SHARDS`, default 16, must be a power of 2 — one per core is the tuning heuristic), each with its own lock:

- **Routing**: each point hashes to `shard = hash(metric_name + sorted_tags) & (N-1)` — a single bitmask, no division, same series always routes to the same shard (N must be a power of 2 for the bitmask to be correct)
- **Write path**: acquires only the relevant shard locks, never two simultaneously
- **Flush path**: removed from the RPC handler entirely — a 200ms periodic sweep scans shards sequentially, draining any that exceed their per-shard threshold
- **WAL safety**: each shard publishes its own `shard_watermarks[i]` after flush; WAL GC is gated on `persisted_watermarks[i]` — a separate watermark that only advances after `save_index()` fsync succeeds — so segments are never deleted before the index snapshot that covers them is durable on disk

```
Before  100 workers × single Mutex →  141k pts/s  P50  69ms  (1,414 req/s)
After   100 workers × 16 shards   →  203k pts/s  P50  46ms  (2,031 req/s)  (10.9× vs baseline)
        300 workers × 16 shards   →  260k pts/s  P50 113ms  (2,598 req/s)  (hardware ceiling)
```

The ceiling is now `batch_size / fsync_latency` — a hardware limit, not a software one.

### N-shard WAL — parallel fsyncs, total wait = max(shard latencies)

Memtable sharding shifted the bottleneck back to the WAL: with a single WAL file, all batches still serialise through one fsync queue, capping throughput at `batch_size / T_fsync`. Sharding the WAL breaks this — N independent segment directories (`wal/shard-{i}/`), each with its own `wal_task` and fsync running concurrently.

The append handler groups each point by series hash, spawns one Tokio task per affected shard, and joins all handles before inserting into the memtable. Total WAL latency becomes `max(shard latencies)`, not `sum`:

```
single WAL:  all requests → 1 fsync queue        → ceiling = batch_size / T_fsync
N-shard WAL: each request → ≤N parallel fsyncs   → total wait = max(T_fsync[i])
```

Per-shard WAL GC is simpler than the single-WAL case: after each shard flush the sweep calls `wals[i].drain_completed_before(persisted_watermarks[i])`. `persisted_watermarks` advance only after a successful `save_index` fsync, decoupling GC eligibility from flush completion (see Durability section below).

```
Before  300 workers × single WAL   → 2,598 req/s → 260k pts/s  P50 113ms
After   300 workers × 16-shard WAL → 3,411 req/s → 341k pts/s  P50  84ms  (+31%)
```

The benefit scales with `T_fsync / spawn_overhead`. On APFS (4.5ms) it yields +31%; on a RAM disk (1.3ms) spawn overhead dominates and the gain disappears. Linux NVMe with truly independent devices per shard would approach linear scaling.

### Read amplification controlled at the index layer

The inverted tag index (`HashMap<(tag_key, tag_value), HashSet<SeriesId>>`) resolves matching series in `O(min(|A|, |B|))` via set intersection — no full scan. `prune_chunks` eliminates chunk files using:
- **Time-range pruning** — BTreeMap `range(..=end)` + overlap filter, zero disk I/O
- **Statistics pushdown** — per-series min/max stored alongside chunk metadata; GT/LT/Between predicates eliminate chunks before decompression

Most queries touch zero disk files before `read_series` is called.

### Write path trade-offs are explicit

Size-tiered compaction (over leveled) was chosen because the workload is write-heavy and append-only: fewer rewrites, lower write amplification, acceptable read amplification controlled by the index above. Delta-encoding timestamps before lz4 exploits the regularity of time-series intervals — consecutive `Δt` values are near-zero, compressing to ~1 byte each vs. 8 bytes raw.

---

## Implementation Status

| Phase | Component                                                             | Status |
| ----- | --------------------------------------------------------------------- | ------ |
| 1     | WAL — fsync, segment rotation, CRC32 torn-write detection             | ✅      |
| 1     | Memtable — BTreeMap, size threshold, atomic flush                     | ✅      |
| 1     | Columnar chunk files — delta-encoding, lz4, bloom filter, CRC32       | ✅      |
| 1     | ChunkIndex — inverted tag index, time-range + stats pruning           | ✅      |
| 1     | Index persistence — bincode snapshot + WAL sequence recovery          | ✅      |
| 1     | Size-tiered compaction                                                | ✅      |
| 1     | gRPC server — Append, Query (streaming), Compact, Snapshot            | ✅      |
| 1     | WAL group commit — channel-based batching, one fsync per N writers    | ✅      |
| 1     | N-shard WAL — parallel fsyncs, per-shard recovery and GC              | ✅      |
| 1     | N-shard memtable — per-shard Mutex, periodic sweep, WAL watermarks    | ✅      |
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

Run a load test:

```bash
# Terminal 1 — start the server (choose one)

# Option A: container
make up && make logs

# Option B: binary directly (smaller flush threshold exposes WAL GC sooner)
MICIUS_MEMTABLE_FLUSH_MB=4 \
MICIUS_WAL_DIR=./data/micius/wal \
MICIUS_CHUNK_DIR=./data/micius/chunks \
MICIUS_INDEX_PATH=./data/micius/index.bin \
MICIUS_GRPC_ADDR=0.0.0.0:50051 \
MICIUS_METRICS_ADDR=0.0.0.0:9091 \
  ./target/release/storage-engine
```

```bash
# Terminal 2 — run the load test
make bench-load WORKERS=100 DURATION=30s
```

`WORKERS` controls concurrent gRPC goroutines; `DURATION` is the wall-clock run window. Results are printed as throughput (pts/s), request rate (req/s), and P50/P95/P99 latencies.

---

## CI

Two workflows run on every push. [`ci.yml`](.github/workflows/ci.yml) enforces formatting, zero clippy warnings, and runs the full test suite including a security audit. [`production.yml`](.github/workflows/production.yml) builds the multi-stage Docker image with GHA layer caching, then runs a live gRPC smoke test — a real `Append` RPC sent to the running container via grpcurl, asserting a valid sequence number response.

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

---

> **Why Micius?** Named after Mozi (墨子), a 5th-century BCE Chinese philosopher and engineer who pioneered empirical measurement — among the first to systematically observe, record, and reason about physical phenomena. A time-series storage engine is the same discipline applied to software: record every observation, answer any question about it later.
