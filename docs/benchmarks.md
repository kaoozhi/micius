# Micius Storage Engine — Performance & Scalability Analysis

**Batch size:** 100 points/request · **Series cardinality:** 100,000 unique series

---

## Executive Summary

Through iterative architectural refinement — moving from a single-threaded mutex-bound model
to a sharded, asynchronous, group-commit architecture — Micius achieved a **29.5× increase**
in durable write throughput while maintaining fsync-before-ack durability guarantees at every
step.

| Milestone                          | pts/s       | req/s  | Key change                      |
| ---------------------------------- | ----------- | ------ | ------------------------------- |
| WAL Mutex baseline                 | **17,700**  | 177    | —                               |
| + Group commit (macOS APFS)        | **141,380** | 1,414  | One fsync per N concurrent writers |
| + 16-shard memtable (macOS APFS)   | **203,126** | 2,031  | Parallel BTreeMap inserts       |
| + 16-shard WAL (macOS APFS)        | **341,099** | 3,411  | Parallel fsyncs across shards   |
| + NVMe ext4 + 500µs delay (AMD EPYC) | **522,852** | 5,229  | CPU-bound ceiling               |

Each row removes one serialisation point. The final result on bare-metal Linux NVMe moves the
bottleneck off storage entirely — the engine becomes CPU-bound with 40% NVMe headroom
remaining.

---

## 1. Baseline — WAL Mutex Contention

**Platform:** macOS host, Apple Intel · **Storage:** APFS

The write-ahead log is protected by a single `Mutex<File>`. Every `Append` RPC acquires the
lock, calls `write_all`, and calls `fsync` before returning. This is correct and durable — but
serialises all writers through a single fsync.

| Workers | Throughput | pts/s      | P50    | P99    |
| ------- | ---------- | ---------- | ------ | ------ |
| 1       | 177 req/s  | **17,700** | 4.4ms  | 20.9ms |
| 100     | 239 req/s  | **23,873** | 414ms  | 604ms  |

**The convoy effect.** 100 workers achieve only 1.35× the throughput of a single worker.
Because each fsync takes ~4.4ms (APFS), 100 concurrent writers form a queue 100 deep — each
waits its turn while the disk drains:

```
P50 ≈ T_fsync × queue_depth = 4.4ms × ~100 = ~440ms  ✓
```

Adding more workers makes latency worse without improving throughput. The Mutex is the wall.

---

## 2. Group Commit — One fsync for N Concurrent Writers

**Insight:** concurrent writers do not each need their own fsync. If they all arrive within the
same ~4.5ms window, one `write_all + fsync` can serve them all. The WAL Mutex is replaced by
an `mpsc` channel: writers enqueue their frames and await a `oneshot` reply. A single
background `wal_task` drains the channel, writes all frames in one `write_all`, calls `fsync`
once, and replies to all waiters.

**Platform:** macOS host, Apple Intel · **Storage:** APFS · **Memtable:** single shard

| Workers | Throughput   | pts/s       | P50    | P99    |
| ------- | ------------ | ----------- | ------ | ------ |
| 1       | 193 req/s    | **19,333**  | 4.5ms  | 13.9ms |
| 100     | 1,414 req/s  | **141,380** | 69ms   | 124ms  |
| 200     | 1,459 req/s  | **145,864** | 132ms  | 206ms  |

**5.9× throughput increase at 100 workers** (23k → 141k pts/s). P50 drops from 414ms to 69ms.
Batch size emerges naturally from fsync latency × arrival rate — no artificial tuning needed
on APFS.

**New bottleneck identified.** Throughput plateaus at ~1,460 req/s regardless of worker count.
Decomposing the latency:

```
T_wal  ≈ 4.5ms    (dominated by APFS fsync — now amortised across ~11 req/batch)
T_mem  ≈ 0.66ms   (memtable BTreeMap insert, 100 pts under a single Mutex)

Memtable ceiling ≈ 1 / T_mem = 1 / 0.66ms ≈ 1,510 req/s  ✓
```

Group commit eliminated the WAL bottleneck and exposed the next one: the single
`Mutex<Memtable>`.

---

## 3. 16-Shard Memtable — Eliminating BTreeMap Lock Contention

**Insight:** the memtable does not need to be a single shared structure. Partition it into
16 independent shards, each with its own `Mutex<BTreeMap>`. Route each point to
`shard = hash(series_key) & 15` — a bitmask, no division, same series always lands on the
same shard. Writers on different series never contend.

Flush is decoupled from the RPC handler entirely: a 200ms periodic sweep drains shards
sequentially, advancing per-shard WAL watermarks so stale WAL segments can be safely deleted.

**Platform:** macOS host, Apple Intel · **Storage:** APFS · **WAL:** single shard

| Workers | Throughput   | pts/s       | P50    | P99    |
| ------- | ------------ | ----------- | ------ | ------ |
| 100     | 2,031 req/s  | **203,126** | 46ms   | 101ms  |
| 200     | 2,326 req/s  | **232,550** | 83ms   | 158ms  |
| 300     | 2,598 req/s  | **259,767** | 113ms  | 186ms  |

**10.9× vs the Mutex baseline.** Throughput continues scaling with workers, but a new ceiling
appears near ~2,600 req/s. The formula:

```
ceiling = batch_size / T_fsync = ~11.7 / 4.5ms ≈ 2,600 req/s  ✓
```

Memtable sharding removed the software ceiling. The new limit is hardware: a single WAL file
serialises all batches through one fsync queue. Parallelising fsyncs requires sharding the WAL
itself.

---

## 4. 16-Shard WAL — Parallel fsyncs

**Insight:** extend the sharding to the WAL. Sixteen independent segment directories
(`wal/shard-{i}/`), each with its own `wal_task` and fsync. The append handler groups points
by series hash, spawns one Tokio task per affected shard, and joins all handles before
inserting into the memtable. Total WAL latency becomes `max(shard latencies)`, not `sum`.

```
single WAL:   all batches → 1 fsync queue      → ceiling = batch_size / T_fsync
16-shard WAL: each request → ≤16 parallel fsyncs → wait = max(T_fsync[i]) ≈ T_fsync
```

**Platform:** macOS host, Apple Intel · **Storage:** APFS · **Shards:** 16

| Workers | Throughput   | pts/s       | P50   | P99    | vs single WAL |
| ------- | ------------ | ----------- | ----- | ------ | ------------- |
| 100     | 2,085 req/s  | **208,529** | 46ms  | 84ms   | +3%           |
| 200     | 2,793 req/s  | **279,306** | 67ms  | 147ms  | +20%          |
| 300     | 3,411 req/s  | **341,099** | 84ms  | 146ms  | **+31%**      |

**+31% at 300 workers** (260k → 341k pts/s), P50 drops from 113ms to 84ms. The benefit grows
with concurrency because more workers means more requests per round-trip, increasing parallel
fsync overlap. APFS provides partial parallelism across files in independent subdirectories —
enough to break the single-WAL ceiling.

WAL sharding also simplifies GC: each shard's watermark advances independently, so
`wals[i].drain_completed_before(watermarks[i])` runs immediately after each flush with no
cross-shard coordination.

#### Batch delay tuning on fast storage

WAL sharding on a macOS RAM disk (T_fsync ≈ 1.3ms) yielded **no benefit** — 3,295 req/s
versus 3,580 req/s for single WAL with a 500µs delay. Two compounding reasons:

1. **Mathematical cancellation.** Each shard receives 1/N of the load. Smaller batches cancel
   the parallelism gain when fsync is already fast.
2. **Spawn overhead.** 6 `tokio::spawn` calls per request at 3,000+ req/s ≈ 18k task
   spawns/sec — significant CPU overhead relative to a 1.3ms fsync window.

This motivated a configurable collect window (`MICIUS_WAL_BATCH_DELAY_US`): sleep D µs after
the first message arrives before draining the channel, extending the natural batch window on
fast storage.

**RAM disk — batch delay comparison (300 workers):**

| Delay  | Throughput   | pts/s       | P50    | batch_size |
| ------ | ------------ | ----------- | ------ | ---------- |
| 0µs    | 3,194 req/s  | 319,404     | 92.3ms | ~4.2       |
| 500µs  | 3,580 req/s  | **358,045** | 82.6ms | ~6.4       |
| 2000µs | 3,463 req/s  | 346,278     | 85.4ms | ~11.4      |

```
Optimal delay rule:  D ≈ 0.3–0.5 × T_fsync
RAM disk (F=1.3ms):  D = 500µs  → 3,580 req/s  (+12% vs no delay)
APFS     (F=4.5ms):  D = 0      → natural batching already large
```

500µs outperforms 2000µs because D < F: cycle time grows by 38% while batch size grows
proportionally — net gain. With D >> F, denominator growth overtakes numerator growth.

---

## 5. Bare Metal NVMe — CPU-Bound at 522k pts/s

**Platform:** Linux · AMD EPYC, 16 cores · NVMe 2 TB · ext4 · T_fsync ≈ 1.37ms

Deploying the full stack (16-shard WAL + 16-shard memtable) on bare-metal NVMe reveals a
fundamentally different operating regime.

**Without batch delay:**

| Workers | Throughput   | pts/s       | P50    | P99    |
| ------- | ------------ | ----------- | ------ | ------ |
| 1       | 731 req/s    | **73,136**  | 1.285ms| 2.575ms|
| 100     | 4,524 req/s  | **452,416** | 22ms   | 27ms   |
| 200     | 4,515 req/s  | **451,496** | 44ms   | 49ms   |
| 300     | 4,482 req/s  | **448,224** | 67ms   | 73ms   |
| 400     | 4,467 req/s  | **446,729** | 89ms   | 96ms   |
| 500     | 4,498 req/s  | **449,835** | 112ms  | 119ms  |

Single-writer throughput is 3.8× higher than APFS (731 vs 193 req/s) — proportional to the
fsync speedup (1.37ms vs 4.5ms). The multi-worker ceiling hits at **~4,500 req/s and stays
flat across a 5× worker range** — the group commit saturation signature.

**With 500µs batch delay (D/F = 0.36 — within the optimal 0.3–0.5 range):**

| Workers | Throughput   | pts/s       | P50   | P99    | vs no delay |
| ------- | ------------ | ----------- | ----- | ------ | ----------- |
| 100     | 5,229 req/s  | **522,852** | 19ms  | 23ms   | **+16%**    |
| 200     | 5,242 req/s  | **524,162** | 38ms  | 43ms   | +16%        |
| 300     | 5,182 req/s  | **518,184** | 57ms  | 65ms   | +16%        |
| 400     | 5,247 req/s  | **524,744** | 76ms  | 83ms   | +17%        |
| 500     | 5,195 req/s  | **519,494** | 95ms  | 108ms  | +15%        |

**The efficiency paradox.** Adding a 500µs delay *decreases* P50 at 100 workers (22ms → 19ms)
despite the artificial sleep. Higher throughput drains the queue faster — the waiting time
saved per request exceeds the 500µs added. The same rule derived on RAM disk holds on NVMe:
`D ≈ 0.3–0.5 × T_fsync`.

> **Note on `f/s = 0` in iostat:** ext4 `fsync()` forces a journal commit rather than an
> explicit `FLUSH CACHE` NVMe command, which is what iostat's `f/s` column counts. The fsyncs
> are executing — they show up as write I/O, not flush operations.

---

## System Utilization at Peak (522k pts/s)

`iostat -xd 1` during the 500-worker run with 500µs delay:

```
avg-cpu:  %user   %system  %iowait  %idle
          62.4%    24.3%     0.86%   12.4%

nvme1n1:  w/s=36,656  wkB/s=315,688  %util=39.5%  f/s=0.00
```

| Metric              | Value | Interpretation                                    |
| ------------------- | ----- | ------------------------------------------------- |
| CPU user + system   | 86%   | 13–14 of 16 cores saturated                       |
| %iowait             | 0.86% | Threads almost never blocked on disk              |
| NVMe %util          | 39%   | 60% of drive capacity unused                      |
| f/s (flush ops)     | 0     | ext4 journal commits, not explicit flush commands |

The engine is **CPU-bound, not I/O-bound.** CPU time is consumed by gRPC decoding, WAL
frame encoding (protobuf serialisation), Tokio task scheduling, and memtable BTreeMap
insertions. Storage is no longer the constraint.

---

## Bottleneck Migration Summary

| Layer             | Symptom                                             | Status     | Resolution                           |
| ----------------- | --------------------------------------------------- | ---------- | ------------------------------------ |
| WAL Mutex         | Throughput flat, P50 ∝ workers                      | ✅ Resolved | Group commit (one fsync per batch)   |
| Memtable lock     | Ceiling at ~146k pts/s · 1,460 req/s               | ✅ Resolved | 16-shard BTreeMap partitioning       |
| Single WAL fsync  | Ceiling at ~260k pts/s · 2,600 req/s               | ✅ Resolved | 16-shard WAL, parallel fsyncs (+31%) |
| Storage hardware  | Ceiling at ~341k pts/s · 3,400 req/s (APFS)        | ✅ Resolved | NVMe ext4: 522k pts/s (+53%)         |
| CPU               | Ceiling at ~522k pts/s · 5,200 req/s (AMD EPYC 16-core) | ⚡ Current  | io_uring · slab alloc · lock-free memtable |

---

## Key Observations

**Batching is math.** The group commit ceiling formula `throughput = batch_size / (D + T_fsync)`
proved accurate across all three platforms (APFS, RAM disk, NVMe ext4). The batch delay
parameter `D ≈ 0.3–0.5 × T_fsync` is a tunable knob, not a guess: under-delay wastes
parallelism, over-delay extends cycle time past the inflection point.

**Sharding must match core count.** Neither group commit nor WAL sharding fully unlocks
hardware until the memtable is also sharded. A single BTreeMap Mutex caps throughput
independent of how fast the WAL or disk is. 16 shards on a 16-core machine is not a
coincidence — each shard maps to roughly one core's worth of serialised work.

**Observability reveals the real bottleneck.** Without iostat, the natural assumption after
seeing a throughput ceiling would be "the disk is slow." The 0.86% iowait and 40% NVMe
utilisation at peak throughput falsify that immediately. The bottleneck migration from I/O
to CPU is only visible through system-level instrumentation — latency percentiles alone
cannot distinguish the two.

---

## Future Evolutions

The CPU ceiling (86% at 522k pts/s) is well-understood. Three independent directions could
push past it, each targeting a specific identified cost.

**io_uring WAL writes.** Standard `write() + fsync()` crosses the kernel boundary twice per
operation. At 5,200 req/s across 16 shards, this is 80k+ syscall pairs per second — the
source of the 24% system CPU. `io_uring` (via `tokio-uring`) submits batched I/O via a shared
ring buffer, eliminating per-operation syscall overhead and aligning naturally with the group
commit model. Estimated: **+15–25% throughput, tighter P99**.

**WAL frame buffer slab.** Each `wal_task` batch cycle allocates a fresh `Vec<u8>` for the
encoded frames, writes to disk, then drops it — 8,500 alloc/dealloc cycles per second across
16 shards. Moving the buffer outside the loop (`frames.clear()` instead of re-allocation)
reuses the same memory every cycle. The same physical pages stay warm in L2/L3 cache;
`extend_from_slice` writes into already-hot cache lines instead of cold heap addresses.
`max_batch` doubles as the sizing constraint: `capacity = max_batch × max_frame_size` ensures
the buffer never reallocates. Estimated: **+3–8%** — modest but a 2-line change.

**Lock-free or append-only memtable.** Each shard insert acquires a `Mutex`, traverses a
BTreeMap (O(log n), cache-unfriendly at 6k+ keys per shard), then releases. A concurrent
skip list (`crossbeam-skiplist`) removes the Mutex entirely; an append-only log-per-shard with
sort-at-flush trades read cost for constant-time writes. Estimated: **+10–20%, more effective
at >32 cores**.

| Evolution                        | CPU cost targeted            | Estimated gain |
| -------------------------------- | ---------------------------- | -------------- |
| io_uring WAL                     | Syscall overhead (24% sys)   | +15–25%        |
| WAL frame buffer slab            | Alloc pressure, cache misses | +3–8%          |
| Lock-free / append-only memtable | BTreeMap Mutex + tree ops    | +10–20%        |

Applied together, the theoretical ceiling approaches **1M+ pts/sec on a single node**,
constrained ultimately by network ingestion bandwidth rather than storage or CPU.
