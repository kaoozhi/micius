## Benchmark Results

**Batch size:** 100 points/request · **Series cardinality:** 100,000 unique series

---

### Baseline — WAL Mutex (pre-group-commit)

**Platform:** macOS host, Apple Intel (no Docker)

| Workers | Throughput | pts/s  | P50   | P90   | P99    |
| ------- | ---------- | ------ | ----- | ----- | ------ |
| 1       | 177 req/s  | 17,700 | 4.4ms | 9.1ms | 20.9ms |
| 100     | 239 req/s  | 23,873 | 414ms | 505ms | 604ms  |

Single-writer P50 reflects WAL fsync latency (~4.4ms APFS) — every Append RPC is durable
before returning. 100 workers serialise on the WAL Mutex: 4.4ms × queue depth ≈ 414ms P50.
Throughput scales only 1.35× from 1 to 100 workers because all concurrency funnels through
a single fsync.

---

### After WAL Group Commit

Group commit batches concurrent writes into one `write_all + fsync`, amortising disk cost
across all in-flight requests. The WAL Mutex is gone; writers enqueue on a channel and wait
on a oneshot reply.

#### macOS host — WAL group commit, single memtable

**Platform:** macOS host, Apple Intel · **WAL fsync:** ~4.5ms (APFS)

| Workers | Throughput  | pts/s   | P50   | P90   | P95   | P99    |
| ------- | ----------- | ------- | ----- | ----- | ----- | ------ |
| 1       | 193 req/s   | 19,333  | 4.5ms | 8.2ms | 9.3ms | 13.9ms |
| 20      | 1,017 req/s | 101,734 | 16ms  | 32ms  | 37ms  | 55ms   |
| 50      | 1,331 req/s | 133,106 | 36ms  | 44ms  | 48ms  | 87ms   |
| 100     | 1,414 req/s | 141,380 | 69ms  | 81ms  | 89ms  | 124ms  |
| 200     | 1,459 req/s | 145,864 | 132ms | 167ms | 184ms | 206ms  |

Ceiling at ~1,460 req/s. P50 scales linearly with workers while throughput flattens —
signature of the single memtable Mutex. Isolating the two components:

```
T_wal  ≈ 4.5ms    (WAL, dominated by APFS fsync)
T_mem  ≈ 0.66ms   (memtable BTreeMap insert, 100 pts under Mutex)
Memtable ceiling ≈ 1 / 0.66ms ≈ 1,510 req/s
```

#### macOS host — WAL group commit + 16-shard memtable

**Platform:** macOS host, Apple Intel · **WAL fsync:** ~4.5ms (APFS) · **Shards:** 16

| Workers | Throughput  | pts/s   | P50   | P90   | P95   | P99   |
| ------- | ----------- | ------- | ----- | ----- | ----- | ----- |
| 100     | 2,031 req/s | 203,126 | 46ms  | 66ms  | 75ms  | 101ms |
| 200     | 2,326 req/s | 232,550 | 83ms  | 106ms | 116ms | 158ms |
| 300     | 2,598 req/s | 259,767 | 113ms | 137ms | 148ms | 186ms |

Sharding removes the memtable bottleneck — throughput improves 44% at 100 workers
(1,414 → 2,031 req/s) and continues scaling toward a new ceiling near ~2,600 req/s.

The remaining ceiling is hardware-bound: `throughput = batch_size / fsync_latency`. With
APFS fsync at ~4.5ms, no software optimisation can push past this limit on the same device.
```
batch_size = 2,598 × 0.0045 ≈ 11.7 requests/fsync
WAL ceiling = 11.7 / 0.0045  ≈ 2,600 req/s  ✓
```

Memtable sharding eliminated the last software-level bottleneck — the ceiling is now set by storage hardware, not by lock contention.

#### macOS RAM disk — WAL group commit + 16-shard memtable + batch delay tuning

**Platform:** macOS host, Apple Intel · **Storage:** RAM disk (macOS `diskutil`) · **WAL fsync:** ~1.3ms

The RAM disk isolates storage latency: same software stack, no flash I/O. Used to probe the
WAL ceiling at lower fsync cost and motivate the `MICIUS_WAL_BATCH_DELAY_US` tuning parameter.

**No batch delay:**

| Workers | Throughput  | pts/s   | P50    | P90     | P99     |
| ------- | ----------- | ------- | ------ | ------- | ------- |
| 1       | 692 req/s   | 69,217  | 1.3ms  | 1.7ms   | 2.7ms   |
| 100     | 3,059 req/s | 305,929 | 31.9ms | 38.2ms  | 69.3ms  |
| 200     | 3,102 req/s | 310,208 | 63.3ms | 75.7ms  | 106.1ms |
| 300     | 3,194 req/s | 319,404 | 92.3ms | 111.1ms | 141.7ms |

Single-writer improves 3.6× over APFS (692 vs 193 req/s) — proportional to the fsync
speedup. Multi-worker ceiling improves only 23% (3,194 vs 2,598 req/s) because faster fsync
shortens the natural batch window, producing smaller batches:

```
batch_size = 3,194 × 0.0013 ≈ 4.2 req/batch  (vs 11.7 on APFS)
WAL ceiling = 4.2 / 0.0013 ≈ 3,230 req/s  ✓
```

The WAL group commit ceiling is confirmed as the bottleneck regardless of storage speed.

**With configurable batch delay (`MICIUS_WAL_BATCH_DELAY_US`):**

A collect window before flushing extends the natural batch window on fast storage, trading
added latency for larger batches. Counterintuitively, P50 improves alongside throughput
because higher throughput reduces queuing time more than the delay adds.

| Workers | Delay  | Throughput  | pts/s   | P50    | batch_size |
| ------- | ------ | ----------- | ------- | ------ | ---------- |
| 300     | 0µs    | 3,194 req/s | 319,404 | 92.3ms | ~4.2       |
| 300     | 2000µs | 3,463 req/s | 346,278 | 85.4ms | ~11.4      |
| 300     | 500µs  | 3,580 req/s | 358,045 | 82.6ms | ~6.4       |

500µs outperforms 2000µs because `D < F` gives the best ratio: the cycle time grows by 38%
while the batch window grows proportionally, yielding net throughput gain. With `D >> F`
(2000µs vs 1.3ms fsync), denominator growth overtakes numerator growth.

```
Optimal delay rule:  D ≈ 0.3–0.5 × fsync_latency
RAM disk (F=1.3ms):  D = 500µs  → 3,580 req/s  → 358k pts/s  (best)
APFS     (F=4.5ms):  D = 0      → natural batching already produces large batches
```

#### EC2 c5.large — EBS gp3 (Linux, no Docker)

**Platform:** AWS EC2 c5.large (2 vCPU, Intel), Ubuntu 24.04 · **WAL fsync:** ~4ms (EBS gp3)

| Workers | Throughput | pts/s  | P50    | P90    | P95    | P99    |
| ------- | ---------- | ------ | ------ | ------ | ------ | ------ |
| 1       | 225 req/s  | 22,526 | 4.3ms  | 4.6ms  | 4.9ms  | 6.3ms  |
| 10      | 791 req/s  | 79,060 | 11.7ms | 17.3ms | 22.5ms | 29.6ms |
| 100     | 788 req/s  | 78,774 | 121ms  | 149ms  | 180ms  | 218ms  |

EBS fsync (~4.3ms) is comparable to APFS (~4.5ms) — the throughput difference between
platforms is not fsync latency. Group commit eliminates WAL Mutex contention on both, but
the ceiling shifts to the **memtable Mutex** at ~790 req/s, lower than macOS due to slower
BTreeMap operations on EC2 Intel Xeon vs macOS Intel Core.

```
T_wal  ≈ 3.5ms   (WAL, dominated by EBS fsync)
T_mem  ≈ 0.9ms   (memtable BTreeMap insert, 100 pts under Mutex)
Memtable ceiling ≈ 1 / 0.9ms ≈ 1,100 req/s
```

---

### Analysis

| Bottleneck       | Symptom                                       | Status                           |
| ---------------- | --------------------------------------------- | -------------------------------- |
| WAL Mutex        | Throughput flat across workers, P50 ∝ workers | Resolved — WAL group commit      |
| Memtable Mutex   | Throughput plateaus at ~1,460 req/s           | Resolved — 16-shard partitioning |
| WAL group commit | Ceiling at ~2,600 req/s (batch_size / T_wal)  | Current ceiling                  |

Each optimisation shifts the bottleneck to the next layer. WAL group commit removed the
single-fsync serialisation; 16-shard memtable partitioning removed per-request Mutex
contention. The WAL group commit ceiling is `batch_size / (delay + T_wal)` regardless of
storage speed — confirmed on both APFS (4.5ms) and RAM disk (1.3ms). The `batch_delay`
parameter extends the collect window on fast storage, trading latency for larger batches.

The next architectural step to break past the single-WAL ceiling is WAL sharding: N
independent WAL files, each with its own wal_task and fsync, eliminating cross-shard
serialisation entirely.
