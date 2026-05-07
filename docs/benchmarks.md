## Benchmark Results

**Batch size:** 100 points/request · **Series cardinality:** 100,000 unique series

---

### Baseline — WAL Mutex (pre-group-commit)

**Platform:** Docker on macOS (Colima VM), Apple Intel

| Workers | Throughput    | P50    | P90    | P99    |
|---------|---------------|--------|--------|--------|
| 1       | 25,780 pts/s  | 3.5ms  | 4.6ms  | 8.5ms  |
| 100     | 39,443 pts/s  | 215ms  | 402ms  | 603ms  |

Single-writer P50 reflects WAL fsync latency — every Append RPC is durable before returning.
100 workers serialise on the WAL Mutex: ~3ms × queue depth = 215ms P50. Throughput barely
scales beyond 1 worker because all concurrency funnels through a single fsync.

---

### After WAL Group Commit

Group commit batches concurrent writes into one `write_all + fsync`, amortising disk cost
across all in-flight requests. The WAL Mutex is gone; writers enqueue on a channel and wait
on a oneshot reply.

#### macOS host (APFS, no Docker)

**Platform:** macOS host, Apple Silicon · **WAL fsync:** ~4.5ms (APFS)

| Workers | Throughput     | pts/s       | P50    | P90    | P95    | P99     |
|---------|----------------|-------------|--------|--------|--------|---------|
| 1       | 193 req/s      | 19,333      | 4.5ms  | 8.2ms  | 9.3ms  | 13.9ms  |
| 20      | 1,017 req/s    | 101,734     | 16ms   | 32ms   | 37ms   | 55ms    |
| 50      | 1,331 req/s    | 133,106     | 36ms   | 44ms   | 48ms   | 87ms    |
| 100     | 1,414 req/s    | 141,380     | 69ms   | 81ms   | 89ms   | 124ms   |
| 200     | 1,459 req/s    | 145,864     | 132ms  | 167ms  | 184ms  | 206ms   |

Throughput scales 7.5× from 1 to 200 workers. Ceiling confirmed at ~1,460 req/s between
100 and 200 workers — P50 continues to scale linearly with workers while throughput flattens,
the signature of a single serialised lock (memtable Mutex).

Isolating the two components from the 1-worker and 200-worker data points:

```
T_wal  ≈ 4.5ms    (WAL, dominated by APFS fsync)
T_mem  ≈ 0.66ms   (memtable BTreeMap insert, 100 pts under Mutex)
Memtable ceiling ≈ 1 / 0.66ms ≈ 1,510 req/s
```

#### EC2 c5.large — EBS gp3 (Linux, no Docker)

**Platform:** AWS EC2 c5.large (2 vCPU, Intel), Ubuntu 24.04 · **WAL fsync:** ~4ms (EBS gp3)

| Workers | Throughput   | pts/s      | P50    | P90    | P95    | P99    |
|---------|--------------|------------|--------|--------|--------|--------|
| 1       | 225 req/s    | 22,526     | 4.3ms  | 4.6ms  | 4.9ms  | 6.3ms  |
| 10      | 791 req/s    | 79,060     | 11.7ms | 17.3ms | 22.5ms | 29.6ms |
| 100     | 788 req/s    | 78,774     | 121ms  | 149ms  | 180ms  | 218ms  |

EBS fsync (~4.3ms) is comparable to APFS (~4.5ms) — the throughput difference between
platforms is not fsync latency. Group commit eliminates WAL Mutex contention on both, but
the ceiling shifts to the **memtable Mutex** at ~790 req/s, lower than macOS due to slower
BTreeMap operations on EC2 Intel vs Apple Silicon.

```
T_wal  ≈ 3.5ms   (WAL, dominated by EBS fsync)
T_mem  ≈ 0.9ms   (memtable BTreeMap insert, 100 pts under Mutex)
Memtable ceiling ≈ 1 / 0.9ms ≈ 1,100 req/s
```

---

### Analysis

| Bottleneck       | Symptom                                          | Status   |
|------------------|--------------------------------------------------|----------|
| WAL Mutex        | Throughput flat across workers, P50 ∝ workers   | Resolved — group commit |
| Memtable Mutex   | Throughput plateaus, P50 ∝ workers              | Next optimisation |

Group commit shifts the bottleneck from WAL to memtable. Both platforms share similar fsync
latency (~4–4.5ms); the throughput difference (1,460 vs 790 req/s) comes from Apple Silicon
handling BTreeMap operations ~35% faster than EC2 Intel (T_mem 0.66ms vs 0.9ms).

The next step is to batch memtable writes the same way: collect all points from a completed
WAL batch and insert them under a single lock acquisition, reducing per-request lock overhead
from O(workers) to O(1).
