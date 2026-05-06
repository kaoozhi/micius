## Benchmark Results

**Platform:** Docker/Linux (macOS Colima), Apple Intel 
**Batch size:** 100 points · **Series cardinality:** 100,000 unique series

| Workers | Throughput   | P50   | P90   | P99   |
| ------- | ------------ | ----- | ----- | ----- |
| 1       | 25,780 pts/s | 3.5ms | 4.6ms | 8.5ms |
| 100     | 39,443 pts/s | 215ms | 402ms | 603ms |

Single-writer P50 of 3.5ms reflects WAL fsync latency —
every Append RPC is durable before returning.
Multi-writer queuing: 100 workers serialise on the WAL Mutex
(~3ms × queue depth = observed 215ms P50).

Next optimisation: WAL group commit — batch concurrent writes
into one fsync to increase throughput without sacrificing durability.
Bare-metal Linux: expect 2–5× better throughput (fsync 0.5–1ms vs ~3ms in Docker VM).
