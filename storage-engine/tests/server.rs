/// Server-level integration tests.
///
/// Tests call the gRPC handler methods directly on `StorageServer` (no network
/// stack) via `StorageServer::open` — the same startup path as production,
/// including WAL recovery and index snapshot loading.
use std::collections::HashMap;
use std::sync::Arc;
use storage_engine::config::StorageConfig;
use storage_engine::proto::storage::v1::{
    AppendRequest, DataPoint, QueryRequest, storage_service_server::StorageService,
};
use storage_engine::server::StorageServer;
use tempfile::{TempDir, tempdir};
use tokio::time::Duration;
use tokio_stream::StreamExt;
use tonic::Request;

// ── Helpers ───────────────────────────────────────────────────────────────────

/// Initialize the tracing subscriber once for the entire test binary.
/// Safe to call from multiple concurrent tests — `OnceLock` guarantees
/// the subscriber is registered exactly once regardless of call order.
fn init_tracing() {
    static TRACING: std::sync::OnceLock<()> = std::sync::OnceLock::new();
    TRACING.get_or_init(|| {
        let _ = tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .try_init();
    });
}

/// Build a `StorageConfig` pointing entirely inside `dir` — safe for parallel tests.
fn test_config(dir: &TempDir) -> StorageConfig {
    init_tracing();
    StorageConfig {
        wal_dir: dir.path().join("wal"),
        chunk_dir: dir.path().join("chunks"),
        index_path: dir.path().join("index.bin"),
        wal_max_segment_bytes: 64 * 1024 * 1024,
        wal_channel_capacity: 1024,
        wal_max_batch: 256,
        wal_batch_delay_us: 0,
        memtable_flush_threshold_bytes: 32 * 1024 * 1024,
        num_shards: 16,
        index_snapshot_interval_secs: 60,
        compaction_interval_secs: 300,
        compaction_min_threshold: 2,
        compaction_size_ratio: 2.0,
        grpc_addr: "0.0.0.0:50051".to_string(),
        metrics_addr: "0.0.0.0:9091".to_string(),
    }
}

fn data_point(metric: &str, host: &str, ts_ns: i64, value: f64) -> DataPoint {
    DataPoint {
        metric_name: metric.to_string(),
        tags: HashMap::from([("host".to_string(), host.to_string())]),
        timestamp_ns: ts_ns,
        value,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────────────

/// Basic write → read round-trip through the real server handler stack.
/// Verifies WAL fsync, memtable insert, and query against unflushed data.
#[tokio::test]
async fn test_append_then_query_round_trip() {
    let dir = tempdir().unwrap();
    let config = test_config(&dir);
    config.ensure_dirs().await.unwrap();
    let server = StorageServer::open(&config).await.expect("open failed");

    // Write 3 points for one series
    let seq = server
        .append(Request::new(AppendRequest {
            points: vec![
                data_point("cpu.load", "web1", 1_000_000_000, 0.25),
                data_point("cpu.load", "web1", 2_000_000_000, 0.50),
                data_point("cpu.load", "web1", 3_000_000_000, 0.75),
            ],
        }))
        .await
        .expect("append failed")
        .into_inner()
        .sequence;

    assert!(seq > 0, "sequence must be positive");

    // Query them back from the memtable (not yet flushed)
    let mut stream = server
        .query(Request::new(QueryRequest {
            metric_name: "cpu.load".to_string(),
            tag_filters: HashMap::from([("host".to_string(), "web1".to_string())]),
            time_start_ns: None,
            time_end_ns: None,
        }))
        .await
        .expect("query failed")
        .into_inner();

    let mut points = Vec::new();
    while let Some(resp) = stream.next().await {
        points.push(resp.expect("stream error"));
    }

    assert_eq!(points.len(), 3, "all 3 written points must be returned");
    assert!(
        points.iter().all(|p| p.series_id != 0),
        "series_id must be set"
    );
}

/// Data written before a simulated crash must survive restart via WAL recovery.
/// This exercises the full `StorageServer::open` path including WAL replay.
#[tokio::test]
async fn test_data_survives_restart() {
    let dir = tempdir().unwrap();
    let config = test_config(&dir);
    config.ensure_dirs().await.unwrap();

    // Session 1 — write data then drop the server (simulates crash / shutdown)
    {
        let server = StorageServer::open(&config).await.expect("open failed");
        server
            .append(Request::new(AppendRequest {
                points: vec![
                    data_point("cpu.load", "web1", 1_000_000_000, 0.25),
                    data_point("cpu.load", "web1", 2_000_000_000, 0.50),
                ],
            }))
            .await
            .expect("append failed");
        // server dropped here — WAL is fsynced but data not yet flushed to chunk
    }

    // Session 2 — reopen at same dirs; WAL replay must recover the 2 points
    {
        let server = StorageServer::open(&config).await.expect("reopen failed");

        let mut stream = server
            .query(Request::new(QueryRequest {
                metric_name: "cpu.load".to_string(),
                tag_filters: HashMap::new(),
                time_start_ns: None,
                time_end_ns: None,
            }))
            .await
            .expect("query failed")
            .into_inner();

        let mut total = 0usize;
        while let Some(resp) = stream.next().await {
            resp.expect("stream error");
            total += 1;
        }
        assert_eq!(
            total, 2,
            "both points must survive the restart via WAL recovery"
        );
    }
}

/// Concurrent appends from multiple tasks — exercises per-shard WAL and
/// memtable locking under real concurrency. Verifies no data loss.
/// Note: sequences are no longer globally unique — each WAL shard has its own
/// sequence space, so two tasks writing to different shards can return the same
/// sequence number. Data integrity (all points queryable) is the meaningful guarantee.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_appends_are_serialised() {
    let dir = tempdir().unwrap();
    let config = test_config(&dir);
    config.ensure_dirs().await.unwrap();
    let server = Arc::new(StorageServer::open(&config).await.expect("open failed"));

    let n_tasks = 8;
    let n_points = 10;

    let mut set = tokio::task::JoinSet::new();
    for task_id in 0..n_tasks {
        let srv = Arc::clone(&server);
        set.spawn(async move {
            let points: Vec<DataPoint> = (0..n_points)
                .map(|i| {
                    data_point(
                        "cpu.load",
                        &format!("host-{task_id}"),
                        (task_id * n_points + i) as i64 * 1_000_000_000,
                        i as f64,
                    )
                })
                .collect();
            srv.append(Request::new(AppendRequest { points }))
                .await
                .expect("append failed")
        });
    }

    while let Some(result) = set.join_next().await {
        result.expect("task panicked");
    }

    // All n_tasks * n_points points must be queryable
    let mut stream = server
        .query(Request::new(QueryRequest {
            metric_name: "cpu.load".to_string(),
            tag_filters: HashMap::new(),
            time_start_ns: None,
            time_end_ns: None,
        }))
        .await
        .expect("query failed")
        .into_inner();

    let mut total = 0usize;
    while let Some(resp) = stream.next().await {
        resp.expect("stream error");
        total += 1;
    }
    assert_eq!(total, n_tasks * n_points, "all points must be queryable");
}

/// Querying a metric that was never written must return an empty stream — not
/// an error. Verifies robustness of the resolve_series + memtable scan path
/// when there are no matches.
#[tokio::test]
async fn test_query_nonexistent_metric_returns_empty() {
    let dir = tempdir().unwrap();
    let config = test_config(&dir);
    config.ensure_dirs().await.unwrap();
    let server = StorageServer::open(&config).await.expect("open failed");

    let mut stream = server
        .query(Request::new(QueryRequest {
            metric_name: "does.not.exist".to_string(),
            tag_filters: HashMap::new(),
            time_start_ns: None,
            time_end_ns: None,
        }))
        .await
        .expect("query must not error on missing metric")
        .into_inner();

    assert!(
        stream.next().await.is_none(),
        "stream must be empty for an unknown metric"
    );
}

/// Writes points across a wide time range, then queries a narrow sub-range.
/// Verifies that the time filter is applied correctly and only matching points
/// are returned.
#[tokio::test]
async fn test_time_range_filter() {
    let dir = tempdir().unwrap();
    let config = test_config(&dir);
    config.ensure_dirs().await.unwrap();
    let server = StorageServer::open(&config).await.expect("open failed");

    // Write 5 points at 1s intervals: 1s, 2s, 3s, 4s, 5s
    server
        .append(Request::new(AppendRequest {
            points: (1..=5)
                .map(|s| data_point("cpu.load", "web1", s * 1_000_000_000, s as f64))
                .collect(),
        }))
        .await
        .expect("append failed");

    // Query only [2s, 4s] — should return 3 points (2s, 3s, 4s)
    let mut stream = server
        .query(Request::new(QueryRequest {
            metric_name: "cpu.load".to_string(),
            tag_filters: HashMap::from([("host".to_string(), "web1".to_string())]),
            time_start_ns: Some(2 * 1_000_000_000),
            time_end_ns: Some(4 * 1_000_000_000),
        }))
        .await
        .expect("query failed")
        .into_inner();

    let mut timestamps = Vec::new();
    while let Some(resp) = stream.next().await {
        timestamps.push(resp.expect("stream error").timestamp_ns);
    }

    assert_eq!(
        timestamps.len(),
        3,
        "only points within [2s, 4s] must return"
    );
    assert!(
        timestamps.contains(&(2 * 1_000_000_000)),
        "2s must be included"
    );
    assert!(
        timestamps.contains(&(4 * 1_000_000_000)),
        "4s must be included"
    );
    assert!(
        !timestamps.contains(&(1 * 1_000_000_000)),
        "1s must be excluded"
    );
    assert!(
        !timestamps.contains(&(5 * 1_000_000_000)),
        "5s must be excluded"
    );
}

/// Writes enough data to exceed the (small) memtable threshold and trigger a
/// flush to a chunk file, then queries and verifies the data is readable from
/// the chunk file — not just the memtable.
#[tokio::test]
async fn test_data_queryable_after_flush() {
    let dir = tempdir().unwrap();
    // Use a tiny flush threshold (512 bytes) so a handful of points trigger a flush.
    // Each point costs ~32 bytes in the memtable (size_of::<(i64,f64)>() * 2).
    let config = StorageConfig {
        memtable_flush_threshold_bytes: 512,
        ..test_config(&dir)
    };
    config.ensure_dirs().await.unwrap();
    let server = StorageServer::open(&config).await.expect("open failed");
    server.spawn_background_tasks(&config);

    // Write 20 points — enough to exceed the per-shard threshold (512/16 = 32 bytes)
    server
        .append(Request::new(AppendRequest {
            points: (0..20)
                .map(|i| data_point("cpu.load", "web1", i * 1_000_000_000, i as f64))
                .collect(),
        }))
        .await
        .expect("append failed");

    // Wait for at least one sweep cycle (200ms) plus chunk write margin
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // All 20 points must be queryable — served from the chunk file
    let mut stream = server
        .query(Request::new(QueryRequest {
            metric_name: "cpu.load".to_string(),
            tag_filters: HashMap::from([("host".to_string(), "web1".to_string())]),
            time_start_ns: None,
            time_end_ns: None,
        }))
        .await
        .expect("query failed")
        .into_inner();

    let mut total = 0usize;
    while let Some(resp) = stream.next().await {
        resp.expect("stream error");
        total += 1;
    }
    assert_eq!(total, 20, "all 20 points must be queryable after flush");
    assert_eq!(
        server.index.read().await.chunk_file_count(),
        1,
        "flush must have produced exactly one chunk file"
    );
}

/// Multiple sequential appends must produce strictly increasing sequence numbers.
/// Verifies the WAL monotonicity guarantee — critical for ordering and recovery.
#[tokio::test]
async fn test_sequence_numbers_are_monotonically_increasing() {
    let dir = tempdir().unwrap();
    let config = test_config(&dir);
    config.ensure_dirs().await.unwrap();
    let server = StorageServer::open(&config).await.expect("open failed");

    let mut prev_seq = 0u64;
    for i in 0..5u64 {
        let seq = server
            .append(Request::new(AppendRequest {
                points: vec![data_point(
                    "cpu.load",
                    "web1",
                    i as i64 * 1_000_000_000,
                    i as f64,
                )],
            }))
            .await
            .expect("append failed")
            .into_inner()
            .sequence;

        assert!(
            seq > prev_seq,
            "sequence {seq} must be > previous {prev_seq}"
        );
        prev_seq = seq;
    }
}

/// Verifies that multiple series with the same metric but different tag sets
/// are stored and queried independently — tag filter must isolate exactly one
/// series.
#[tokio::test]
async fn test_tag_filter_isolates_series() {
    let dir = tempdir().unwrap();
    let config = test_config(&dir);
    config.ensure_dirs().await.unwrap();
    let server = StorageServer::open(&config).await.expect("open failed");

    // Write to two hosts
    server
        .append(Request::new(AppendRequest {
            points: vec![
                data_point("cpu.load", "web1", 1_000_000_000, 0.1),
                data_point("cpu.load", "web1", 2_000_000_000, 0.2),
                data_point("cpu.load", "web2", 1_000_000_000, 0.9),
                data_point("cpu.load", "web2", 2_000_000_000, 0.8),
            ],
        }))
        .await
        .expect("append failed");

    // Query only web1 — must return exactly 2 points
    let mut stream = server
        .query(Request::new(QueryRequest {
            metric_name: "cpu.load".to_string(),
            tag_filters: HashMap::from([("host".to_string(), "web1".to_string())]),
            time_start_ns: None,
            time_end_ns: None,
        }))
        .await
        .expect("query failed")
        .into_inner();

    let mut count = 0usize;
    while let Some(resp) = stream.next().await {
        resp.expect("stream error");
        count += 1;
    }
    assert_eq!(
        count, 2,
        "tag filter must return only web1 points, not web2"
    );
}

/// Concurrent reads and writes — the most critical concurrency scenario for a
/// storage engine. Writer tasks append continuously while reader tasks query at
/// the same time, exercising the RwLock contention between the flush write path
/// (acquires index write lock) and the query read path (acquires index read lock).
///
/// Invariants:
///   - No query panics or returns a stream error
///   - Every returned point has a valid (non-zero) series_id
///   - The final query after all writes sees at least as many points as were written
///     (never fewer — data must not be lost)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_concurrent_reads_and_writes() {
    let dir = tempdir().unwrap();
    // Small flush threshold so flushes happen frequently during the test,
    // maximising the chance of a query racing with an index write lock.
    let config = StorageConfig {
        memtable_flush_threshold_bytes: 512,
        ..test_config(&dir)
    };
    config.ensure_dirs().await.unwrap();
    let server = Arc::new(StorageServer::open(&config).await.expect("open failed"));
    server.spawn_background_tasks(&config);

    let n_writer_tasks = 4;
    let n_reader_tasks = 4;
    let points_per_writer = 20;

    let mut set = tokio::task::JoinSet::new();

    // Writer tasks — each appends points_per_writer points for its own host
    for writer_id in 0..n_writer_tasks {
        let srv = Arc::clone(&server);
        set.spawn(async move {
            for i in 0..points_per_writer {
                srv.append(Request::new(AppendRequest {
                    points: vec![data_point(
                        "cpu.load",
                        &format!("writer-{writer_id}"),
                        (writer_id * points_per_writer + i) as i64 * 1_000_000,
                        i as f64,
                    )],
                }))
                .await
                .expect("append must not fail under concurrent reads");
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            "writer done"
        });
    }

    // Reader tasks — each runs repeated queries while writers are active.
    // They must not panic and every returned point must have a valid series_id.
    for reader_id in 0..n_reader_tasks {
        let srv = Arc::clone(&server);
        set.spawn(async move {
            for _ in 0..10 {
                let mut stream = srv
                    .query(Request::new(QueryRequest {
                        metric_name: "cpu.load".to_string(),
                        tag_filters: HashMap::new(),
                        time_start_ns: None,
                        time_end_ns: None,
                    }))
                    .await
                    .expect("query must not error during concurrent writes")
                    .into_inner();

                while let Some(resp) = stream.next().await {
                    let point = resp.expect("stream must not error during concurrent writes");
                    assert_ne!(
                        point.series_id, 0,
                        "reader-{reader_id}: series_id must be non-zero"
                    );
                }
                // Small yield between queries so writers get scheduled
                tokio::task::yield_now().await;
            }
            "reader done"
        });
    }

    // Wait for all tasks
    while let Some(result) = set.join_next().await {
        result.expect("task panicked");
    }

    // Give the periodic sweep time to flush any remaining data (≥1 sweep interval)
    tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    // Final consistency check — all written points must be visible
    let mut stream = server
        .query(Request::new(QueryRequest {
            metric_name: "cpu.load".to_string(),
            tag_filters: HashMap::new(),
            time_start_ns: None,
            time_end_ns: None,
        }))
        .await
        .expect("final query failed")
        .into_inner();

    let mut total = 0usize;
    while let Some(resp) = stream.next().await {
        resp.expect("final stream error");
        total += 1;
    }

    let expected = n_writer_tasks * points_per_writer;
    assert_eq!(
        total, expected,
        "all {expected} written points must be visible after concurrent read+write"
    );
}

/// Crash-between-flush-and-snapshot scenario — the dangerous case where:
///   1. Points are appended and WAL-fsynced
///   2. The memtable is flushed to a chunk file (chunk registered in index)
///   3. The process crashes before the index snapshot is saved
///
/// Without the persisted_watermarks guard the WAL GC would have deleted segments
/// immediately after flush, leaving no way to reconstruct the index on restart.
/// With the fix, WAL segments are retained until a snapshot confirms them on
/// disk — recovery replays those segments and the data must be queryable.
///
/// Simulated by: write → force-flush → skip snapshot → reopen (crash recovery).
#[tokio::test]
async fn test_no_data_loss_crash_before_snapshot() {
    let dir = tempdir().unwrap();
    let config = StorageConfig {
        // Tiny threshold so a single append triggers a flush on the next sweep.
        memtable_flush_threshold_bytes: 512,
        ..test_config(&dir)
    };
    config.ensure_dirs().await.unwrap();

    // ── Session 1: write, flush, then "crash" (drop without saving snapshot) ─
    {
        let server = StorageServer::open(&config).await.expect("open failed");
        server.spawn_background_tasks(&config);

        server
            .append(Request::new(AppendRequest {
                points: (0..10)
                    .map(|i| data_point("cpu.load", "crash-host", i * 1_000_000_000, i as f64))
                    .collect(),
            }))
            .await
            .expect("append failed");

        // Wait for the sweep to flush to a chunk file (≥1 sweep cycle = 200ms).
        // The snapshot task fires every 60s — it will NOT have run yet, so
        // persisted_watermarks remain at 0 and WAL GC does nothing. That's the
        // invariant this test verifies.
        tokio::time::sleep(Duration::from_millis(600)).await;

        // server drops here — simulates crash. No snapshot was saved.
        // WAL segments for the flushed data must still exist on disk.
    }

    // ── Session 2: reopen — recovery must replay WAL and restore all points ─
    {
        let server = StorageServer::open(&config).await.expect("reopen failed");

        let mut stream = server
            .query(Request::new(QueryRequest {
                metric_name: "cpu.load".to_string(),
                tag_filters: HashMap::from([("host".to_string(), "crash-host".to_string())]),
                time_start_ns: None,
                time_end_ns: None,
            }))
            .await
            .expect("query after recovery failed")
            .into_inner();

        let mut total = 0usize;
        while let Some(resp) = stream.next().await {
            resp.expect("stream error after recovery");
            total += 1;
        }

        assert_eq!(
            total, 10,
            "all 10 points must survive a crash before snapshot via WAL recovery"
        );
    }
}

/// Verifies that after a successful snapshot the WAL GC is unblocked and
/// segments covered by the snapshot are eventually deleted, while data
/// remains fully queryable after a subsequent restart.
///
/// Timeline:
///   1. Write points → flush to chunk (persisted_watermarks still 0)
///   2. Save snapshot explicitly (persisted_watermarks advance)
///   3. Trigger WAL GC (next sweep cycle)
///   4. Reopen — no WAL to replay (segments deleted), index loaded from snapshot
///   5. All points must still be queryable
#[tokio::test]
async fn test_wal_gc_unblocked_after_snapshot_no_data_loss() {
    let dir = tempdir().unwrap();
    let config = StorageConfig {
        memtable_flush_threshold_bytes: 512,
        ..test_config(&dir)
    };
    config.ensure_dirs().await.unwrap();

    // ── Session 1: write → flush → snapshot → let GC run ────────────────────
    {
        use storage_engine::proto::storage::v1::{
            SnapshotRequest, storage_service_server::StorageService,
        };

        let server = StorageServer::open(&config).await.expect("open failed");
        server.spawn_background_tasks(&config);

        server
            .append(Request::new(AppendRequest {
                points: (0..10)
                    .map(|i| data_point("cpu.load", "gc-host", i * 1_000_000_000, i as f64))
                    .collect(),
            }))
            .await
            .expect("append failed");

        // Wait for the sweep to flush to a chunk (≥1 sweep cycle).
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Explicitly save the snapshot — this advances persisted_watermarks,
        // which unblocks WAL GC for the next sweep cycle.
        server
            .snapshot(Request::new(SnapshotRequest {}))
            .await
            .expect("snapshot failed");

        // Wait for at least one more sweep — GC should now delete the segments
        // whose watermarks are covered by the snapshot.
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Verify persisted_watermarks advanced: at least one shard must be non-zero
        // (the shard that received the append).
        let any_persisted_nonzero = server
            .persisted_watermarks
            .iter()
            .any(|w| w.load(std::sync::atomic::Ordering::Relaxed) > 0);
        assert!(
            any_persisted_nonzero,
            "at least one persisted watermark must be non-zero after snapshot"
        );
    }

    // ── Session 2: reopen — data must come from index snapshot, not WAL ──────
    {
        let server = StorageServer::open(&config)
            .await
            .expect("reopen after GC failed");

        let mut stream = server
            .query(Request::new(QueryRequest {
                metric_name: "cpu.load".to_string(),
                tag_filters: HashMap::from([("host".to_string(), "gc-host".to_string())]),
                time_start_ns: None,
                time_end_ns: None,
            }))
            .await
            .expect("query after GC+restart failed")
            .into_inner();

        let mut total = 0usize;
        while let Some(resp) = stream.next().await {
            resp.expect("stream error after GC+restart");
            total += 1;
        }

        assert_eq!(
            total, 10,
            "all 10 points must be queryable after WAL GC + restart (served from chunk via index)"
        );
    }
}

/// WAL partial-replay scenario — the most nuanced recovery case:
///
///   Batch A: written → flushed to chunk → snapshot saved (watermark = seq_A)
///            WAL entries for A are still on disk (GC not yet run — periodic sweep
///            fires every 200ms but snapshot was just saved, GC at next sweep)
///   Batch B: written → WAL-fsynced → NOT flushed yet → crash
///
/// On restart:
///   - Snapshot loaded → watermark = seq_A
///   - WAL recovery per shard: entries ≤ seq_A SKIPPED (A already in chunk)
///                              entries >  seq_A REPLAYED (B not yet in any chunk)
///   - Recovery chunk written for B only
///   - drain_completed_before(last_sequence) cleans up all WAL segments
///
/// Invariants:
///   - Batch A points queryable exactly once (from chunk, not double-counted)
///   - Batch B points queryable (from recovery chunk)
///   - Total = A + B, not 2×A + B
#[tokio::test]
async fn test_partial_wal_replay_skips_snapshotted_entries() {
    let dir = tempdir().unwrap();
    let config = StorageConfig {
        memtable_flush_threshold_bytes: 512,
        ..test_config(&dir)
    };
    config.ensure_dirs().await.unwrap();

    const BATCH_A: usize = 10;
    const BATCH_B: usize = 8;

    // ── Session 1 ────────────────────────────────────────────────────────────
    {
        use storage_engine::proto::storage::v1::{
            SnapshotRequest, storage_service_server::StorageService,
        };

        let server = StorageServer::open(&config).await.expect("open failed");
        server.spawn_background_tasks(&config);

        // Batch A — distinct host so we can count each batch independently.
        server
            .append(Request::new(AppendRequest {
                points: (0..BATCH_A as i64)
                    .map(|i| data_point("cpu.load", "host-a", i * 1_000_000_000, i as f64))
                    .collect(),
            }))
            .await
            .expect("batch A append failed");

        // Wait for flush (≥1 sweep cycle).
        tokio::time::sleep(Duration::from_millis(600)).await;

        // Save snapshot → watermark advances to cover batch A's WAL entries.
        // WAL entries for A are now safe to GC, but GC has not run yet.
        server
            .snapshot(Request::new(SnapshotRequest {}))
            .await
            .expect("snapshot failed");

        // Batch B — written after snapshot. These WAL entries have seq > watermark
        // and are NOT covered by the snapshot. They will need replay on recovery.
        server
            .append(Request::new(AppendRequest {
                points: (0..BATCH_B as i64)
                    .map(|i| {
                        // Offset timestamps so they don't collide with batch A.
                        data_point(
                            "cpu.load",
                            "host-b",
                            (BATCH_A as i64 + i) * 1_000_000_000,
                            i as f64,
                        )
                    })
                    .collect(),
            }))
            .await
            .expect("batch B append failed");

        // Crash — batch B is WAL-durable but not flushed to a chunk.
        // Snapshot covers only batch A. WAL segments for B survive on disk.
    }

    // ── Session 2: recovery ───────────────────────────────────────────────────
    {
        let server = StorageServer::open(&config).await.expect("reopen failed");

        // Query batch A (host-a) — must come from the pre-crash chunk exactly once.
        let mut stream_a = server
            .query(Request::new(QueryRequest {
                metric_name: "cpu.load".to_string(),
                tag_filters: HashMap::from([("host".to_string(), "host-a".to_string())]),
                time_start_ns: None,
                time_end_ns: None,
            }))
            .await
            .expect("query host-a failed")
            .into_inner();
        let mut count_a = 0usize;
        while let Some(resp) = stream_a.next().await {
            resp.expect("stream error host-a");
            count_a += 1;
        }

        // Query batch B (host-b) — must come from the recovery chunk.
        let mut stream_b = server
            .query(Request::new(QueryRequest {
                metric_name: "cpu.load".to_string(),
                tag_filters: HashMap::from([("host".to_string(), "host-b".to_string())]),
                time_start_ns: None,
                time_end_ns: None,
            }))
            .await
            .expect("query host-b failed")
            .into_inner();
        let mut count_b = 0usize;
        while let Some(resp) = stream_b.next().await {
            resp.expect("stream error host-b");
            count_b += 1;
        }

        assert_eq!(
            count_a, BATCH_A,
            "batch A must appear exactly {BATCH_A} times — not double-counted from WAL replay"
        );
        assert_eq!(
            count_b, BATCH_B,
            "batch B must be fully recovered from WAL replay ({BATCH_B} points)"
        );
    }
}
