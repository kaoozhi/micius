use std::sync::Arc;
use storage_engine::chunk::writer::ChunkWriter;
use tokio::sync::{Mutex, RwLock};
use tonic::transport::Server;

use storage_engine::compaction::CompactionWorker;
use storage_engine::config::StorageConfig;
use storage_engine::index;
use storage_engine::index::chunk_index::ChunkIndex;
use storage_engine::memtable::Memtable;
use storage_engine::proto::storage::v1::storage_service_server::StorageServiceServer;
use storage_engine::server::StorageServer;
use storage_engine::wal;
use storage_engine::wal::writer::WalWriter;
use tokio::time::Duration;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize tracing
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("storage_engine=info".parse()?),
        )
        .init();

    let config = StorageConfig::load()?;
    config.ensure_dirs().await?;

    // ── Startup sequence ──────────────────────────────────────────────────────
    //
    // 1. Load index snapshot → fast path: skip scanning chunk files already known
    // 2. Replay WAL entries written after the snapshot's last_wal_sequence
    // 3. Flush recovered points to a new chunk file and register in the index
    // 4. Open the WAL writer and delete segments now made redundant
    // 5. Start gRPC server — accept live traffic only after recovery is complete

    // 1. Index snapshot ─────────────────────────────────────────────────────────
    // None = first run or snapshot missing → start from an empty index.
    // The WAL replay below will reconstruct any un-flushed series.
    let (mut index, last_seq) = match index::persistence::load_index(&config.index_path).await? {
        None => (ChunkIndex::new(), 0),
        Some((index, seq)) => (index, seq),
    };

    tracing::info!(
        series = index.series_count(),
        chunks = index.chunk_file_count(),
        last_seq,
        "index snapshot loaded"
    );

    // 2. WAL replay ─────────────────────────────────────────────────────────────
    // Reads all WAL segments, verifies CRC32 per frame, stops at first torn write.
    // Returns points not yet flushed to chunk files.
    let recovery = wal::recovery::recover(&config.wal_dir).await?;
    tracing::info!(
        points = recovery.points.len(),
        segments = recovery.segments_replayed,
        last_sequence = recovery.last_sequence,
        "WAL recovered"
    );

    // 3. Flush recovered points ─────────────────────────────────────────────────
    // Insert all recovered points into a temporary memtable then flush immediately,
    // bypassing the size threshold — on startup we always want a clean slate.
    let mut memtable = Memtable::new(config.memtable_flush_threshold_bytes);
    if !recovery.points.is_empty() {
        for point in recovery.points {
            memtable.insert(point);
        }
        let results = ChunkWriter::new(&config.chunk_dir)
            .write(memtable.drain())
            .await?;
        let meta = results.chunk_meta;
        for result in results.series_results {
            index.register(&result.series_key, result.entry, result.stats, meta.clone());
        }
        tracing::info!(chunk = ?meta.file_path, "recovery chunk written");
    }

    // 4. WAL writer + segment cleanup ───────────────────────────────────────────
    // Resume from recovery.last_sequence so new appends don't reuse already-
    // assigned sequence numbers. All pre-existing WAL segments were fully replayed
    // above — drain them all (u64::MAX covers every completed segment regardless
    // of the sequence stored against them).
    let mut wal_writer = WalWriter::open(
        &config.wal_dir,
        config.wal_max_segment_bytes,
        recovery.last_sequence,
    )
    .await?;

    let to_delete = wal_writer.drain_completed_before(u64::MAX);
    for path in &to_delete {
        if let Err(e) = tokio::fs::remove_file(path).await {
            // NotFound is benign — segment was cleaned up in a previous run.
            tracing::warn!(path = ?path, error = %e, "failed to delete WAL segment");
        }
    }
    tracing::info!(deleted = to_delete.len(), "WAL segments cleaned up");

    // ── Shared state ──────────────────────────────────────────────────────────
    let wal = Arc::new(Mutex::new(wal_writer));
    let mem = Arc::new(Mutex::new(memtable));
    let idx = Arc::new(RwLock::new(index));
    let writer = Arc::new(ChunkWriter::new(&config.chunk_dir));

    let compaction_worker = Arc::new(Mutex::new(CompactionWorker::new(
        Arc::clone(&idx),
        Arc::clone(&writer),
        config.compaction_min_threshold,
        config.compaction_size_ratio,
    )));

    // ── Background tasks ──────────────────────────────────────────────────────
    //
    // All tasks share state through Arc-wrapped locks. Three tokio tasks run
    // concurrently on the same single-threaded scheduler:
    //
    //  ┌────────────────────────────────────────────────────────────────┐
    //  │  tokio runtime                                                 │
    //  │                                                                │
    //  │  ┌──────────────────┐   ┌──────────────────┐                   │
    //  │  │ Compaction task  │   │  Snapshot task   │                   │
    //  │  │  every N secs    │   │   every 60 secs  │                   │
    //  │  │                  │   │                  │                   │
    //  │  │ Mutex<Compact>   │   │ RwLock<ChunkIdx> │                   │
    //  │  │   lock / unlock  │   │   read / drop    │                   │
    //  │  └────────┬─────────┘   └────────┬─────────┘                   │
    //  │           │                      │                             │
    //  │           └──────────┬───────────┘                             │
    //  │                      │                                         │
    //  │              Arc shared state                                  │
    //  │   Arc<RwLock<ChunkIndex>>  Arc<Mutex<WalWriter>>               │
    //  │   Arc<Mutex<Memtable>>     Arc<ChunkWriter>                    │
    //  │                      │                                         │
    //  │  ┌───────────────────┴──────────────────────────────────────┐  │
    //  │  │  gRPC server (tonic)  — one task per incoming RPC        │  │
    //  │  │  Append | Query | Compact | Snapshot                     │  │
    //  │  └──────────────────────────────────────────────────────────┘  │
    //  └────────────────────────────────────────────────────────────────┘
    //
    // Lock ordering (never hold two simultaneously unless noted):
    //   WAL Mutex → released → Memtable Mutex → released → Index RwLock (write)
    //   WAL Mutex (temporary, released at semicolon) → Index RwLock (read)

    // Size-tiered compaction — runs every compaction_interval_secs.
    // The Mutex is acquired only for each compact_once() call and released
    // immediately after, so the gRPC Compact handler can interject between cycles.
    let bg_worker = Arc::clone(&compaction_worker);
    tokio::spawn(async move {
        let mut ticker =
            tokio::time::interval(Duration::from_secs(config.compaction_interval_secs));
        loop {
            ticker.tick().await;
            let w = bg_worker.lock().await;
            if let Err(e) = w.compact_once().await {
                tracing::error!(error = %e, "background compaction failed");
            }
            // MutexGuard dropped here — lock released before next tick
        }
    });

    // Periodic index snapshot — saves every 60 seconds.
    // WAL sequence is read first (lock acquired + released at semicolon, no binding),
    // then the index read lock is acquired for serialisation. Two locks are never
    // held simultaneously, avoiding contention with the flush write path.
    {
        let idx_clone = Arc::clone(&idx);
        let wal_clone = Arc::clone(&wal);
        let index_path = config.index_path.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(60));
            loop {
                ticker.tick().await;
                // Temporary guard — WAL lock released at the semicolon
                let seq = wal_clone.lock().await.current_sequence();
                let index = idx_clone.read().await;
                if let Err(e) = index::persistence::save_index(&index, &index_path, seq).await {
                    tracing::error!(error = %e, "periodic index snapshot failed");
                }
                // index read lock released here (RwLockReadGuard dropped)
            }
        });
    }

    // ── gRPC server ───────────────────────────────────────────────────────────
    let storage_server = StorageServer {
        wal: Arc::clone(&wal),
        memtable: Arc::clone(&mem),
        index: Arc::clone(&idx),
        chunk_writer: Arc::clone(&writer),
        compaction_worker: Arc::clone(&compaction_worker),
        snapshot_path: config.index_path,
    };

    let grpc_addr = config.grpc_addr.parse()?;
    tracing::info!(addr = %grpc_addr, "storage engine gRPC server starting");

    // Graceful shutdown on SIGINT/SIGTERM — drains in-flight RPCs before exit.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        let _ = shutdown_tx.send(());
    });
    Server::builder()
        .add_service(StorageServiceServer::new(storage_server))
        .serve_with_shutdown(grpc_addr, async {
            shutdown_rx.await.ok();
        })
        .await?;

    Ok(())
}
