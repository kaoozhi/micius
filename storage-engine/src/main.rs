use std::sync::Arc;
use storage_engine::config::StorageConfig;
use storage_engine::index;
use storage_engine::proto::storage::v1::storage_service_server::StorageServiceServer;
use storage_engine::server::StorageServer;
use tokio::time::Duration;
use tonic::transport::Server;

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

    // Open the server — runs crash recovery if needed (snapshot load + WAL replay).
    let server = StorageServer::open(&config).await?;

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
    let bg_worker = Arc::clone(&server.compaction_worker);
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
        let idx_clone  = Arc::clone(&server.index);
        let wal_clone  = Arc::clone(&server.wal);
        let index_path = server.snapshot_path.clone();
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

    let grpc_addr = config.grpc_addr.parse()?;
    tracing::info!(addr = %grpc_addr, "storage engine gRPC server starting");

    // Graceful shutdown on SIGINT/SIGTERM — drains in-flight RPCs before exit.
    let (shutdown_tx, shutdown_rx) = tokio::sync::oneshot::channel::<()>();
    tokio::spawn(async move {
        tokio::signal::ctrl_c().await.ok();
        let _ = shutdown_tx.send(());
    });
    Server::builder()
        .add_service(StorageServiceServer::new(server))
        .serve_with_shutdown(grpc_addr, async { shutdown_rx.await.ok(); })
        .await?;

    Ok(())
}
