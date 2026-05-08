use storage_engine::config::StorageConfig;
use storage_engine::metrics;
use storage_engine::proto::storage::v1::storage_service_server::StorageServiceServer;
use storage_engine::server::StorageServer;
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
    // Background tasks (compaction, snapshot, memtable sweep) are started here
    // before the server is moved into tonic. See StorageServer::spawn_background_tasks.
    let server = StorageServer::open(&config).await?;
    server.spawn_background_tasks(&config);

    // ── Prometheus metrics server ─────────────────────────────────────────────

    let metrics_addr = config.metrics_addr.parse()?;
    tokio::spawn(async move {
        if let Err(e) = metrics::serve(metrics_addr).await {
            tracing::error!(error = %e, "metrics server failed");
        }
    });

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
        .serve_with_shutdown(grpc_addr, async {
            shutdown_rx.await.ok();
        })
        .await?;

    Ok(())
}
