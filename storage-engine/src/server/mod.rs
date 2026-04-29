use crate::types::*;
use crate::{
    chunk::reader::ChunkReader,
    chunk::writer::ChunkWriter,
    compaction::CompactionWorker,
    index::chunk_index::ChunkIndex,
    memtable::Memtable,
    proto::storage::v1::{
        AppendRequest, AppendResponse, CompactRequest, CompactResponse, QueryRequest,
        QueryResponse, SnapshotRequest, SnapshotResponse,
        storage_service_server::{StorageService, StorageServiceServer},
    },
    wal::writer::WalWriter,
};
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};
// use tracing_subscriber::registry::Data;

pub struct StorageServer {
    pub wal: Arc<Mutex<WalWriter>>,
    pub memtable: Arc<Mutex<Memtable>>,
    pub index: Arc<RwLock<ChunkIndex>>,
    pub chunk_writer: Arc<ChunkWriter>,
    pub compaction_worker: Arc<Mutex<CompactionWorker>>,
}

#[tonic::async_trait]
impl StorageService for StorageServer {
    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        // ── Step 1: decode proto DataPoints into internal types ──────────────
        // tags arrive as HashMap from proto — collect into BTreeMap for
        // canonical ordering required by SeriesKey hashing.
        let points: Vec<DataPoint> = request
            .into_inner()
            .points
            .into_iter()
            .map(|pt| DataPoint {
                metric_name: pt.metric_name,
                tags: pt.tags.into_iter().collect(),
                timestamp_ns: pt.timestamp_ns,
                value: pt.value,
            })
            .collect();

        // ── Step 2: WAL append (must complete before memtable insert) ─────────
        // fsync happens inside append(). seq is the monotonic token returned
        // to the caller and used later to bound WAL segment deletion.
        let seq = {
            let mut wal = self.wal.lock().await;
            wal.append(&points)
                .await
                .map_err(|e| Status::internal(format!("WAL error: {}", e)))?
        }; // WAL lock released here

        // ── Step 3: Memtable insert ───────────────────────────────────────────
        let mut mem = self.memtable.lock().await;
        for point in points {
            mem.insert(point);
        }

        // ── Step 4: Trigger async flush if threshold exceeded ─────────────────
        // Drain is atomic under the memtable lock. The flush itself runs in a
        // background task so this RPC returns immediately after the WAL fsync.
        if mem.should_flush() {
            let drained = mem.drain();
            drop(mem); // release memtable lock before spawning

            let chunk_writer = Arc::clone(&self.chunk_writer);
            let index = Arc::clone(&self.index);
            let wal = Arc::clone(&self.wal);
            tokio::spawn(async move {
                match chunk_writer.write(drained).await {
                    Ok(result) => {
                        // Register all series from the new chunk under a single
                        // write lock acquisition — atomic from the query path's view.
                        let mut index = index.write().await;
                        for s in &result.series_results {
                            index.register(
                                &s.series_key,
                                s.entry.clone(),
                                s.stats.clone(),
                                result.chunk_meta.clone(),
                            );
                        }
                        drop(index); // release index lock before acquiring WAL lock

                        // Delete completed WAL segments whose max_seq ≤ seq.
                        // seq was captured at append time — only segments fully
                        // covered by the flushed memtable are eligible.
                        let paths = {
                            let mut wal = wal.lock().await;
                            wal.drain_completed_before(seq)
                        }; // WAL lock released before file I/O

                        for path in paths {
                            if let Err(e) = tokio::fs::remove_file(&path).await {
                                // NotFound is benign — cleaned up by a previous run.
                                tracing::warn!(path = ?path, error = %e, "failed to delete WAL segment");
                            }
                        }
                    }
                    Err(e) => tracing::error!(error = %e, "flush failed"),
                }
            });
        } else {
            drop(mem); // release lock when no flush needed
        }

        Ok(Response::new(AppendResponse { sequence: seq }))
    }

    async fn compact(
        &self,
        request: Request<CompactRequest>,
    ) -> Result<Response<CompactResponse>, Status> {
        todo!()
    }
    type QueryStream = ReceiverStream<Result<QueryResponse, Status>>;
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        todo!()
    }

    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        todo!()
    }
}
