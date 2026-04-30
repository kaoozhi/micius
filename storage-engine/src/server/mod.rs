use crate::types::*;
use crate::{
    chunk::reader::ChunkReader,
    chunk::writer::ChunkWriter,
    compaction::CompactionWorker,
    index::chunk_index::ChunkIndex,
    memtable::Memtable,
    proto::storage::v1::{
        AppendRequest, AppendResponse, CompactRequest, CompactResponse, QueryRequest,
        QueryResponse, SnapshotRequest, SnapshotResponse, storage_service_server::StorageService,
    },
    wal::writer::WalWriter,
};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{Mutex, RwLock};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

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

    type QueryStream = ReceiverStream<Result<QueryResponse, Status>>;
    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        struct Query {
            metric_name: String,
            tags: HashMap<String, String>,
            time_start_ns: i64,
            time_end_ns: i64,
        }

        let req = request.into_inner();
        let query = Query {
            metric_name: req.metric_name,
            tags: req.tag_filters,
            time_start_ns: req.time_start_ns.unwrap_or(i64::MIN),
            time_end_ns: req.time_end_ns.unwrap_or(i64::MAX),
        };

        // Spawn the query execution task. The RPC returns immediately with the
        // stream handle; results are sent incrementally as they are read.
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let index = Arc::clone(&self.index);
        let memtable = Arc::clone(&self.memtable);
        tokio::spawn(async move {
            // ── Stage 1: Memtable scan ────────────────────────────────────────
            // Collect all matching points under the memtable lock, then release
            // it before streaming — the lock must not be held across channel
            // sends, which can block when the receiver is slow.
            let mem_response: Vec<QueryResponse> = {
                let mem = memtable.lock().await;
                let series = mem.resolve_series(&query.metric_name, &query.tags);
                let mut responses = Vec::new();
                for series_key in series {
                    let points =
                        mem.read_series(&series_key, query.time_start_ns, query.time_end_ns, None);
                    let series_id = SeriesId::from(&series_key);
                    for point in points {
                        responses.push(QueryResponse {
                            series_id,
                            timestamp_ns: point.timestamp_ns,
                            value: point.value,
                        })
                    }
                }
                responses
            }; // memtable lock released here

            for response in mem_response {
                if tx.send(Ok(response)).await.is_err() {
                    return; // client disconnected — stop streaming
                }
            }

            // ── Stage 2: Chunk index scan ─────────────────────────────────────
            // Collect chunk metadata under the index read lock, then release it
            // before any disk I/O — the lock must not be held across async file
            // reads, which would block concurrent flush writes to the index.
            let (chunks, paths, keys) = {
                let idx = index.read().await;

                // resolve_series → prune_chunks: two in-memory stages, no I/O.
                // predicate: None — value filter not in proto for Phase 1.
                let series_ids = idx.resolve_series(&query.metric_name, &query.tags);
                let chunks: Vec<SeriesChunkEntry> = series_ids
                    .iter()
                    .flat_map(|id| {
                        idx.prune_chunks(id, query.time_start_ns, query.time_end_ns, None)
                    })
                    .collect();

                // Materialise file paths and series keys while the lock is held.
                let paths: HashMap<ChunkId, std::path::PathBuf> = chunks
                    .iter()
                    .filter_map(|c| {
                        idx.chunk_files
                            .get(&c.chunk_id)
                            .map(|m| (c.chunk_id, m.file_path.clone()))
                    })
                    .collect();
                let keys: HashMap<SeriesId, SeriesKey> = chunks
                    .iter()
                    .filter_map(|c| {
                        idx.series_registry
                            .get(&c.series_id)
                            .map(|k| (c.series_id, k.clone()))
                    })
                    .collect();

                (chunks, paths, keys)
            }; // index read lock released here — all disk I/O below is lock-free

            // ── Stage 3: Chunk reads ──────────────────────────────────────────
            // For each surviving chunk entry: full decompression. Stream each point as it is decoded.
            for chunk in chunks {
                let Some(file_path) = paths.get(&chunk.chunk_id) else {
                    tracing::warn!(chunk_id = ?chunk.chunk_id, "chunk file path missing from index");
                    continue;
                };

                let Some(series_key) = keys.get(&chunk.series_id) else {
                    tracing::warn!(series_id = ?chunk.series_id, "series key missing from index");
                    continue;
                };

                // Full decompression: read, delta-decode, time-filter.
                match ChunkReader::read_series(
                    file_path,
                    series_key,
                    query.time_start_ns,
                    query.time_end_ns,
                )
                .await
                {
                    Ok(Some(points)) => {
                        for point in points {
                            let response = QueryResponse {
                                series_id: chunk.series_id,
                                timestamp_ns: point.timestamp_ns,
                                value: point.value,
                            };
                            if tx.send(Ok(response)).await.is_err() {
                                return; // client disconnected
                            }
                        }
                    }
                    Ok(None) => continue, // series absent or no points in range
                    Err(e) => tracing::warn!(error = %e, "series read failed"),
                }
            }
        });

        Ok(Response::new(Self::QueryStream::new(rx)))
    }
    async fn compact(
        &self,
        request: Request<CompactRequest>,
    ) -> Result<Response<CompactResponse>, Status> {
        todo!()
    }
    async fn snapshot(
        &self,
        request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        todo!()
    }
}
