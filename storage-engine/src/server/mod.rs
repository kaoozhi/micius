use crate::types::*;
use crate::{
    chunk::reader::ChunkReader,
    chunk::writer::ChunkWriter,
    compaction::CompactionWorker,
    config::StorageConfig,
    index::{self, chunk_index::ChunkIndex},
    memtable::Memtable,
    metrics,
    proto::storage::v1::{
        AppendRequest, AppendResponse, CompactRequest, CompactResponse, QueryRequest,
        QueryResponse, SnapshotRequest, SnapshotResponse, storage_service_server::StorageService,
    },
    wal::{self, writer::WalWriter},
};
use std::collections::HashMap;
use std::path::PathBuf;
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
    pub snapshot_path: PathBuf,
}

impl StorageServer {
    /// Opens the storage engine at the paths described by `config`, running
    /// crash recovery if needed, and returns a ready-to-use server.
    ///
    /// Handles three cases transparently:
    ///   - First start: no snapshot, no WAL → fresh empty server
    ///   - Crash recovery: loads snapshot, replays WAL delta, flushes to disk
    ///   - Graceful restart: loads recent snapshot, minimal WAL replay
    ///
    /// The caller is responsible for starting background tasks (compaction,
    /// periodic snapshot) and binding the gRPC listener.
    /// `config.ensure_dirs()` must be called before this.
    pub async fn open(config: &StorageConfig) -> anyhow::Result<Self> {
        // 1. Index snapshot ─────────────────────────────────────────────────
        // None = first run or missing snapshot → start from empty index.
        let (mut idx, last_seq) = match index::persistence::load_index(&config.index_path).await? {
            None => (ChunkIndex::new(), 0),
            Some((idx, seq)) => (idx, seq),
        };
        tracing::info!(
            series = idx.series_count(),
            chunks = idx.chunk_file_count(),
            last_seq,
            "index snapshot loaded"
        );

        // 2. WAL replay ─────────────────────────────────────────────────────
        // Verifies CRC32 per frame, stops at first torn write.
        // Returns only the points not yet flushed to chunk files.
        let recovery = wal::recovery::recover(&config.wal_dir).await?;
        tracing::info!(
            points = recovery.points.len(),
            segments = recovery.segments_replayed,
            last_seq = recovery.last_sequence,
            "WAL recovered"
        );

        // 3. Flush recovered points ─────────────────────────────────────────
        // Bypass the size threshold — on open we always flush to a clean slate.
        let mut memtable = Memtable::new(config.memtable_flush_threshold_bytes);
        if !recovery.points.is_empty() {
            for point in recovery.points {
                memtable.insert(point);
            }
            let results = ChunkWriter::new(&config.chunk_dir)
                .write(memtable.drain())
                .await?;
            let meta = results.chunk_meta;
            for s in results.series_results {
                idx.register(&s.series_key, s.entry, s.stats, meta.clone());
            }
            tracing::info!(chunk = ?meta.file_path, "recovery chunk written");
        }

        // 4. WAL writer + segment cleanup ───────────────────────────────────
        // Resume from recovery.last_sequence so new appends don't reuse
        // already-assigned sequence numbers. u64::MAX covers all pre-existing
        // completed segments — they were fully replayed above.
        let mut wal_writer = WalWriter::open(
            &config.wal_dir,
            config.wal_max_segment_bytes,
            recovery.last_sequence,
        )
        .await?;

        if recovery.segments_replayed > 0 {
            wal_writer.rotate().await?;
        }
        let to_delete = wal_writer.drain_completed_before(u64::MAX);
        for path in &to_delete {
            if let Err(e) = tokio::fs::remove_file(path).await {
                tracing::warn!(path = ?path, error = %e, "failed to delete WAL segment");
            }
        }
        tracing::info!(deleted = to_delete.len(), "WAL segments cleaned up");

        // 5. Wrap in Arc ────────────────────────────────────────────────────
        let wal = Arc::new(Mutex::new(wal_writer));
        let mem = Arc::new(Mutex::new(memtable));
        let index = Arc::new(RwLock::new(idx));
        let writer = Arc::new(ChunkWriter::new(&config.chunk_dir));

        let compaction_worker = Arc::new(Mutex::new(CompactionWorker::new(
            Arc::clone(&index),
            Arc::clone(&writer),
            config.compaction_min_threshold,
            config.compaction_size_ratio,
        )));

        Ok(Self {
            wal,
            memtable: mem,
            index,
            chunk_writer: writer,
            compaction_worker,
            snapshot_path: config.index_path.clone(),
        })
    }
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
        let wal_start = std::time::Instant::now();
        let seq = {
            let mut wal = self.wal.lock().await;
            wal.append(&points)
                .await
                .map_err(|e| {
                    metrics::wal_entries_total().with_label_values(&["error"]).inc();
                    Status::internal(format!("WAL error: {}", e))
                })?
        }; // WAL lock released here
        metrics::wal_append_duration().with_label_values(&["ok"])
            .observe(wal_start.elapsed().as_secs_f64());
        metrics::wal_entries_total().with_label_values(&["ok"]).inc();

        tracing::info!(points = points.len(), seq = seq, "append");

        // ── Step 3: Memtable insert ───────────────────────────────────────────
        let mut mem = self.memtable.lock().await;
        for point in points {
            mem.insert(point);
        }
        metrics::memtable_size_bytes().set(mem.size_bytes() as i64);

        // ── Step 4: Trigger async flush if threshold exceeded ─────────────────
        // Drain is atomic under the memtable lock. The flush itself runs in a
        // background task so this RPC returns immediately after the WAL fsync.
        if mem.should_flush() {
            let drained = mem.drain();
            drop(mem); // release memtable lock before spawning
            tracing::debug!(series = drained.len(), seq, "flush triggered");

            let chunk_writer = Arc::clone(&self.chunk_writer);
            let index = Arc::clone(&self.index);
            let wal = Arc::clone(&self.wal);
            tokio::spawn(async move {
                let flush_start = std::time::Instant::now();
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
                        metrics::chunk_files_total().set(index.chunk_file_count() as i64);
                        metrics::index_series_count().set(index.series_count() as i64);
                        tracing::info!(
                            series  = result.series_results.len(),
                            chunk   = ?result.chunk_meta.file_path,
                            "memtable flushed"
                        );
                        metrics::chunk_bytes_written_total()
                            .inc_by(result.chunk_meta.file_size);
                        metrics::memtable_flush_duration_seconds()
                            .observe(flush_start.elapsed().as_secs_f64());
                        metrics::memtable_flush_total().with_label_values(&["ok"]).inc();
                        drop(index); // release index lock before acquiring WAL lock

                        // Delete completed WAL segments whose max_seq ≤ seq.
                        // seq was captured at append time — only segments fully
                        // covered by the flushed memtable are eligible.
                        let paths = {
                            let mut wal = wal.lock().await;
                            if let Err(e) = wal.rotate().await {
                                tracing::error!(error = %e, "WAL rotation after flush failed");
                            }
                            wal.drain_completed_before(u64::MAX)
                        }; // WAL lock released before file I/O

                        let deleted = paths.len();
                        for path in paths {
                            if let Err(e) = tokio::fs::remove_file(&path).await {
                                // NotFound is benign — cleaned up by a previous run.
                                tracing::warn!(path = ?path, error = %e, "failed to delete WAL segment");
                            }
                        }
                        tracing::info!(deleted, seq, "WAL segments cleaned up");
                    }
                    Err(e) => {
                        metrics::memtable_flush_total().with_label_values(&["error"]).inc();
                        tracing::error!(error = %e, "flush failed");
                    }
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

        let tags_display = query
            .tags
            .iter()
            .map(|(k, v)| format!("{k}:{v}"))
            .collect::<Vec<_>>()
            .join(",");
        tracing::info!(metric = %query.metric_name, tags = %tags_display, "query");

        // Spawn the query execution task. The RPC returns immediately with the
        // stream handle; results are sent incrementally as they are read.
        let (tx, rx) = tokio::sync::mpsc::channel(256);
        let index = Arc::clone(&self.index);
        let memtable = Arc::clone(&self.memtable);
        tokio::spawn(async move {
            let query_start = std::time::Instant::now();
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

            tracing::debug!(points = mem_response.len(), "memtable scan complete");

            let mut streamed: usize = 0;
            for response in mem_response {
                if tx.send(Ok(response)).await.is_err() {
                    return; // client disconnected — stop streaming
                }
                streamed += 1;
            }

            // ── Stage 2: Chunk index scan ─────────────────────────────────────
            // Collect chunk metadata under the index read lock, then release it
            // before any disk I/O — the lock must not be held across async file
            // reads, which would block concurrent flush writes to the index.
            let (chunks, paths, keys, series_count) = {
                let idx = index.read().await;

                // resolve_series → prune_chunks: two in-memory stages, no I/O.
                // predicate: None — value filter not in proto for Phase 1.
                let series_ids = idx.resolve_series(&query.metric_name, &query.tags);
                let series_count: usize = series_ids.len();
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

                (chunks, paths, keys, series_count)
            }; // index read lock released here — all disk I/O below is lock-free

            // Record pruning effectiveness: how many chunk entries existed vs survived.
            metrics::query_chunks_scanned().with_label_values(&["total"])
                .observe(series_count as f64);
            metrics::query_chunks_scanned().with_label_values(&["after_pruning"])
                .observe(chunks.len() as f64);

            tracing::debug!(series = series_count, chunks = chunks.len(), "index pruned");

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
                            streamed += 1;
                        }
                    }
                    Ok(None) => continue, // series absent or no points in range
                    Err(e) => tracing::warn!(error = %e, "series read failed"),
                }
            }
            tracing::debug!(points = streamed, "query complete");
            metrics::query_duration_seconds().observe(query_start.elapsed().as_secs_f64());
        });

        Ok(Response::new(Self::QueryStream::new(rx)))
    }
    async fn compact(
        &self,
        _request: Request<CompactRequest>,
    ) -> Result<Response<CompactResponse>, Status> {
        tracing::info!("compact");
        let result = self
            .compaction_worker
            .lock()
            .await
            .compact_once()
            .await
            .map_err(|e| {
                metrics::compaction_runs_total().with_label_values(&["error"]).inc();
                Status::internal(e.to_string())
            })?;

        metrics::compaction_runs_total().with_label_values(&["ok"]).inc();
        metrics::compaction_chunks_merged_total().inc_by(result.chunks_merged as u64);
        tracing::info!(
            chunks_merged = result.chunks_merged,
            bytes_freed   = result.bytes_freed,
            "compaction complete"
        );
        Ok(Response::new(CompactResponse {
            chunks_merged: result.chunks_merged,
            bytes_freed: result.bytes_freed,
        }))
    }

    async fn snapshot(
        &self,
        _request: Request<SnapshotRequest>,
    ) -> Result<Response<SnapshotResponse>, Status> {
        tracing::info!("snapshot");
        let seq = self.wal.lock().await.current_sequence();
        let index = self.index.read().await;

        crate::index::persistence::save_index(&index, &self.snapshot_path, seq)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        tracing::info!(seq, path = %self.snapshot_path.display(), "snapshot saved");
        Ok(Response::new(SnapshotResponse {
            snapshot_path: self.snapshot_path.to_string_lossy().into_owned(),
        }))
    }
}
