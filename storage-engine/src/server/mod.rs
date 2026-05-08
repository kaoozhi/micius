use crate::types::*;
use crate::{
    chunk::reader::ChunkReader,
    chunk::writer::ChunkWriter,
    compaction::CompactionWorker,
    config::StorageConfig,
    index::{self, chunk_index::ChunkIndex},
    memtable::{self, Memtable},
    metrics,
    proto::storage::v1::{
        AppendRequest, AppendResponse, CompactRequest, CompactResponse, QueryRequest,
        QueryResponse, SnapshotRequest, SnapshotResponse, storage_service_server::StorageService,
    },
    wal::{self, group_commit::WalSender, writer::WalWriter},
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use tokio::sync::{Mutex, RwLock};
use tokio::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

pub struct StorageServer {
    pub wal: WalSender,
    pub memtables: Vec<Arc<Mutex<Memtable>>>,
    pub index: Arc<RwLock<ChunkIndex>>,
    pub chunk_writer: Arc<ChunkWriter>,
    pub compaction_worker: Arc<Mutex<CompactionWorker>>,
    pub snapshot_path: PathBuf,
    pub mem_shard_watermarks: Vec<Arc<AtomicU64>>,
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
        let mut deleted = 0usize;
        for path in &to_delete {
            if let Err(e) = tokio::fs::remove_file(path).await {
                tracing::warn!(path = ?path, error = %e, "failed to delete WAL segment");
                continue;
            }
            deleted += 1;
        }
        if deleted == 0 && !to_delete.is_empty() {
            tracing::warn!(to_delete = to_delete.len(), "failed to clean up segments");
        }

        // 5. Spawn WAL group commit task ───────────────────────────────────
        let wal = WalSender::spawn(
            wal_writer,
            config.wal_channel_capacity,
            config.wal_max_batch,
        );
        let index = Arc::new(RwLock::new(idx));
        let writer = Arc::new(ChunkWriter::new(&config.chunk_dir));

        let compaction_worker = Arc::new(Mutex::new(CompactionWorker::new(
            Arc::clone(&index),
            Arc::clone(&writer),
            config.compaction_min_threshold,
            config.compaction_size_ratio,
        )));
        let memtables: Vec<Arc<Mutex<Memtable>>> = (0..config.memtable_shards)
            .map(|_| {
                Arc::new(Mutex::new(Memtable::new(
                    config.memtable_flush_threshold_bytes / config.memtable_shards,
                )))
            })
            .collect();
        let initial_seq = wal.current_sequence();
        let watermarks: Vec<Arc<AtomicU64>> = (0..config.memtable_shards)
            .map(|_| Arc::new(AtomicU64::new(initial_seq)))
            .collect();
        Ok(Self {
            wal,
            memtables,
            index,
            chunk_writer: writer,
            compaction_worker,
            snapshot_path: config.index_path.clone(),
            mem_shard_watermarks: watermarks,
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

        let pts = Arc::new(points);
        let seq = self.wal.append(Arc::clone(&pts)).await.map_err(|e| {
            metrics::wal_entries_total()
                .with_label_values(&["error"])
                .inc();
            Status::internal(format!("WAL error: {}", e))
        })?;
        metrics::wal_append_duration()
            .with_label_values(&["ok"])
            .observe(wal_start.elapsed().as_secs_f64());
        metrics::wal_entries_total()
            .with_label_values(&["ok"])
            .inc();

        tracing::info!(points = pts.len(), seq = seq, "append");

        // ── Step 3: Memtable insert ───────────────────────────────────────────
        // Route each point to its shard by hashing the series key, then acquire
        // only the relevant shard locks. Flush decisions are handled by the
        // periodic sweep task — this path returns immediately after WAL + insert.
        // Sort point indices by shard so each shard lock is acquired exactly once.
        // Allocates one Vec of length N instead of shards_num empty inner Vecs,
        // which matters for small batches (single-point appends, low-volume paths).
        let shards_num = self.memtables.len();
        let mut indexed: Vec<(usize, usize)> = pts
            .iter()
            .enumerate()
            .map(|(i, p)| (memtable::shard_index(p, shards_num), i))
            .collect();
        indexed.sort_unstable_by_key(|(shard, _)| *shard);

        let mut j = 0;
        while j < indexed.len() {
            let shard = indexed[j].0;
            let end = indexed[j..].partition_point(|(s, _)| *s == shard) + j;
            let mut mem = self.memtables[shard].lock().await;
            for &(_, idx) in &indexed[j..end] {
                mem.insert(pts[idx].clone());
            }
            // shard lock released here
            j = end;
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
        let memtables: Vec<Arc<Mutex<Memtable>>> = self.memtables.iter().map(Arc::clone).collect();
        tokio::spawn(async move {
            let query_start = std::time::Instant::now();
            // ── Stage 1: Memtable scan ────────────────────────────────────────
            // Tag filters may match series across multiple shards — fan out to
            // all shards. Each shard lock is acquired and released individually;
            // results are collected before streaming to avoid holding any lock
            // across channel sends, which can block when the receiver is slow.
            let mut mem_response: Vec<QueryResponse> = Vec::new();
            for shard in &memtables {
                let mem = shard.lock().await;
                let series = mem.resolve_series(&query.metric_name, &query.tags);
                for series_key in series {
                    let points =
                        mem.read_series(&series_key, query.time_start_ns, query.time_end_ns, None);
                    let series_id = SeriesId::from(&series_key);
                    for point in points {
                        mem_response.push(QueryResponse {
                            series_id,
                            timestamp_ns: point.timestamp_ns,
                            value: point.value,
                        });
                    }
                }
                // shard lock released here before moving to the next shard
            }

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
            metrics::query_chunks_scanned()
                .with_label_values(&["total"])
                .observe(series_count as f64);
            metrics::query_chunks_scanned()
                .with_label_values(&["after_pruning"])
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
                metrics::compaction_runs_total()
                    .with_label_values(&["error"])
                    .inc();
                Status::internal(e.to_string())
            })?;

        metrics::compaction_runs_total()
            .with_label_values(&["ok"])
            .inc();
        metrics::compaction_chunks_merged_total().inc_by(result.chunks_merged as u64);
        tracing::info!(
            chunks_merged = result.chunks_merged,
            bytes_freed = result.bytes_freed,
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
        let seq = self.wal.current_sequence();
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

impl StorageServer {
    /// Spawns all background Tokio tasks. Must be called after `open()` and before
    /// the server is moved into tonic. All tasks share state via Arc handles cloned
    /// from `self` fields.
    ///
    /// ```text
    ///  ┌─────────────────────────────────────────────────────────────────────┐
    ///  │  tokio runtime                                                      │
    ///  │                                                                     │
    ///  │  ┌─────────────────┐  ┌─────────────────┐  ┌──────────────────────┐ │
    ///  │  │ WAL group commit│  │ Compaction task │  │   Snapshot task      │ │
    ///  │  │   continuous    │  │  every N secs   │  │   every 60 secs      │ │
    ///  │  │  WalSender ch.  │  │ Mutex<Compact>  │  │ RwLock<ChunkIdx>read │ │
    ///  │  └────────┬────────┘  └────────┬────────┘  └──────────┬───────────┘ │
    ///  │           │                    │                      │             │
    ///  │  ┌────────┴────────────────────┴──────────────────────┴───────────┐ │
    ///  │  │              Arc shared state                                  │ │
    ///  │  │  WalSender · Vec<Arc<Mutex<Memtable>>> · Arc<RwLock<ChunkIdx>> │ │
    ///  │  │  Arc<ChunkWriter> · Vec<Arc<AtomicU64>> (shard watermarks)     │ │
    ///  │  └────────────────────────────────────────────────────────────────┘ │
    ///  │           │                                                         │
    ///  │  ┌────────┴───────────────────────────────────────────────────────┐ │
    ///  │  │  Memtable sweep + WAL GC  (every 200ms)                        │ │
    ///  │  │  for each shard: drain → ChunkWriter → Index write → watermark │ │
    ///  │  │  then: min(watermarks) → WalSender::drain_completed_before     │ │
    ///  │  └────────────────────────────────────────────────────────────────┘ │
    ///  │           │                                                         │
    ///  │  ┌────────┴───────────────────────────────────────────────────────┐ │
    ///  │  │  gRPC server (tonic) — one task per incoming RPC               │ │
    ///  │  │  Append | Query | Compact | Snapshot                           │ │
    ///  │  └────────────────────────────────────────────────────────────────┘ │
    ///  └─────────────────────────────────────────────────────────────────────┘
    /// ```
    ///
    /// Lock acquisition order (never hold two simultaneously unless noted):
    ///   WAL channel send → oneshot wait → Memtable Mutex[i] → released
    ///   → ChunkWriter (no lock) → Index RwLock write → released
    ///   → WAL channel send (DrainBefore)
    ///   Query path: Index RwLock read → released before any disk I/O
    pub fn spawn_background_tasks(&self, config: &StorageConfig) {
        // Size-tiered compaction — runs every compaction_interval_secs.
        // The Mutex is acquired only for each compact_once() call and released
        // immediately after, so the gRPC Compact handler can interject between cycles.
        let bg_worker = Arc::clone(&self.compaction_worker);
        let compaction_interval_secs = config.compaction_interval_secs;
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(Duration::from_secs(compaction_interval_secs));
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
            let idx_clone = Arc::clone(&self.index);
            let wal_clone = self.wal.clone();
            let index_path = self.snapshot_path.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(60));
                loop {
                    ticker.tick().await;
                    // Temporary guard — WAL lock released at the semicolon
                    let seq = wal_clone.current_sequence();
                    let index = idx_clone.read().await;
                    if let Err(e) = index::persistence::save_index(&index, &index_path, seq).await {
                        tracing::error!(error = %e, "periodic index snapshot failed");
                    }
                    // index read lock released here (RwLockReadGuard dropped)
                }
            });
        }

        // Periodic memtable sweep
        {
            let memtables: Vec<Arc<Mutex<Memtable>>> =
                self.memtables.iter().map(|m| Arc::clone(m)).collect();
            let chunk_writer = Arc::clone(&self.chunk_writer);
            let index = Arc::clone(&self.index);
            let wal = self.wal.clone();
            let watermarks: Vec<Arc<AtomicU64>> = self
                .mem_shard_watermarks
                .iter()
                .map(|w| Arc::clone(w))
                .collect();

            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_millis(200));
                loop {
                    ticker.tick().await;
                    let mut total_memtable_bytes: usize = 0;

                    for (i, mem) in memtables.iter().enumerate() {
                        let mut mem = mem.lock().await;
                        // Accumulate size while the lock is held — avoids a second
                        // lock acquisition pass just for the metric.
                        total_memtable_bytes += mem.size_bytes();

                        if mem.should_flush() {
                            let drained = mem.drain();
                            drop(mem); // release shard lock before disk I/O
                            // Transient visibility gap: between drain() and index.register()
                            // below, flushed points are neither in the memtable nor in the
                            // chunk index. Queries arriving mid-flush may return fewer results
                            // than expected — this is an architectural trade-off, not a bug.
                            // WAL durability guarantees no data loss on crash.

                            let flush_start = std::time::Instant::now();
                            match chunk_writer.write(drained).await {
                                Ok(result) => {
                                    // Register all series under a single write lock
                                    // acquisition — atomic from the query path's view.
                                    let mut index = index.write().await;
                                    for s in &result.series_results {
                                        index.register(
                                            &s.series_key,
                                            s.entry.clone(),
                                            s.stats.clone(),
                                            result.chunk_meta.clone(),
                                        );
                                    }
                                    metrics::chunk_files_total()
                                        .set(index.chunk_file_count() as i64);
                                    metrics::index_series_count()
                                        .set(index.series_count() as i64);
                                    drop(index);

                                    metrics::chunk_bytes_written_total()
                                        .inc_by(result.chunk_meta.file_size);
                                    metrics::memtable_flush_duration_seconds()
                                        .observe(flush_start.elapsed().as_secs_f64());
                                    metrics::memtable_flush_total()
                                        .with_label_values(&["ok"])
                                        .inc();
                                    tracing::info!(
                                        shard   = i,
                                        series  = result.series_results.len(),
                                        chunk   = ?result.chunk_meta.file_path,
                                        "memtable flushed"
                                    );

                                    let seq = wal.current_sequence();
                                    watermarks[i].store(seq, Ordering::Release);
                                }
                                Err(e) => {
                                    metrics::memtable_flush_total()
                                        .with_label_values(&["error"])
                                        .inc();
                                    tracing::error!(shard = i, error = %e, "flush failed");
                                }
                            }
                        }
                    }

                    metrics::memtable_size_bytes().set(total_memtable_bytes as i64);

                    let safe_seq = watermarks
                        .iter()
                        .map(|w| w.load(Ordering::Acquire))
                        .min()
                        .unwrap_or(0);
                    match wal.drain_completed_before(safe_seq).await {
                        Ok(paths) => {
                            let mut deleted = 0usize;
                            for path in &paths {
                                if let Err(e) = tokio::fs::remove_file(path).await {
                                    // NotFound is benign — cleaned up by a previous run.
                                    tracing::warn!(path = ?path, error = %e, "failed to delete WAL segment");
                                    continue;
                                }
                                deleted += 1;
                            }
                            if deleted == 0 && !paths.is_empty() {
                                tracing::warn!(to_delete = paths.len(), "failed to clean up WAL segments");
                            } else if deleted > 0 {
                                tracing::info!(deleted, safe_seq, "WAL segments cleaned up");
                            }
                        }
                        Err(e) => tracing::error!(error = %e, "WAL segment GC failed"),
                    };
                }
            });
        }
    }
}
