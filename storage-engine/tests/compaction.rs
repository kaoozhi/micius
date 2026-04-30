mod common;
use std::collections::BTreeMap;
use std::sync::Arc;
use storage_engine::chunk::reader::ChunkReader;
use storage_engine::chunk::writer::ChunkWriter;
use storage_engine::compaction::CompactionWorker;
use storage_engine::index::chunk_index::ChunkIndex;
use storage_engine::types::*;
use tempfile::tempdir;
use tokio::sync::RwLock;

fn cpu_key(host: &str) -> SeriesKey {
    SeriesKey {
        metric_name: "cpu".into(),
        tags: BTreeMap::from([("host".into(), host.into())]),
    }
}

/// Write one chunk for `key` starting at `ts_start` with `n` points and
/// register it in `index`. Returns the written file path.
async fn write_and_register(
    writer: &ChunkWriter,
    index: &mut ChunkIndex,
    key: SeriesKey,
    ts_start: i64,
    n: usize,
) -> std::path::PathBuf {
    let mut data = BTreeMap::new();
    data.insert(
        key,
        (0..n)
            .map(|i| (ts_start + i as i64 * 1_000_000_000, i as f64))
            .collect::<Vec<_>>(),
    );
    let result = writer.write(data).await.expect("write failed");
    let path = result.chunk_meta.file_path.clone();
    for s in &result.series_results {
        index.register(
            &s.series_key,
            s.entry.clone(),
            s.stats.clone(),
            result.chunk_meta.clone(),
        );
    }
    path
}

// ── Gate test (path: compaction::tests::compacted_chunks_queryable) ──────────

mod tests {
    use super::*;

    #[tokio::test]
    async fn compacted_chunks_queryable() {
        let dir = tempdir().unwrap();
        let writer = Arc::new(ChunkWriter::new(dir.path()));
        let index = Arc::new(RwLock::new(ChunkIndex::new()));
        let key = cpu_key("web1");

        // Write 3 chunks — each covers a disjoint 10-second window (10 points each)
        for chunk in 0..3i64 {
            write_and_register(
                &writer,
                &mut *index.write().await,
                key.clone(),
                chunk * 10 * 1_000_000_000,
                10,
            )
            .await;
        }
        assert_eq!(
            index.read().await.chunk_file_count(),
            3,
            "should start with 3 chunks"
        );

        // Compact — min_threshold=2, size_ratio=10.0 (all 3 files merge)
        let worker = CompactionWorker::new(Arc::clone(&index), Arc::clone(&writer), 2, 10.0);
        let result = worker.compact_once().await.expect("compaction failed");
        assert_eq!(result.chunks_merged, 3, "all 3 input files must be counted");
        assert!(
            result.bytes_freed > 0,
            "merged file must be smaller than 3 separate files"
        );

        // Fewer chunks in the index after compaction
        let chunk_count = index.read().await.chunk_file_count();
        assert!(
            chunk_count < 3,
            "compaction should reduce chunk count, got {chunk_count}"
        );

        // All 30 data points are still queryable
        let series_id = SeriesId::from(&key);
        let chunks = index
            .read()
            .await
            .prune_chunks(&series_id, i64::MIN, i64::MAX, None);
        assert!(!chunks.is_empty(), "no chunks found after compaction");

        let mut total_points = 0usize;
        for entry in &chunks {
            let file_path = index
                .read()
                .await
                .chunk_files
                .get(&entry.chunk_id)
                .expect("chunk_files must have entry for merged chunk")
                .file_path
                .clone();
            let points = ChunkReader::read_series(&file_path, &key, i64::MIN, i64::MAX)
                .await
                .expect("read_series failed")
                .unwrap_or_default();
            total_points += points.len();
        }
        assert_eq!(total_points, 30, "all 30 points must survive compaction");
    }
}

// ── File deletion and index cleanup ──────────────────────────────────────────

#[tokio::test]
async fn test_old_chunk_files_deleted_after_compaction() {
    let dir = tempdir().unwrap();
    let writer = Arc::new(ChunkWriter::new(dir.path()));
    let index = Arc::new(RwLock::new(ChunkIndex::new()));
    let key = cpu_key("web1");

    let mut old_paths = Vec::new();
    for chunk in 0..3i64 {
        let path = write_and_register(
            &writer,
            &mut *index.write().await,
            key.clone(),
            chunk * 10 * 1_000_000_000,
            10,
        )
        .await;
        old_paths.push(path);
    }

    for path in &old_paths {
        assert!(
            path.exists(),
            "old chunk file should exist before compaction: {path:?}"
        );
    }

    let worker = CompactionWorker::new(Arc::clone(&index), Arc::clone(&writer), 2, 10.0);
    let result = worker.compact_once().await.expect("compaction failed");
    assert_eq!(result.chunks_merged, 3, "3 input files merged");

    for path in &old_paths {
        assert!(
            !path.exists(),
            "old chunk file should be deleted after compaction: {path:?}"
        );
    }
}

#[tokio::test]
async fn test_old_chunk_ids_removed_from_index() {
    let dir = tempdir().unwrap();
    let writer = Arc::new(ChunkWriter::new(dir.path()));
    let index = Arc::new(RwLock::new(ChunkIndex::new()));
    let key = cpu_key("web1");

    let mut old_chunk_ids = Vec::new();
    for chunk in 0..3i64 {
        let mut data = BTreeMap::new();
        data.insert(
            key.clone(),
            (0..10)
                .map(|i| {
                    (
                        chunk * 10 * 1_000_000_000 + i as i64 * 1_000_000_000,
                        i as f64,
                    )
                })
                .collect::<Vec<_>>(),
        );
        let result = writer.write(data).await.expect("write failed");
        old_chunk_ids.push(result.chunk_id);
        let mut idx = index.write().await;
        for s in &result.series_results {
            idx.register(
                &s.series_key,
                s.entry.clone(),
                s.stats.clone(),
                result.chunk_meta.clone(),
            );
        }
    }

    let worker = CompactionWorker::new(Arc::clone(&index), Arc::clone(&writer), 2, 10.0);
    let result = worker.compact_once().await.expect("compaction failed");
    assert_eq!(result.chunks_merged, 3, "3 input files merged");

    let idx = index.read().await;
    for old_id in &old_chunk_ids {
        assert!(
            !idx.chunk_files.contains_key(old_id),
            "old chunk_id {old_id} must be removed from chunk_files after compaction"
        );
    }
    assert_eq!(
        idx.chunk_file_count(),
        1,
        "exactly one merged chunk file in index"
    );
}

// ── Threshold and edge cases ──────────────────────────────────────────────────

#[tokio::test]
async fn test_no_compaction_when_below_threshold() {
    let dir = tempdir().unwrap();
    let writer = Arc::new(ChunkWriter::new(dir.path()));
    let index = Arc::new(RwLock::new(ChunkIndex::new()));
    let key = cpu_key("web1");

    let mut old_paths = Vec::new();
    for chunk in 0..2i64 {
        let path = write_and_register(
            &writer,
            &mut *index.write().await,
            key.clone(),
            chunk * 10 * 1_000_000_000,
            10,
        )
        .await;
        old_paths.push(path);
    }

    // min_threshold=3 but only 2 chunks — no merge should happen
    let worker = CompactionWorker::new(Arc::clone(&index), Arc::clone(&writer), 3, 10.0);
    let result = worker.compact_once().await.expect("compact_once failed");
    assert_eq!(result.chunks_merged, 0, "no merge below threshold");
    assert_eq!(result.bytes_freed, 0, "no bytes freed below threshold");

    assert_eq!(
        index.read().await.chunk_file_count(),
        2,
        "chunk count must be unchanged"
    );
    for path in &old_paths {
        assert!(
            path.exists(),
            "file should not be deleted when below threshold: {path:?}"
        );
    }
}

#[tokio::test]
async fn test_compact_once_empty_index_is_noop() {
    let dir = tempdir().unwrap();
    let writer = Arc::new(ChunkWriter::new(dir.path()));
    let index = Arc::new(RwLock::new(ChunkIndex::new()));

    let worker = CompactionWorker::new(Arc::clone(&index), Arc::clone(&writer), 2, 2.0);
    let result = worker
        .compact_once()
        .await
        .expect("compact_once must succeed on empty index");

    assert_eq!(result.chunks_merged, 0, "nothing to merge on empty index");
    assert_eq!(result.bytes_freed, 0, "no bytes freed on empty index");
    assert_eq!(index.read().await.chunk_file_count(), 0);
}

#[tokio::test]
async fn test_no_compaction_when_sizes_exceed_ratio() {
    let dir = tempdir().unwrap();
    let writer = Arc::new(ChunkWriter::new(dir.path()));
    let index = Arc::new(RwLock::new(ChunkIndex::new()));
    let key = cpu_key("web1");

    // Small chunk: 5 points
    write_and_register(&writer, &mut *index.write().await, key.clone(), 0, 5).await;
    // Large chunk: 500 points — much larger than small chunk
    write_and_register(
        &writer,
        &mut *index.write().await,
        key.clone(),
        1_000 * 1_000_000_000,
        500,
    )
    .await;

    // size_ratio=1.1 — only files within 10% of each other merge
    let worker = CompactionWorker::new(Arc::clone(&index), Arc::clone(&writer), 2, 1.1);
    let result = worker.compact_once().await.expect("compact_once failed");
    assert_eq!(result.chunks_merged, 0, "sizes exceed ratio — no merge");

    assert_eq!(
        index.read().await.chunk_file_count(),
        2,
        "chunks with very different sizes must not be merged"
    );
}

// ── Merge correctness ────────────────────────────────────────────────────────

#[tokio::test]
async fn test_merged_data_is_sorted_and_complete() {
    // Two chunks covering disjoint time windows — the normal case for size-tiered
    // compaction. Verifies the merged chunk contains all points in timestamp order.
    let dir = tempdir().unwrap();
    let writer = Arc::new(ChunkWriter::new(dir.path()));
    let index = Arc::new(RwLock::new(ChunkIndex::new()));
    let key = cpu_key("web1");

    // Chunk 1: [0s, 9s] — 10 points
    write_and_register(&writer, &mut *index.write().await, key.clone(), 0, 10).await;
    // Chunk 2: [200s, 209s] — 10 points (strictly after chunk 1)
    write_and_register(
        &writer,
        &mut *index.write().await,
        key.clone(),
        200 * 1_000_000_000,
        10,
    )
    .await;

    let worker = CompactionWorker::new(Arc::clone(&index), Arc::clone(&writer), 2, 10.0);
    let result = worker.compact_once().await.expect("compaction failed");
    assert_eq!(result.chunks_merged, 2, "2 input files merged into 1");

    let series_id = SeriesId::from(&key);
    let chunks = index
        .read()
        .await
        .prune_chunks(&series_id, i64::MIN, i64::MAX, None);
    assert_eq!(chunks.len(), 1, "two chunks must merge into one");

    let file_path = index
        .read()
        .await
        .chunk_files
        .get(&chunks[0].chunk_id)
        .expect("chunk_files entry must exist")
        .file_path
        .clone();

    let points = ChunkReader::read_series(&file_path, &key, i64::MIN, i64::MAX)
        .await
        .expect("read_series failed")
        .expect("series must be present in merged chunk");

    // All 20 points preserved
    assert_eq!(points.len(), 20, "all 20 points must be in merged chunk");

    // Points are sorted by timestamp
    let sorted = points
        .windows(2)
        .all(|w| w[0].timestamp_ns < w[1].timestamp_ns);
    assert!(sorted, "merged points must be in ascending timestamp order");

    // First and last timestamps are correct
    assert_eq!(
        points.first().unwrap().timestamp_ns,
        0,
        "first point at ts=0"
    );
    assert_eq!(
        points.last().unwrap().timestamp_ns,
        209 * 1_000_000_000,
        "last point at ts=19s"
    );
}
