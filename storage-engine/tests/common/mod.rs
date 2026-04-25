#![allow(dead_code)]

use crc32fast::Hasher as CrcHasher;
use std::collections::BTreeMap;
use std::ops::Range;
use std::path::Path;
use storage_engine::chunk::writer::{ChunkWriteResult, ChunkWriter};
use storage_engine::types::SeriesKey;
use tempfile::{TempDir, tempdir};

/// Build a SeriesKey with a single "host" tag.
pub fn series_key(metric: &str, host: &str) -> SeriesKey {
    SeriesKey {
        metric_name: metric.to_string(),
        tags: BTreeMap::from([("host".to_string(), host.to_string())]),
    }
}

/// Produce n points starting at ts_start, incrementing by step_ns.
/// Values are i as f64.
pub fn make_points(ts_start: i64, step_ns: i64, n: usize) -> Vec<(i64, f64)> {
    (0..n)
        .map(|i| (ts_start + i as i64 * step_ns, i as f64))
        .collect()
}

/// Build a single-series BTreeMap with one series and n points.
pub fn single_series_data(
    metric: &str,
    host: &str,
    n: usize,
) -> BTreeMap<SeriesKey, Vec<(i64, f64)>> {
    let mut data = BTreeMap::new();
    data.insert(
        series_key(metric, host),
        make_points(1_000_000_000, 1_000_000, n),
    );
    data
}

/// Build a multi-series BTreeMap with m series, each having n points.
pub fn multi_series_data(m: usize, n: usize) -> BTreeMap<SeriesKey, Vec<(i64, f64)>> {
    let mut data = BTreeMap::new();
    for i in 0..m {
        data.insert(
            series_key("cpu.usage", &format!("node-{}", i)),
            make_points(1_000_000_000 + i as i64 * 1000, 1_000_000, n),
        );
    }
    data
}

/// Write a chunk and return (dir guard, result, raw file bytes).
pub async fn write_and_read_bytes(
    data: BTreeMap<SeriesKey, Vec<(i64, f64)>>,
) -> (TempDir, ChunkWriteResult, Vec<u8>) {
    let dir = tempdir().expect("failed to create temp dir");
    let writer = ChunkWriter::new(dir.path());
    let result = writer.write(data).await.expect("chunk write failed");
    let bytes = std::fs::read(&result.chunk_meta.file_path).expect("failed to read chunk file");
    (dir, result, bytes)
}

/// Write a chunk and return (dir guard, path to the chunk file).
pub async fn write_chunk(
    data: BTreeMap<SeriesKey, Vec<(i64, f64)>>,
) -> (TempDir, std::path::PathBuf) {
    let dir = tempdir().expect("failed to create temp dir");
    let writer = ChunkWriter::new(dir.path());
    let result = writer.write(data).await.expect("chunk write failed");
    let path = result.chunk_meta.file_path;
    (dir, path)
}

/// Write a chunk and return (dir guard, path to the chunk file).
pub async fn write_chunk_with_results(
    data: BTreeMap<SeriesKey, Vec<(i64, f64)>>,
) -> (TempDir, ChunkWriteResult) {
    let dir = tempdir().expect("failed to create temp dir");
    let writer = ChunkWriter::new(dir.path());
    let result = writer.write(data).await.expect("chunk write failed");
    (dir, result)
}

/// Overwrite bytes at `range` in the file at `path` with `new_bytes`.
/// Panics if range length != new_bytes length.
pub fn corrupt_bytes(path: &Path, range: Range<usize>, new_bytes: &[u8]) {
    assert_eq!(
        range.len(),
        new_bytes.len(),
        "corrupt_bytes: length mismatch"
    );
    let mut bytes = std::fs::read(path).expect("failed to read file for corruption");
    bytes[range].copy_from_slice(new_bytes);
    std::fs::write(path, &bytes).expect("failed to write corrupted file");
}

/// Overwrite bytes at `range` then recompute and patch the trailing CRC32
/// so the file passes the checksum stage. Used when corrupting bytes that
/// would otherwise be caught by the CRC check before reaching the target stage.
pub fn corrupt_bytes_recompute_crc(path: &Path, range: Range<usize>, new_bytes: &[u8]) {
    assert_eq!(
        range.len(),
        new_bytes.len(),
        "corrupt_bytes_recompute_crc: length mismatch"
    );
    let mut bytes = std::fs::read(path).expect("failed to read file for corruption");
    bytes[range].copy_from_slice(new_bytes);
    // CRC covers all bytes except the last 4 (the checksum field itself).
    let mut hasher = CrcHasher::new();
    hasher.update(&bytes[..bytes.len() - 4]);
    let checksum = hasher.finalize();
    let len = bytes.len();
    bytes[len - 4..].copy_from_slice(&checksum.to_le_bytes());
    std::fs::write(path, &bytes).expect("failed to write corrupted file");
}
