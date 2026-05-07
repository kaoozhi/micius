// storage-engine/tests/chunkreader_test.rs
mod common;

use common::*;
use std::collections::BTreeMap;
use storage_engine::chunk::reader::ChunkReader;
use storage_engine::types::SeriesKey;

// ---------------------------------------------------------------------------
// Happy-path roundtrip tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_read_single_series_roundtrip() {
    let key = series_key("cpu.usage", "node-0");
    let points = make_points(1_000_000_000, 1_000_000, 50);
    let mut data = BTreeMap::new();
    data.insert(key.clone(), points.clone());

    let (_dir, path) = write_chunk(data).await;

    let result = ChunkReader::read_series(&path, &key, i64::MIN, i64::MAX)
        .await
        .expect("read_series failed");

    let got = result.expect("expected Some, got None");
    assert_eq!(got.len(), 50);
    for (i, dp) in got.iter().enumerate() {
        assert_eq!(
            dp.timestamp_ns, points[i].0,
            "timestamp mismatch at index {}",
            i
        );
        assert_eq!(dp.value, points[i].1, "value mismatch at index {}", i);
    }
}

#[tokio::test]
async fn test_read_time_range_filter() {
    let key = series_key("cpu.usage", "node-0");
    // 100 points at 1s intervals: timestamps [0, 1s, 2s, ..., 99s]
    let points = make_points(0, 1_000_000_000, 100);
    let mut data = BTreeMap::new();
    data.insert(key.clone(), points.clone());

    let (_dir, path) = write_chunk(data).await;

    let time_start = 25 * 1_000_000_000i64;
    let time_end = 74 * 1_000_000_000i64;

    let got = ChunkReader::read_series(&path, &key, time_start, time_end)
        .await
        .expect("read_series failed")
        .expect("expected Some, got None");

    // Points 25..=74 — 50 points
    assert_eq!(got.len(), 50, "expected 50 points in [25s, 74s]");
    assert!(
        got.iter()
            .all(|dp| dp.timestamp_ns >= time_start && dp.timestamp_ns <= time_end),
        "all returned points must be within the queried range"
    );
    assert_eq!(got.first().unwrap().timestamp_ns, time_start);
    assert_eq!(got.last().unwrap().timestamp_ns, time_end);
}

#[tokio::test]
async fn test_read_multi_series_isolation() {
    let key_a = series_key("cpu.usage", "node-0");
    let key_b = series_key("mem.free", "node-1");
    let key_c = series_key("disk.io", "node-2");

    let pts_a = make_points(1_000, 100, 5);
    let pts_b = make_points(2_000, 100, 7);
    let pts_c = make_points(3_000, 100, 3);

    let mut data = BTreeMap::new();
    data.insert(key_a.clone(), pts_a.clone());
    data.insert(key_b.clone(), pts_b.clone());
    data.insert(key_c.clone(), pts_c.clone());

    let (_dir, path) = write_chunk(data).await;

    for (key, expected_pts) in [(&key_a, &pts_a), (&key_b, &pts_b), (&key_c, &pts_c)] {
        let got = ChunkReader::read_series(&path, key, i64::MIN, i64::MAX)
            .await
            .expect("read_series failed")
            .expect("expected Some, got None");

        assert_eq!(got.len(), expected_pts.len());
        for (i, dp) in got.iter().enumerate() {
            assert_eq!(dp.timestamp_ns, expected_pts[i].0);
            assert_eq!(dp.value, expected_pts[i].1);
        }
    }
}

#[tokio::test]
async fn test_read_series_absent_returns_none() {
    let written_key = series_key("cpu.usage", "node-0");
    let absent_key = series_key("mem.free", "node-99");

    let mut data = BTreeMap::new();
    data.insert(written_key, make_points(1_000_000_000, 1_000_000, 10));

    let (_dir, path) = write_chunk(data).await;

    let result = ChunkReader::read_series(&path, &absent_key, i64::MIN, i64::MAX)
        .await
        .expect("read_series failed");

    assert!(result.is_none(), "expected None for absent series");
}

#[tokio::test]
async fn test_read_no_time_overlap_returns_none() {
    let key = series_key("cpu.usage", "node-0");
    // chunk covers [1_000_000_000, ~1_099_000_000]
    let mut data = BTreeMap::new();
    data.insert(key.clone(), make_points(1_000_000_000, 1_000_000, 100));

    let (_dir, path) = write_chunk(data).await;

    let result = ChunkReader::read_series(&path, &key, i64::MAX - 1, i64::MAX)
        .await
        .expect("read_series failed");

    assert!(
        result.is_none(),
        "expected None when query is entirely outside chunk range"
    );
}

#[tokio::test]
async fn test_read_partial_time_overlap() {
    let key = series_key("cpu.usage", "node-0");
    // 10 points at [100, 200, 300, ..., 1000]
    let points = make_points(100, 100, 10);
    let mut data = BTreeMap::new();
    data.insert(key.clone(), points);

    let (_dir, path) = write_chunk(data).await;

    // Query [500, 800] — should return points at 500, 600, 700, 800
    let got = ChunkReader::read_series(&path, &key, 500, 800)
        .await
        .expect("read_series failed")
        .expect("expected Some, got None");

    assert_eq!(got.len(), 4);
    assert_eq!(got[0].timestamp_ns, 500);
    assert_eq!(got[3].timestamp_ns, 800);
}

// ---------------------------------------------------------------------------
// Failure-path tests — one per pruning stage
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_corrupt_magic_returns_error() {
    let key = series_key("cpu.usage", "node-0");
    let mut data = BTreeMap::new();
    data.insert(key.clone(), make_points(1_000_000_000, 1_000_000, 10));

    let (_dir, path) = write_chunk(data).await;

    // Overwrite the 4-byte magic field at offset 0
    corrupt_bytes(&path, 0..4, &0xDEADBEEFu32.to_le_bytes());

    let result = ChunkReader::read_series(&path, &key, i64::MIN, i64::MAX).await;
    assert!(
        result.is_err(),
        "expected Err for corrupt magic, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_corrupt_checksum_returns_error() {
    let key = series_key("cpu.usage", "node-0");
    let mut data = BTreeMap::new();
    data.insert(key.clone(), make_points(1_000_000_000, 1_000_000, 10));

    let (_dir, path) = write_chunk(data).await;

    // Overwrite the last 4 bytes (CRC32 field) with all 0xFF
    let file_len = std::fs::metadata(&path).unwrap().len() as usize;
    corrupt_bytes(&path, (file_len - 4)..file_len, &[0xFF, 0xFF, 0xFF, 0xFF]);

    let result = ChunkReader::read_series(&path, &key, i64::MIN, i64::MAX).await;
    assert!(
        result.is_err(),
        "expected Err for corrupt checksum, got {:?}",
        result
    );
}

#[tokio::test]
async fn test_check_bloom_present_series_returns_true() {
    let key = series_key("cpu", "host-0");
    let (_dir, path) = write_chunk(single_series_data("cpu", "host-0", 10)).await;
    let result = ChunkReader::check_bloom(&path, &key)
        .await
        .expect("check_bloom failed");
    assert!(result);
}

#[tokio::test]
async fn test_bloom_absent_series_returns_none() {
    // Write a chunk containing only "cpu.usage,host=node-0"
    let written_key = series_key("cpu.usage", "node-0");
    let mut data = BTreeMap::new();
    data.insert(written_key, make_points(1_000_000_000, 1_000_000, 10));
    let (_dir, path) = write_chunk(data).await;

    // Bloom is sized for 1 item at 1% FP rate — any single absent key has ~1% chance
    // of being a false positive. Check 50 distinct absent keys: P(all 50 are false
    // positives) ≈ (0.01)^50 ≈ 10^-100.
    let mut any_rejected = false;
    for i in 0u32..50 {
        let absent = SeriesKey {
            metric_name: format!("absent.metric.{}", i),
            tags: BTreeMap::from([("k".to_string(), format!("v{}", i))]),
        };
        if !ChunkReader::check_bloom(&path, &absent)
            .await
            .expect("bloom check failed")
        {
            any_rejected = true;
            break;
        }
    }
    assert!(
        any_rejected,
        "bloom returned true for all 50 absent keys — filter may be saturated"
    );
}

#[tokio::test]
async fn test_header_time_range_no_overlap_returns_none() {
    let key = series_key("cpu.usage", "node-0");
    // Chunk covers [1_000_000_000, ~1_099_000_000]
    let mut data = BTreeMap::new();
    data.insert(key.clone(), make_points(1_000_000_000, 1_000_000, 100));

    let (_dir, path) = write_chunk(data).await;

    // Query entirely beyond the chunk's time range
    let result = ChunkReader::read_series(&path, &key, i64::MAX - 1_000, i64::MAX)
        .await
        .expect("read_series failed");

    assert!(
        result.is_none(),
        "expected None — query outside chunk time range"
    );
}

#[tokio::test]
async fn test_corrupt_directory_key_returns_none() {
    let key = series_key("cpu.usage", "node-0");
    let mut data = BTreeMap::new();
    data.insert(key.clone(), make_points(1_000_000_000, 1_000_000, 10));

    let (_dir, path) = write_chunk(data).await;

    // The first directory entry starts at HEADER_SIZE (48).
    // Layout: [key_len: u32 at 48..52][key_bytes at 52..52+key_len][...]
    // Zero out the key bytes and recompute CRC so the file passes the checksum
    // stage and reaches the directory scan.
    let file_bytes = std::fs::read(&path).unwrap();
    let key_len = u32::from_le_bytes(file_bytes[48..52].try_into().unwrap()) as usize;
    corrupt_bytes_recompute_crc(&path, 52..(52 + key_len), &vec![0u8; key_len]);

    let result = ChunkReader::read_series(&path, &key, i64::MIN, i64::MAX)
        .await
        .expect("read_series must not error — CRC is valid");

    assert!(
        result.is_none(),
        "expected None — directory key corrupted, series not found"
    );
}
