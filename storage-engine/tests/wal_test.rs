#![allow(unused)]
use lz4_flex::frame;
use prost::Message;
use std::collections::BTreeMap;
use storage_engine::types::DataPoint;
use storage_engine::wal::writer::WalWriter;
use tempfile::tempdir;

fn sample_points(n: usize) -> Vec<DataPoint> {
    (0..n)
        .map(|i| DataPoint {
            metric_name: "cpu.usage".to_string(),
            tags: BTreeMap::from([("host".to_string(), format!("node-{}", i % 3))]),
            timestamp_ns: 1_000_000_000 + i as i64,
            value: 42.0 + i as f64,
        })
        .collect()
}

fn get_wal_entry_size(points: &[DataPoint], sequence: u64) -> usize {
    use storage_engine::wal::proto::WalEntry;
    let entry = WalEntry {
        sequence,
        points: points.iter().map(Into::into).collect(),
    };
    let payload_len = entry.encode_to_vec().len();
    payload_len + 4 + 4 // length prefix + checksum + payload
}

// ---------------------------------------------------------------------------
// Writer tests (no recovery needed)
// ---------------------------------------------------------------------------
const MAX_SEGMENT_BYTES: u64 = 256;

#[tokio::test]
async fn test_append_returns_incrementing_sequence() {
    // todo!("append 3 batches, assert returned sequences are 1, 2, 3")
    let test_dir = tempdir().expect("failed to create temp dir");
    let mut wal = WalWriter::open(test_dir.path(), MAX_SEGMENT_BYTES)
        .await
        .expect("failed to open WAL");

    for ii in 0..3 {
        let sequence = wal
            .append(&sample_points(5))
            .await
            .expect("failed to append");
        assert_eq!(
            wal.current_sequence(),
            ii + 1,
            "returned sequence: {}, expected sequence {}",
            wal.current_sequence(),
            ii + 1
        );
    }
}

#[tokio::test]
async fn test_segment_rotation() {
    // todo!("open with small max_segment_bytes, write until rotation, assert second .wal file exists")
    let mut wal_size: usize = 0;
    let mut batch: u8 = 0;

    let test_dir = tempdir().expect("failed to create temp dir");
    let mut wal = WalWriter::open(test_dir.path(), MAX_SEGMENT_BYTES)
        .await
        .expect("failed to open WAL");
    loop {
        let points = sample_points(5);
        let sequence = wal.append(&points).await.expect("failed to append");
        wal_size += get_wal_entry_size(&points, sequence);
        batch += 1;

        let wal_files: Vec<_> = std::fs::read_dir(test_dir.path())
            .unwrap()
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
            .collect();
        println!("current wal size: {wal_size}");
        if wal_files.len() == 2 {
            println!("rotation happened after {batch} batches ({wal_size} bytes)");
            return;
        }

        assert!(
            batch < 3,
            "expected rotation within a few batches but didn't happen"
        );
    }
}

// ---------------------------------------------------------------------------
// Recovery tests (need recovery.rs)
// ---------------------------------------------------------------------------

// #[tokio::test]
// async fn test_append_and_recover() {
//     todo!("write 3 batches, drop writer, recover — all points returned")
// }

// #[tokio::test]
// async fn test_torn_write_stops_recovery() {
//     todo!("write 2 batches, truncate last entry mid-payload, recover — only first batch returned")
// }

// #[tokio::test]
// async fn test_checksum_mismatch_stops_recovery() {
//     todo!("write 2 batches, flip a bit in second payload, recover — only first batch returned")
// }

// #[tokio::test]
// async fn test_recovery_across_segments() {
//     todo!(
//         "open with small max_segment_bytes, write across 2 segments, recover — all entries returned"
//     )
// }
