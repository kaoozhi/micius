#![allow(unused)] // imports reserved for torn write / checksum tests
use lz4_flex::frame;
use prost::Message;
use std::collections::BTreeMap;
use storage_engine::types::DataPoint;
use storage_engine::wal::recovery::recover;
use storage_engine::wal::writer::WalWriter;
use tempfile::tempdir;

fn assert_points_eq(expected: &[DataPoint], actual: &[DataPoint], offset: usize) {
    for i in 0..expected.len() {
        assert_eq!(expected[i], actual[offset + i]);
    }
}

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
    let test_dir = tempdir().expect("failed to create temp dir");
    let mut wal = WalWriter::open(test_dir.path(), MAX_SEGMENT_BYTES, 0)
        .await
        .expect("failed to open WAL");

    for ii in 0..3 {
        let seq = wal
            .append(&sample_points(5))
            .await
            .expect("failed to append");
        assert_eq!(seq, ii + 1);
    }
}

#[tokio::test]
async fn test_segment_rotation() {
    let mut wal_size: usize = 0;
    let mut batch: u8 = 0;

    let test_dir = tempdir().expect("failed to create temp dir");
    let mut wal = WalWriter::open(test_dir.path(), MAX_SEGMENT_BYTES, 0)
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

#[tokio::test]
async fn test_append_and_recover() {
    // Single segment — large max to prevent rotation
    let test_dir = tempdir().expect("failed to create temp dir");
    let mut wal = WalWriter::open(test_dir.path(), 1024 * 1024, 0)
        .await
        .expect("failed to open WAL");
    let batch_1 = sample_points(1);
    let batch_2 = sample_points(2);
    wal.append(&batch_1).await.expect("failed to append");
    wal.append(&batch_2).await.expect("failed to append");

    drop(wal);
    let recovered = recover(test_dir.path()).await.expect("failed to recover");
    assert_eq!(recovered.points.len(), batch_1.len() + batch_2.len());
    assert_points_eq(&batch_1, &recovered.points, 0);
    assert_points_eq(&batch_2, &recovered.points, batch_1.len());
}

#[tokio::test]
async fn test_torn_write_stops_recovery() {
    // Write 2 batches, truncate last entry mid-payload, recover — only first batch returned
    let test_dir = tempdir().expect("failed to create temp dir");
    let mut wal = WalWriter::open(test_dir.path(), 1024 * 1024, 0)
        .await
        .expect("failed to open WAL");
    let batch_1 = sample_points(3);
    let batch_2 = sample_points(3);
    wal.append(&batch_1).await.expect("failed to append");
    wal.append(&batch_2).await.expect("failed to append");

    drop(wal);

    // Truncate the segment mid-payload of the last entry to simulate a torn write.
    let segment = std::fs::read_dir(test_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
        .map(|e| e.path())
        .expect("no wal file found");
    let file = std::fs::OpenOptions::new()
        .write(true)
        .open(&segment)
        .unwrap();
    let size = file.metadata().unwrap().len();
    // Chop 5 bytes — small enough to leave the header intact,
    // large enough to land inside batch_2's payload.
    file.set_len(size - 5).unwrap();

    let recovered = recover(test_dir.path()).await.expect("failed to recover");
    assert!(
        recovered.torn_write_detected,
        "expected torn write to be detected"
    );
    assert_eq!(recovered.points.len(), batch_1.len());
    assert_points_eq(&batch_1, &recovered.points, 0);
}

#[tokio::test]
async fn test_checksum_mismatch_stops_recovery() {
    // write 2 batches, flip a bit in second payload, recover — only first batch returned
    let test_dir = tempdir().expect("failed to create temp dir");
    let mut wal = WalWriter::open(test_dir.path(), 1024 * 1024, 0)
        .await
        .expect("failed to open WAL");
    println!("batch size {}", get_wal_entry_size(&sample_points(3), 0));
    let batch_1 = sample_points(3);
    let batch_2 = sample_points(3);
    // let batch_3 = sample_points(3);

    wal.append(&batch_1).await.expect("failed to append");
    wal.append(&batch_2).await.expect("failed to append");
    // wal.append(&batch_3).await.expect("failed to append");

    drop(wal);
    let segment = std::fs::read_dir(test_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .find(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
        .map(|e| e.path())
        .expect("no wal file found");
    let mut bytes = std::fs::read(&segment).unwrap();
    // Target a byte inside batch_2's payload.
    // Frame layout: [len:4][crc:4][payload]. batch_1 frame is ~140 bytes.
    // So an offset past the first frame and inside batch_2's payload works.
    let target = bytes.len() - 10; // somewhere in the last payload
    bytes[target] ^= 0x01;
    std::fs::write(&segment, &bytes).unwrap();
    let recovered = recover(test_dir.path()).await.expect("failed to recover");
    assert!(
        recovered.torn_write_detected,
        "expected torn write to be detected"
    );
    assert_eq!(recovered.points.len(), batch_1.len());
    assert_points_eq(&batch_1, &recovered.points, 0);
}

#[tokio::test]
async fn test_recovery_across_segments() {
    // Small max_segment_bytes forces rotation across multiple segments
    let test_dir = tempdir().expect("failed to create temp dir");
    let mut wal = WalWriter::open(test_dir.path(), MAX_SEGMENT_BYTES, 0)
        .await
        .expect("failed to open WAL");
    let batch_1 = sample_points(100);
    let batch_2 = sample_points(200);
    wal.append(&batch_1).await.expect("failed to append");
    wal.append(&batch_2).await.expect("failed to append");

    drop(wal);
    let recovered = recover(test_dir.path()).await.expect("failed to recover");
    assert_eq!(recovered.points.len(), batch_1.len() + batch_2.len());
    assert_points_eq(&batch_1, &recovered.points, 0);
    assert_points_eq(&batch_2, &recovered.points, batch_1.len());
    assert!(
        recovered.segments_replayed > 1,
        "expected multi-segment recovery but only replayed one segment"
    );
}
