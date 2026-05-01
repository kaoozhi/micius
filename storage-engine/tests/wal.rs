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
// WAL clearing tests
// ---------------------------------------------------------------------------

fn count_wal_files(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .filter(|e| e.path().extension().is_some_and(|ext| ext == "wal"))
        .count()
}

#[tokio::test]
async fn test_resume_seq_continuous_across_restart() {
    let test_dir = tempdir().expect("failed to create temp dir");

    // Session 1: write 3 batches — sequences 1, 2, 3
    let mut wal = WalWriter::open(test_dir.path(), 1024 * 1024, 0)
        .await
        .expect("failed to open WAL");
    for _ in 0..3 {
        wal.append(&sample_points(1))
            .await
            .expect("failed to append");
    }
    assert_eq!(wal.current_sequence(), 3);
    drop(wal);

    // Simulate restart: recover to find last_sequence
    let recovered = recover(test_dir.path()).await.expect("failed to recover");
    assert_eq!(recovered.last_sequence, 3);

    // Session 2: open with resume_seq from recovery
    let mut wal = WalWriter::open(test_dir.path(), 1024 * 1024, recovered.last_sequence)
        .await
        .expect("failed to reopen WAL");

    // First append after restart must produce 4, not 1
    let seq = wal
        .append(&sample_points(1))
        .await
        .expect("failed to append");
    assert_eq!(
        seq, 4,
        "sequence must continue from last_sequence + 1 across restarts"
    );
}

#[tokio::test]
async fn test_drain_completed_before_no_rotation() {
    // Single segment, no rotation → drain always returns empty
    let test_dir = tempdir().expect("failed to create temp dir");
    let mut wal = WalWriter::open(test_dir.path(), 1024 * 1024, 0)
        .await
        .expect("failed to open WAL");
    wal.append(&sample_points(1))
        .await
        .expect("failed to append");

    assert!(
        wal.drain_completed_before(0).is_empty(),
        "no completed segments → empty even at seq 0"
    );
    assert!(
        wal.drain_completed_before(u64::MAX).is_empty(),
        "no completed segments → empty even at u64::MAX"
    );
}

#[tokio::test]
async fn test_drain_completed_before_basic() {
    let test_dir = tempdir().expect("failed to create temp dir");
    let mut wal = WalWriter::open(test_dir.path(), MAX_SEGMENT_BYTES, 0)
        .await
        .expect("failed to open WAL");

    // Write until 2 rotations have occurred (3 segment files on disk)
    let mut last_seq = 0u64;
    loop {
        last_seq = wal
            .append(&sample_points(5))
            .await
            .expect("failed to append");
        if count_wal_files(test_dir.path()) >= 3 {
            break;
        }
    }

    // All completed segments should be returned
    let paths = wal.drain_completed_before(u64::MAX);
    assert_eq!(
        paths.len(),
        2,
        "expected 2 completed segments after 2 rotations"
    );
    for path in &paths {
        assert!(
            path.exists(),
            "returned path must exist on disk: {:?}",
            path
        );
    }

    // Second call is idempotent — list was drained
    assert!(
        wal.drain_completed_before(u64::MAX).is_empty(),
        "drain must be idempotent"
    );
}

#[tokio::test]
async fn test_drain_completed_before_boundary() {
    // Tests the <= boundary: a segment with max_seq N is returned when
    // flushed_seq == N but not when flushed_seq == N - 1.
    let test_dir = tempdir().expect("failed to create temp dir");
    let mut wal = WalWriter::open(test_dir.path(), MAX_SEGMENT_BYTES, 0)
        .await
        .expect("failed to open WAL");

    // Write batches until exactly one rotation — detect by file count change.
    // The append that triggers rotation returns the max_seq of the closed segment.
    let mut rotation_seq: Option<u64> = None;
    loop {
        let before = count_wal_files(test_dir.path());
        let seq = wal
            .append(&sample_points(5))
            .await
            .expect("failed to append");
        let after = count_wal_files(test_dir.path());
        if after > before {
            rotation_seq = Some(seq); // this seq is the max_seq of the completed segment
            break;
        }
    }
    let max_seq = rotation_seq.expect("rotation must have occurred");

    // One below the boundary — segment must NOT be returned
    let below = wal.drain_completed_before(max_seq - 1);
    assert!(
        below.is_empty(),
        "segment with max_seq={max_seq} must not be returned at flushed_seq={}",
        max_seq - 1
    );

    // Exactly at the boundary — segment MUST be returned
    let at = wal.drain_completed_before(max_seq);
    assert_eq!(
        at.len(),
        1,
        "segment with max_seq={max_seq} must be returned at flushed_seq={max_seq}"
    );
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
