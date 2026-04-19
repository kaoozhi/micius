use bloomfilter::Bloom;
use crc32fast::Hasher as CrcHasher;
use std::collections::BTreeMap;
use storage_engine::chunk::format::{
    DIR_ENTRY_FIXED_SIZE, HEADER_SIZE, MAGIC, VERSION, delta_decode, delta_encode,
};
use storage_engine::chunk::writer::{ChunkWriteResult, ChunkWriter};
use storage_engine::types::SeriesKey;
use tempfile::{TempDir, tempdir};

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

/// Build a SeriesKey with a single "host" tag.
fn series_key(metric: &str, host: &str) -> SeriesKey {
    SeriesKey {
        metric_name: metric.to_string(),
        tags: BTreeMap::from([("host".to_string(), host.to_string())]),
    }
}

/// Produce n points starting at ts_start, incrementing by step_ns.
/// Values are just i as f64.
fn make_points(ts_start: i64, step_ns: i64, n: usize) -> Vec<(i64, f64)> {
    (0..n)
        .map(|i| (ts_start + i as i64 * step_ns, i as f64))
        .collect()
}

/// Build a single-series BTreeMap with one series and n points.
fn single_series_data(metric: &str, host: &str, n: usize) -> BTreeMap<SeriesKey, Vec<(i64, f64)>> {
    let mut data = BTreeMap::new();
    data.insert(
        series_key(metric, host),
        make_points(1_000_000_000, 1_000_000, n),
    );
    data
}

/// Build a multi-series BTreeMap with m series, each having n points.
fn multi_series_data(m: usize, n: usize) -> BTreeMap<SeriesKey, Vec<(i64, f64)>> {
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
async fn write_and_read_bytes(
    data: BTreeMap<SeriesKey, Vec<(i64, f64)>>,
) -> (TempDir, ChunkWriteResult, Vec<u8>) {
    let dir = tempdir().expect("failed to create temp dir");
    let writer = ChunkWriter::new(dir.path().to_path_buf());
    let result = writer.write(data).await.expect("chunk write failed");
    let bytes = std::fs::read(&result.file_path).expect("failed to read chunk file");
    (dir, result, bytes)
}

/// Parse the 40-byte chunk header from the start of the file.
/// Returns (magic, version, chunk_id, time_start_ns, time_end_ns, series_count, total_entries).
fn parse_header(bytes: &[u8]) -> (u32, u8, u64, i64, i64, u32, u32, u64) {
    assert!(bytes.len() >= 40, "file too small to contain header");
    let magic = u32::from_le_bytes(bytes[0..4].try_into().unwrap());
    let version = bytes[4];
    // bytes[5..8] is padding
    let chunk_id = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    let time_start_ns = i64::from_le_bytes(bytes[16..24].try_into().unwrap());
    let time_end_ns = i64::from_le_bytes(bytes[24..32].try_into().unwrap());
    let series_count = u32::from_le_bytes(bytes[32..36].try_into().unwrap());
    let total_entries = u32::from_le_bytes(bytes[36..40].try_into().unwrap());
    let col_data_offset = u64::from_le_bytes(bytes[40..48].try_into().unwrap());
    (
        magic,
        version,
        chunk_id,
        time_start_ns,
        time_end_ns,
        series_count,
        total_entries,
        col_data_offset,
    )
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_write_produces_file() {
    let serie = single_series_data("example", "host", 100);
    let dir = tempdir().expect("failed to create temp dir");
    let writer = ChunkWriter::new(dir.path().to_path_buf());
    let result = writer.write(serie).await.expect("chunk write failed");
    assert!(result.file_path.exists(), "chunk file does not exist");
    assert!(result.file_size > HEADER_SIZE as u64);
}

#[tokio::test]
async fn test_header_layout() {
    let series = multi_series_data(3, 5);
    let (_, result, bytes) = write_and_read_bytes(series).await;

    let (
        magic,
        version,
        chunk_id,
        time_start_ns,
        time_end_ns,
        series_count,
        total_entries,
        _col_data_offset,
    ) = parse_header(&bytes);

    assert_eq!(magic, MAGIC);
    assert_eq!(version, VERSION);
    assert_eq!(series_count, 3);
    assert_eq!(total_entries, 3 * 5);
    assert!(time_start_ns <= time_end_ns);
    assert_eq!(chunk_id, result.chunk_id);
}

#[tokio::test]
async fn test_series_write_result_per_series_metadata() {
    let key_a = series_key("cpu.usage", "node-0");
    let key_b = series_key("mem.free", "node-1");

    // Series A: 3 points starting at 1000, values 0.0, 1.0, 2.0
    // Series B: 2 points starting at 5000, values 0.0, 1.0
    let mut data = BTreeMap::new();
    data.insert(key_a.clone(), make_points(1000, 100, 3));
    data.insert(key_b.clone(), make_points(5000, 100, 2));

    let (_dir, result, _bytes) = write_and_read_bytes(data).await;

    assert_eq!(result.series_results.len(), 2);

    for sr in &result.series_results {
        assert_eq!(sr.meta.chunk_id, result.chunk_id);
        assert!(sr.meta.time_start_ns <= sr.meta.time_end_ns);

        if sr.series_key == key_a {
            assert_eq!(sr.meta.time_start_ns, 1000);
            assert_eq!(sr.meta.time_end_ns, 1200); // 1000 + 2 * 100
            assert_eq!(sr.stats.min_value, 0.0);
            assert_eq!(sr.stats.max_value, 2.0);
        } else if sr.series_key == key_b {
            assert_eq!(sr.meta.time_start_ns, 5000);
            assert_eq!(sr.meta.time_end_ns, 5100); // 5000 + 1 * 100
            assert_eq!(sr.stats.min_value, 0.0);
            assert_eq!(sr.stats.max_value, 1.0);
        } else {
            panic!("unexpected series key: {:?}", sr.series_key);
        }
    }
}

#[tokio::test]
async fn test_multi_series_global_time_bounds() {
    let key_a = series_key("cpu.usage", "node-0");
    let key_b = series_key("mem.free", "node-1");

    let mut data = BTreeMap::new();
    data.insert(key_a.clone(), vec![(100, 1.0), (200, 2.0), (300, 3.0)]);
    data.insert(key_b.clone(), vec![(50, 4.0), (400, 5.0)]);

    let (_dir, result, bytes) = write_and_read_bytes(data).await;

    // Header reflects global bounds across all series
    let (_, _, _, time_start_ns, time_end_ns, _, _, _) = parse_header(&bytes);
    assert_eq!(time_start_ns, 50);
    assert_eq!(time_end_ns, 400);

    // Each series still has its own per-series bounds
    let sr_a = result
        .series_results
        .iter()
        .find(|r| r.series_key == key_a)
        .unwrap();
    let sr_b = result
        .series_results
        .iter()
        .find(|r| r.series_key == key_b)
        .unwrap();

    assert_eq!(sr_a.meta.time_start_ns, 100);
    assert_eq!(sr_a.meta.time_end_ns, 300);
    assert_eq!(sr_b.meta.time_start_ns, 50);
    assert_eq!(sr_b.meta.time_end_ns, 400);
}

#[tokio::test]
async fn test_series_count_matches_input() {
    let mut data = BTreeMap::new();
    data.insert(series_key("cpu.usage", "node-0"), make_points(1000, 100, 3));
    data.insert(series_key("mem.free", "node-1"), make_points(2000, 100, 7));
    data.insert(series_key("disk.io", "node-2"), make_points(3000, 100, 1));

    let (_, result, bytes) = write_and_read_bytes(data).await;
    let (_, _, _, _, _, series_count, total_entries, _) = parse_header(&bytes);

    assert_eq!(series_count, 3);
    assert_eq!(total_entries, 3 + 7 + 1);
    assert_eq!(result.series_results.len(), 3);
}

#[tokio::test]
async fn test_chunk_id_unique_across_writes() {
    let serie_1 = single_series_data("cpu.usage", "host", 100);
    let serie_2 = single_series_data("mem.free", "host", 100);
    let dir = tempdir().expect("failed to create temp dir");
    let writer = ChunkWriter::new(dir.path().to_path_buf());
    let result_1 = writer.write(serie_1).await.expect("chunk write failed");
    let result_2 = writer.write(serie_2).await.expect("chunk write failed");

    assert_ne!(result_1.chunk_id, result_2.chunk_id);
    assert!(result_1.file_path.exists());
    assert!(result_2.file_path.exists());
    assert_ne!(result_1.file_path, result_2.file_path);
}

#[tokio::test]
async fn test_file_ends_with_crc_checksum() {
    let key_a = series_key("cpu.usage", "node-0");
    let key_b = series_key("mem.free", "node-1");

    // Series A: 3 points starting at 1000, values 0.0, 1.0, 2.0
    // Series B: 2 points starting at 5000, values 0.0, 1.0

    let mut data = BTreeMap::new();
    data.insert(key_a.clone(), make_points(1000, 100, 3));
    data.insert(key_b.clone(), make_points(5000, 100, 2));

    let (_dir, result, bytes) = write_and_read_bytes(data).await;

    // Read the last 4 bytes of the file to get the stored checksum
    let stored_checksum = u32::from_le_bytes(
        (&bytes[result.file_size as usize - 4..])
            .try_into()
            .expect("Slice must be a 4 bytes long"),
    );

    // Recompute the checksum over everything before the last 4 bytes
    let mut crc = CrcHasher::new();
    crc.update(&bytes[..result.file_size as usize - 4]);
    let computed_checksum = crc.finalize();

    assert_eq!(
        stored_checksum, computed_checksum,
        "computed CRC32 must be the same as the stored one"
    );
}

#[tokio::test]
async fn test_bloom_filter_in_footer() {
    // Write 5 series
    // Locate the bloom filter bytes in the footer (before the trailing u32 checksum)
    // Reconstruct the bloom filter from those bytes
    // For each written series_key, bloom.check(&key.to_bytes()) returns true
    let series = multi_series_data(5, 10);
    let (_, result, bytes) = write_and_read_bytes(series).await;
    let (
        _magic,
        _version,
        _chunk_id,
        _time_start_ns,
        _time_end_ns,
        series_count,
        _total_entries,
        _col_data_offset,
    ) = parse_header(&bytes);

    let mut cursor: usize = HEADER_SIZE;
    let mut key_bytes: Vec<Vec<u8>> = Vec::new();
    for _ in 0..series_count {
        let key_len = (u32::from_le_bytes(
            (&bytes[cursor..cursor + 4])
                .try_into()
                .expect("failed to extract the key len"),
        )) as usize;
        let key_byte = Vec::from(&bytes[cursor + 4..cursor + 4 + key_len]);
        key_bytes.push(key_byte);
        cursor += key_len + DIR_ENTRY_FIXED_SIZE; // offset to next entry
    } // cursor is now at the start of column data

    // for _ in 0..series_count {
    //     let ts_len = u32::from_le_bytes(
    //         (&bytes[cursor..cursor + 4])
    //             .try_into()
    //             .expect("failed to extract ts len"),
    //     ) as usize;
    //     cursor += 4 + ts_len;
    //     let val_len = u32::from_le_bytes(
    //         (&bytes[cursor..cursor + 4])
    //             .try_into()
    //             .expect("failed to extract val len"),
    //     ) as usize;
    //     cursor += 4 + val_len;
    // } // cursor is now at the footer
    let footer_offset = result.file_size as usize - 12;
    let mut cursor =
        u64::from_le_bytes(bytes[footer_offset..footer_offset + 8].try_into().unwrap()) as usize;
    let bitmap_bits = u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap());
    cursor += 8;
    let k_num = u32::from_le_bytes(bytes[cursor..cursor + 4].try_into().unwrap());
    cursor += 4;
    let sip_keys = [
        (
            u64::from_le_bytes(bytes[cursor..cursor + 8].try_into().unwrap()),
            u64::from_le_bytes(bytes[cursor + 8..cursor + 16].try_into().unwrap()),
        ),
        (
            u64::from_le_bytes(bytes[cursor + 16..cursor + 24].try_into().unwrap()),
            u64::from_le_bytes(bytes[cursor + 24..cursor + 32].try_into().unwrap()),
        ),
    ];
    cursor += 32;

    let bloom_len = u32::from_le_bytes((&bytes[cursor..cursor + 4]).try_into().unwrap()) as usize;
    cursor += 4;
    let bloom: Bloom<Vec<u8>> = Bloom::from_existing(
        &bytes[cursor..cursor + bloom_len],
        bitmap_bits,
        k_num,
        sip_keys,
    );

    for key_byte in key_bytes {
        assert!(bloom.check(&key_byte));
    }
}

#[tokio::test]
async fn test_delta_encode_decode_roundtrip() {
    let slice: &[i64] = &[1000, 1001, 1003, 2000, 5000];

    let deltas = delta_encode(slice);
    let decoded = delta_decode(&deltas);
    assert_eq!(decoded, slice);
}

#[tokio::test]
async fn test_delta_encode_empty_input() {
    assert!(delta_encode(&[]).is_empty());
    assert!(delta_decode(&[]).is_empty());
}

#[tokio::test]
async fn test_lz4_compress_decompress_roundtrip() {
    use lz4_flex::block::{compress_prepend_size, decompress_size_prepended};

    let original: Vec<u8> = (0u8..=255).cycle().take(1024).collect();
    let compressed = compress_prepend_size(&original);
    let decompressed = decompress_size_prepended(&compressed).expect("decompression failed");
    assert_eq!(decompressed, original);
}
