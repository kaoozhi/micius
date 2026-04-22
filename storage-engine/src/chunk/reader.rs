pub struct ChunkReader;
use crate::chunk::format::*;
use crate::types::*;
use anyhow::Result;
use bloomfilter::Bloom;
use crc32fast::Hasher as CrcHasher;
use lz4_flex::block::decompress_size_prepended;
use std::path::Path;

impl ChunkReader {
    pub async fn check_bloom(path: &Path, series_key: &SeriesKey) -> Result<bool> {
        use tokio::io::{AsyncReadExt, AsyncSeekExt};
        let mut file = tokio::fs::File::open(path).await?;
        let file_size = file.metadata().await?.len() as usize;
        // Read the last FOOTER_TAIL_SIZE(12) bytes: [footer_offset: u64][checksum: u32]
        file.seek(tokio::io::SeekFrom::Start(
            (file_size - FOOTER_TAIL_SIZE) as u64,
        ))
        .await?;
        let mut tail = [0u8; FOOTER_TAIL_SIZE];
        file.read_exact(&mut tail).await?;
        let footer_offset = u64::from_le_bytes(tail[..FOOTER_OFFSET_SIZE].try_into()?) as usize;

        // Read bloom section from footer_offset to end of bloom data
        anyhow::ensure!(
            CHECKSUM_SIZE + footer_offset <= file_size,
            "bloom footer offset points outside file bounds"
        );
        let bloom_section_len = file_size - CHECKSUM_SIZE - footer_offset;
        file.seek(tokio::io::SeekFrom::Start(footer_offset as u64))
            .await?;
        let mut bloom_buf = vec![0u8; bloom_section_len];
        file.read_exact(&mut bloom_buf).await?;

        let bloom = parse_bloom_from_buf(&bloom_buf)?;
        Ok(bloom.check(&series_key.to_bytes()))
    }
    /// Reads all data points for `series_key` within `[time_start_ns, time_end_ns]`
    /// from a single chunk file. Returns `Ok(None)` if the series is absent or has
    /// no points in the requested range — never an error in those cases.
    ///
    /// The query path calls `check_bloom` first for cheap series-existence rejection
    /// without loading the full file. `read_series` is only called for bloom-positive
    /// chunks and performs the remaining pruning stages:
    ///   1. Magic + CRC32 — reject corrupt files before touching any data
    ///   2. Header range  — skip if chunk time extent doesn't overlap the query
    ///   3. Directory scan — locate column offsets for the specific series
    ///   4. Decompress + filter — return matching points
    pub async fn read_series(
        path: &Path,
        series_key: &SeriesKey,
        time_start_ns: i64,
        time_end_ns: i64,
    ) -> Result<Option<Vec<DataPoint>>> {
        let bytes = tokio::fs::read(path).await?;

        is_magic_valid(&bytes)?;
        is_checksum_valid(&bytes)?;

        let header = parse_header(&bytes)?;
        if !is_within_global_range(time_start_ns, time_end_ns, &header) {
            return Ok(None);
        }

        let Some((ts_offset, val_offset)) =
            find_series(&bytes, header.series_count as usize, series_key)?
        else {
            return Ok(None);
        };

        retrieve_data(
            &bytes,
            series_key,
            ts_offset,
            val_offset,
            time_start_ns,
            time_end_ns,
        )
    }
}

fn is_checksum_valid(bytes: &[u8]) -> Result<()> {
    let mut crc = CrcHasher::new();
    crc.update(&bytes[..bytes.len() - CHECKSUM_SIZE]);
    let computed_checksum = crc.finalize();
    let stored_checksum = u32::from_le_bytes(bytes[bytes.len() - CHECKSUM_SIZE..].try_into()?);
    anyhow::ensure!(
        computed_checksum == stored_checksum,
        "CRC32 mismatch: computed 0x{:08X}, stored 0x{:08X}",
        computed_checksum,
        stored_checksum
    );
    Ok(())
}

fn is_magic_valid(bytes: &[u8]) -> Result<()> {
    let magic = u32::from_le_bytes(bytes[0..4].try_into()?);
    anyhow::ensure!(
        magic == MAGIC,
        "Invalid chunk magic: expected 0x{:08X}, got 0x{:08X}",
        MAGIC,
        magic
    );
    Ok(())
}

fn is_within_global_range(time_start_ns: i64, time_end_ns: i64, header: &ChunkHeader) -> bool {
    !(time_start_ns > header.time_end_ns || time_end_ns < header.time_start_ns)
}

fn parse_header(bytes: &[u8]) -> Result<ChunkHeader> {
    anyhow::ensure!(
        bytes.len() >= HEADER_SIZE,
        "file too small to contain header"
    );
    Ok(ChunkHeader {
        magic: u32::from_le_bytes(bytes[0..4].try_into()?),
        version: bytes[4],
        _padding: bytes[5..8].try_into()?,
        chunk_id: u64::from_le_bytes(bytes[8..16].try_into()?),
        time_start_ns: i64::from_le_bytes(bytes[16..24].try_into()?),
        time_end_ns: i64::from_le_bytes(bytes[24..32].try_into()?),
        series_count: u32::from_le_bytes(bytes[32..36].try_into()?),
        total_entries: u32::from_le_bytes(bytes[36..40].try_into()?),
        col_data_offset: u64::from_le_bytes(bytes[40..48].try_into()?),
    })
}

fn parse_bloom_from_buf(bloom_buf: &[u8]) -> Result<Bloom<Vec<u8>>> {
    let mut cursor = 0;
    anyhow::ensure!(
        cursor + BLOOM_HEADER_SIZE <= bloom_buf.len(),
        "bloom footer offset 0x{:x} points outside file bounds (file size: {})",
        cursor,
        bloom_buf.len()
    );
    let bitmap_bits = u64::from_le_bytes(bloom_buf[cursor..cursor + U64_SIZE].try_into()?);
    cursor += U64_SIZE;
    let k_num = u32::from_le_bytes(bloom_buf[cursor..cursor + U32_SIZE].try_into()?);
    cursor += U32_SIZE;
    let sip_keys = [
        (
            u64::from_le_bytes(bloom_buf[cursor..cursor + U64_SIZE].try_into()?),
            u64::from_le_bytes(bloom_buf[cursor + U64_SIZE..cursor + 2 * U64_SIZE].try_into()?),
        ),
        (
            u64::from_le_bytes(bloom_buf[cursor + 2 * U64_SIZE..cursor + 3 * U64_SIZE].try_into()?),
            u64::from_le_bytes(bloom_buf[cursor + 3 * U64_SIZE..cursor + 4 * U64_SIZE].try_into()?),
        ),
    ];
    cursor += BLOOM_SIP_KEYS_SIZE;
    let bloom_len = u32::from_le_bytes(bloom_buf[cursor..cursor + U32_SIZE].try_into()?) as usize;
    cursor += U32_SIZE;
    anyhow::ensure!(
        cursor + bloom_len <= bloom_buf.len(),
        "bloom bitmap extends beyond file bounds"
    );
    Ok(Bloom::from_existing(
        &bloom_buf[cursor..cursor + bloom_len],
        bitmap_bits,
        k_num,
        sip_keys,
    ))
}

/// Linear scan of the series directory to locate column offsets for `series_key`.
/// Returns `(ts_col_offset, val_col_offset)` — absolute byte positions in the file.
/// The length-prefix check short-circuits key comparison for entries with a different
/// key length, avoiding a memcmp on every non-matching entry.
fn find_series(
    bytes: &[u8],
    series_count: usize,
    series_key: &SeriesKey,
) -> Result<Option<(usize, usize)>> {
    let key_bytes = series_key.to_bytes();
    let mut cursor = HEADER_SIZE;
    for _ in 0..series_count {
        let key_len = u32::from_le_bytes(bytes[cursor..cursor + U32_SIZE].try_into()?) as usize;
        if key_bytes.len() == key_len
            && key_bytes == bytes[cursor + U32_SIZE..cursor + U32_SIZE + key_len]
        {
            // Entry layout: [key_len: u32][key_bytes][entry_count: u32][ts_offset: u64][val_offset: u64]...
            let ts_cursor = cursor + U32_SIZE + key_len + U32_SIZE;
            let ts_col_offset =
                u64::from_le_bytes(bytes[ts_cursor..ts_cursor + U64_SIZE].try_into()?);
            let val_cursor = ts_cursor + U64_SIZE;
            let val_col_offset =
                u64::from_le_bytes(bytes[val_cursor..val_cursor + U64_SIZE].try_into()?);
            return Ok(Some((ts_col_offset as usize, val_col_offset as usize)));
        }
        // DIR_ENTRY_FIXED_SIZE includes the key_len u32 field itself plus all other fixed fields.
        cursor += key_len + DIR_ENTRY_FIXED_SIZE;
    }
    Ok(None)
}

/// Decompresses the timestamp and value columns for a series and returns all
/// points within `[time_start_ns, time_end_ns]`. Timestamps are delta-decoded
/// after decompression — the column stores deltas, not absolute values.
/// Returns `Ok(Some([]))` when the series has no points in the requested range.
fn retrieve_data(
    bytes: &[u8],
    series_key: &SeriesKey,
    ts_offset: usize,
    val_offset: usize,
    time_start_ns: i64,
    time_end_ns: i64,
) -> Result<Option<Vec<DataPoint>>> {
    let ts_len =
        u32::from_le_bytes(bytes[ts_offset..ts_offset + COL_LEN_SIZE].try_into()?) as usize;
    let ts_decompressed = decompress_size_prepended(
        &bytes[ts_offset + COL_LEN_SIZE..ts_offset + COL_LEN_SIZE + ts_len],
    )?;
    let val_len =
        u32::from_le_bytes(bytes[val_offset..val_offset + COL_LEN_SIZE].try_into()?) as usize;
    let val_decompressed = decompress_size_prepended(
        &bytes[val_offset + COL_LEN_SIZE..val_offset + COL_LEN_SIZE + val_len],
    )?;
    // Both columns must have the same number of entries — enforced by the writer.
    anyhow::ensure!(
        val_decompressed.len() == ts_decompressed.len(),
        "length mismatch between timestamp and value columns"
    );

    let ts_deltas = bytes_to_i64_slice(&ts_decompressed);
    let ts_decoded = delta_decode(&ts_deltas);
    let val_decoded = bytes_to_f64_slice(&val_decompressed);

    let points: Vec<DataPoint> = ts_decoded
        .into_iter()
        .zip(val_decoded.into_iter())
        .filter(|(ts, _)| *ts >= time_start_ns && *ts <= time_end_ns)
        .map(|(ts, value)| DataPoint {
            metric_name: series_key.metric_name.clone(),
            tags: series_key.tags.clone(),
            timestamp_ns: ts,
            value,
        })
        .collect();
    Ok(Some(points))
}
