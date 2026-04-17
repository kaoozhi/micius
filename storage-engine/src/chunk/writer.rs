// The ChunkWriter takes a `BTreeMap<SeriesKey, Vec<(i64, f64)>>` — the
// drained memtable contents — and writes it to a single immutable `.mcs`
// file on disk. It also computes the ChunkMeta and ChunkStats for each
// series so the ChunkIndex can be updated after the write.

// ### Two-pass approach

// The chunk writer uses two passes over the data because column offsets in
// the series directory must point to the actual byte positions of each column
// in the file, but those positions are not known until after the directory
// itself has been written. The two-pass approach:

// **Pass 1** (in memory): encode and compress all columns. Compute sizes.
// Calculate the byte offset for each column relative to the file start.

// **Pass 2** (write): assemble the complete buffer — header, directory
// with correct offsets, column data, footer.

// ### Full binary layout

// ```
// ┌─────────────────────────────────────────────────────┐
// │ FILE HEADER — 48 bytes                              │
// │                                                     │
// │  magic           : u32   = 0x4D494349  ("MICI")     │
// │  version         : u8    = 1                        │
// │  _padding        : [u8;3]                           │
// │  chunk_id        : u64                              │
// │  time_start_ns   : i64   earliest timestamp         │
// │  time_end_ns     : i64   latest timestamp           │
// │  series_count    : u32   number of series           │
// │  total_entries   : u32   total data points          │
// │  col_data_offset : u64   absolute byte offset to    │
// │                          start of column data.      │
// │                          Skips directory scan on    │
// │                          reads that go straight to  │
// │                          column data.               │
// ├─────────────────────────────────────────────────────┤
// │ SERIES DIRECTORY — series_count entries             │
// │                                                     │
// │  Per entry:                                         │
// │    key_len         : u32  byte length of series key │
// │    series_key      : [u8; key_len]  canonical bytes │
// │    entry_count     : u32  number of points          │
// │    ts_col_offset   : u64  byte offset from file     │
// │                          start to timestamp column  │
// │    val_col_offset  : u64  byte offset to value col  │
// │    min_value       : f64  for predicate pushdown    │
// │    max_value       : f64  for predicate pushdown    │
// ├─────────────────────────────────────────────────────┤
// │ COLUMN DATA — interleaved per series                │
// │                                                     │
// │  For each series (in directory order):              │
// │                                                     │
// │    TIMESTAMP COLUMN:                                │
// │      compressed_len : u32  byte length after lz4    │
// │      data           : [u8; compressed_len]          │
// │        → lz4-block-compressed delta-encoded i64s    │
// │                                                     │
// │    VALUE COLUMN:                                    │
// │      compressed_len : u32                           │
// │      data           : [u8; compressed_len]          │
// │        → lz4-block-compressed little-endian f64s    │
// │                                                     │
// ├─────────────────────────────────────────────────────┤
// │ FOOTER                                              │
// │                                                     │
// │  bloom_bitmap_bits : u64  number of bits in bitmap  │
// │  bloom_k_num       : u32  number of hash functions  │
// │  bloom_sip_keys    : [u64;4]  two SipHash key pairs │
// │  bloom_len         : u32  byte length of bitmap     │
// │  bloom_data        : [u8; bloom_len]                │
// │    → bloom filter over all series_key bytes in      │
// │      this chunk. Used to skip chunks that           │
// │      definitely do not contain a queried series.    │
// │      All fields required for Bloom::from_existing() │
// │                                                     │
// │  footer_offset : u64                                │
// │    → absolute byte offset to the start of this      │
// │      footer (i.e. to bloom_bitmap_bits). Written    │
// │      as the last 12 bytes: [footer_offset u64]      │
// │      [file_checksum u32]. Reader seeks to           │
// │      file_size - 12, reads offset, jumps to bloom.  │
// │                                                     │
// │  file_checksum : u32                                │
// │    → CRC32 of all bytes from start of file header   │
// │      to end of footer_offset (exclusive of this     │
// │      field). Detects chunk file corruption on disk. │
// └─────────────────────────────────────────────────────┘
// ```

use crate::chunk::format::*;
use crate::types::*;
use anyhow::Result;
use bloomfilter::Bloom;
use crc32fast::Hasher as CrcHasher;
use lz4_flex::block::compress_prepend_size;
use std::collections::BTreeMap;
use std::path::PathBuf;

pub struct ChunkWriter {
    chunk_dir: PathBuf,
}

/// Result of writing a chunk — one entry per series
pub struct ChunkWriteResult {
    pub chunk_id: ChunkId,
    pub file_path: PathBuf,
    pub file_size: u64,
    /// Per-series metadata for registering in the ChunkIndex
    pub series_results: Vec<SeriesWriteResult>,
}

pub struct SeriesWriteResult {
    pub series_key: SeriesKey,
    pub meta: ChunkMeta,
    pub stats: ChunkStats,
}

struct EncodedSeries {
    key: SeriesKey,
    key_bytes: Vec<u8>, // cached — used in directory write and bloom filter
    entry_count: u32,
    time_start_ns: i64,
    time_end_ns: i64,
    ts_compressed: Vec<u8>,  // lz4-compressed delta-encoded timestamps
    val_compressed: Vec<u8>, // lz4-compressed f64 values
    min_value: f64,
    max_value: f64,
    stats: ChunkStats,
}

fn build_directory_entry(s: &EncodedSeries, ts_offset: u64, val_offset: u64) -> Vec<u8> {
    // [key_len: u32][key_bytes: [u8; key_len]][entry_count: u32][ts_offset: u64][val_offset: u64][min_val: f64][max_val: f64]
    let mut entry = Vec::with_capacity(DIR_ENTRY_FIXED_SIZE + s.key_bytes.len());
    entry.extend_from_slice(&(s.key_bytes.len() as u32).to_le_bytes());
    entry.extend_from_slice(&s.key_bytes);
    entry.extend_from_slice(&s.entry_count.to_le_bytes());
    entry.extend_from_slice(&ts_offset.to_le_bytes());
    entry.extend_from_slice(&val_offset.to_le_bytes());
    entry.extend_from_slice(&s.min_value.to_le_bytes());
    entry.extend_from_slice(&s.max_value.to_le_bytes());
    entry
}

impl ChunkWriter {
    pub fn new(dir: PathBuf) -> Self {
        ChunkWriter { chunk_dir: dir }
    }

    pub async fn write(
        &self,
        series_data: BTreeMap<SeriesKey, Vec<(i64, f64)>>,
    ) -> Result<ChunkWriteResult> {
        let chunk_id = new_chunk_id();
        let file_path = self.chunk_dir.join(format!("chunk-{:016x}.mcs", chunk_id));
        // ── Pass 1: encode and compress all columns ──────────────────

        let mut encoded: Vec<EncodedSeries> = Vec::new();
        let mut series_results: Vec<SeriesWriteResult> = Vec::new();
        let mut global_min_ts = i64::MAX;
        let mut global_max_ts = i64::MIN;
        let mut total_entries: u32 = 0;
        let mut bloom = Bloom::new_for_fp_rate(series_data.len().max(1), 0.01);

        for (key, points) in &series_data {
            let (timestamps, values): (Vec<i64>, Vec<f64>) = points.iter().copied().unzip();
            let deltas = delta_encode(&timestamps);
            let stats = ChunkStats::from_values(&values)
                .ok_or_else(|| anyhow::anyhow!("no chunk stats found"))?;

            global_min_ts = global_min_ts.min(*timestamps.first().unwrap_or(&0));
            global_max_ts = global_max_ts.max(*timestamps.last().unwrap_or(&0));
            total_entries += timestamps.len() as u32;

            let ts_bytes = i64_slice_to_bytes(&deltas);
            let ts_compressed = compress_prepend_size(&ts_bytes);

            let val_bytes = f64_slice_to_bytes(&values);
            let val_compressed = compress_prepend_size(&val_bytes);

            let key_bytes = key.to_bytes();
            bloom.set(&key_bytes);
            encoded.push(EncodedSeries {
                key: key.clone(),
                key_bytes,
                entry_count: timestamps.len() as u32,
                time_start_ns: *timestamps.first().unwrap(),
                time_end_ns: *timestamps.last().unwrap(),
                ts_compressed,
                val_compressed,
                min_value: stats.min_value,
                max_value: stats.max_value,
                stats,
            });
        }

        // ── Pass 2: compute byte offsets and build series_results ────────
        //
        // Column offsets must be absolute (from byte 0 of the file) so the
        // reader can seek directly without knowing the directory size.
        // Layout: HEADER_SIZE + dir_size + column data (ts+val per series).
        let dir_size: usize = encoded
            .iter()
            .map(|s| DIR_ENTRY_FIXED_SIZE + s.key_bytes.len())
            .sum();
        let mut current_offset = HEADER_SIZE + dir_size;
        let mut offsets: Vec<(u64, u64)> = Vec::new();

        for s in &encoded {
            let ts_offset = current_offset as u64;
            current_offset += 4 + s.ts_compressed.len();
            let val_offset = current_offset as u64;
            current_offset += 4 + s.val_compressed.len();
            offsets.push((ts_offset, val_offset));
            series_results.push(SeriesWriteResult {
                series_key: s.key.clone(),
                meta: ChunkMeta {
                    chunk_id,
                    series_id: (&s.key).into(),
                    time_start_ns: s.time_start_ns,
                    time_end_ns: s.time_end_ns,
                    file_path: file_path.clone(),
                    size_bytes: s.ts_compressed.len() + s.val_compressed.len() + 8,
                },
                stats: s.stats.clone(),
            });
        }

        // ── Assemble the complete file buffer ─────────────────────────
        let mut buf: Vec<u8> = Vec::new();
        let mut crc = CrcHasher::new();

        // Header
        let series_count = encoded.len();
        let header = ChunkHeader {
            magic: MAGIC,
            version: VERSION,
            _padding: [0u8; 3],
            chunk_id,
            time_start_ns: global_min_ts,
            time_end_ns: global_max_ts,
            series_count: series_count as u32,
            total_entries,
            col_data_offset: (HEADER_SIZE + dir_size) as u64,
        };
        let header_bytes = header.to_bytes();

        buf.extend_from_slice(&header_bytes);

        // series directory
        for (i, s) in encoded.iter().enumerate() {
            let (ts_off, val_off) = offsets[i];
            buf.extend_from_slice(&build_directory_entry(s, ts_off, val_off));
        }

        let mut col_size = 0usize;
        // column data — interleaved per series: [ts_len][ts_data][val_len][val_data]
        for s in &encoded {
            buf.extend_from_slice(&(s.ts_compressed.len() as u32).to_le_bytes());
            buf.extend_from_slice(&s.ts_compressed);
            col_size += 4 + s.ts_compressed.len();
            buf.extend_from_slice(&(s.val_compressed.len() as u32).to_le_bytes());
            buf.extend_from_slice(&s.val_compressed);
            col_size += 4 + s.val_compressed.len();
        }

        // Footer — bloom filter
        let bloom_bitmap_bits = bloom.number_of_bits();
        let bloom_k_num = bloom.number_of_hash_functions();
        let bloom_sip_keys = bloom.sip_keys();
        let bloom_bytes = bloom.bitmap();
        buf.extend_from_slice(&bloom_bitmap_bits.to_le_bytes());
        buf.extend_from_slice(&bloom_k_num.to_le_bytes());
        for (k0, k1) in &bloom_sip_keys {
            buf.extend_from_slice(&k0.to_le_bytes());
            buf.extend_from_slice(&k1.to_le_bytes());
        }
        buf.extend_from_slice(&(bloom_bytes.len() as u32).to_le_bytes());
        buf.extend_from_slice(&bloom_bytes);

        // Footer offset
        let footer_offset = (HEADER_SIZE + dir_size + col_size) as u64;
        buf.extend_from_slice(&footer_offset.to_le_bytes());

        // Footer — CRC32 over everything written so far
        crc.update(&buf);
        let checksum = crc.finalize();
        buf.extend_from_slice(&checksum.to_le_bytes());

        // ── Write to disk ─────────────────────────────────────────────
        tokio::fs::write(&file_path, &buf).await?;

        Ok(ChunkWriteResult {
            chunk_id,
            file_path,
            file_size: buf.len() as u64,
            series_results,
        })
    }
}
