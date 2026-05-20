use crate::types::*;
use anyhow::Result;

/// Magic number identifying a Micius chunk file ("MICI" in ASCII, 0x4D494349).
/// Must match at the start of every chunk file — mismatches indicate corruption or wrong format.
pub const MAGIC: u32 = 0x4D494349;

/// Current chunk file format version.
/// Incremented when the on-disk layout changes; recovery stops at version mismatch.
pub const VERSION: u8 = 1;

// Header layout (48 bytes total):
//   magic        : u32 = 4
//   version      : u8  = 1
//   _padding      : [u8; 3] = 3
//   chunk_id     : u64 = 8
//   time_start_ns: i64 = 8
//   time_end_ns  : i64 = 8
//   series_count : u32 = 4
//   total_entries: u32 = 4
//   col_data_offset: u64 = 8
/// Fixed size of the chunk file header in bytes.
pub const HEADER_SIZE: usize = 48;

/// Fixed-size file header written at byte 0 of every chunk file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkHeader {
    /// Magic bytes identifying the file as a Micius chunk.
    pub magic: u32,
    /// Format version — checked on open; mismatches abort reads.
    pub version: u8,
    /// Alignment padding to 8-byte boundary.
    pub _padding: [u8; 3],
    /// Unique chunk identifier (timestamp-derived).
    pub chunk_id: ChunkId,
    /// Earliest timestamp across all series in this chunk.
    pub time_start_ns: i64,
    /// Latest timestamp across all series in this chunk.
    pub time_end_ns: i64,
    /// Number of distinct series stored in this chunk.
    pub series_count: u32,
    /// Total number of data points across all series.
    pub total_entries: u32,
    /// Absolute byte offset to the start of the column data section.
    pub col_data_offset: u64,
}

impl ChunkHeader {
    /// Serializes the header to its 48-byte little-endian on-disk representation.
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes: Vec<u8> = Vec::with_capacity(HEADER_SIZE);
        bytes.extend_from_slice(&self.magic.to_le_bytes());
        bytes.extend_from_slice(&self.version.to_le_bytes());
        bytes.extend_from_slice(&self._padding);
        bytes.extend_from_slice(&self.chunk_id.to_le_bytes());
        bytes.extend_from_slice(&self.time_start_ns.to_le_bytes());
        bytes.extend_from_slice(&self.time_end_ns.to_le_bytes());
        bytes.extend_from_slice(&self.series_count.to_le_bytes());
        bytes.extend_from_slice(&self.total_entries.to_le_bytes());
        bytes.extend_from_slice(&self.col_data_offset.to_le_bytes());

        bytes
    }
}

/// Fixed byte length of a series directory entry (excluding the variable-length key bytes).
pub const DIR_ENTRY_FIXED_SIZE: usize =
    U32_SIZE + U32_SIZE + U64_SIZE + U64_SIZE + F64_SIZE + F64_SIZE; // 40

/// Byte size of a `u32` field on disk.
pub const U32_SIZE: usize = 4;
/// Byte size of a `u64` field on disk.
pub const U64_SIZE: usize = 8;
/// Byte size of an `i64` field on disk.
pub const I64_SIZE: usize = 8;
/// Byte size of an `f64` field on disk.
pub const F64_SIZE: usize = 8;

/// Byte size of the compressed-column length prefix written before each lz4 block.
pub const COL_LEN_SIZE: usize = U32_SIZE;

// Series directory entry fixed fields (excluding variable-length key bytes):
//   key_len       : u32 = U32_SIZE
//   entry_count   : u32 = U32_SIZE
//   ts_col_offset : u64 = U64_SIZE
//   val_col_offset: u64 = U64_SIZE
//   min_value     : f64 = F64_SIZE
//   max_value     : f64 = F64_SIZE
// Note: key_bytes ([u8; key_len]) are variable and added separately at call sites.

/// Byte size of the `footer_offset` field at the end of every chunk file.
pub const FOOTER_OFFSET_SIZE: usize = U64_SIZE;
/// Byte size of the CRC32 file checksum field written as the very last bytes.
pub const CHECKSUM_SIZE: usize = U32_SIZE;
/// Combined size of the last two fields — reader seeks to `file_size - FOOTER_TAIL_SIZE` to locate the bloom offset.
pub const FOOTER_TAIL_SIZE: usize = FOOTER_OFFSET_SIZE + CHECKSUM_SIZE; // 12

/// Byte size of the two SipHash key pairs stored in the bloom footer.
pub const BLOOM_SIP_KEYS_SIZE: usize = 4 * U64_SIZE; // 32
/// Fixed-size prefix of the bloom footer section (before the bitmap bytes).
pub const BLOOM_HEADER_SIZE: usize = U64_SIZE + U32_SIZE + BLOOM_SIP_KEYS_SIZE + U32_SIZE; // 48

/// Generate a chunk ID from the current timestamp.
/// IDs are monotonically increasing and sort chronologically.
pub fn new_chunk_id() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_nanos() as u64
}

/// Encode a slice of i64 timestamps using delta encoding.
///
/// Delta encoding exploits the fact that consecutive timestamps in a
/// time-series are close together. Instead of storing full nanosecond
/// timestamps (8 bytes each), we store:
///   [first_timestamp, delta1, delta2, ...]
///
/// For 1-second interval data, deltas are ~1_000_000_000 (1 billion).
/// For 1-millisecond data, deltas are ~1_000_000 (1 million).
/// These small values compress dramatically better under lz4 than
/// raw nanosecond timestamps which all start with ~1700000000000000000.
#[allow(clippy::indexing_slicing)] // guarded by is_empty() + loop bounds 1..len
pub fn delta_encode(input: &[i64]) -> Vec<i64> {
    if input.is_empty() {
        return vec![];
    }
    let mut deltas: Vec<i64> = Vec::with_capacity(input.len());
    deltas.push(input[0]);

    for i in 1..input.len() {
        deltas.push(input[i] - input[i - 1])
    }
    deltas
}

/// Reconstruct absolute timestamps from a delta-encoded slice.
#[allow(clippy::indexing_slicing)] // guarded by is_empty() + loop bounds 1..len; out[i-1] valid as out grows with i
pub fn delta_decode(deltas: &[i64]) -> Vec<i64> {
    if deltas.is_empty() {
        return vec![];
    }
    let mut out: Vec<i64> = Vec::with_capacity(deltas.len());
    out.push(deltas[0]);

    for i in 1..deltas.len() {
        out.push(out[i - 1] + deltas[i])
    }
    out
}

/// Serialize a slice of i64 to little-endian bytes
pub fn i64_slice_to_bytes(values: &[i64]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// Serialize a slice of f64 to little-endian bytes
pub fn f64_slice_to_bytes(values: &[f64]) -> Vec<u8> {
    values.iter().flat_map(|v| v.to_le_bytes()).collect()
}

/// Deserialize little-endian bytes into a `Vec<i64>`.
pub fn bytes_to_i64_slice(bytes: &[u8]) -> Result<Vec<i64>> {
    bytes
        .chunks_exact(I64_SIZE)
        .map(|b| Ok(i64::from_le_bytes(b.try_into()?)))
        .collect()
}

/// Deserialize little-endian bytes into a `Vec<f64>`.
pub fn bytes_to_f64_slice(bytes: &[u8]) -> Result<Vec<f64>> {
    bytes
        .chunks_exact(F64_SIZE)
        .map(|b| Ok(f64::from_le_bytes(b.try_into()?)))
        .collect()
}
