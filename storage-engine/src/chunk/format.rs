pub const MAGIC: u32 = 0x4D494349; // "MICI" in ASCII
pub const VERSION: u8 = 1;

// Header layout (40 bytes total):
//   magic        : u32 = 4
//   version      : u8  = 1
//   _padding      : [u8; 3] = 3
//   chunk_id     : u64 = 8
//   time_start_ns: i64 = 8
//   time_end_ns  : i64 = 8
//   series_count : u32 = 4
//   total_entries: u32 = 4
pub const HEADER_SIZE: usize = 40;

// Series directory entry fixed fields (excluding variable-length key bytes): 40 bytes
//   key_len      : u32  = 4
//   entry_count  : u32  = 4
//   ts_col_offset: u64  = 8
//   val_col_offset: u64 = 8
//   min_value    : f64  = 8
//   max_value    : f64  = 8
pub const DIR_ENTRY_FIXED_SIZE: usize = 4 + 4 + 8 + 8 + 8 + 8;

/// Generate a chunk ID from the current timestamp.
/// IDs are monotonically increasing and sort chronologically.
pub fn new_chunk_id() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
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

pub fn bytes_to_i64_slice(bytes: &[u8]) -> Vec<i64> {
    bytes
        .chunks_exact(8)
        .map(|b| i64::from_le_bytes(b.try_into().unwrap()))
        .collect()
}

pub fn bytes_to_f64_slice(bytes: &[u8]) -> Vec<f64> {
    bytes
        .chunks_exact(8)
        .map(|b| f64::from_le_bytes(b.try_into().unwrap()))
        .collect()
}
