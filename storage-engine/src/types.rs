use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::chunk::format::HEADER_SIZE;

/// A single measurement at a point in time
#[derive(Debug, Clone, PartialEq)]
pub struct DataPoint {
    pub metric_name: String,
    pub tags: BTreeMap<String, String>,
    pub timestamp_ns: i64, // unix nanoseconds
    pub value: f64,
}

/// Uniquely identifies a time series — the combination of metric name
/// and its complete tag set. Two series with the same metric name but
/// different tags are entirely independent series.
///
/// BTreeMap is used for tags rather than HashMap to guarantee a
/// deterministic byte representation for hashing and bloom filters.
/// HashMap iteration order is not stable across runs.
#[derive(
    Debug, Clone, Ord, PartialOrd, PartialEq, Eq, Hash, serde::Serialize, serde::Deserialize,
)]
pub struct SeriesKey {
    pub metric_name: String,
    pub tags: BTreeMap<String, String>,
}

impl SeriesKey {
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut s = self.metric_name.clone();
        for (k, v) in &self.tags {
            s.push(',');
            s.push_str(k);
            s.push('=');
            s.push_str(v);
        }
        s.into_bytes()
    }
}

impl From<&SeriesKey> for SeriesId {
    fn from(value: &SeriesKey) -> Self {
        let bytes = value.to_bytes();
        xxhash_rust::xxh64::xxh64(&bytes, 0)
    }
}

/// Opaque stable identifier assigned to a SeriesKey on first registration.
/// Used internally to avoid storing full SeriesKey strings in every index
/// data structure.
pub type SeriesId = u64;

/// Opaque identifier for a chunk file. Derived from a timestamp at
/// flush time so chunk IDs sort chronologically.
pub type ChunkId = u64;

/// WAL sequence number — monotonically increasing per batch.
pub type Sequence = u64;

/// Metadata about a chunk file stored in the ChunkIndex.
/// Does not contain the chunk data itself — only enough information
/// to locate and evaluate the chunk during query planning.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkMeta {
    pub chunk_id: ChunkId,
    pub series_id: SeriesId,
    pub time_start_ns: i64,
    pub time_end_ns: i64,
    pub file_path: PathBuf,
    pub size_bytes: usize,
}

/// Per-chunk value statistics used for predicate pushdown.
/// Stored alongside ChunkMeta in the ChunkIndex.
/// Computed during the chunk write from the raw value column.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkStats {
    pub min_value: f64,
    pub max_value: f64,
    pub null_count: u64, // reserved for future nullable value support
}

impl ChunkStats {
    pub fn from_values(values: &[f64]) -> Option<Self> {
        let min = values.iter().cloned().reduce(f64::min)?;
        let max = values.iter().cloned().reduce(f64::max)?;
        Some(Self {
            min_value: min,
            max_value: max,
            null_count: 0,
        })
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkHeader {
    pub magic: u32,         // 4 bytes — 0x4D494349 ("MICI")
    pub version: u8,        // 1 byte
    pub _padding: [u8; 3],  // 3 bytes — alignment to 8-byte boundary
    pub chunk_id: ChunkId,  // 8 bytes
    pub time_start_ns: i64, // 8 bytes — earliest timestamp in file
    pub time_end_ns: i64,   // 8 bytes — latest timestamp in file
    pub series_count: u32,  // 4 bytes
    pub total_entries: u32, // 4 bytes
} // total: 40 bytes

impl ChunkHeader {
    // fn new(&mut self, chunk_id)
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

        bytes
    }
}
