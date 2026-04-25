use std::collections::BTreeMap;
use std::path::PathBuf;

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

/// Records a single series' presence within a specific chunk file.
/// Stored in `ChunkIndex.time_index` keyed by (SeriesId, time_start_ns).
/// Series-level: each series in a multi-series chunk has its own entry
/// with its own time range and column footprint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SeriesChunkEntry {
    pub chunk_id: ChunkId,
    pub series_id: SeriesId,
    pub time_start_ns: i64,  // this series' earliest timestamp in this chunk
    pub time_end_ns: i64,    // this series' latest timestamp in this chunk
    pub size_bytes: usize,   // byte footprint of this series' columns in the file
}

/// Value statistics for one series within one chunk — used for predicate pushdown.
/// Stored in `ChunkIndex.chunk_stats` keyed by (ChunkId, SeriesId).
/// Series-level: min/max reflect only this series' values, not the whole file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SeriesChunkStats {
    pub min_value: f64,
    pub max_value: f64,
    pub null_count: u64, // reserved for future nullable value support
}

impl SeriesChunkStats {
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

/// Chunk-level metadata stored in `ChunkIndex.chunk_files` keyed by ChunkId.
/// Describes the chunk file as a whole — shared by all series flushed together.
/// Used by the compaction worker to locate files and group them by size.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ChunkMeta {
    pub file_path: PathBuf,
    pub file_size: u64,
}
