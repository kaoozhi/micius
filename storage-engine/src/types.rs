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
    pub size_bytes: u64,
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
