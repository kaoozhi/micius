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
        let capacity = self.metric_name.len()
            + self
                .tags
                .iter()
                .map(|(k, v)| k.len() + v.len() + 2)
                .sum::<usize>();
        let mut buf = Vec::with_capacity(capacity);
        buf.extend_from_slice(self.metric_name.as_bytes());
        for (k, v) in &self.tags {
            buf.push(b',');
            buf.extend_from_slice(k.as_bytes());
            buf.push(b'=');
            buf.extend_from_slice(v.as_bytes());
        }
        buf
    }

    pub fn from_bytes(bytes: &[u8]) -> anyhow::Result<Self> {
        let s = std::str::from_utf8(bytes)
            .map_err(|e| anyhow::anyhow!("invalid UTF-8 in series key: {e}"))?;
        let mut parts = s.split(',');
        let metric_name = parts
            .next()
            .ok_or_else(|| anyhow::anyhow!("empty series key bytes"))?
            .to_string();
        let mut tags = BTreeMap::new();
        for kv in parts {
            let (k, v) = kv
                .split_once('=')
                .ok_or_else(|| anyhow::anyhow!("malformed tag pair: {kv:?}"))?;
            tags.insert(k.to_string(), v.to_string());
        }
        Ok(Self { metric_name, tags })
    }
}

impl From<&SeriesKey> for SeriesId {
    fn from(value: &SeriesKey) -> Self {
        xxhash_rust::xxh64::xxh64(&value.to_bytes(), 0)
    }
}

pub fn series_id_from_parts(metric_name: &str, tags: &BTreeMap<String, String>) -> SeriesId {
    let mut h = xxhash_rust::xxh64::Xxh64::new(0);
    h.update(metric_name.as_bytes());
    for (k, v) in tags {
        h.update(b",");
        h.update(k.as_bytes());
        h.update(b"=");
        h.update(v.as_bytes());
    }
    h.digest()
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
    pub time_start_ns: i64, // this series' earliest timestamp in this chunk
    pub time_end_ns: i64,   // this series' latest timestamp in this chunk
    pub size_bytes: usize,  // byte footprint of this series' columns in the file
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

pub enum ValuePredicate {
    GreaterThan(f64),
    LessThan(f64),
    Between(f64, f64),
}

impl ValuePredicate {
    pub fn matches(&self, min_val: f64, max_val: f64) -> bool {
        match self {
            Self::GreaterThan(t) => max_val > *t,
            Self::LessThan(t) => min_val < *t,
            Self::Between(lo, hi) => min_val <= *hi && max_val >= *lo,
        }
    }

    pub fn satisfies(&self, value: f64) -> bool {
        match self {
            Self::GreaterThan(t) => value > *t,
            Self::LessThan(t) => value < *t,
            Self::Between(lo, hi) => value >= *lo && value <= *hi,
        }
    }
}
