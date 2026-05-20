use std::collections::BTreeMap;
use std::path::PathBuf;

/// A single measurement at a point in time
#[derive(Debug, Clone, PartialEq)]
pub struct DataPoint {
    /// Name of the metric (e.g. `"cpu.load"`).
    pub metric_name: String,
    /// Tag key-value pairs identifying the series (BTreeMap for stable ordering).
    pub tags: BTreeMap<String, String>,
    /// Unix timestamp in nanoseconds.
    pub timestamp_ns: i64,
    /// Observed value.
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
    /// Metric name component of the series identity.
    pub metric_name: String,
    /// Complete tag set (BTreeMap ensures stable byte representation for hashing).
    pub tags: BTreeMap<String, String>,
}

impl SeriesKey {
    /// Encodes the series key as canonical bytes: `metric_name,k1=v1,k2=v2` (tags in BTreeMap order).
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

    /// Decodes a series key from the canonical byte format produced by `to_bytes`.
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

/// Computes the `SeriesId` for a (metric_name, tags) pair without allocating a `SeriesKey`.
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
    /// Chunk file containing this series.
    pub chunk_id: ChunkId,
    /// Series this entry belongs to.
    pub series_id: SeriesId,
    /// Earliest timestamp for this series in this chunk.
    pub time_start_ns: i64,
    /// Latest timestamp for this series in this chunk.
    pub time_end_ns: i64,
    /// Byte footprint of this series' compressed columns in the file.
    pub size_bytes: usize,
}

/// Value statistics for one series within one chunk — used for predicate pushdown.
/// Stored in `ChunkIndex.chunk_stats` keyed by (ChunkId, SeriesId).
/// Series-level: min/max reflect only this series' values, not the whole file.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct SeriesChunkStats {
    /// Minimum observed value for this series in this chunk.
    pub min_value: f64,
    /// Maximum observed value for this series in this chunk.
    pub max_value: f64,
    /// Reserved for future nullable value support — always 0 today.
    pub null_count: u64,
}

impl SeriesChunkStats {
    /// Computes min/max stats from a value slice. Returns `None` if `values` is empty.
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
    /// Absolute path to the `.mcs` file.
    pub file_path: PathBuf,
    /// Total file size in bytes — used by the compaction worker for size-tier grouping.
    pub file_size: u64,
}

/// Filter applied during chunk and memtable reads to eliminate non-matching points.
#[derive(Debug)]
pub enum ValuePredicate {
    /// Matches series/points where values exceed the threshold.
    GreaterThan(f64),
    /// Matches series/points where values are below the threshold.
    LessThan(f64),
    /// Matches series/points where values fall within `[lo, hi]`.
    Between(f64, f64),
}

impl ValuePredicate {
    /// Returns `true` if a chunk with `[min_val, max_val]` can contain matching points (for pruning).
    pub fn matches(&self, min_val: f64, max_val: f64) -> bool {
        match self {
            Self::GreaterThan(t) => max_val > *t,
            Self::LessThan(t) => min_val < *t,
            Self::Between(lo, hi) => min_val <= *hi && max_val >= *lo,
        }
    }

    /// Returns `true` if a single `value` satisfies this predicate (for point filtering).
    pub fn satisfies(&self, value: f64) -> bool {
        match self {
            Self::GreaterThan(t) => value > *t,
            Self::LessThan(t) => value < *t,
            Self::Between(lo, hi) => value >= *lo && value <= *hi,
        }
    }
}
