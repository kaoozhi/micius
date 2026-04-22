use crate::types::*;
use std::collections::{BTreeMap, HashMap, HashSet};

#[derive(Default)]
pub struct ChunkIndex {
    series_registry: HashMap<SeriesId, SeriesKey>, // Reverse map between SeriesId and SeriesKey, for O(1) metric-name lookup during query filtering
    time_index: HashMap<SeriesId, BTreeMap<i64, ChunkMeta>>, // a sorted map from chunk start time to chunk metadata per SeriesID
    tag_index: HashMap<(String, String), HashSet<SeriesId>>, // Inverted tag index (tag key, tag value) map with SeriesID
    chunk_stats: HashMap<ChunkId, ChunkStats>,               // Chunk stats map per ChunkID
    /// File sizes keyed by ChunkId — used by the compaction worker to group
    /// chunks by size without opening any files.
    file_sizes: HashMap<ChunkId, u64>, // Chunk size map per ChunkID
}

impl ChunkIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Register a new chunk after a successful memtable flush.
    /// Called once per series per flush. `file_size` comes from `ChunkWriteResult`.
    pub fn register(
        &mut self,
        series_key: &SeriesKey,
        meta: ChunkMeta,
        stats: ChunkStats,
        file_size: u64,
    ) -> SeriesId {
        let series_id = SeriesId::from(series_key);

        // register series_key
        if self
            .series_registry
            .insert(series_id, series_key.clone())
            .is_none()
        {
            // new series — register tags
            for (tag_key, tag_val) in &series_key.tags {
                self.tag_index
                    .entry((tag_key.clone(), tag_val.clone()))
                    .or_default()
                    .insert(series_id);
            }
        }

        // register time index
        let chunk_id = meta.chunk_id;
        let time_start_ns = meta.time_start_ns;
        self.time_index
            .entry(series_id)
            .or_default()
            .insert(time_start_ns, meta);

        self.chunk_stats.insert(chunk_id, stats);
        self.file_sizes.insert(chunk_id, file_size);
        series_id
    }

    /// Remove a chunk from the index — called after compaction deletes old chunks.
    pub fn deregister(&mut self, series_id: SeriesId, chunk_id: ChunkId, time_start_ns: i64) {
        if let Some(time_map) = self.time_index.get_mut(&series_id) {
            time_map.remove(&time_start_ns);
        }

        self.chunk_stats.remove(&chunk_id);
        self.file_sizes.remove(&chunk_id);
    }

    /// Resolve which series IDs match the given metric name and tag filters.
    ///
    /// Uses the tag inverted index to intersect sets efficiently.
    /// Falls back to full scan only when tag_filters is empty.
    pub fn resolve_series(
        &self,
        metric: &str,
        tag_filters: &HashMap<String, String>,
    ) -> Vec<SeriesId> {
        if tag_filters.is_empty() {
            return self
                .series_registry
                .iter()
                .filter(|(_, s_k)| s_k.metric_name == metric)
                .map(|(&id, _)| id)
                .collect();
        }

        let mut filters_iter = tag_filters.iter();
        let Some((first_key, first_val)) = filters_iter.next() else {
            return vec![];
        };
        let Some(first_set) = self.tag_index.get(&(first_key.clone(), first_val.clone())) else {
            return vec![];
        };
        let mut candidate_ids: HashSet<SeriesId> = first_set.clone();

        loop {
            let Some((tag_key, tag_val)) = filters_iter.next() else {
                break;
            };
            match self.tag_index.get(&(tag_key.clone(), tag_val.clone())) {
                None => return vec![],
                Some(set) => {
                    candidate_ids.retain(|id| set.contains(id));
                    if candidate_ids.is_empty() {
                        return vec![];
                    }
                }
            }
        }
        candidate_ids
            .into_iter()
            .filter(|id| {
                let Some(s_key) = self.series_registry.get(id) else {
                    return false;
                };
                s_key.metric_name == metric
            })
            .collect()
    }
    /// Find chunks to read for a given series and time range.
    /// Applies three stages of pruning in order of cost:
    ///
    /// Stage 1 — Time range pruning (pure in-memory BTreeMap range scan)
    ///   Eliminates chunks whose time range doesn't overlap the query window.
    ///   Zero disk I/O. Always applied.
    ///
    /// Stage 2 — Min/max statistics pruning (in-memory HashMap lookup)
    ///   Eliminates chunks where max_value < threshold (for GT predicates)
    ///   or min_value > threshold (for LT predicates).
    ///   Zero disk I/O. Applied only when query has a value predicate.
    ///
    /// Stage 3 — Bloom filter (disk footer read — see ChunkReader)
    ///   Applied by ChunkReader.check_bloom() before reading column data.
    ///   Not applied here — ChunkIndex has no file I/O.
    pub fn prune_chunks(
        &self,
        series_id: &SeriesId,
        time_start_ns: i64,
        time_end_ns: i64,
        predicate: Option<&ValuePredicate>,
    ) -> Vec<ChunkMeta> {
        let Some(time_map) = self.time_index.get(series_id) else {
            return vec![];
        };

        time_map
            .range(..=time_end_ns)
            .filter(|(_, meta)| time_start_ns <= meta.time_end_ns)
            .filter(|(_, meta)| {
                predicate.is_none_or(|p| {
                    let Some(stats) = self.chunk_stats.get(&meta.chunk_id) else {
                        return true; // no stats — must read
                    };
                    p.matches(stats.min_value, stats.max_value)
                })
            })
            .map(|(_, meta)| meta.clone())
            .collect()
    }
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
}
