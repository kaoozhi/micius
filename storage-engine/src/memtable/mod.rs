use crate::types::{DataPoint, SeriesKey, ValuePredicate};
use std::collections::{BTreeMap, HashMap};
use std::mem::size_of;

pub struct Memtable {
    entries: BTreeMap<SeriesKey, Vec<(i64, f64)>>,
    size_bytes: usize,
    flush_threshold: usize,
    entry_count: u64,
}

impl Memtable {
    pub fn new(threshold: usize) -> Self {
        Memtable {
            entries: BTreeMap::new(),
            size_bytes: 0usize,
            flush_threshold: threshold,
            entry_count: 0,
        }
    }

    pub fn insert(&mut self, point: DataPoint) {
        let key = SeriesKey {
            metric_name: point.metric_name,
            tags: point.tags,
        };
        let vec = self.entries.entry(key).or_default();
        match vec.binary_search_by_key(&point.timestamp_ns, |&(ts, _)| ts) {
            // duplicate timestamp found, overwrite it
            Ok(pos) => vec[pos].1 = point.value,
            // insert in sorted order if timestamp not found
            Err(pos) => {
                vec.insert(pos, (point.timestamp_ns, point.value));
                // 16 bytes for (i64, f64) + 16 bytes amortized for Vec/BTreeMap overhead
                self.size_bytes += size_of::<(i64, f64)>() * 2;
                self.entry_count += 1;
            }
        }
    }

    pub fn should_flush(&self) -> bool {
        self.size_bytes >= self.flush_threshold
    }

    pub fn size_bytes(&self) -> usize {
        self.size_bytes
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn entry_count(&self) -> u64 {
        self.entry_count
    }

    pub fn drain(&mut self) -> BTreeMap<SeriesKey, Vec<(i64, f64)>> {
        self.size_bytes = 0usize;
        self.entry_count = 0;
        std::mem::take(&mut self.entries)
    }

    pub fn resolve_series(
        &self,
        metric: &str,
        tag_filters: &HashMap<String, String>,
    ) -> Vec<SeriesKey> {
        if tag_filters.is_empty() {
            return self
                .entries
                .iter()
                .filter(|(sk, _)| sk.metric_name == metric)
                .map(|(sk, _)| sk.clone())
                .collect();
        }

        self.entries
            .iter()
            .filter(|(sk, _)| sk.metric_name == metric)
            .filter(|(sk, _)| {
                tag_filters.iter().all(|(tag_key, tag_val)| {
                    let Some(val) = sk.tags.get(tag_key) else {
                        return false;
                    };
                    val == tag_val
                })
            })
            .map(|(sk, _)| sk.clone())
            .collect()
    }

    pub fn read_series(
        &self,
        series_key: &SeriesKey,
        time_start_ns: i64,
        time_end_ns: i64,
        predicate: Option<&ValuePredicate>,
    ) -> Vec<DataPoint> {
        let Some(time_map) = self.entries.get(series_key) else {
            return vec![];
        };

        time_map
            .iter()
            .filter(|(ts, _)| time_start_ns <= *ts && time_end_ns >= *ts)
            .filter(|(_, val)| predicate.is_none_or(|p| p.satisfies(*val)))
            .map(|(ts, val)| DataPoint {
                metric_name: series_key.metric_name.clone(),
                tags: series_key.tags.clone(),
                timestamp_ns: *ts,
                value: *val,
            })
            .collect()
    }
}
