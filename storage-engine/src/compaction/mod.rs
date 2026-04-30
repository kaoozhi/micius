use crate::chunk::{reader::ChunkReader, writer::ChunkWriter};
use crate::index::chunk_index::ChunkIndex;
use crate::types::*;
use anyhow::Result;
use std::collections::{BTreeMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

pub struct CompactionWorker {
    index: Arc<RwLock<ChunkIndex>>,
    writer: Arc<ChunkWriter>,
    min_threshold: usize, // minimum chunk files per tier to trigger a merge
    size_ratio: f64,      // max/min file size ratio within a merge group
}

#[derive(Default)]
struct MergeGroup {
    chunks: Vec<(ChunkId, PathBuf)>, // (id, path) for each file to merge
    deregister: Vec<(SeriesId, ChunkId, i64)>,
}

impl CompactionWorker {
    pub fn new(
        index: Arc<RwLock<ChunkIndex>>,
        writer: Arc<ChunkWriter>,
        min_threshold: usize,
        size_ratio: f64,
    ) -> Self {
        assert!(size_ratio >= 1.0, "size_ratio must be >= 1.0");
        Self {
            index,
            writer,
            min_threshold,
            size_ratio,
        }
    }

    fn find_candidates(&self, index: &ChunkIndex) -> Vec<MergeGroup> {
        let mut files: Vec<(ChunkId, PathBuf, u64)> = index
            .chunk_files
            .iter()
            .filter(|(_, meta)| meta.file_size > 0) // ← skip zero-byte files
            .map(|(&id, meta)| (id, meta.file_path.clone(), meta.file_size))
            .collect();

        files.sort_by_key(|(_, _, size)| *size);

        let mut raw_groups: Vec<Vec<(ChunkId, PathBuf)>> = Vec::new();
        let mut current: Vec<(ChunkId, PathBuf)> = Vec::new();
        let mut group_min_size: u64 = 0;

        for (id, path, size) in files {
            if current.is_empty() {
                group_min_size = size;
                current.push((id, path));
            } else if size as f64 / group_min_size as f64 <= self.size_ratio {
                current.push((id, path));
            } else {
                if current.len() >= self.min_threshold {
                    raw_groups.push(current);
                }
                current = vec![(id, path)];
                group_min_size = size;
            }
        }
        if current.len() >= self.min_threshold {
            raw_groups.push(current);
        }

        // For each group, collect (series_id, chunk_id, time_start_ns) for deregistration
        // by scanning time_index for entries whose chunk_id is in this group's chunk set.
        raw_groups
            .into_iter()
            .map(|chunks| {
                let chunk_ids: HashSet<ChunkId> = chunks.iter().map(|(id, _)| *id).collect();

                let deregister = index
                    .time_index
                    .iter()
                    .flat_map(|(&series_id, time_map)| {
                        time_map
                            .iter()
                            .filter(|(_, entry)| chunk_ids.contains(&entry.chunk_id))
                            .map(move |(&time_start_ns, entry)| {
                                (series_id, entry.chunk_id, time_start_ns)
                            })
                    })
                    .collect();

                MergeGroup { chunks, deregister }
            })
            .collect()
    }

    async fn merge_group(&self, group: MergeGroup) -> Result<()> {
        let mut entries: BTreeMap<SeriesKey, Vec<(i64, f64)>> = BTreeMap::new();
        for (_, path) in group.chunks.iter() {
            let points = ChunkReader::read_chunk(path).await?;
            for point in points.iter() {
                let key = SeriesKey {
                    metric_name: point.metric_name.clone(),
                    tags: point.tags.clone(),
                };
                let vec = entries.entry(key).or_default();
                match vec.binary_search_by_key(&point.timestamp_ns, |&(ts, _)| ts) {
                    // duplicate timestamp found, overwrite it
                    Ok(pos) => vec[pos].1 = point.value,
                    // insert in sorted order if timestamp not found
                    Err(pos) => {
                        vec.insert(pos, (point.timestamp_ns, point.value));
                    }
                }
            }
        }
        let result = self.writer.write(entries).await?;
        let mut index = self.index.write().await;
        for s in result.series_results.iter() {
            index.register(
                &s.series_key,
                s.entry.clone(),
                s.stats.clone(),
                result.chunk_meta.clone(),
            );
        }

        for (series_id, chunk_id, time_start_ns) in group.deregister {
            index.deregister(series_id, chunk_id, time_start_ns);
        }

        drop(index);

        for (_, path) in &group.chunks {
            if let Err(e) = tokio::fs::remove_file(path).await {
                if e.kind() != std::io::ErrorKind::NotFound {
                    tracing::warn!(path = ?path, error = %e, "failed to delete old chunk file");
                }
            }
        }

        Ok(())
    }

    pub async fn compact_once(&self) -> Result<()> {
        let candidates = {
            let index = self.index.read().await;
            self.find_candidates(&index)
        };

        if candidates.is_empty() {
            tracing::debug!("No compaction candidates found");
            return Ok(());
        }

        tracing::info!(
            groups = candidates.len(),
            "Compaction: merging chunk groups"
        );

        for group in candidates {
            self.merge_group(group).await?;
        }

        Ok(())
    }
}
