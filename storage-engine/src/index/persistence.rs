use crate::index::chunk_index::ChunkIndex;
use crate::types::*;
use anyhow::Result;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::path::Path;

/// Snapshot format is JSON for Phase 1 — simple and debuggable.
/// In a production system this would be a compact binary format.
///
/// Note: `tag_index` is NOT persisted. It is rebuilt from `series_registry`
/// at load time — `SeriesKey.tags` already contains all tag pairs, so no
/// extra data is required. This keeps the snapshot format minimal.
#[derive(serde::Serialize, serde::Deserialize, Default)]
struct IndexSnapshot {
    version: u8,
    last_wal_sequence: u64,
    series_registry: Vec<(SeriesId, SeriesKey)>,
    time_index: Vec<(SeriesId, Vec<(i64, SeriesChunkEntry)>)>,
    chunk_stats: Vec<(ChunkId, SeriesId, SeriesChunkStats)>,
    file_sizes: Vec<(ChunkId, u64)>,
}

pub async fn save_index(index: &ChunkIndex, path: &Path, last_wal_sequence: u64) -> Result<()> {
    let snapshot = IndexSnapshot {
        version: 1,
        last_wal_sequence,
        series_registry: index
            .series_registry
            .iter()
            .map(|(sid, key)| (*sid, key.clone()))
            .collect(),

        time_index: index
            .time_index
            .iter()
            .map(|(sid, timemap)| {
                let entries = timemap
                    .iter()
                    .map(|(time, chunk)| (*time, chunk.clone()))
                    .collect();
                (*sid, entries)
            })
            .collect(),

        chunk_stats: index
            .chunk_stats
            .iter()
            .map(|((cid, sid), stat)| (*cid, *sid, stat.clone()))
            .collect(),

        file_sizes: index
            .file_sizes
            .iter()
            .map(|(cid, size)| (*cid, *size))
            .collect(),
    };

    let bytes = bincode::serialize(&snapshot)?;
    let tmp = path.with_extension("tmp");
    tokio::fs::write(&tmp, &bytes).await?;
    tokio::fs::rename(&tmp, path).await?;

    Ok(())
}

pub async fn load_index(path: &Path) -> Result<Option<(ChunkIndex, u64)>> {
    if !path.exists() {
        return Ok(None);
    }
    let bytes = tokio::fs::read(path).await?;
    let snapshot: IndexSnapshot = bincode::deserialize(&bytes)?;
    anyhow::ensure!(
        snapshot.version == 1,
        "unsupported snapshot version {}",
        snapshot.version
    );
    let last_wal_sequence = snapshot.last_wal_sequence;
    let index = rebuild_index(snapshot);
    Ok(Some((index, last_wal_sequence)))
}

fn rebuild_index(snapshot: IndexSnapshot) -> ChunkIndex {
    ChunkIndex {
        series_registry: snapshot
            .series_registry
            .iter()
            .map(|(sid, sk)| (*sid, sk.clone()))
            .collect(),
        time_index: snapshot
            .time_index
            .into_iter()
            .map(|(sid, time_vec)| {
                let timemap: BTreeMap<i64, SeriesChunkEntry> = time_vec.into_iter().collect();
                (sid, timemap)
            })
            .collect(),
        tag_index: {
            let mut tag_index: HashMap<(String, String), HashSet<SeriesId>> = HashMap::new();
            snapshot.series_registry.iter().for_each(|(sid, sk)| {
                sk.tags.iter().for_each(|(tk, tv)| {
                    tag_index
                        .entry((tk.clone(), tv.clone()))
                        .or_default()
                        .insert(*sid);
                });
            });
            tag_index
        },
        chunk_stats: snapshot
            .chunk_stats
            .iter()
            .map(|(cid, sid, stat)| ((*cid, *sid), stat.clone()))
            .collect(),
        file_sizes: snapshot
            .file_sizes
            .iter()
            .map(|(cid, size)| (*cid, *size))
            .collect(),
        ..ChunkIndex::default()
    }
}
