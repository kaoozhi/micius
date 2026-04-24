// storage-engine/tests/index_test.rs
mod common;

use common::*;
use std::collections::{BTreeMap, HashMap};
use storage_engine::index::chunk_index::{ChunkIndex, ValuePredicate};
use storage_engine::types::*;

#[tokio::test]
async fn test_single_tag_resolution() {
    let metric = "payments";
    let host = "service";
    let written_key = series_key(metric, host);

    let mut data = single_series_data(metric, host, 10);
    data.insert(
        series_key("payment", "host-0"),
        make_points(1_000_000_000, 1_000_000, 20),
    );

    let (_dir, write_result) = write_chunk_with_results(data).await;

    let mut index = ChunkIndex::new();
    for s in write_result.series_results.iter() {
        index.register(
            &s.series_key,
            s.meta.clone(),
            s.stats.clone(),
            write_result.file_size,
        );
    }

    let resolved = index.resolve_series(
        metric,
        &HashMap::from([("host".to_string(), host.to_string())]),
    );
    assert_eq!(resolved.len(), 1, "Only one serie should be matched");
    assert!(resolved.contains(&SeriesId::from(&written_key)));
}

#[tokio::test]
async fn test_multi_tag_intersection() {
    // Only web1+prod should survive a two-tag filter
    let make_key = |host: &str, env: &str| SeriesKey {
        metric_name: "cpu".into(),
        tags: BTreeMap::from([("env".into(), env.into()), ("host".into(), host.into())]),
    };

    let key_match = make_key("web1", "prod");
    let key_wrong_env = make_key("web1", "staging");
    let key_wrong_host = make_key("web2", "prod");
    let key_no_match = make_key("web2", "staging");
    let key_also_match = SeriesKey {
        metric_name: "cpu".into(),
        tags: BTreeMap::from([
            ("env".into(), "prod".into()),
            ("host".into(), "web1".into()),
            ("region".into(), "us-east".into()), // extra tag — still matches
        ]),
    };

    let mut data = BTreeMap::new();
    data.insert(key_match.clone(), make_points(0, 1_000_000_000, 10));
    data.insert(key_wrong_env.clone(), make_points(0, 1_000_000_000, 10));
    data.insert(key_wrong_host.clone(), make_points(0, 1_000_000_000, 10));
    data.insert(key_no_match.clone(), make_points(0, 1_000_000_000, 10));
    data.insert(key_also_match.clone(), make_points(0, 1_000_000_000, 10));

    let (_dir, write_result) = write_chunk_with_results(data).await;
    let mut index = ChunkIndex::new();
    for s in &write_result.series_results {
        index.register(
            &s.series_key,
            s.meta.clone(),
            s.stats.clone(),
            write_result.file_size,
        );
    }

    let resolved = index.resolve_series(
        "cpu",
        &HashMap::from([
            ("env".to_string(), "prod".to_string()),
            ("host".to_string(), "web1".to_string()),
        ]),
    );

    assert_eq!(resolved.len(), 2, "should return 2 matches");
    assert!(resolved.contains(&SeriesId::from(&key_match)));
    assert!(resolved.contains(&SeriesId::from(&key_also_match)));
}

#[tokio::test]
async fn test_no_match_tags() {
    // Only web1+prod should survive a two-tag filter
    let make_key = |host: &str, env: &str| SeriesKey {
        metric_name: "cpu".into(),
        tags: BTreeMap::from([("env".into(), env.into()), ("host".into(), host.into())]),
    };

    let key_wrong_env = make_key("web1", "staging");
    let key_wrong_host = make_key("web2", "prod");

    let mut data = BTreeMap::new();
    data.insert(key_wrong_env.clone(), make_points(0, 1_000_000_000, 10));
    data.insert(key_wrong_host.clone(), make_points(0, 1_000_000_000, 10));

    let (_dir, write_result) = write_chunk_with_results(data).await;
    let mut index = ChunkIndex::new();
    for s in &write_result.series_results {
        index.register(
            &s.series_key,
            s.meta.clone(),
            s.stats.clone(),
            write_result.file_size,
        );
    }

    let resolved = index.resolve_series(
        "cpu",
        &HashMap::from([
            ("env".to_string(), "prod".to_string()),
            ("host".to_string(), "web3".to_string()),
        ]),
    );
    assert!(resolved.is_empty());
}

#[tokio::test]
async fn test_time_range_pruning() {
    let make_key = |host: &str, env: &str| SeriesKey {
        metric_name: "cpu".into(),
        tags: BTreeMap::from([("env".into(), env.into()), ("host".into(), host.into())]),
    };

    let key = make_key("web1", "prod");

    // 5 chunks, each 10 points × 1s step, back to back:
    // Chunk 0: [0s,  9s]
    // Chunk 1: [10s, 19s]
    // Chunk 2: [20s, 29s]
    // Chunk 3: [30s, 39s]
    // Chunk 4: [40s, 49s]
    let step_ns: i64 = 1_000_000_000;
    let points_per_chunk = 10;

    let mut index = ChunkIndex::new();
    let mut dirs = Vec::new(); // keep TempDir guards alive for the test's duration
    let mut time_start = 0i64;
    for _ in 0..5 {
        let mut data = BTreeMap::new();
        data.insert(
            key.clone(),
            make_points(time_start, step_ns, points_per_chunk),
        );
        time_start += step_ns * points_per_chunk as i64;
        let (dir, write_result) = write_chunk_with_results(data).await;
        for s in &write_result.series_results {
            index.register(
                &s.series_key,
                s.meta.clone(),
                s.stats.clone(),
                write_result.file_size,
            );
        }
        dirs.push(dir);
    }
    assert_eq!(index.series_count(), 1);
    assert_eq!(index.chunk_file_count(), 5);

    let series_id = SeriesId::from(&key);
    // Query [15s, 35s] — overlaps chunks 1, 2, 3 only.
    // Chunk 0 ends at 9s  (before query start 15s) → pruned.
    // Chunk 4 starts at 40s (after query end 35s)   → pruned.
    let chunks = index.prune_chunks(&series_id, 15 * step_ns, 35 * step_ns, None);

    assert_eq!(chunks.len(), 3, "expected 3 overlapping chunks");
    let starts: Vec<i64> = chunks.iter().map(|c| c.time_start_ns).collect();
    assert!(
        starts.contains(&(10 * step_ns)),
        "chunk 1 [10s,19s] should be included"
    );
    assert!(
        starts.contains(&(20 * step_ns)),
        "chunk 2 [20s,29s] should be included"
    );
    assert!(
        starts.contains(&(30 * step_ns)),
        "chunk 3 [30s,39s] should be included"
    );
}

#[tokio::test]
async fn test_stats_predicate_gt() {
    // 3 chunks for the same series, each with a different number of points
    // (different n → different max value, since make_points values are 0..n-1):
    //   Chunk 0: n=5  → values 0.0-4.0, max=4  — pruned by GT(5)
    //   Chunk 1: n=20 → values 0.0-19.0, max=19 — kept   by GT(5)
    //   Chunk 2: n=3  → values 0.0-2.0, max=2   — pruned by GT(5)
    let key = series_key("cpu", "web1");
    let step_ns: i64 = 1_000_000_000;
    let configs: &[(i64, usize)] = &[(0, 5), (5 * step_ns, 20), (25 * step_ns, 3)];

    let mut index = ChunkIndex::new();
    let mut dirs = Vec::new();
    for &(ts, n) in configs {
        let mut data = BTreeMap::new();
        data.insert(key.clone(), make_points(ts, step_ns, n));
        let (dir, write_result) = write_chunk_with_results(data).await;
        for s in &write_result.series_results {
            index.register(
                &s.series_key,
                s.meta.clone(),
                s.stats.clone(),
                write_result.file_size,
            );
        }
        dirs.push(dir);
    }

    let series_id = SeriesId::from(&key);
    let chunks = index.prune_chunks(
        &series_id,
        i64::MIN,
        i64::MAX,
        Some(&ValuePredicate::GreaterThan(5.0)),
    );

    assert_eq!(
        chunks.len(),
        1,
        "only chunk with max=19 should survive GT(5)"
    );
    assert_eq!(
        chunks[0].time_start_ns,
        5 * step_ns,
        "surviving chunk starts at 5s"
    );
}

#[tokio::test]
async fn test_stats_predicate_between() {
    // Between(lo, hi) keeps a chunk when its [min,max] overlaps [lo,hi]:
    // condition: min_chunk <= hi && max_chunk >= lo
    //
    //   Chunk 0: n=20 → max=19, min=0 — Between(3,25): 0<=25 && 19>=3 → kept
    //   Chunk 1: n=2  → max=1,  min=0 — Between(3,25): 0<=25 && 1>=3  → pruned
    //   Chunk 2: n=10 → max=9,  min=0 — Between(3,25): 0<=25 && 9>=3  → kept
    let key = series_key("cpu", "web1");
    let step_ns: i64 = 1_000_000_000;
    let configs: &[(i64, usize)] = &[(0, 20), (20 * step_ns, 2), (22 * step_ns, 10)];

    let mut index = ChunkIndex::new();
    let mut dirs = Vec::new();
    for &(ts, n) in configs {
        let mut data = BTreeMap::new();
        data.insert(key.clone(), make_points(ts, step_ns, n));
        let (dir, write_result) = write_chunk_with_results(data).await;
        for s in &write_result.series_results {
            index.register(
                &s.series_key,
                s.meta.clone(),
                s.stats.clone(),
                write_result.file_size,
            );
        }
        dirs.push(dir);
    }

    let series_id = SeriesId::from(&key);
    let chunks = index.prune_chunks(
        &series_id,
        i64::MIN,
        i64::MAX,
        Some(&ValuePredicate::Between(3.0, 25.0)),
    );

    assert_eq!(
        chunks.len(),
        2,
        "chunks 0 and 2 should survive Between(3,25)"
    );
    let starts: Vec<i64> = chunks.iter().map(|c| c.time_start_ns).collect();
    assert!(starts.contains(&0), "chunk 0 [max=19] should be kept");
    assert!(
        starts.contains(&(22 * step_ns)),
        "chunk 2 [max=9]  should be kept"
    );
}

#[tokio::test]
async fn test_register_deregister() {
    let key = series_key("cpu", "web1");
    let mut data = BTreeMap::new();
    data.insert(key.clone(), make_points(0, 1_000_000_000, 10));

    let (_dir, write_result) = write_chunk_with_results(data).await;
    let s = write_result
        .series_results
        .first()
        .expect("one series result");

    let mut index = ChunkIndex::new();
    index.register(
        &s.series_key,
        s.meta.clone(),
        s.stats.clone(),
        write_result.file_size,
    );

    let series_id = SeriesId::from(&key);

    // After register: chunk is visible
    let before = index.prune_chunks(&series_id, i64::MIN, i64::MAX, None);
    assert_eq!(before.len(), 1, "chunk should be visible after register");

    // Deregister using the chunk's own metadata
    index.deregister(series_id, s.meta.chunk_id, s.meta.time_start_ns);

    // After deregister: chunk is gone
    assert_eq!(
        index.chunk_file_count(),
        0,
        "file_sizes entry should be gone after deregister"
    );

    let after = index.prune_chunks(&series_id, i64::MIN, i64::MAX, None);
    assert!(after.is_empty(), "chunk should be gone after deregister");
}
