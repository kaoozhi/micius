use std::collections::BTreeMap;
use storage_engine::memtable::Memtable;
use storage_engine::types::DataPoint;
use storage_engine::types::SeriesKey;

fn create_point(metric: &str, ts: i64, value: f64) -> DataPoint {
    DataPoint {
        metric_name: metric.to_string(),
        tags: BTreeMap::from([("host".to_string(), "node-0".to_string())]),
        timestamp_ns: ts,
        value,
    }
}

#[test]
fn test_insert_and_drain() {
    let mut memtable = Memtable::new(256);
    for i in 0..3 {
        let point = create_point("example", (i + 1) * 100, i as f64);
        memtable.insert(point);
    }

    assert_eq!(memtable.entry_count(), 3);

    let drained = memtable.drain();
    assert!(memtable.is_empty());
    let key = SeriesKey {
        metric_name: "example".to_string(),
        tags: BTreeMap::from([("host".to_string(), "node-0".to_string())]),
    };
    let entries = drained.get(&key).expect("series key not found");
    assert_eq!(entries.len(), 3);
    let mut prev = 0;
    for entry in entries.iter() {
        assert!(entry.0 > prev, "Timestamp should be in increasing order");
        prev = entry.0;
    }
}

#[test]
fn test_out_of_order_insert() {
    // Insert timestamps [300, 100, 200], drain
    // - verify the vec comes back as [100, 200, 300]
    let mut memtable = Memtable::new(256);
    for i in &[300, 100, 200] {
        let point = create_point("example", *i, *i as f64);
        memtable.insert(point);
    }

    assert_eq!(memtable.entry_count(), 3);

    let drained = memtable.drain();
    assert!(memtable.is_empty());
    let key = SeriesKey {
        metric_name: "example".to_string(),
        tags: BTreeMap::from([("host".to_string(), "node-0".to_string())]),
    };
    let entries = drained.get(&key).expect("series key not found");
    assert_eq!(entries.len(), 3);
    let expected = &[100, 200, 300];
    for (i, entry) in entries.iter().enumerate() {
        assert_eq!(
            entry.0, expected[i],
            "Timestamp should be back in increasing order"
        );
    }
}

#[test]
fn test_flush_threshold() {
    // Create memtable with small threshold (e.g. 32 * 5 = 160 bytes)
    // Insert points one at a time
    // - should_flush() is false before reaching threshold
    // - should_flush() is true once size_bytes >= threshold
    let mut memtable = Memtable::new(32 * 5);
    for i in 0..4 {
        memtable.insert(create_point("example", (i + 1) * 100, i as f64));
        assert!(!memtable.should_flush());
    }
    memtable.insert(create_point("example", 500, 4.0));
    assert!(memtable.should_flush());
}

#[test]
fn test_double_buffer_pattern() {
    let mut memtable = Memtable::new(1024);

    // Batch 1: insert and drain (simulates flush)
    for i in 0..3 {
        memtable.insert(create_point("cpu.usage", (i + 1) * 100, i as f64));
    }
    let batch_1 = memtable.drain();
    assert!(memtable.is_empty());
    assert_eq!(memtable.entry_count(), 0);
    assert_eq!(memtable.size_bytes(), 0);

    // Batch 2: insert into the fresh memtable
    for i in 0..2 {
        memtable.insert(create_point("mem.free", (i + 1) * 100, i as f64));
    }

    // Verify batch_1 has only the first series
    assert_eq!(batch_1.len(), 1);
    assert!(batch_1.contains_key(&SeriesKey {
        metric_name: "cpu.usage".to_string(),
        tags: BTreeMap::from([("host".to_string(), "node-0".to_string())]),
    }));

    // Drain again — should only contain batch_2's data
    let batch_2 = memtable.drain();
    assert_eq!(batch_2.len(), 1);
    let points = batch_2.get(&SeriesKey {
        metric_name: "mem.free".to_string(),
        tags: BTreeMap::from([("host".to_string(), "node-0".to_string())]),
    }).expect("batch_2 series key not found");
    assert_eq!(points.len(), 2);
}

#[test]
fn test_size_tracking() {
    let mut memtable = Memtable::new(1024);

    // Each point costs 32 bytes (16 for tuple + 16 amortized overhead)
    let n = 5;
    for i in 0..n {
        memtable.insert(create_point("example", (i + 1) * 100, i as f64));
    }
    assert_eq!(memtable.size_bytes(), n as usize * 32);

    // Duplicate timestamp — overwrite, size should not change
    memtable.insert(create_point("example", 300, 99.0));
    assert_eq!(memtable.size_bytes(), n as usize * 32);
    assert_eq!(memtable.entry_count(), n as u64);

    // Drain resets to zero
    memtable.drain();
    assert_eq!(memtable.size_bytes(), 0);
}

#[test]
fn test_multi_series_drain_order() {
    let mut memtable = Memtable::new(1024);

    // Insert across 3 series in non-alphabetical order
    memtable.insert(create_point("mem.free", 100, 1.0));
    memtable.insert(create_point("mem.free", 200, 2.0));
    memtable.insert(create_point("cpu.usage", 100, 3.0));
    memtable.insert(create_point("cpu.usage", 200, 4.0));
    memtable.insert(create_point("cpu.usage", 300, 5.0));
    memtable.insert(create_point("disk.io", 100, 6.0));

    assert_eq!(memtable.entry_count(), 6);

    let drained = memtable.drain();
    assert_eq!(drained.len(), 3);

    // BTreeMap keys come out in sorted order — chunk writer depends on this
    let keys: Vec<&str> = drained.keys().map(|k| k.metric_name.as_str()).collect();
    assert_eq!(keys, vec!["cpu.usage", "disk.io", "mem.free"]);

    // Each series has the correct number of points, no cross-contamination
    let make_key = |name: &str| SeriesKey {
        metric_name: name.to_string(),
        tags: BTreeMap::from([("host".to_string(), "node-0".to_string())]),
    };
    assert_eq!(drained[&make_key("cpu.usage")].len(), 3);
    assert_eq!(drained[&make_key("disk.io")].len(), 1);
    assert_eq!(drained[&make_key("mem.free")].len(), 2);
}
