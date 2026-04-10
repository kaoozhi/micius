# Micius — Implementation Plan: Phases 1 to 3

> **Project**: Micius — AI-powered observability agent backed by a custom time-series storage engine  
> **Stack**: Rust (storage engine) · Go (ingestion, query, alerts) · TypeScript (LLM gateway)  
> **Phases covered**: Phase 1 (Storage Engine) · Phase 2 (Ingestion Layer) · Phase 3 (Query & Alert Layer)

---

## Repository Structure

```
micius/
├── proto/                        # Shared protobuf definitions
│   └── storage/
│       └── v1/
│           └── storage.proto     # gRPC contract between Go and Rust
├── storage-engine/               # Rust — WAL, chunk store, gRPC server
│   ├── src/
│   │   ├── main.rs
│   │   ├── wal/
│   │   │   ├── mod.rs
│   │   │   ├── writer.rs
│   │   │   └── recovery.rs
│   │   ├── memtable/
│   │   │   └── mod.rs
│   │   ├── chunk/
│   │   │   ├── mod.rs
│   │   │   ├── writer.rs
│   │   │   ├── reader.rs
│   │   │   └── format.rs
│   │   ├── index/
│   │   │   ├── chunk_index.rs
│   │   │   └── tag_index.rs
│   │   ├── compaction/
│   │   │   └── mod.rs
│   │   └── server/
│   │       └── mod.rs            # tonic gRPC server
│   ├── build.rs                  # prost-build codegen
│   └── Cargo.toml
├── ingestion/                    # Go — adapters, write buffer, gRPC client
│   ├── cmd/
│   │   └── ingestion/
│   │       └── main.go
│   ├── internal/
│   │   ├── buffer/
│   │   │   └── write_buffer.go
│   │   ├── adapter/
│   │   │   ├── dogstatsd.go
│   │   │   ├── alpaca.go
│   │   │   ├── prometheus.go
│   │   │   └── synthetic.go
│   │   └── storage/
│   │       └── client.go         # gRPC client to Rust engine
│   └── go.mod
├── query/                        # Go — query parser, aggregation, alert worker
│   ├── cmd/
│   │   └── query/
│   │       └── main.go
│   ├── internal/
│   │   ├── parser/
│   │   │   └── query_parser.go
│   │   ├── router/
│   │   │   └── chunk_router.go
│   │   ├── aggregation/
│   │   │   └── engine.go
│   │   └── alert/
│   │       ├── worker.go
│   │       └── webhook.go
│   └── go.mod
├── docker-compose.yml
└── Makefile
```

---

## Phase 1 — Rust Storage Engine

**Goal**: A working Rust storage engine that accepts writes and serves range queries via gRPC, with WAL crash recovery verified by tests.

**Estimated duration**: 6–8 weeks

---

### 1.1 Project Setup

**Cargo.toml dependencies**

```toml
[dependencies]
tokio          = { version = "1", features = ["full"] }
tonic          = "0.11"
prost          = "0.12"
bytes          = "1"
lz4_flex       = "0.11"
crc32fast      = "1"
bloomfilter    = "1"
btreemultimap  = "0.1"
serde          = { version = "1", features = ["derive"] }
tracing        = "0.1"
tracing-subscriber = "0.3"
prometheus     = "0.13"

[build-dependencies]
tonic-build = "0.11"
```

**build.rs — protobuf code generation**

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(true)
        .build_client(false)
        .compile(
            &["../../proto/storage/v1/storage.proto"],
            &["../../proto"],
        )?;
    Ok(())
}
```

---

### 1.2 Protobuf Contract

```protobuf
// proto/storage/v1/storage.proto
syntax = "proto3";
package storage.v1;

service StorageService {
  rpc Append(AppendRequest)       returns (AppendResponse);
  rpc Query(QueryRequest)         returns (stream QueryResponse);
  rpc Compact(CompactRequest)     returns (CompactResponse);
  rpc Snapshot(SnapshotRequest)   returns (SnapshotResponse);
}

message DataPoint {
  string metric_name = 1;
  map<string, string> tags = 2;
  int64 timestamp_ns = 3;        // unix nanoseconds
  double value = 4;
}

message AppendRequest {
  repeated DataPoint points = 1;
}

message AppendResponse {
  uint64 sequence = 1;           // WAL sequence number of this batch
}

message QueryRequest {
  string metric_name = 1;
  map<string, string> tag_filters = 2;
  int64 time_start_ns = 3;
  int64 time_end_ns = 4;
  string aggregation = 5;        // "none" | "mean" | "sum" | "min" | "max" | "p99"
  int64 resolution_ns = 6;       // 0 = raw, >0 = bucketed
}

message QueryResponse {
  int64 timestamp_ns = 1;
  double value = 2;
}

message CompactRequest {
  int64 up_to_time_ns = 1;       // compact all chunks ending before this time
}

message CompactResponse {
  uint32 chunks_merged = 1;
}

message SnapshotRequest {}
message SnapshotResponse {
  string snapshot_path = 1;
}
```

---

### 1.3 Core Data Types

```rust
// src/types.rs
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq)]
pub struct DataPoint {
    pub metric_name: String,
    pub tags:        BTreeMap<String, String>,
    pub timestamp_ns: i64,
    pub value:       f64,
}

/// Stable identifier for a unique metric+tag combination
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SeriesKey {
    pub metric_name: String,
    pub tags:        BTreeMap<String, String>,
}

impl SeriesKey {
    /// Canonical byte representation for hashing and bloom filters
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut s = self.metric_name.clone();
        for (k, v) in &self.tags {
            s.push_str(&format!(",{}={}", k, v));
        }
        s.into_bytes()
    }
}

pub type SeriesId = u64;
pub type ChunkId  = u64;
pub type Sequence = u64;
```

---

### 1.4 WAL Implementation

**WAL entry format**

Each entry is: `[length: u32][checksum: u32][payload: bytes]`

The payload is a protobuf-serialized `WalEntry`. Length prefix enables recovery scanning. Checksum enables torn-write detection.

```rust
// src/wal/writer.rs
use crc32fast::Hasher;
use prost::Message;
use std::path::PathBuf;
use tokio::fs::{File, OpenOptions};
use tokio::io::AsyncWriteExt;

pub struct WalWriter {
    file:         File,
    current_seq:  Sequence,
    current_size: u64,
    segment_path: PathBuf,
    max_segment_bytes: u64,        // rotate WAL file after this size
}

impl WalWriter {
    pub async fn open(dir: &Path, max_segment_bytes: u64) -> Result<Self> {
        let path = dir.join("wal-000001.log");
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .await?;
        Ok(Self {
            file,
            current_seq: 0,
            current_size: 0,
            segment_path: path,
            max_segment_bytes,
        })
    }

    pub async fn append(&mut self, points: &[DataPoint]) -> Result<Sequence> {
        self.current_seq += 1;

        let entry = WalEntry {
            sequence: self.current_seq,
            points: points.iter().map(encode_point).collect(),
        };

        let payload = entry.encode_to_vec();

        // CRC32 over payload bytes
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let checksum = hasher.finalize();

        // Write: [length u32][checksum u32][payload]
        let mut buf = Vec::with_capacity(8 + payload.len());
        buf.extend_from_slice(&(payload.len() as u32).to_le_bytes());
        buf.extend_from_slice(&checksum.to_le_bytes());
        buf.extend_from_slice(&payload);

        self.file.write_all(&buf).await?;
        self.file.sync_data().await?;         // fsync — durability guarantee
        self.current_size += buf.len() as u64;

        Ok(self.current_seq)
    }
}
```

**WAL recovery**

```rust
// src/wal/recovery.rs
pub async fn recover(dir: &Path) -> Result<Vec<DataPoint>> {
    let mut recovered = Vec::new();

    // Find all WAL segment files sorted by name
    let mut segments = list_wal_segments(dir).await?;
    segments.sort();

    for segment in segments {
        let mut file = File::open(&segment).await?;
        let mut buf = Vec::new();
        file.read_to_end(&mut buf).await?;

        let mut cursor = 0usize;

        while cursor + 8 <= buf.len() {
            // Read length prefix
            let length = u32::from_le_bytes(buf[cursor..cursor+4].try_into()?) as usize;
            let checksum = u32::from_le_bytes(buf[cursor+4..cursor+8].try_into()?);
            cursor += 8;

            if cursor + length > buf.len() {
                // Torn write — stop recovery here
                tracing::warn!("Torn WAL entry detected at offset {}", cursor);
                break;
            }

            let payload = &buf[cursor..cursor + length];

            // Verify checksum
            let mut hasher = Hasher::new();
            hasher.update(payload);
            if hasher.finalize() != checksum {
                tracing::warn!("WAL checksum mismatch at offset {}", cursor);
                break;
            }

            let entry = WalEntry::decode(payload)?;
            for point in entry.points {
                recovered.push(decode_point(point)?);
            }

            cursor += length;
        }
    }

    Ok(recovered)
}
```

**Tests**

```rust
#[cfg(test)]
mod tests {
    #[tokio::test]
    async fn test_wal_recovery_after_crash() {
        let dir = tempdir().unwrap();
        let mut writer = WalWriter::open(dir.path(), 64 * 1024 * 1024).await.unwrap();

        let points = vec![make_test_point("cpu.usage", 42.0)];
        writer.append(&points).await.unwrap();

        // Simulate crash — drop writer without clean shutdown
        drop(writer);

        // Recovery should return all appended points
        let recovered = recover(dir.path()).await.unwrap();
        assert_eq!(recovered.len(), 1);
        assert_eq!(recovered[0].value, 42.0);
    }

    #[tokio::test]
    async fn test_torn_write_detection() {
        let dir = tempdir().unwrap();
        // Write valid entry then corrupt last bytes
        // Recovery should stop at corruption boundary
        // ...
    }
}
```

---

### 1.5 Memtable

```rust
// src/memtable/mod.rs
use std::collections::BTreeMap;

pub struct Memtable {
    /// Series key → sorted vec of (timestamp_ns, value)
    entries:          BTreeMap<SeriesKey, Vec<(i64, f64)>>,
    size_bytes:       usize,
    flush_threshold:  usize,       // flush when size exceeds this
}

impl Memtable {
    pub fn insert(&mut self, point: DataPoint) {
        let key = SeriesKey {
            metric_name: point.metric_name,
            tags: point.tags,
        };
        let entry = self.entries.entry(key).or_default();
        // Insert maintaining time order (most appends are in order)
        match entry.binary_search_by_key(&point.timestamp_ns, |(ts, _)| *ts) {
            Ok(pos) => entry[pos].1 = point.value,   // overwrite duplicate timestamp
            Err(pos) => entry.insert(pos, (point.timestamp_ns, point.value)),
        }
        self.size_bytes += 32;     // approximate cost per entry
    }

    pub fn should_flush(&self) -> bool {
        self.size_bytes >= self.flush_threshold
    }

    pub fn drain(&mut self) -> BTreeMap<SeriesKey, Vec<(i64, f64)>> {
        self.size_bytes = 0;
        std::mem::take(&mut self.entries)
    }
}
```

---

### 1.6 Chunk File Format

**Binary layout**

```
┌──────────────────────────────────────────┐
│ HEADER (32 bytes)                        │
│   magic:         u32  = 0x48454C58       │  "HELX"
│   version:       u8   = 1                │
│   _padding:      u8   × 3                │
│   time_start_ns: i64                     │
│   time_end_ns:   i64                     │
│   series_count:  u32                     │
│   entry_count:   u32                     │
├──────────────────────────────────────────┤
│ SERIES DIRECTORY (per series)            │
│   series_key_len:  u32                   │
│   series_key:      bytes                 │
│   ts_col_offset:   u64  (from file start)│
│   val_col_offset:  u64                   │
│   entry_count:     u32                   │
├──────────────────────────────────────────┤
│ TIMESTAMP COLUMN                         │
│   delta-encoded i64 array                │
│   lz4-block compressed                  │
├──────────────────────────────────────────┤
│ VALUE COLUMN                             │
│   f64 array (little-endian)              │
│   lz4-block compressed                  │
├──────────────────────────────────────────┤
│ FOOTER                                   │
│   bloom_filter_len: u32                  │
│   bloom_filter:     bytes                │
│   checksum:         u32  (CRC32 of all   │
│                          preceding bytes)│
└──────────────────────────────────────────┘
```

**Chunk writer**

```rust
// src/chunk/writer.rs
pub struct ChunkWriter {
    data_dir: PathBuf,
}

impl ChunkWriter {
    pub async fn write(
        &self,
        series_data: BTreeMap<SeriesKey, Vec<(i64, f64)>>,
        time_start_ns: i64,
        time_end_ns: i64,
    ) -> Result<ChunkMeta> {
        let chunk_id = generate_chunk_id();
        let path = self.data_dir.join(format!("chunk-{:016x}.mcs", chunk_id));

        let mut buf = Vec::new();
        let mut bloom = BloomFilter::with_rate(0.01, series_data.len() as u32);

        // Write series directory entries and collect column data
        let mut series_directory = Vec::new();
        let mut all_timestamps: Vec<Vec<i64>> = Vec::new();
        let mut all_values: Vec<Vec<f64>> = Vec::new();

        for (key, points) in &series_data {
            bloom.set(&key.to_bytes());
            let (timestamps, values): (Vec<i64>, Vec<f64>) = points.iter().copied().unzip();

            // Delta encode timestamps
            let deltas = delta_encode(&timestamps);

            series_directory.push((key.clone(), timestamps.len() as u32));
            all_timestamps.push(deltas);
            all_values.push(values);
        }

        // Compress and write columns
        // ... (write header, directory, columns, footer with bloom filter and checksum)

        tokio::fs::write(&path, &buf).await?;

        Ok(ChunkMeta {
            chunk_id,
            time_start_ns,
            time_end_ns,
            file_path: path,
            size_bytes: buf.len() as u64,
            entry_count: series_data.values().map(|v| v.len() as u32).sum(),
        })
    }
}

fn delta_encode(timestamps: &[i64]) -> Vec<i64> {
    if timestamps.is_empty() { return vec![]; }
    let mut deltas = vec![timestamps[0]];
    for i in 1..timestamps.len() {
        deltas.push(timestamps[i] - timestamps[i-1]);
    }
    deltas
}

fn delta_decode(deltas: &[i64]) -> Vec<i64> {
    if deltas.is_empty() { return vec![]; }
    let mut ts = vec![deltas[0]];
    for i in 1..deltas.len() {
        ts.push(ts[i-1] + deltas[i]);
    }
    ts
}
```

---

### 1.7 Chunk Index and Tag Inverted Index

```rust
// src/index/chunk_index.rs
use std::collections::{BTreeMap, HashMap, HashSet};

pub struct ChunkIndex {
    /// series key → stable series ID
    series_registry: HashMap<SeriesKey, SeriesId>,

    /// series ID → sorted chunk metadata by time_start
    time_index: HashMap<SeriesId, BTreeMap<i64, ChunkMeta>>,

    /// (tag_key, tag_value) → set of series IDs
    tag_index: HashMap<(String, String), HashSet<SeriesId>>,

    /// chunk ID → statistics for predicate pushdown
    chunk_stats: HashMap<ChunkId, ChunkStats>,

    next_series_id: SeriesId,
}

#[derive(Debug, Clone)]
pub struct ChunkMeta {
    pub chunk_id:     ChunkId,
    pub time_start_ns: i64,
    pub time_end_ns:   i64,
    pub file_path:    PathBuf,
    pub size_bytes:   u64,
    pub entry_count:  u32,
}

#[derive(Debug, Clone)]
pub struct ChunkStats {
    pub min_value: f64,
    pub max_value: f64,
}

impl ChunkIndex {
    /// Register a new chunk — called after each memtable flush
    pub fn register(&mut self, key: &SeriesKey, meta: ChunkMeta, stats: ChunkStats) {
        let series_id = *self.series_registry
            .entry(key.clone())
            .or_insert_with(|| {
                let id = self.next_series_id;
                self.next_series_id += 1;
                // Register all tag pairs in the inverted index
                for (k, v) in &key.tags {
                    self.tag_index
                        .entry((k.clone(), v.clone()))
                        .or_default()
                        .insert(id);
                }
                id
            });

        self.time_index
            .entry(series_id)
            .or_default()
            .insert(meta.time_start_ns, meta.clone());

        self.chunk_stats.insert(meta.chunk_id, stats);
    }

    /// Multi-tag intersection: find series matching ALL tag filters
    pub fn resolve_series(&self, metric: &str, tag_filters: &HashMap<String, String>) -> Vec<SeriesId> {
        if tag_filters.is_empty() {
            // Return all series for this metric
            return self.series_registry.iter()
                .filter(|(k, _)| k.metric_name == metric)
                .map(|(_, id)| *id)
                .collect();
        }

        // Intersect series sets for each tag filter
        let mut result: Option<HashSet<SeriesId>> = None;
        for (tag_key, tag_value) in tag_filters {
            let matching = self.tag_index
                .get(&(tag_key.clone(), tag_value.clone()))
                .cloned()
                .unwrap_or_default();

            result = Some(match result {
                None => matching,
                Some(existing) => existing.intersection(&matching).copied().collect(),
            });
        }

        result.unwrap_or_default()
            .into_iter()
            .filter(|id| {
                // Also filter by metric name
                self.series_registry.iter()
                    .any(|(k, sid)| sid == id && k.metric_name == metric)
            })
            .collect()
    }

    /// Three-stage chunk pruning pipeline
    pub fn prune_chunks(
        &self,
        series_id: SeriesId,
        time_start_ns: i64,
        time_end_ns: i64,
        predicate: Option<&ValuePredicate>,
    ) -> Vec<&ChunkMeta> {
        let Some(time_map) = self.time_index.get(&series_id) else {
            return vec![];
        };

        time_map
            .range(..=time_end_ns)
            .filter(|(_, meta)| meta.time_end_ns >= time_start_ns)    // stage 1: time pruning
            .filter(|(_, meta)| {                                       // stage 2: stats pruning
                predicate.map_or(true, |p| {
                    let stats = &self.chunk_stats[&meta.chunk_id];
                    p.matches(stats.min_value, stats.max_value)
                })
            })
            .map(|(_, meta)| meta)
            .collect()
    }
}

pub enum ValuePredicate {
    GreaterThan(f64),
    LessThan(f64),
    Between(f64, f64),
}

impl ValuePredicate {
    fn matches(&self, min: f64, max: f64) -> bool {
        match self {
            Self::GreaterThan(t) => max > *t,
            Self::LessThan(t)   => min < *t,
            Self::Between(lo, hi) => min <= *hi && max >= *lo,
        }
    }
}
```

---

### 1.8 Compaction Worker

```rust
// src/compaction/mod.rs
pub enum CompactionStrategy {
    SizeTiered {
        size_ratio_threshold: f64,   // merge chunks within this size ratio
        min_threshold:        usize, // minimum number of chunks to trigger
    },
}

pub struct CompactionWorker {
    index:    Arc<RwLock<ChunkIndex>>,
    writer:   ChunkWriter,
    strategy: CompactionStrategy,
    interval: Duration,
}

impl CompactionWorker {
    pub async fn run(self) {
        let mut ticker = tokio::time::interval(self.interval);
        loop {
            ticker.tick().await;
            if let Err(e) = self.compact_once().await {
                tracing::error!("Compaction error: {}", e);
            }
        }
    }

    async fn compact_once(&self) -> Result<()> {
        // Find candidate chunks for merging based on strategy
        // Merge selected chunks into a single larger chunk
        // Atomically update chunk index
        // Delete superseded chunk files
        Ok(())
    }
}
```

---

### 1.9 gRPC Server

```rust
// src/server/mod.rs
pub struct StorageServer {
    wal:      Arc<Mutex<WalWriter>>,
    memtable: Arc<Mutex<Memtable>>,
    index:    Arc<RwLock<ChunkIndex>>,
    writer:   Arc<ChunkWriter>,
    reader:   Arc<ChunkReader>,
}

#[tonic::async_trait]
impl StorageService for StorageServer {
    async fn append(
        &self,
        request: Request<AppendRequest>,
    ) -> Result<Response<AppendResponse>, Status> {
        let points = decode_points(request.into_inner().points)?;

        // Write to WAL first — durability guarantee
        let seq = self.wal.lock().await
            .append(&points)
            .await
            .map_err(|e| Status::internal(e.to_string()))?;

        // Then insert into memtable
        let mut mem = self.memtable.lock().await;
        for point in points {
            mem.insert(point);
        }

        // Trigger async flush if memtable is full
        if mem.should_flush() {
            let data = mem.drain();
            let index = Arc::clone(&self.index);
            let writer = Arc::clone(&self.writer);
            tokio::spawn(async move {
                flush_memtable(data, index, writer).await
            });
        }

        Ok(Response::new(AppendResponse { sequence: seq }))
    }

    type QueryStream = ReceiverStream<Result<QueryResponse, Status>>;

    async fn query(
        &self,
        request: Request<QueryRequest>,
    ) -> Result<Response<Self::QueryStream>, Status> {
        let req = request.into_inner();
        let (tx, rx) = tokio::sync::mpsc::channel(128);

        let index = Arc::clone(&self.index);
        let reader = Arc::clone(&self.reader);

        tokio::spawn(async move {
            execute_query(req, index, reader, tx).await;
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }
}
```

---

### 1.10 Phase 1 Deliverables and Tests

**Deliverables**

- WAL with fsync, length-prefixed entries, CRC32 checksums
- Memtable with configurable flush threshold
- Immutable chunk files with columnar layout, delta encoding, lz4 compression, bloom filter footer
- Chunk index with time-range pruning, tag inverted index, statistics-based predicate pushdown
- Size-tiered compaction worker as background Tokio task
- tonic gRPC server exposing Append, Query, Compact

**Required tests before moving to Phase 2**

```
storage-engine/
└── tests/
    ├── wal_recovery_test.rs          # crash recovery replays all committed entries
    ├── torn_write_detection_test.rs  # corrupt WAL stops replay at boundary
    ├── chunk_write_read_test.rs      # written data is queryable after flush
    ├── bloom_filter_test.rs          # false positive rate within configured bound
    ├── tag_intersection_test.rs      # multi-tag queries return correct series
    ├── time_pruning_test.rs          # chunks outside time range are not read
    ├── predicate_pushdown_test.rs    # chunks pruned by min/max stats
    └── compaction_test.rs            # compacted chunks remain queryable
```

---

## Phase 2 — Ingestion Layer (Go)

**Goal**: Four working ingestion adapters — DogStatsD, Alpaca, Prometheus, Synthetic — with real data flowing into the Rust storage engine.

**Estimated duration**: 3–4 weeks

---

### 2.1 Internal Data Model

```go
// ingestion/internal/model/datapoint.go
package model

type DataPoint struct {
    MetricName  string            `json:"metric"`
    Tags        map[string]string `json:"tags"`
    TimestampNs int64             `json:"timestamp_ns"`
    Value       float64           `json:"value"`
}
```

---

### 2.2 Write Buffer with Backpressure

```go
// ingestion/internal/buffer/write_buffer.go
package buffer

import (
    "context"
    "time"
    "micius/ingestion/internal/model"
)

type WriteBuffer struct {
    ch           chan model.DataPoint
    flushSize    int
    flushInterval time.Duration
    flushFn      func([]model.DataPoint) error
}

func New(capacity, flushSize int, flushInterval time.Duration, flushFn func([]model.DataPoint) error) *WriteBuffer {
    return &WriteBuffer{
        ch:            make(chan model.DataPoint, capacity),
        flushSize:     flushSize,
        flushInterval: flushInterval,
        flushFn:       flushFn,
    }
}

// Write is non-blocking — returns ErrBufferFull if at capacity (backpressure signal)
func (b *WriteBuffer) Write(p model.DataPoint) error {
    select {
    case b.ch <- p:
        return nil
    default:
        return ErrBufferFull
    }
}

// Run batches points and flushes to the storage engine
func (b *WriteBuffer) Run(ctx context.Context) error {
    ticker := time.NewTicker(b.flushInterval)
    defer ticker.Stop()

    batch := make([]model.DataPoint, 0, b.flushSize)

    flush := func() {
        if len(batch) == 0 { return }
        if err := b.flushFn(batch); err != nil {
            // log error — do not drop; retry on next tick
        }
        batch = batch[:0]
    }

    for {
        select {
        case p := <-b.ch:
            batch = append(batch, p)
            if len(batch) >= b.flushSize {
                flush()
            }
        case <-ticker.C:
            flush()
        case <-ctx.Done():
            flush()    // final flush on shutdown
            return ctx.Err()
        }
    }
}
```

---

### 2.3 DogStatsD UDP Adapter

DogStatsD format: `metric.name:value|type|#tag1:val1,tag2:val2`

```go
// ingestion/internal/adapter/dogstatsd.go
package adapter

import (
    "context"
    "net"
    "strings"
    "strconv"
    "time"
    "micius/ingestion/internal/model"
    "micius/ingestion/internal/buffer"
)

type DogStatsDAdapter struct {
    addr   string
    buf    *buffer.WriteBuffer
}

func (a *DogStatsDAdapter) Run(ctx context.Context) error {
    conn, err := net.ListenPacket("udp", a.addr)
    if err != nil { return err }
    defer conn.Close()

    raw := make([]byte, 65535)
    for {
        select {
        case <-ctx.Done():
            return nil
        default:
        }

        n, _, err := conn.ReadFrom(raw)
        if err != nil { continue }

        points, err := parseStatsDLine(string(raw[:n]))
        if err != nil { continue }

        for _, p := range points {
            if err := a.buf.Write(p); err != nil {
                // backpressure — increment dropped_points_total counter
            }
        }
    }
}

func parseStatsDLine(line string) ([]model.DataPoint, error) {
    // Format: metric.name:value|type|#tag1:val1,tag2:val2|@sample_rate
    parts := strings.Split(strings.TrimSpace(line), "|")
    if len(parts) < 2 { return nil, ErrInvalidFormat }

    nameValue := strings.SplitN(parts[0], ":", 2)
    if len(nameValue) != 2 { return nil, ErrInvalidFormat }

    metricName := nameValue[0]
    value, err := strconv.ParseFloat(nameValue[1], 64)
    if err != nil { return nil, err }

    tags := map[string]string{"type": parts[1]}    // counter/gauge/histogram

    // Parse tags: #tag1:val1,tag2:val2
    for _, part := range parts[2:] {
        if strings.HasPrefix(part, "#") {
            for _, tag := range strings.Split(part[1:], ",") {
                kv := strings.SplitN(tag, ":", 2)
                if len(kv) == 2 {
                    tags[kv[0]] = kv[1]
                }
            }
        }
    }

    return []model.DataPoint{{
        MetricName:  metricName,
        Tags:        tags,
        TimestampNs: time.Now().UnixNano(),
        Value:       value,
    }}, nil
}
```

---

### 2.4 Alpaca Markets WebSocket Adapter

```go
// ingestion/internal/adapter/alpaca.go
package adapter

import (
    "context"
    "encoding/json"
    "time"
    "github.com/gorilla/websocket"
    "micius/ingestion/internal/model"
    "micius/ingestion/internal/buffer"
)

type AlpacaAdapter struct {
    wsURL   string
    apiKey  string
    apiSecret string
    symbols []string
    buf     *buffer.WriteBuffer
}

type alpacaTrade struct {
    Symbol    string  `json:"S"`
    Price     float64 `json:"p"`
    Size      float64 `json:"s"`
    Timestamp string  `json:"t"`
}

func (a *AlpacaAdapter) Run(ctx context.Context) error {
    for {
        if err := a.connect(ctx); err != nil {
            if ctx.Err() != nil { return nil }
            time.Sleep(5 * time.Second)    // reconnect with backoff
        }
    }
}

func (a *AlpacaAdapter) connect(ctx context.Context) error {
    conn, _, err := websocket.DefaultDialer.DialContext(ctx, a.wsURL, nil)
    if err != nil { return err }
    defer conn.Close()

    // Authenticate and subscribe
    a.authenticate(conn)
    a.subscribe(conn, a.symbols)

    for {
        _, msg, err := conn.ReadMessage()
        if err != nil { return err }

        var trades []alpacaTrade
        if err := json.Unmarshal(msg, &trades); err != nil { continue }

        for _, trade := range trades {
            ts, _ := time.Parse(time.RFC3339Nano, trade.Timestamp)
            a.buf.Write(model.DataPoint{
                MetricName:  "trade.price",
                Tags:        map[string]string{"symbol": trade.Symbol, "exchange": "alpaca"},
                TimestampNs: ts.UnixNano(),
                Value:       trade.Price,
            })
            a.buf.Write(model.DataPoint{
                MetricName:  "trade.volume",
                Tags:        map[string]string{"symbol": trade.Symbol},
                TimestampNs: ts.UnixNano(),
                Value:       trade.Size,
            })
        }
    }
}
```

---

### 2.5 Prometheus Scrape Adapter

```go
// ingestion/internal/adapter/prometheus.go
package adapter

// Scrapes /metrics endpoints on a configurable interval
// Parses Prometheus exposition format
// Translates gauge/counter/histogram to DataPoints with service tag

type PrometheusAdapter struct {
    targets  []ScrapeTarget    // {url, interval, tags}
    buf      *buffer.WriteBuffer
}

type ScrapeTarget struct {
    URL      string
    Interval time.Duration
    Tags     map[string]string
}

func (a *PrometheusAdapter) Run(ctx context.Context) error {
    for _, target := range a.targets {
        go a.scrapeLoop(ctx, target)
    }
    <-ctx.Done()
    return nil
}
```

---

### 2.6 Synthetic Generator

```go
// ingestion/internal/adapter/synthetic.go
package adapter

import (
    "context"
    "math"
    "math/rand"
    "time"
    "micius/ingestion/internal/model"
    "micius/ingestion/internal/buffer"
)

type MetricShape struct {
    Name      string
    Tags      map[string]string
    Baseline  float64
    Noise     float64          // standard deviation
    Amplitude float64          // diurnal cycle amplitude
    Period    time.Duration    // diurnal cycle period (24h)
}

type SyntheticGenerator struct {
    shapes   []MetricShape
    rate     time.Duration    // emit one batch per rate
    buf      *buffer.WriteBuffer
}

func (g *SyntheticGenerator) Run(ctx context.Context) error {
    ticker := time.NewTicker(g.rate)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            now := time.Now()
            for _, shape := range g.shapes {
                value := g.generate(shape, now)
                g.buf.Write(model.DataPoint{
                    MetricName:  shape.Name,
                    Tags:        shape.Tags,
                    TimestampNs: now.UnixNano(),
                    Value:       value,
                })
            }
        case <-ctx.Done():
            return nil
        }
    }
}

func (g *SyntheticGenerator) generate(s MetricShape, t time.Time) float64 {
    phase := float64(t.UnixNano()) / float64(s.Period)
    diurnal := s.Amplitude * math.Sin(2*math.Pi*phase)
    noise := rand.NormFloat64() * s.Noise
    return s.Baseline + diurnal + noise
}
```

---

### 2.7 gRPC Storage Client

```go
// ingestion/internal/storage/client.go
package storage

import (
    "context"
    "micius/ingestion/internal/model"
    storagev1 "micius/gen/storage/v1"
    "google.golang.org/grpc"
)

type Client struct {
    conn   *grpc.ClientConn
    client storagev1.StorageServiceClient
}

func (c *Client) Append(ctx context.Context, points []model.DataPoint) error {
    req := &storagev1.AppendRequest{
        Points: encodePoints(points),
    }
    _, err := c.client.Append(ctx, req)
    return err
}
```

---

### 2.8 Phase 2 Deliverables and Tests

**Deliverables**

- Write buffer with configurable capacity, flush size, flush interval, and backpressure signaling
- DogStatsD UDP listener parsing counters, gauges, histograms, timers with full tag support
- Alpaca Markets WebSocket client with automatic reconnection
- Prometheus scrape client with per-target intervals
- Synthetic generator with diurnal cycles, configurable noise, correlated series support
- gRPC client wrapping the Rust storage engine Append endpoint

**Required tests**

```
ingestion/
└── tests/
    ├── dogstatsd_parse_test.go       # valid and malformed line protocol
    ├── backpressure_test.go          # buffer full returns ErrBufferFull
    ├── flush_test.go                 # batch flushed on size and interval trigger
    ├── reconnect_test.go             # WebSocket reconnects after disconnect
    └── integration_test.go           # data written through each adapter is queryable
```

---

## Phase 3 — Query and Alert Layer (Go)

**Goal**: Working query API and alerting system with exactly-once webhook delivery and live application metrics from URL shortener.

**Estimated duration**: 3–4 weeks

---

### 3.1 Query API

**HTTP endpoints**

```
POST /query              # structured query
POST /query/raw          # raw data points (no aggregation)
GET  /schema             # available metrics and tag keys
GET  /health
```

**Query request/response**

```go
// query/internal/api/types.go
type QueryRequest struct {
    Metric      string            `json:"metric"`
    TagFilters  map[string]string `json:"tags"`
    From        string            `json:"from"`        // ISO8601 or relative: "1h", "24h"
    To          string            `json:"to"`
    Aggregation string            `json:"aggregation"` // mean|sum|min|max|p50|p95|p99|none
    Resolution  string            `json:"resolution"`  // "raw"|"1m"|"5m"|"1h"
}

type QueryResponse struct {
    Metric  string        `json:"metric"`
    Tags    map[string]string `json:"tags"`
    Points  []QueryPoint  `json:"points"`
}

type QueryPoint struct {
    TimestampNs int64   `json:"timestamp_ns"`
    Value       float64 `json:"value"`
}
```

---

### 3.2 Query Execution Pipeline

```go
// query/internal/router/chunk_router.go
package router

// Full query execution pipeline:
// 1. Parse time range (relative → absolute)
// 2. Resolve series via tag intersection (calls Rust gRPC)
// 3. Route to raw or downsampled chunks based on time range
// 4. Prune chunks by time, bloom filter, stats
// 5. Read and decompress relevant chunks
// 6. Apply aggregation

func (r *ChunkRouter) Execute(ctx context.Context, req QueryRequest) ([]QueryResponse, error) {
    // Step 1 — resolve time range
    start, end, err := parseTimeRange(req.From, req.To)
    if err != nil { return nil, err }

    // Step 2 — resolve series IDs via storage engine
    seriesIDs, err := r.storageClient.ResolveSeries(ctx, req.Metric, req.TagFilters)
    if err != nil { return nil, err }

    // Step 3 — fetch chunks for each series
    var results []QueryResponse
    for _, seriesID := range seriesIDs {
        points, err := r.storageClient.Query(ctx, &storagev1.QueryRequest{
            MetricName:   req.Metric,
            TagFilters:   req.TagFilters,
            TimeStartNs:  start.UnixNano(),
            TimeEndNs:    end.UnixNano(),
            Aggregation:  req.Aggregation,
            ResolutionNs: parseResolution(req.Resolution),
        })
        if err != nil { continue }
        results = append(results, toQueryResponse(points))
    }

    return results, nil
}
```

---

### 3.3 Aggregation Engine

```go
// query/internal/aggregation/engine.go
package aggregation

import (
    "math"
    "sort"
)

type Aggregator interface {
    Add(value float64)
    Result() float64
}

type P99Aggregator struct{ values []float64 }

func (a *P99Aggregator) Add(v float64)    { a.values = append(a.values, v) }
func (a *P99Aggregator) Result() float64 {
    if len(a.values) == 0 { return 0 }
    sort.Float64s(a.values)
    idx := int(math.Ceil(0.99*float64(len(a.values)))) - 1
    return a.values[idx]
}

type MeanAggregator struct{ sum float64; count int }
func (a *MeanAggregator) Add(v float64)    { a.sum += v; a.count++ }
func (a *MeanAggregator) Result() float64 {
    if a.count == 0 { return 0 }
    return a.sum / float64(a.count)
}

// BucketedAggregation groups raw points into time buckets
func BucketedAggregation(points []QueryPoint, resolutionNs int64, agg Aggregator) []QueryPoint {
    if len(points) == 0 || resolutionNs == 0 { return points }

    buckets := map[int64]Aggregator{}
    for _, p := range points {
        bucket := (p.TimestampNs / resolutionNs) * resolutionNs
        if _, ok := buckets[bucket]; !ok {
            buckets[bucket] = newAggregator(agg)
        }
        buckets[bucket].Add(p.Value)
    }

    result := make([]QueryPoint, 0, len(buckets))
    for ts, a := range buckets {
        result = append(result, QueryPoint{TimestampNs: ts, Value: a.Result()})
    }
    sort.Slice(result, func(i, j int) bool { return result[i].TimestampNs < result[j].TimestampNs })
    return result
}
```

---

### 3.4 Alert Worker

**Database schema (Postgres)**

```sql
-- Alert rule definitions
CREATE TABLE alert_rules (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    metric_name  TEXT NOT NULL,
    tag_filters  JSONB NOT NULL DEFAULT '{}',
    condition    TEXT NOT NULL,         -- "gt" | "lt" | "eq"
    threshold    DOUBLE PRECISION NOT NULL,
    window_secs  INTEGER NOT NULL,
    webhook_url  TEXT NOT NULL,
    enabled      BOOLEAN NOT NULL DEFAULT true,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Webhook delivery outbox (transactional outbox pattern)
CREATE TABLE webhook_deliveries (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rule_id         UUID NOT NULL REFERENCES alert_rules(id),
    idempotency_key TEXT NOT NULL UNIQUE,    -- prevents duplicate delivery
    payload         JSONB NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending|delivered|dead_letter
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    delivered_at    TIMESTAMPTZ
);

CREATE INDEX idx_deliveries_pending ON webhook_deliveries(next_attempt_at)
    WHERE status = 'pending';
```

**Alert evaluation worker**

```go
// query/internal/alert/worker.go
package alert

import (
    "context"
    "crypto/sha256"
    "fmt"
    "time"
)

type AlertWorker struct {
    db          *pgxpool.Pool
    queryClient QueryClient
    interval    time.Duration
}

func (w *AlertWorker) Run(ctx context.Context) error {
    ticker := time.NewTicker(w.interval)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            if err := w.evaluate(ctx); err != nil {
                // log — do not stop the worker on evaluation error
            }
        case <-ctx.Done():
            return nil
        }
    }
}

func (w *AlertWorker) evaluate(ctx context.Context) error {
    rules, err := w.loadEnabledRules(ctx)
    if err != nil { return err }

    for _, rule := range rules {
        value, err := w.queryRecent(ctx, rule)
        if err != nil { continue }

        if rule.Condition.Matches(value, rule.Threshold) {
            if err := w.enqueueDelivery(ctx, rule, value); err != nil {
                continue
            }
        }
    }
    return nil
}

func (w *AlertWorker) enqueueDelivery(ctx context.Context, rule AlertRule, value float64) error {
    // Idempotency key: hash of rule_id + time window bucket
    // Prevents duplicate alerts for the same event
    windowBucket := time.Now().Truncate(time.Duration(rule.WindowSecs) * time.Second)
    key := fmt.Sprintf("%x", sha256.Sum256([]byte(fmt.Sprintf("%s:%s", rule.ID, windowBucket))))

    payload := map[string]any{
        "rule_id":    rule.ID,
        "metric":     rule.MetricName,
        "value":      value,
        "threshold":  rule.Threshold,
        "fired_at":   time.Now().UTC(),
    }

    _, err := w.db.Exec(ctx, `
        INSERT INTO webhook_deliveries (rule_id, idempotency_key, payload)
        VALUES ($1, $2, $3)
        ON CONFLICT (idempotency_key) DO NOTHING
    `, rule.ID, key, payload)

    return err
}
```

**Webhook delivery worker — exactly-once with exponential backoff**

```go
// query/internal/alert/webhook.go
package alert

type WebhookDeliveryWorker struct {
    db         *pgxpool.Pool
    httpClient *http.Client
    maxRetries int
}

func (w *WebhookDeliveryWorker) Run(ctx context.Context) error {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            w.deliverPending(ctx)
        case <-ctx.Done():
            return nil
        }
    }
}

func (w *WebhookDeliveryWorker) deliverPending(ctx context.Context) {
    rows, _ := w.db.Query(ctx, `
        SELECT id, rule_id, idempotency_key, payload, attempt_count, webhook_url
        FROM webhook_deliveries
        JOIN alert_rules ON alert_rules.id = webhook_deliveries.rule_id
        WHERE status = 'pending' AND next_attempt_at <= now()
        ORDER BY next_attempt_at
        LIMIT 10
    `)
    defer rows.Close()

    for rows.Next() {
        var d Delivery
        rows.Scan(&d.ID, &d.RuleID, &d.IdempotencyKey, &d.Payload, &d.AttemptCount, &d.WebhookURL)
        w.deliver(ctx, d)
    }
}

func (w *WebhookDeliveryWorker) deliver(ctx context.Context, d Delivery) {
    err := w.post(ctx, d)

    if err == nil {
        w.db.Exec(ctx, `
            UPDATE webhook_deliveries
            SET status = 'delivered', delivered_at = now()
            WHERE id = $1
        `, d.ID)
        return
    }

    d.AttemptCount++
    if d.AttemptCount >= w.maxRetries {
        w.db.Exec(ctx, `
            UPDATE webhook_deliveries SET status = 'dead_letter' WHERE id = $1
        `, d.ID)
        return
    }

    // Exponential backoff: 5s, 10s, 20s, 40s, 80s...
    backoff := time.Duration(5*(1<<d.AttemptCount)) * time.Second
    w.db.Exec(ctx, `
        UPDATE webhook_deliveries
        SET attempt_count = $1, next_attempt_at = now() + $2
        WHERE id = $3
    `, d.AttemptCount, backoff, d.ID)
}
```

---

### 3.5 URL Shortener slog Integration

**In your URL shortener — add TCP log forwarding**

```go
// url-shortener/main.go — add alongside existing stdout handler
logger := slog.New(slog.NewJSONHandler(
    io.MultiWriter(
        os.Stdout,
        newTCPWriter("micius-ingestion:9998"),
    ),
    &slog.HandlerOptions{Level: slog.LevelDebug},
))
slog.SetDefault(logger)
```

**In Micius ingestion — slog adapter**

```go
// ingestion/internal/adapter/slog.go
// Listens on TCP :9998
// Parses JSON slog lines
// Extracts configured numeric fields as DataPoints
// Config-driven field extraction via YAML

type SlogAdapter struct {
    addr       string
    extractors []FieldExtractor
    buf        *buffer.WriteBuffer
}

type FieldExtractor struct {
    Field      string            // slog field name: "latency_ms"
    Metric     string            // target metric: "http.request.latency"
    TagFields  []string          // fields to promote to tags: ["service", "status"]
}
```

---

### 3.6 Phase 3 Deliverables and Tests

**Deliverables**

- HTTP query API with tag-based filtering, time range parsing, aggregation, resolution
- Bucketed aggregation engine supporting mean, sum, min, max, p50, p95, p99
- Alert evaluation worker scanning enabled rules on configurable interval
- Transactional outbox webhook delivery with idempotency keys and exponential backoff
- Dead-letter handling after max retry exhaustion
- slog TCP adapter extracting metrics from URL shortener logs
- Postgres schema for alert rules and webhook delivery outbox

**Required tests**

```
query/
└── tests/
    ├── query_parser_test.go          # relative time range parsing: "1h", "24h", "7d"
    ├── aggregation_test.go           # p99, mean, sum correctness
    ├── bucketed_aggregation_test.go  # correct bucket boundaries
    ├── alert_evaluation_test.go      # threshold breach triggers enqueue
    ├── idempotency_test.go           # duplicate alert not enqueued twice
    ├── webhook_retry_test.go         # exponential backoff schedule correct
    ├── dead_letter_test.go           # exhausted retries move to dead_letter
    └── slog_extraction_test.go       # latency_ms field extracted correctly
```

---

## Docker Compose — Phases 1–3

```yaml
# docker-compose.yml
services:

  storage-primary:
    build: ./storage-engine
    ports:
      - "50051:50051"    # gRPC
      - "9091:9091"      # Prometheus metrics
    volumes:
      - storage-primary-data:/var/micius/data
    environment:
      MICIUS_WAL_DIR: /var/micius/data/wal
      MICIUS_CHUNK_DIR: /var/micius/data/chunks
      MICIUS_GRPC_PORT: 50051
      MICIUS_FLUSH_THRESHOLD_MB: 64
      MICIUS_COMPACTION_INTERVAL_SECS: 300

  ingestion:
    build: ./ingestion
    ports:
      - "8080:8080"      # HTTP ingestion API
      - "8125:8125/udp"  # DogStatsD UDP
      - "9998:9998"      # slog TCP
      - "9092:9092"      # Prometheus metrics
    environment:
      STORAGE_GRPC_ADDR: storage-primary:50051
      ALPACA_API_KEY: ${ALPACA_API_KEY}
      ALPACA_API_SECRET: ${ALPACA_API_SECRET}
      BUFFER_CAPACITY: 100000
      BUFFER_FLUSH_SIZE: 1000
      BUFFER_FLUSH_INTERVAL_MS: 500
    depends_on:
      - storage-primary

  query:
    build: ./query
    ports:
      - "8081:8081"      # HTTP query API
      - "9093:9093"      # Prometheus metrics
    environment:
      STORAGE_GRPC_ADDR: storage-primary:50051
      DATABASE_URL: postgres://micius:micius@postgres:5432/micius
      ALERT_EVAL_INTERVAL_SECS: 30
      WEBHOOK_MAX_RETRIES: 5
    depends_on:
      - storage-primary
      - postgres

  postgres:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: micius
      POSTGRES_PASSWORD: micius
      POSTGRES_DB: micius
    volumes:
      - postgres-data:/var/lib/postgresql/data
      - ./query/migrations:/docker-entrypoint-initdb.d

  prometheus:
    image: prom/prometheus:latest
    volumes:
      - ./monitoring/prometheus.yml:/etc/prometheus/prometheus.yml
    ports:
      - "9090:9090"

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3000:3000"
    volumes:
      - grafana-data:/var/lib/grafana
      - ./monitoring/grafana/dashboards:/etc/grafana/provisioning/dashboards

volumes:
  storage-primary-data:
  postgres-data:
  grafana-data:
```

---

## Makefile

```makefile
.PHONY: proto build test chaos

# Generate protobuf code for Go and Rust
proto:
	protoc --go_out=./gen --go-grpc_out=./gen \
		-I proto proto/storage/v1/storage.proto
	cd storage-engine && cargo build    # triggers build.rs prost-build

build:
	cd storage-engine && cargo build --release
	cd ingestion && go build ./...
	cd query && go build ./...

test:
	cd storage-engine && cargo test
	cd ingestion && go test ./...
	cd query && go test ./...

# Spin up full stack
up:
	docker compose up --build -d

# Run chaos scenarios
chaos:
	docker compose exec storage-primary kill -9 1    # crash storage engine
	sleep 5
	docker compose restart storage-primary            # verify WAL recovery
```

---

## Phase Completion Criteria

| Phase | Gate |
|-------|------|
| Phase 1 | All 8 storage engine tests pass. WAL recovery test demonstrates zero data loss after simulated crash. Query returns correct data after memtable flush and compaction. |
| Phase 2 | Integration test confirms data written via each of the 4 adapters is queryable from storage engine. Backpressure test confirms ErrBufferFull under saturation. |
| Phase 3 | End-to-end test: URL shortener emits log → slog adapter extracts metric → alert worker detects threshold breach → webhook delivered exactly once with correct idempotency behavior under retry. |
```
